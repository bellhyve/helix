use std::{
    fs,
    future::poll_fn,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    task::Poll,
    time::Duration,
};

use anyhow::{ensure, Context, Result};
use helix_core::{diagnostic::Severity, LineEnding, Range, Rope, Selection, Transaction};
use helix_term::{application::Application, config::Config};
use helix_view::{
    current, current_ref,
    document::Mode,
    editor::Action,
    input::parse_macro,
    recovery::{self, Snapshot, Swap},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::helpers::{test_config, test_key_sequence, AppBuilder};

#[cfg(windows)]
use crossterm::event::{Event, KeyEvent};
#[cfg(not(windows))]
use termina::event::{Event, KeyEvent};

const TIMEOUT: Duration = Duration::from_secs(5);

fn config(directory: &Path) -> Config {
    let mut config = test_config();
    config.editor.recovery.enable = true;
    config.editor.recovery.directories = vec![directory.to_owned()];
    config.editor.recovery.suffix = ".test-swap".into();
    config.editor.recovery.update_count = 1;
    // Tests of immediate preservation must not pass by waiting for the timer.
    config.editor.recovery.update_time = 60;
    config
}

// Unlike test_key_sequence, leave the application open for lifecycle assertions.
async fn input(app: &mut Application, events: impl IntoIterator<Item = Event>) -> Result<()> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    for event in events {
        tx.send(Ok(event))?;
    }
    let mut stream = UnboundedReceiverStream::new(rx);
    ensure!(
        tokio::time::timeout(TIMEOUT, app.event_loop_until_idle(&mut stream)).await?,
        "application exited unexpectedly"
    );
    Ok(())
}

async fn keys(app: &mut Application, keys: &str) -> Result<()> {
    input(
        app,
        parse_macro(keys)?
            .into_iter()
            .map(|key| Event::Key(KeyEvent::from(key))),
    )
    .await
}

// Execute input without servicing queued jobs or editor save/recovery events.
async fn direct_keys(app: &mut Application, keys: &str) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        for key in parse_macro(keys)? {
            app.handle_terminal_events(Ok(Event::Key(KeyEvent::from(key))))
                .await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

async fn settle(app: &mut Application) -> Result<Vec<String>> {
    let mut notices = Vec::new();
    tokio::time::timeout(
        TIMEOUT,
        poll_fn(|cx| {
            while let Poll::Ready(result) = app.editor.poll_recovery(cx) {
                match result {
                    Ok(Some(notice)) => notices.push(notice),
                    Ok(None) => (),
                    Err(error) => return Poll::Ready(Err(error)),
                }
            }
            if app.editor.recovery_pending() {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }),
    )
    .await??;
    Ok(notices)
}

fn candidates(app: &Application) -> Result<Vec<PathBuf>> {
    let (_, doc) = current_ref!(app.editor);
    let found = recovery::discover(
        &app.editor.config().recovery,
        doc.path(),
        doc.recovery_cwd(),
    )?;
    ensure!(found.warnings.is_empty(), "{found:?}");
    Ok(found.candidates)
}

fn only_swap(app: &Application) -> Result<PathBuf> {
    let paths = candidates(app)?;
    ensure!(paths.len() == 1, "expected one snapshot, got {paths:?}");
    Ok(paths.into_iter().next().unwrap())
}

fn edit(app: &mut Application, from: usize, to: usize, text: &str) {
    let (view, doc) = current!(app.editor);
    let transaction = Transaction::change(
        doc.text(),
        std::iter::once((from, to, (!text.is_empty()).then(|| text.into()))),
    );
    assert!(doc.apply(&transaction, view.id));
    doc.append_changes_to_history(view);
}

fn snapshot(directory: &Path, path: Option<PathBuf>, text: &str) -> Snapshot {
    Snapshot {
        path,
        cwd: directory.to_owned(),
        text: Rope::from_str(text),
        encoding: "UTF-8".into(),
        has_bom: false,
        line_ending: "lf".into(),
        selections: vec![(0, 0)],
        primary: 0,
    }
}

fn crashed_snapshot(config: &Config, snapshot: &Snapshot) -> Result<PathBuf> {
    Swap::default()
        .write(snapshot, &config.editor.recovery, 1)?
        .context("fixture snapshot was not published")
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_by_default_but_manual_preserve_and_recover_work() -> Result<()> {
    assert!(!Config::default().editor.recovery.enable);
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut config = config(&root);
    config.editor.recovery.enable = false;
    let mut app = AppBuilder::new()
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    assert!(app
        .editor
        .get_status()
        .is_none_or(|(_, severity)| *severity != Severity::Warning));
    assert_eq!(fs::read_dir(&root)?.count(), 1);
    keys(&mut app, "ix<esc>").await?;
    settle(&mut app).await?;
    assert!(candidates(&app)?.is_empty());
    assert!(!current_ref!(app.editor).1.recovery_pending());

    keys(&mut app, ":preserve<ret>").await?;
    let swap = only_swap(&app)?;
    keys(&mut app, ":pre<ret>").await?;
    assert_eq!(only_swap(&app)?, swap);
    let saved = recovery::read(&swap)?;
    assert_eq!(saved.path.as_deref(), Some(original.as_path()));
    assert_eq!(saved.text.to_string(), "xdisk\n");
    let bytes = fs::read(&swap)?;
    drop(app);
    assert_eq!(fs::read(&swap)?, bytes, "Drop must not clean up recovery");

    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    assert!(app
        .editor
        .get_status()
        .is_none_or(|(_, severity)| *severity != Severity::Warning));
    assert_eq!(fs::read_dir(&root)?.count(), 2);
    assert_eq!(fs::read(&swap)?, bytes);
    keys(&mut app, ":rec<ret>").await?;
    assert_eq!(current_ref!(app.editor).1.text(), &saved.text);
    assert!(current_ref!(app.editor).1.recovery_requires_save());
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&swap)?, bytes);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn first_edit_is_preserved_without_leaving_insert_mode() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut config = config(&root);
    config.editor.recovery.update_count = usize::MAX;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    assert!(candidates(&app)?.is_empty());
    keys(&mut app, "ix").await?;
    assert_eq!(app.editor.mode(), Mode::Insert);
    // No explicit preserve or direct recovery polling: the application did it.
    assert!(!current_ref!(app.editor).1.recovery_pending());
    assert_eq!(
        recovery::read(&only_swap(&app)?)?.text.to_string(),
        "xdisk\n"
    );
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unicode_deletions_and_replacements_count_characters() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "a\u{e9}\u{754c}z\n")?;
    let mut config = config(&root);
    config.editor.recovery.update_count = 3;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 0, "!");
    settle(&mut app).await?;
    let swap = only_swap(&app)?;
    let previous = fs::read(&swap)?;

    edit(&mut app, 3, 4, ""); // One character, but three UTF-8 bytes.
    assert!(current_ref!(app.editor).1.recovery_pending());
    assert!(poll_fn(|cx| Poll::Ready(app.editor.poll_recovery(cx)))
        .await
        .is_pending());
    assert_eq!(fs::read(&swap)?, previous);
    edit(&mut app, 2, 3, "XY"); // Removal plus insertion crosses the threshold.
    settle(&mut app).await?;
    assert_eq!(only_swap(&app)?, swap);
    assert_eq!(recovery::read(&swap)?.text.to_string(), "!aXYz\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unnamed_snapshot_recovers_without_creating_an_original() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let config = config(&root);
    let mut app = AppBuilder::new().with_config(config.clone()).build()?;
    let len = current_ref!(app.editor).1.text().len_chars();
    edit(&mut app, 0, len, "scratch \u{1f980}\n");
    settle(&mut app).await?;
    let swap = only_swap(&app)?;
    let saved = recovery::read(&swap)?;
    assert!(saved.path.is_none());
    assert_eq!(saved.cwd, current_ref!(app.editor).1.recovery_cwd());
    let bytes = fs::read(&swap)?;
    drop(app);

    let mut app = AppBuilder::new().with_config(config).build()?;
    keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
    let (_, doc) = current_ref!(app.editor);
    assert!(doc.path().is_none());
    assert_eq!(doc.text(), &saved.text);
    assert!(doc.is_modified());
    assert!(!doc.recovery_pending());
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&swap)?, bytes);
    assert_eq!(fs::read_dir(&root)?.count(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_inhibits_delay_and_focus_autosave_until_explicit_save() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut config = config(&root);
    config.editor.auto_save.after_delay.enable = true;
    config.editor.auto_save.after_delay.timeout = 10;
    config.editor.auto_save.focus_lost = true;
    let saved = snapshot(&root, Some(original.clone()), "recovered\n");
    let swap = crashed_snapshot(&config, &saved)?;
    let bytes = fs::read(&swap)?;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;

    // Arm a real autosave timer before recovery, then return to a clean target.
    edit(&mut app, 0, 0, "x");
    {
        let (view, doc) = current!(app.editor);
        assert!(doc.undo(view));
        assert!(!doc.is_modified());
    }
    // Drain recovery only; leave the earlier autosave callback queued.
    settle(&mut app).await?;
    let owned_swap = candidates(&app)?
        .into_iter()
        .find(|path| path != &swap)
        .context("missing snapshot of the pre-recovery buffer")?;
    let owned_bytes = fs::read(&owned_swap)?;
    {
        let (view, doc) = current!(app.editor);
        doc.recover(recovery::read(&swap)?, view)?;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    #[cfg(windows)]
    let focus_events = [Event::FocusLost, Event::FocusGained];
    #[cfg(not(windows))]
    let focus_events = [Event::FocusOut, Event::FocusIn];
    input(&mut app, focus_events).await?;
    settle(&mut app).await?;
    assert!(current_ref!(app.editor).1.recovery_requires_save());
    assert_eq!(current_ref!(app.editor).1.text(), &saved.text);
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    assert_eq!(fs::read(&swap)?, bytes);
    assert_eq!(fs::read(&owned_swap)?, owned_bytes);
    assert_eq!(candidates(&app)?.len(), 2);

    keys(&mut app, ":write<ret>").await?;
    assert!(!current_ref!(app.editor).1.recovery_requires_save());
    assert_eq!(fs::read_to_string(&original)?, "recovered\n");
    test_key_sequence(&mut app, Some(":q<ret>"), None, true).await?;
    assert_eq!(fs::read(&swap)?, bytes);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_refuses_modified_readonly_and_pending_targets() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let saved = snapshot(&root, Some(original.clone()), "recovered\n");
    let swap = crashed_snapshot(&config, &saved)?;
    let bytes = fs::read(&swap)?;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 0, "local ");
    {
        let (view, doc) = current!(app.editor);
        assert!(doc.recover(saved.clone(), view).is_err());
    }
    let target = current_ref!(app.editor).1.id();
    app.editor.new_file(Action::Replace);
    keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
    let (message, severity) = app.editor.get_status().context("missing refusal")?;
    assert_eq!(*severity, Severity::Error);
    assert!(message.contains("modified buffer"), "{message}");
    assert!(current_ref!(app.editor).1.path().is_none());
    app.editor.switch(target, Action::Replace);
    {
        let (view, doc) = current!(app.editor);
        assert_eq!(doc.text().to_string(), "local disk\n");
        assert!(doc.undo(view));
        assert!(!doc.is_modified());
        assert!(doc.recovery_pending());
        let error = doc.recover(saved.clone(), view).unwrap_err();
        assert!(error.to_string().contains("pending recovery I/O"));
        assert_eq!(doc.text().to_string(), "disk\n");
        assert!(!doc.recovery_requires_save());
    }
    settle(&mut app).await?;
    {
        let (view, doc) = current!(app.editor);
        doc.readonly = true;
        let error = doc.recover(saved.clone(), view).unwrap_err();
        assert!(error.to_string().contains("readonly"));
        assert_eq!(doc.text().to_string(), "disk\n");
        assert!(!doc.recovery_requires_save());
        doc.readonly = false;
        doc.recover(saved, view)?;
        assert_eq!(doc.text().to_string(), "recovered\n");
    }
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    assert_eq!(fs::read(&swap)?, bytes);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_waits_for_deferred_followup_and_inflight_saves_before_reading() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut config = config(&root);
    config.editor.auto_format = true;
    let saved = snapshot(&root, Some(original.clone()), "recovered\n");
    let swap = crashed_snapshot(&config, &saved)?;
    let bytes = fs::read(&swap)?;
    let missing_swap = root.join(".helix-recovery-missing.test-swap");
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    let target = current_ref!(app.editor).1.id();
    // With auto-format enabled, :write queues its on-save tail even without an LSP.
    direct_keys(&mut app, ":write<ret>").await?;

    for deferred in [true, false] {
        assert_eq!(app.editor.write_count == 0, deferred);
        for path in [&missing_swap, &swap] {
            direct_keys(&mut app, &format!(":recover \"{}\"<ret>", path.display())).await?;
            let (message, severity) = app
                .editor
                .get_status()
                .context("missing pending-save error")?;
            assert_eq!(*severity, Severity::Error);
            assert!(
                message.contains("pending saves and on-save jobs"),
                "{message}"
            );
            assert_eq!(current_ref!(app.editor).1.text().to_string(), "disk\n");
            assert!(!current_ref!(app.editor).1.recovery_requires_save());
            assert_eq!(fs::read_to_string(&original)?, "disk\n");
            assert_eq!(fs::read(&swap)?, bytes);
        }
        if deferred {
            keys(&mut app, "").await?;
            // Separately leave an actual asynchronous save completion unprocessed.
            app.editor.save(target, None::<PathBuf>, false)?;
        }
    }
    tokio::time::timeout(TIMEOUT, app.editor.flush_writes()).await??;
    settle(&mut app).await?;
    keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
    assert_eq!(current_ref!(app.editor).1.text(), &saved.text);
    assert!(current_ref!(app.editor).1.recovery_requires_save());
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&swap)?, bytes);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ghost_preview_does_not_replace_the_pending_real_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut config = config(&root);
    config.editor.recovery.update_count = usize::MAX;
    config.editor.recovery.update_time = 1;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 0, "x");
    settle(&mut app).await?;
    let swap = only_swap(&app)?;
    edit(&mut app, 0, 1, "y");
    {
        let (view, doc) = current!(app.editor);
        let preview = Transaction::change(
            doc.text(),
            std::iter::once((0, doc.text().len_chars(), Some("preview\n".into()))),
        );
        assert!(doc.apply_temporary(&preview, view.id));
        assert_eq!(doc.text().to_string(), "preview\n");
        assert!(doc.recovery_pending());
    }
    settle(&mut app).await?;
    assert_eq!(only_swap(&app)?, swap);
    assert_eq!(recovery::read(&swap)?.text.to_string(), "ydisk\n");
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "preview\n");
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn configuration_changes_recheck_pending_size_and_migrate_the_suffix() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(&original, None)
        .build()?;
    keys(&mut app, "ix<esc>").await?;
    let old_path = only_swap(&app)?;
    keys(&mut app, ":set-option recovery.update-count 1000<ret>").await?;
    let old_bytes = fs::read(&old_path)?;
    edit(&mut app, 0, 0, "pending");
    keys(&mut app, ":set-option recovery.size-threshold 1<ret>").await?;
    assert_eq!(fs::read(&old_path)?, old_bytes);
    keys(&mut app, ":set-option recovery.suffix .new-swap<ret>").await?;
    assert!(old_path.exists());
    keys(&mut app, ":set-option recovery.size-threshold 0<ret>").await?;
    let new_path = only_swap(&app)?;
    assert_ne!(old_path, new_path);
    assert!(!old_path.exists());
    assert!(new_path.to_str().unwrap().ends_with(".new-swap"));
    assert_eq!(
        recovery::read(&new_path)?.text.to_string(),
        "pendingxdisk\n"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_changes_and_saved_text_refresh_existing_snapshots() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 0, "x");
    settle(&mut app).await?;
    let swap = only_swap(&app)?;
    keys(&mut app, ":encoding utf-16le<ret>").await?;
    assert_eq!(recovery::read(&swap)?.encoding, "UTF-16LE");
    {
        let (view, doc) = current!(app.editor);
        let transaction = Transaction::change(
            doc.text(),
            std::iter::once((0, doc.text().len_chars(), Some("saved preview\n".into()))),
        );
        assert!(doc.apply_temporary(&transaction, view.id));
        doc.append_changes_to_history(view);
    }
    let id = current_ref!(app.editor).1.id();
    app.editor.save(id, None::<PathBuf>, false)?;
    app.editor.flush_writes().await?;
    settle(&mut app).await?;
    assert_eq!(recovery::read(&swap)?.text.to_string(), "saved preview\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn preserved_scratch_survives_switching_until_explicit_close() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut app = AppBuilder::new().with_config(config(&root)).build()?;
    let scratch = current_ref!(app.editor).1.id();
    edit(&mut app, 0, 0, "scratch");
    settle(&mut app).await?;
    let swap = only_swap(&app)?;
    {
        let (view, doc) = current!(app.editor);
        assert!(doc.undo(view));
        assert!(!doc.is_modified());
        assert!(doc.recovery_pending());
    }
    app.editor.open(&original, Action::Replace)?;
    assert!(app.editor.document(scratch).is_some());
    assert!(swap.exists());
    assert!(app.editor.close_document(scratch, false).is_ok());
    assert!(app.editor.document(scratch).is_none());
    assert!(!swap.exists());
    settle(&mut app).await?;
    assert!(
        !swap.exists(),
        "pending work must not recreate a removed scratch swap"
    );
    assert_eq!(fs::read_dir(&root)?.count(), 1);
    test_key_sequence(&mut app, Some(":q<ret>"), None, true).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_does_not_unlink_a_preserved_clean_scratch_when_switching() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let crashed = crashed_snapshot(&config, &snapshot(&root, Some(original), "recovered\n"))?;
    let mut app = AppBuilder::new().with_config(config).build()?;
    keys(&mut app, ":preserve<ret>").await?;
    let scratch_swap = only_swap(&app)?;
    let before = fs::read(&scratch_swap)?;
    keys(
        &mut app,
        &format!(":recover \"{}\"<ret>", crashed.display()),
    )
    .await?;
    assert_eq!(fs::read(&scratch_swap)?, before);
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
    test_key_sequence(&mut app, Some(":qa!<ret>"), None, true).await?;
    assert!(!scratch_swap.exists());
    assert!(crashed.exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn signal_exit_retains_snapshots_when_application_close_runs() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        let original = root.join(format!("signal-{signal}.txt"));
        fs::write(&original, "disk\n")?;
        let mut app = AppBuilder::new()
            .with_config(config(&root))
            .with_file(&original, None)
            .build()?;
        edit(&mut app, 0, 0, "x");
        settle(&mut app).await?;
        let swap = only_swap(&app)?;
        let bytes = fs::read(&swap)?;
        assert!(!tokio::time::timeout(TIMEOUT, app.handle_signals(signal)).await?);
        assert!(
            !app.editor.should_close(),
            "signals must not empty the view tree"
        );
        let errors = tokio::time::timeout(TIMEOUT, app.close()).await?;
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(fs::read(&swap)?, bytes);
        assert_eq!(fs::read_to_string(&original)?, "disk\n");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_is_one_undo_step_even_for_empty_to_empty() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    for (name, before, after) in [("text", "disk\n", "recovered\n"), ("empty", "", "")] {
        let original = root.join(name);
        fs::write(&original, before)?;
        let mut app = AppBuilder::new()
            .with_config(config(&root))
            .with_file(&original, None)
            .build()?;
        let (view, doc) = current!(app.editor);
        doc.recover(snapshot(&root, Some(original.clone()), after), view)?;
        assert_eq!(doc.text().to_string(), after);
        assert!(doc.is_modified());
        let last_edit = doc.history.get_mut().last_edit_pos();
        if before.is_empty() {
            assert_eq!(last_edit, None);
        } else {
            assert!(last_edit.is_some());
        }
        assert!(doc.undo(view));
        assert_eq!(doc.text().to_string(), before);
        assert!(!doc.is_modified());
        assert!(!doc.undo(view), "recovery must create only one revision");
        assert!(doc.redo(view));
        assert_eq!(doc.text().to_string(), after);
        assert_eq!(fs::read_to_string(&original)?, before);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn preexisting_candidates_require_explicit_choice() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let first = crashed_snapshot(&config, &snapshot(&root, Some(original.clone()), "first\n"))?;
    let second = crashed_snapshot(
        &config,
        &snapshot(&root, Some(original.clone()), "second\n"),
    )?;
    let first_bytes = fs::read(&first)?;
    let second_bytes = fs::read(&second)?;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    keys(&mut app, "").await?;
    let (message, _) = app
        .editor
        .get_status()
        .context("missing recovery warning")?;
    assert!(message.contains("Recovery available"), "{message}");
    assert!(message.contains(first.to_str().unwrap()));
    assert!(message.contains(second.to_str().unwrap()));
    assert_eq!(candidates(&app)?.len(), 2);
    keys(&mut app, ":recover<ret>").await?;
    let (message, severity) = app.editor.get_status().context("missing ambiguity error")?;
    assert_eq!(*severity, Severity::Error);
    assert!(message.contains("Multiple recovery snapshots"), "{message}");
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "disk\n");
    keys(&mut app, &format!(":rec \"{}\"<ret>", second.display())).await?;
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "second\n");
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&first)?, first_bytes);
    assert_eq!(fs::read(&second)?, second_bytes);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn original_path_argument_resolves_the_recorded_working_directory() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original file.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let saved = snapshot(
        &root,
        Some(PathBuf::from("original file.txt")),
        "recovered\n",
    );
    let swap = crashed_snapshot(&config, &saved)?;
    let mut app = AppBuilder::new().with_config(config).build()?;
    keys(
        &mut app,
        &format!(":recover \"{}\"<ret>", original.display()),
    )
    .await?;
    let (_, doc) = current_ref!(app.editor);
    assert_eq!(doc.path(), Some(original.as_path()));
    assert_eq!(doc.text(), &saved.text);
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert!(swap.exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn split_close_retains_snapshot_but_successful_exit_cleans_it() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(&original, None)
        .build()?;
    keys(&mut app, "ix<esc>:sp<ret>").await?;
    let swap = only_swap(&app)?;
    let bytes = fs::read(&swap)?;
    assert_eq!(app.editor.tree.views().count(), 2);
    keys(&mut app, ":q<ret>").await?;
    assert_eq!(app.editor.tree.views().count(), 1);
    assert_eq!(fs::read(&swap)?, bytes);
    test_key_sequence(&mut app, Some(":wq<ret>"), None, true).await?;
    assert_eq!(fs::read_to_string(&original)?, "xdisk\n");
    assert!(!swap.exists());
    assert_eq!(fs::read_dir(&root)?.count(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn save_as_migrates_only_after_success_and_same_path_stays_stable() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    let renamed = root.join("renamed.txt");
    fs::write(&original, "disk\n")?;
    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 0, "x");
    settle(&mut app).await?;
    let old_swap = only_swap(&app)?;
    let id = current_ref!(app.editor).1.id();
    app.editor.save(id, None::<PathBuf>, false)?;
    tokio::time::timeout(TIMEOUT, app.editor.flush_writes()).await??;
    settle(&mut app).await?;
    assert_eq!(only_swap(&app)?, old_swap);

    app.editor.save(id, Some(renamed.clone()), false)?;
    tokio::time::timeout(TIMEOUT, app.editor.flush_writes()).await??;
    settle(&mut app).await?;
    let new_swap = only_swap(&app)?;
    assert_ne!(old_swap, new_swap);
    assert!(!old_swap.exists());
    assert_eq!(recovery::read(&new_swap)?.path, Some(renamed.clone()));
    assert_eq!(fs::read_to_string(&renamed)?, "xdisk\n");

    let bytes = fs::read(&new_swap)?;
    // A regular file as the parent fails on all platforms, even when run as root.
    app.editor
        .save(id, Some(original.join("cannot-save.txt")), false)?;
    assert!(tokio::time::timeout(TIMEOUT, app.editor.flush_writes())
        .await?
        .is_err());
    settle(&mut app).await?;
    assert_eq!(current_ref!(app.editor).1.path(), Some(renamed.as_path()));
    assert_eq!(only_swap(&app)?, new_swap);
    assert_eq!(fs::read(&new_swap)?, bytes);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn completing_an_older_save_preserves_later_edits() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 4, "first");
    settle(&mut app).await?;
    let swap = only_swap(&app)?;
    let id = current_ref!(app.editor).1.id();
    app.editor.save(id, None::<PathBuf>, false)?;
    // Save captured the old rope, but its completion has not been processed yet.
    edit(&mut app, 0, 5, "later");
    tokio::time::timeout(TIMEOUT, app.editor.flush_writes()).await??;
    settle(&mut app).await?;
    assert_eq!(fs::read_to_string(&original)?, "first\n");
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "later\n");
    assert!(current_ref!(app.editor).1.is_modified());
    assert_eq!(only_swap(&app)?, swap);
    assert_eq!(recovery::read(&swap)?.text.to_string(), "later\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn byte_limit_retains_the_last_valid_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "ok\n")?;
    let mut config = config(&root);
    config.editor.recovery.size_threshold = 5;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 0, "a");
    settle(&mut app).await?;
    let swap = only_swap(&app)?;
    let bytes = fs::read(&swap)?;
    edit(&mut app, 0, 0, "\u{754c}"); // Five characters, seven bytes.
    let notices = settle(&mut app).await?;
    assert!(notices
        .iter()
        .any(|notice| notice.contains("size-threshold")));
    assert_eq!(fs::read(&swap)?, bytes);
    {
        let (view, doc) = current!(app.editor);
        assert!(doc.preserve(view.id).is_err());
    }
    assert_eq!(fs::read(&swap)?, bytes);
    edit(&mut app, 0, 1, "b");
    settle(&mut app).await?;
    assert_eq!(only_swap(&app)?, swap);
    assert_eq!(recovery::read(&swap)?.text.to_string(), "baok\n");
    assert_eq!(fs::read_to_string(&original)?, "ok\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_restores_encoding_bom_line_endings_and_selections() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let mut saved = snapshot(&root, Some(original.clone()), "a\u{e9}\r\n\u{754c}z\r\n");
    saved.encoding = "UTF-16LE".into();
    saved.has_bom = true;
    saved.line_ending = "crlf".into();
    saved.selections = vec![(2, 1), (4, 6)];
    saved.primary = 1;
    let swap = crashed_snapshot(&config, &saved)?;
    let bytes = fs::read(&swap)?;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    let owned_swap = {
        let (view, doc) = current!(app.editor);
        doc.recover(recovery::read(&swap)?, view)?;
        assert_eq!(doc.text(), &saved.text);
        assert_eq!(doc.encoding().name(), "UTF-16LE");
        assert_eq!(doc.line_ending, LineEnding::Crlf);
        assert_eq!(
            doc.selection(view.id),
            &Selection::new(vec![Range::new(2, 1), Range::new(4, 6)].into(), 1)
        );
        doc.preserve(view.id)?
    };
    assert_ne!(owned_swap, swap);
    let roundtrip = recovery::read(&owned_swap)?;
    assert_eq!(roundtrip.encoding, saved.encoding);
    assert!(roundtrip.has_bom);
    assert_eq!(roundtrip.line_ending, saved.line_ending);
    assert_eq!(roundtrip.selections, saved.selections);
    assert_eq!(roundtrip.primary, saved.primary);
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    let id = current_ref!(app.editor).1.id();
    app.editor.save(id, None::<PathBuf>, false)?;
    tokio::time::timeout(TIMEOUT, app.editor.flush_writes()).await??;
    let mut expected = vec![0xff, 0xfe];
    expected.extend(
        saved
            .text
            .to_string()
            .encode_utf16()
            .flat_map(u16::to_le_bytes),
    );
    assert_eq!(fs::read(&original)?, expected);
    assert_eq!(fs::read(&swap)?, bytes);
    Ok(())
}

// Reap on every exit path, including assertion unwinding and readiness timeout.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_process_leaves_an_automatically_recoverable_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "test::recovery::recovery_crash_child",
                "--nocapture",
            ])
            .env("HELIX_RECOVERY_TEST_CHILD_DIR", &root)
            .stdin(Stdio::null())
            .spawn()?,
    );
    let ready = root.join("ready");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            ensure!(
                child.0.try_wait()?.is_none(),
                "child exited before termination"
            );
            if ready.exists() {
                ensure!(
                    fs::read(&ready)? == b"ready\n",
                    "incomplete readiness marker"
                );
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("child did not publish a recovery snapshot in time")??;
    child.0.kill()?;
    let status = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Some(status) = child.0.try_wait()? {
                return Ok::<_, anyhow::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert!(
        !status.success(),
        "child must terminate without graceful cleanup"
    );

    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(&original, None)
        .build()?;
    let swap = only_swap(&app)?;
    let bytes = fs::read(&swap)?;
    assert_eq!(recovery::read(&swap)?.text.to_string(), "unsaved disk\n");
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    keys(&mut app, ":recover<ret>").await?;
    assert_eq!(
        current_ref!(app.editor).1.text().to_string(),
        "unsaved disk\n"
    );
    assert!(current_ref!(app.editor).1.is_modified());
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&swap)?, bytes);
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "subprocess fixture; launched by killed_process_leaves_an_automatically_recoverable_snapshot"]
async fn recovery_crash_child() -> Result<()> {
    let Some(root) = std::env::var_os("HELIX_RECOVERY_TEST_CHILD_DIR") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    ensure!(root.is_absolute(), "child directory must be absolute");
    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(root.join("original.txt"), None)
        .build()?;
    keys(&mut app, "iunsaved ").await?;
    assert_eq!(app.editor.mode(), Mode::Insert);
    assert!(!current_ref!(app.editor).1.recovery_pending());
    assert_eq!(
        recovery::read(&only_swap(&app)?)?.text.to_string(),
        "unsaved disk\n"
    );
    // Publish readiness atomically only after both snapshot and marker are synced.
    let temporary = root.join("ready.pending");
    let mut marker = fs::File::create(&temporary)?;
    marker.write_all(b"ready\n")?;
    marker.sync_all()?;
    drop(marker);
    fs::rename(temporary, root.join("ready"))?;
    #[cfg(unix)]
    fs::File::open(&root)?.sync_all()?;
    std::future::pending::<()>().await;
    drop(app);
    Ok(())
}
