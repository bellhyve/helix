use std::{
    fs,
    future::poll_fn,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    task::Poll,
    time::{Duration, UNIX_EPOCH},
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

fn archives(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".helix-recovered-")
        {
            assert!(path.to_str().unwrap().ends_with(".recovered"));
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn retained_backup(app: &Application, bytes: &[u8]) -> Result<PathBuf> {
    let paths = &app.editor.retained_recovery_backups;
    ensure!(
        paths.len() == 1,
        "expected one retained backup, got {paths:?}"
    );
    let path = &paths[0];
    assert!(path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with(".helix-recovered-"));
    assert!(path.to_str().unwrap().ends_with(".recovered"));
    assert_eq!(
        fs::read(path)?,
        bytes,
        "backup must preserve the original bytes"
    );
    Ok(path.clone())
}

fn set_modified(path: &Path, seconds: u64, nanos: u32) -> Result<()> {
    fs::File::options()
        .write(true)
        .open(path)?
        .set_times(fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::new(seconds, nanos)))?;
    Ok(())
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
    // Publish like manual :preserve, then drop the writer to leave an unowned file.
    let mut recovery = config.editor.recovery.clone();
    recovery.enable = true;
    Swap::default()
        .write(snapshot, &recovery, 1)?
        .context("fixture snapshot was not published")
}

#[tokio::test(flavor = "multi_thread")]
async fn recovered_backup_close_policy_depends_on_a_successful_write() -> Result<()> {
    for keep in [false, true] {
        for write in [false, true] {
            let directory = tempfile::tempdir()?;
            let root = directory.path().canonicalize()?;
            let original = root.join("original.txt");
            fs::write(&original, "disk\n")?;
            let mut config = config(&root);
            config.editor.recovery.keep_recovered = keep;
            let swap = crashed_snapshot(
                &config,
                &snapshot(&root, Some(original.clone()), "recovered\n"),
            )?;
            let bytes = fs::read(&swap)?;
            let mut app = AppBuilder::new()
                .with_config(config)
                .with_file(&original, None)
                .build()?;
            keys(&mut app, ":recover<ret>").await?;
            assert_eq!(fs::read(&swap)?, bytes);
            assert!(archives(&root)?.is_empty());
            if write {
                // Create a backup before save so cleanup must remove an existing archive.
                edit(&mut app, 0, "recovered\n".len(), "later edits\n");
                settle(&mut app).await?;
                let backups = archives(&root)?;
                assert_eq!(backups.len(), 1);
                assert_eq!(fs::read(&backups[0])?, bytes);
                assert_eq!(only_swap(&app)?, swap);
                keys(&mut app, ":write<ret>").await?;
                assert!(!current_ref!(app.editor).1.is_modified());
                assert_eq!(fs::read_to_string(&original)?, "later edits\n");
                assert_eq!(fs::read(&backups[0])?, bytes);
            }
            test_key_sequence(
                &mut app,
                Some(if write { ":q<ret>" } else { ":q!<ret>" }),
                None,
                true,
            )
            .await?;
            assert!(!swap.exists());
            if keep || !write {
                let backup = retained_backup(&app, &bytes)?;
                assert_eq!(archives(&root)?, vec![backup]);
            } else {
                assert!(archives(&root)?.is_empty());
                assert!(app.editor.retained_recovery_backups.is_empty());
            }
            if !write {
                assert_eq!(fs::read_to_string(&original)?, "disk\n");
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn undo_before_recovery_then_close_keeps_backup_even_after_writing_old_revision() -> Result<()>
{
    for save in ["none", "undone", "inflight-before-recovery"] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let original = root.join("original.txt");
        fs::write(&original, "disk\n")?;
        let mut config = config(&root);
        config.editor.recovery.enable = false;
        let swap = crashed_snapshot(
            &config,
            &snapshot(&root, Some(original.clone()), "recovered\n"),
        )?;
        let bytes = fs::read(&swap)?;
        let mut app = AppBuilder::new()
            .with_config(config.clone())
            .with_file(&original, None)
            .build()?;
        let id = current_ref!(app.editor).1.id();
        if save == "inflight-before-recovery" {
            // Exercise the document API race independently of the UI's pending-save guard.
            app.editor.save(id, None::<PathBuf>, false)?;
            let (view, doc) = current!(app.editor);
            doc.recover(recovery::claim(&swap, &config.editor.recovery)?, view)?;
            tokio::time::timeout(TIMEOUT, app.editor.flush_writes()).await??;
            assert!(current_ref!(app.editor).1.recovery_requires_save());
            assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
        } else {
            keys(&mut app, ":recover<ret>").await?;
        }
        keys(&mut app, "u").await?;
        assert_eq!(current_ref!(app.editor).1.text().to_string(), "disk\n");
        assert!(!current_ref!(app.editor).1.is_modified());
        if save == "undone" {
            keys(&mut app, ":write<ret>").await?;
        }
        keys(&mut app, ":bc<ret>").await?;
        assert!(app.editor.document(id).is_none());
        assert!(!swap.exists());
        let backup = retained_backup(&app, &bytes)?;
        let (message, severity) = app
            .editor
            .get_status()
            .context("missing retention warning")?;
        assert_eq!(*severity, Severity::Warning);
        assert!(message.contains(backup.to_str().unwrap()), "{message}");
        assert_eq!(fs::read_to_string(&original)?, "disk\n");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ordinary_forced_close_does_not_retain_regular_swaps() -> Result<()> {
    for keep in [false, true] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let original = root.join("original.txt");
        fs::write(&original, "disk\n")?;
        let mut config = config(&root);
        config.editor.recovery.keep_recovered = keep;
        let mut app = AppBuilder::new()
            .with_config(config)
            .with_file(&original, None)
            .build()?;
        keys(&mut app, "ilocal <esc>").await?;
        let swap = only_swap(&app)?;
        test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
        assert!(!swap.exists());
        assert!(archives(&root)?.is_empty());
        assert!(app.editor.retained_recovery_backups.is_empty());
        assert_eq!(fs::read_dir(&root)?.count(), 1);
        assert_eq!(fs::read_to_string(&original)?, "disk\n");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn consuming_a_retained_archive_removes_it_from_exit_notices() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let swap = crashed_snapshot(
        &config,
        &snapshot(&root, Some(original.clone()), "recovered\n"),
    )?;
    let bytes = fs::read(&swap)?;
    let mut app = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    keys(&mut app, ":recover<ret>").await?;
    keys(&mut app, ":bc!<ret>").await?;
    let backup = retained_backup(&app, &bytes)?;
    keys(&mut app, &format!(":recover \"{}\"<ret>", backup.display())).await?;
    keys(&mut app, ":wbc<ret>").await?;
    assert!(!backup.exists());
    assert!(archives(&root)?.is_empty());
    assert!(app.editor.retained_recovery_backups.is_empty());
    assert_eq!(fs::read_to_string(&original)?, "recovered\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_archive_is_explicitly_recoverable_but_never_an_active_candidate() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let mut config = config(&root);
    config.editor.recovery.suffix = ".recovered".into();
    let swap = crashed_snapshot(
        &config,
        &snapshot(&root, Some(original.clone()), "recovered\n"),
    )?;
    let bytes = fs::read(&swap)?;
    let timestamp = recovery::claim(&swap, &config.editor.recovery)?
        .metadata
        .timestamp;
    let mut app = AppBuilder::new()
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    keys(&mut app, ":recover<ret>").await?;
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    let backup = retained_backup(&app, &bytes)?;
    drop(app);
    fs::write(&original, "new disk\n")?;
    set_modified(&original, timestamp + 10, 0)?;
    let mut app = AppBuilder::new()
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    keys(&mut app, "").await?;
    assert!(candidates(&app)?.is_empty());
    keys(&mut app, &format!(":recover \"{}\"<ret>", backup.display())).await?;
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "new disk\n");
    assert!(recovery::claim(&backup, &config.editor.recovery).is_err());
    keys(&mut app, "yes<ret>").await?;
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
    assert_eq!(fs::read(&backup)?, bytes);
    assert!(candidates(&app)?.is_empty());
    edit(&mut app, 0, "recovered\n".len(), "new edit\n");
    settle(&mut app).await?;
    let active = only_swap(&app)?;
    assert_ne!(active, backup);
    assert!(active
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with(".helix-recovery-"));
    assert_eq!(recovery::read(&active)?.text.to_string(), "new edit\n");
    assert!(!backup.exists());
    let replacement_backup = archives(&root)?;
    assert_eq!(replacement_backup.len(), 1);
    assert_eq!(fs::read(&replacement_backup[0])?, bytes);
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert!(!active.exists());
    assert_eq!(retained_backup(&app, &bytes)?, replacement_backup[0]);
    assert_eq!(fs::read_to_string(&original)?, "new disk\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn older_recovery_prompt_cancels_without_opening_switching_or_adopting() -> Result<()> {
    for target in ["current", "unopened", "other"] {
        for answer in ["<ret>", "n<ret>", "<esc>", "maybe<ret>"] {
            let directory = tempfile::tempdir()?;
            let root = directory.path().canonicalize()?;
            let original = root.join("original.txt");
            fs::write(&original, "disk\n")?;
            let config = config(&root);
            let swap = crashed_snapshot(
                &config,
                &snapshot(&root, Some(original.clone()), "recovered\n"),
            )?;
            let bytes = fs::read(&swap)?;
            let timestamp = recovery::claim(&swap, &config.editor.recovery)?
                .metadata
                .timestamp;
            set_modified(&original, timestamp + 10, 0)?;
            let mut app = AppBuilder::new().with_config(config.clone()).build()?;
            if target != "unopened" {
                app.editor.open(&original, Action::Replace)?;
            }
            if target == "other" {
                app.editor.new_file(Action::Replace);
            }
            let (view, doc) = current_ref!(app.editor);
            let before = (view.id, doc.id(), doc.text().clone());
            let documents = app.editor.documents().count();
            keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
            assert_eq!(current_ref!(app.editor).1.id(), before.1);
            assert_eq!(current_ref!(app.editor).1.text(), &before.2);
            assert_eq!(app.editor.documents().count(), documents);
            assert!(recovery::claim(&swap, &config.editor.recovery).is_err());
            assert_eq!(fs::read(&swap)?, bytes);
            assert!(archives(&root)?.is_empty());
            keys(&mut app, answer).await?;
            let (view, doc) = current_ref!(app.editor);
            assert_eq!(view.id, before.0);
            assert_eq!(doc.id(), before.1);
            assert_eq!(doc.text(), &before.2);
            assert!(!doc.recovery_requires_save());
            assert_eq!(app.editor.documents().count(), documents);
            assert!(recovery::claim(&swap, &config.editor.recovery).is_ok());
            test_key_sequence(&mut app, Some(":qa!<ret>"), None, true).await?;
            assert_eq!(fs::read(&swap)?, bytes);
            assert_eq!(fs::read_to_string(&original)?, "disk\n");
            assert!(archives(&root)?.is_empty());
            assert!(app.editor.retained_recovery_backups.is_empty());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn older_recovery_prompt_accepts_y_and_uppercase_yes() -> Result<()> {
    for answer in ["y<ret>", "YES<ret>"] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let original = root.join("original.txt");
        fs::write(&original, "disk\n")?;
        let config = config(&root);
        let swap = crashed_snapshot(
            &config,
            &snapshot(&root, Some(original.clone()), "recovered\n"),
        )?;
        let bytes = fs::read(&swap)?;
        let timestamp = recovery::claim(&swap, &config.editor.recovery)?
            .metadata
            .timestamp;
        set_modified(&original, timestamp + 10, 0)?;
        let mut app = AppBuilder::new().with_config(config.clone()).build()?;
        let before = current_ref!(app.editor).1.id();
        keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
        assert_eq!(current_ref!(app.editor).1.id(), before);
        assert!(app.editor.document_by_path(&original).is_none());
        assert!(
            recovery::claim(&swap, &config.editor.recovery).is_err(),
            "confirmation did not retain the claim; status: {:?}",
            app.editor.get_status()
        );
        keys(&mut app, answer).await?;
        assert_eq!(current_ref!(app.editor).1.path(), Some(original.as_path()));
        assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
        assert_eq!(fs::read(&swap)?, bytes);
        assert!(archives(&root)?.is_empty());
        assert_eq!(fs::read_to_string(&original)?, "disk\n");
        test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
        retained_backup(&app, &bytes)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn equal_or_newer_recovery_snapshot_does_not_prompt() -> Result<()> {
    for seconds_older in [0, 10] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let original = root.join("original.txt");
        fs::write(&original, "disk\n")?;
        let config = config(&root);
        let swap = crashed_snapshot(
            &config,
            &snapshot(&root, Some(original.clone()), "recovered\n"),
        )?;
        let bytes = fs::read(&swap)?;
        let timestamp = recovery::claim(&swap, &config.editor.recovery)?
            .metadata
            .timestamp;
        // Even a fractional mtime later in the same second must not trigger v1's prompt.
        set_modified(&original, timestamp - seconds_older, 500_000_000)?;
        let mut app = AppBuilder::new()
            .with_config(config)
            .with_file(&original, None)
            .build()?;
        keys(&mut app, ":recover<ret>").await?;
        assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
        assert_eq!(fs::read(&swap)?, bytes);
        assert_eq!(fs::read_to_string(&original)?, "disk\n");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_confirmation_rechecks_disk_view_target_and_pending_write() -> Result<()> {
    for change in [
        "disk",
        "modified",
        "readonly",
        "recovery-io",
        "write",
        "document",
        "view",
    ] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let original = root.join("original.txt");
        fs::write(&original, "disk\n")?;
        let mut config = config(&root);
        config.editor.recovery.enable = change == "recovery-io";
        let swap = crashed_snapshot(
            &config,
            &snapshot(&root, Some(original.clone()), "recovered\n"),
        )?;
        let bytes = fs::read(&swap)?;
        let timestamp = recovery::claim(&swap, &config.editor.recovery)?
            .metadata
            .timestamp;
        set_modified(&original, timestamp + 10, 100_000_000)?;
        let mut app = AppBuilder::new()
            .with_config(config.clone())
            .with_file(&original, None)
            .build()?;
        let target = current_ref!(app.editor).1.id();
        keys(&mut app, ":recover<ret>").await?;
        assert_eq!(current_ref!(app.editor).1.text().to_string(), "disk\n");
        assert!(recovery::claim(&swap, &config.editor.recovery).is_err());
        let expected_error = match change {
            "disk" => {
                let before = fs::metadata(&original)?.modified()?;
                // Same size and same timestamp second: validation must use full precision.
                fs::write(&original, "else\n")?;
                set_modified(&original, timestamp + 10, 600_000_000)?;
                assert_ne!(fs::metadata(&original)?.modified()?, before);
                "file changed while waiting"
            }
            "modified" => {
                edit(&mut app, 0, 0, "local ");
                "modified buffer"
            }
            "readonly" => {
                current!(app.editor).1.readonly = true;
                "readonly buffer"
            }
            "recovery-io" => {
                edit(&mut app, 0, 0, "local ");
                let (view, doc) = current!(app.editor);
                assert!(doc.undo(view));
                assert!(!doc.is_modified());
                assert!(doc.recovery_pending());
                "pending recovery I/O"
            }
            "write" => {
                app.editor.save(target, None::<PathBuf>, false)?;
                assert!(app.editor.write_count > 0);
                "pending saves and on-save jobs"
            }
            "document" => {
                app.editor.new_file(Action::Replace);
                "current view or buffer changed"
            }
            "view" => {
                app.editor.switch(target, Action::HorizontalSplit);
                "current view or buffer changed"
            }
            _ => unreachable!(),
        };
        let (view, doc) = current_ref!(app.editor);
        let before = (view.id, doc.id(), doc.text().clone());
        // Do not service pending save/recovery work before the confirmation callback.
        direct_keys(&mut app, "y<ret>").await?;
        let (message, severity) = app
            .editor
            .get_status()
            .context("missing revalidation error")?;
        assert_eq!(*severity, Severity::Error, "{change}: {message}");
        assert!(message.contains(expected_error), "{change}: {message}");
        let (view, doc) = current_ref!(app.editor);
        assert_eq!(view.id, before.0);
        assert_eq!(doc.id(), before.1);
        assert_eq!(doc.text(), &before.2);
        assert!(!app
            .editor
            .document(target)
            .unwrap()
            .recovery_requires_save());
        assert_eq!(fs::read(&swap)?, bytes);
        assert!(archives(&root)?.is_empty());
        assert!(recovery::claim(&swap, &config.editor.recovery).is_ok());
        keys(&mut app, "").await?;
        test_key_sequence(&mut app, Some(":qa!<ret>"), None, true).await?;
        assert_eq!(fs::read(&swap)?, bytes);
        assert!(archives(&root)?.is_empty());
        assert!(app.editor.retained_recovery_backups.is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_report_describes_metadata_utc_restoration_and_retention_policy() -> Result<()> {
    for keep in [false, true] {
        for disk in ["matched", "changed", "missing"] {
            let directory = tempfile::tempdir()?;
            let root = directory.path().canonicalize()?;
            let original = root.join("original.txt");
            fs::write(&original, "disk\n")?;
            set_modified(&original, 1_600_000_000, 0)?;
            let mut config = config(&root);
            config.editor.recovery.keep_recovered = keep;
            let swap = crashed_snapshot(
                &config,
                &snapshot(&root, Some(original.clone()), "recovered\n"),
            )?;
            let bytes = fs::read(&swap)?;
            let recovered = recovery::claim(&swap, &config.editor.recovery)?;
            assert_eq!(recovered.metadata.original_size, Some(5));
            assert_eq!(recovered.metadata.original_modified, Some(1_600_000_000));
            let timestamp =
                jiff::Timestamp::from_second(recovered.metadata.timestamp.try_into()?)?.to_string();
            assert!(timestamp.ends_with('Z'));
            drop(recovered);
            let expected_disk = match disk {
                "matched" => "size/mtime match; content not verified",
                "changed" => {
                    fs::write(&original, "changed disk\n")?;
                    set_modified(&original, 1_600_000_001, 0)?;
                    "size/mtime differ: apparent disk change"
                }
                "missing" => {
                    fs::remove_file(&original)?;
                    "metadata unknown/incomplete"
                }
                _ => unreachable!(),
            };
            let mut app = AppBuilder::new().with_config(config).build()?;
            keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
            let (message, severity) = app.editor.get_status().context("missing recovery report")?;
            assert_eq!(*severity, Severity::Warning);
            for expected in [
                swap.to_str().unwrap(),
                original.to_str().unwrap(),
                timestamp.as_str(),
                "10 text bytes",
                expected_disk,
                "Restored text, encoding/BOM, line ending and selections",
                "Nothing written",
                if keep {
                    "keep-recovered=true"
                } else {
                    "successfully written"
                },
            ] {
                assert!(
                    message.contains(expected),
                    "missing {expected:?}: {message}"
                );
            }
            assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
            assert_eq!(fs::read(&swap)?, bytes);
            assert!(archives(&root)?.is_empty());
            match disk {
                "matched" => assert_eq!(fs::read_to_string(&original)?, "disk\n"),
                "changed" => assert_eq!(fs::read_to_string(&original)?, "changed disk\n"),
                "missing" => assert!(!original.exists()),
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovered_write_quit_reopen_does_not_offer_the_old_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let swap = crashed_snapshot(
        &config,
        &snapshot(&root, Some(original.clone()), "recovered\n"),
    )?;
    let mut app = AppBuilder::new()
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    keys(&mut app, ":recover<ret>").await?;
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
    edit(&mut app, 0, "recovered\n".chars().count(), "new edits\n");
    test_key_sequence(&mut app, Some(":wq<ret>"), None, true).await?;
    assert_eq!(fs::read_to_string(&original)?, "new edits\n");
    assert!(
        !swap.exists(),
        "the selected recovery source must not survive clean quit"
    );
    assert!(archives(&root)?.is_empty());
    assert!(app.editor.retained_recovery_backups.is_empty());
    let mut reopened = AppBuilder::new()
        .with_config(config)
        .with_file(&original, None)
        .build()?;
    keys(&mut reopened, "").await?;
    assert!(candidates(&reopened)?.is_empty());
    assert_eq!(
        current_ref!(reopened.editor).1.text().to_string(),
        "new edits\n"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn selected_same_path_updates_after_edit_and_preserve() -> Result<()> {
    for automatic in [true, false] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let original = root.join("original.txt");
        fs::write(&original, "disk\n")?;
        let mut config = config(&root);
        config.editor.recovery.enable = automatic;
        let swap = crashed_snapshot(
            &config,
            &snapshot(&root, Some(original.clone()), "recovered\n"),
        )?;
        let bytes = fs::read(&swap)?;
        let mut app = AppBuilder::new()
            .with_config(config)
            .with_file(&original, None)
            .build()?;
        keys(&mut app, ":recover<ret>").await?;
        assert_eq!(fs::read(&swap)?, bytes);
        assert!(!current_ref!(app.editor).1.recovery_pending());
        assert!(
            archives(&root)?.is_empty(),
            "adoption must not write an archive"
        );
        edit(&mut app, 0, "recovered\n".len(), "edited\n");
        settle(&mut app).await?;
        if automatic {
            assert_eq!(recovery::read(&swap)?.text.to_string(), "edited\n");
            let backups = archives(&root)?;
            assert_eq!(backups.len(), 1);
            assert_eq!(fs::read(&backups[0])?, bytes);
        } else {
            assert_eq!(fs::read(&swap)?, bytes);
        }
        assert_eq!(only_swap(&app)?, swap);
        keys(&mut app, ":preserve<ret>").await?;
        assert_eq!(only_swap(&app)?, swap);
        assert_eq!(recovery::read(&swap)?.text.to_string(), "edited\n");
        let backups = archives(&root)?;
        assert_eq!(backups.len(), 1);
        assert_eq!(fs::read(&backups[0])?, bytes);
        assert_eq!(fs::read_to_string(&original)?, "disk\n");
        keys(&mut app, ":set-option recovery.enable false<ret>").await?;
        test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
        assert!(!swap.exists(), "cleanup must still track the selected path");
        assert_eq!(retained_backup(&app, &bytes)?, backups[0]);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn adoption_retains_previous_owned_file_until_successful_preserve() -> Result<()> {
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
    keys(&mut app, ":preserve<ret>").await?;
    let owned = only_swap(&app)?;
    let owned_bytes = fs::read(&owned)?;
    let saved = snapshot(&root, Some(original.clone()), "recovered\n");
    let selected = crashed_snapshot(&config, &saved)?;
    let unselected = crashed_snapshot(&config, &saved)?;
    let selected_bytes = fs::read(&selected)?;
    let unselected_bytes = fs::read(&unselected)?;

    keys(
        &mut app,
        &format!(":recover \"{}\"<ret>", selected.display()),
    )
    .await?;
    assert_eq!(current_ref!(app.editor).1.text(), &saved.text);
    assert_eq!(fs::read(&owned)?, owned_bytes);
    assert_eq!(fs::read(&selected)?, selected_bytes);
    assert_eq!(candidates(&app)?.len(), 3);
    assert!(archives(&root)?.is_empty());
    keys(&mut app, ":set-option recovery.size-threshold 1<ret>").await?;
    keys(&mut app, ":preserve<ret>").await?;
    let (_, severity) = app.editor.get_status().context("missing preserve error")?;
    assert_eq!(*severity, Severity::Error);
    assert_eq!(fs::read(&owned)?, owned_bytes);
    assert_eq!(fs::read(&selected)?, selected_bytes);
    assert!(
        archives(&root)?.is_empty(),
        "refused preserve must not archive"
    );
    keys(&mut app, ":set-option recovery.size-threshold 0<ret>").await?;
    keys(&mut app, ":preserve<ret>").await?;
    assert!(!owned.exists());
    assert_eq!(recovery::read(&selected)?.text, saved.text);
    assert_eq!(candidates(&app)?.len(), 2);
    assert_eq!(fs::read(&unselected)?, unselected_bytes);
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert!(!selected.exists());
    retained_backup(&app, &selected_bytes)?;
    assert_eq!(fs::read(&unselected)?, unselected_bytes);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovered_write_buffer_close_reopen_does_not_offer_the_old_snapshot() -> Result<()> {
    for automatic in [true, false] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let original = root.join("original.txt");
        fs::write(&original, "disk\n")?;
        let mut config = config(&root);
        config.editor.recovery.enable = automatic;
        let swap = crashed_snapshot(
            &config,
            &snapshot(&root, Some(original.clone()), "recovered\n"),
        )?;
        let mut app = AppBuilder::new()
            .with_config(config)
            .with_file(&original, None)
            .build()?;
        let id = current_ref!(app.editor).1.id();
        keys(&mut app, ":recover<ret>:write<ret>").await?;
        assert_eq!(fs::read_to_string(&original)?, "recovered\n");
        assert!(!current_ref!(app.editor).1.recovery_requires_save());
        keys(&mut app, ":buffer-close<ret>").await?;
        assert!(app.editor.document(id).is_none());
        assert!(!swap.exists());
        assert!(archives(&root)?.is_empty());
        assert!(app.editor.retained_recovery_backups.is_empty());
        app.editor.open(&original, Action::Replace)?;
        keys(&mut app, "").await?;
        assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
        assert!(candidates(&app)?.is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn write_buffer_close_variants_close_the_recovered_document() -> Result<()> {
    for auto_format in [false, true] {
        for force in [false, true] {
            for save_as in [false, true] {
                let directory = tempfile::tempdir()?;
                let root = directory.path().canonicalize()?;
                let original = root.join("original.txt");
                let destination = root.join("new path.txt");
                let other = root.join("other.txt");
                fs::write(&original, "disk\n")?;
                fs::write(&other, "other\n")?;
                let mut config = config(&root);
                // Even without an LSP, auto-format defers the on-save tail.
                config.editor.auto_format = auto_format;
                let swap = crashed_snapshot(
                    &config,
                    &snapshot(&root, Some(original.clone()), "recovered\n"),
                )?;
                let mut app = AppBuilder::new()
                    .with_config(config)
                    .with_file(&original, None)
                    .build()?;
                keys(&mut app, ":recover<ret>").await?;
                let target = current_ref!(app.editor).1.id();
                let other_id = app.editor.open(&other, Action::Replace)?;
                app.editor.switch(target, Action::Replace);
                let bang = if force { "!" } else { "" };
                let argument = if save_as {
                    format!(" \"{}\"", destination.display())
                } else {
                    String::new()
                };
                keys(
                    &mut app,
                    &format!(":write-buffer-close{bang}{argument}<ret>"),
                )
                .await?;
                assert!(
                    app.editor.document(target).is_none(),
                    "target still open: auto_format={auto_format}, force={force}, save_as={save_as}"
                );
                assert_eq!(
                    app.editor.document(other_id).unwrap().text().to_string(),
                    "other\n"
                );
                assert!(!swap.exists());
                assert!(archives(&root)?.is_empty());
                assert!(app.editor.retained_recovery_backups.is_empty());
                assert_eq!(fs::read_to_string(&other)?, "other\n");
                assert_eq!(
                    fs::read_to_string(&original)?,
                    if save_as { "disk\n" } else { "recovered\n" }
                );
                if save_as {
                    assert_eq!(fs::read_to_string(&destination)?, "recovered\n");
                    app.editor.open(&destination, Action::Replace)?;
                    assert!(candidates(&app)?.is_empty());
                } else {
                    assert!(!destination.exists());
                }
                app.editor.open(&original, Action::Replace)?;
                assert!(candidates(&app)?.is_empty());
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_write_buffer_close_and_refused_close_keep_the_adopted_source() -> Result<()> {
    for auto_format in [false, true] {
        for force in [false, true] {
            let directory = tempfile::tempdir()?;
            let root = directory.path().canonicalize()?;
            let original = root.join("original.txt");
            fs::write(&original, "disk\n")?;
            let mut config = config(&root);
            config.editor.auto_format = auto_format;
            let swap = crashed_snapshot(
                &config,
                &snapshot(&root, Some(original.clone()), "recovered\n"),
            )?;
            let bytes = fs::read(&swap)?;
            let mut app = AppBuilder::new()
                .with_config(config.clone())
                .with_file(&original, None)
                .build()?;
            keys(&mut app, ":recover<ret>").await?;
            let id = current_ref!(app.editor).1.id();
            for command in [
                ":buffer-close<ret>".to_owned(),
                ":q<ret>".to_owned(),
                format!(
                    ":write-buffer-close{} \"{}\"<ret>",
                    if force { "!" } else { "" },
                    original.join("cannot-save.txt").display()
                ),
            ] {
                keys(&mut app, &command).await?;
                let (_, severity) = app.editor.get_status().context("missing refusal")?;
                assert_eq!(*severity, Severity::Error, "{command}");
                let doc = app
                    .editor
                    .document(id)
                    .context("failed close removed buffer")?;
                assert_eq!(doc.text().to_string(), "recovered\n");
                assert_eq!(doc.path(), Some(original.as_path()));
                assert!(doc.is_modified());
                assert!(doc.recovery_requires_save());
                assert_eq!(fs::read(&swap)?, bytes);
                assert_eq!(fs::read_to_string(&original)?, "disk\n");
                assert!(recovery::claim(&swap, &config.editor.recovery).is_err());
            }
            // Repeat the failure after overwriting the active swap: the backup
            // must still contain the originally recovered snapshot, not this edit.
            edit(&mut app, 0, "recovered\n".len(), "later edit\n");
            keys(&mut app, ":preserve<ret>").await?;
            let backups = archives(&root)?;
            assert_eq!(backups.len(), 1);
            assert_eq!(fs::read(&backups[0])?, bytes);
            keys(&mut app, ":bc<ret>").await?;
            keys(
                &mut app,
                &format!(
                    ":wbc{} \"{}\"<ret>",
                    if force { "!" } else { "" },
                    original.join("cannot-save.txt").display()
                ),
            )
            .await?;
            assert!(app.editor.document(id).is_some());
            assert_eq!(fs::read(&backups[0])?, bytes);
            assert_eq!(recovery::read(&swap)?.text.to_string(), "later edit\n");
            let active_bytes = fs::read(&swap)?;
            drop(app);
            assert_eq!(fs::read(&swap)?, active_bytes);
            assert_eq!(fs::read(&backups[0])?, bytes);
            assert!(recovery::claim(&swap, &config.editor.recovery).is_ok());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_recovery_does_not_adopt_cleanup_responsibility() -> Result<()> {
    for refusal in [
        "modified",
        "readonly",
        "encoding",
        "line-ending",
        "selection",
    ] {
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
        keys(&mut app, ":preserve<ret>").await?;
        let owned = only_swap(&app)?;
        let owned_bytes = fs::read(&owned)?;
        let swap = crashed_snapshot(
            &config,
            &snapshot(&root, Some(original.clone()), "recovered\n"),
        )?;
        let bytes = fs::read(&swap)?;
        let mut recovered = recovery::claim(&swap, &config.editor.recovery)?;
        assert_eq!(recovered.path(), swap.as_path());
        match refusal {
            "modified" => edit(&mut app, 0, 0, "local "),
            "readonly" => current!(app.editor).1.readonly = true,
            "encoding" => recovered.snapshot.encoding = "not-an-encoding".into(),
            "line-ending" => recovered.snapshot.line_ending = "not-a-line-ending".into(),
            "selection" => recovered.snapshot.primary = recovered.snapshot.selections.len(),
            _ => unreachable!(),
        }
        {
            let (view, doc) = current!(app.editor);
            let before = doc.text().clone();
            assert!(doc.recover(recovered, view).is_err(), "{refusal}");
            assert_eq!(doc.text(), &before);
            assert!(!doc.recovery_requires_save());
        }
        assert_eq!(fs::read(&swap)?, bytes);
        assert_eq!(fs::read(&owned)?, owned_bytes);
        assert!(recovery::claim(&swap, &config.editor.recovery).is_ok());
        assert!(archives(&root)?.is_empty());
        test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
        assert!(!owned.exists());
        assert_eq!(fs::read(&swap)?, bytes);
        assert_eq!(fs::read_to_string(&original)?, "disk\n");
        assert!(archives(&root)?.is_empty());
        assert!(app.editor.retained_recovery_backups.is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_deferred_write_close_keeps_a_clean_undone_recovery() -> Result<()> {
    for command in [":wbc<ret>", ":wbc!<ret>"] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let mut config = config(&root);
        config.editor.recovery.enable = false;
        config.editor.auto_format = true;
        let swap = crashed_snapshot(&config, &snapshot(&root, None, "recovered\n"))?;
        let before = fs::read(&swap)?;
        let mut app = AppBuilder::new().with_config(config).build()?;
        keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
        let id = current_ref!(app.editor).1.id();
        keys(&mut app, "u").await?;
        assert!(!current_ref!(app.editor).1.is_modified());
        keys(&mut app, command).await?;
        assert!(
            app.editor.document(id).is_some(),
            "a failed write must not close even a clean buffer"
        );
        assert_eq!(current_ref!(app.editor).1.id(), id);
        assert_eq!(fs::read(&swap)?, before);
        let (message, severity) = app.editor.get_status().context("missing save error")?;
        assert_eq!(*severity, Severity::Error, "{message}");
        test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
        assert!(!swap.exists());
        retained_backup(&app, &before)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn live_swap_rejects_recovery_without_changing_the_target_or_source() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let saved = snapshot(&root, Some(original.clone()), "live writer\n");
    let mut writer = Swap::default();
    let swap = writer.write(&saved, &config.editor.recovery, 1)?.unwrap();
    let bytes = fs::read(&swap)?;
    let mut app = AppBuilder::new()
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    assert_eq!(only_swap(&app)?, swap);
    assert_eq!(recovery::read(&swap)?.text, saved.text);
    assert!(recovery::claim(&swap, &config.editor.recovery).is_err());
    keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
    let (_, severity) = app
        .editor
        .get_status()
        .context("missing live-owner error")?;
    assert_eq!(*severity, Severity::Error);
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "disk\n");
    assert!(!current_ref!(app.editor).1.recovery_requires_save());
    assert_eq!(fs::read(&swap)?, bytes);
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&swap)?, bytes);
    drop(writer);
    let recovered = recovery::claim(&swap, &config.editor.recovery)?;
    assert_eq!(recovered.snapshot.text, saved.text);
    assert_eq!(fs::read(&swap)?, bytes);
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn live_buffer_rejects_a_second_claim_without_losing_its_source() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let config = config(&root);
    let swap = crashed_snapshot(
        &config,
        &snapshot(&root, Some(original.clone()), "recovered\n"),
    )?;
    let bytes = fs::read(&swap)?;
    let mut owner = AppBuilder::new()
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    keys(&mut owner, ":recover<ret>").await?;
    let mut other = AppBuilder::new()
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    keys(&mut other, ":recover<ret>").await?;
    let (_, severity) = other
        .editor
        .get_status()
        .context("missing live-buffer error")?;
    assert_eq!(*severity, Severity::Error);
    assert_eq!(current_ref!(other.editor).1.text().to_string(), "disk\n");
    assert!(!current_ref!(other.editor).1.recovery_requires_save());
    assert_eq!(
        current_ref!(owner.editor).1.text().to_string(),
        "recovered\n"
    );
    assert_eq!(fs::read(&swap)?, bytes);
    test_key_sequence(&mut other, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&swap)?, bytes);
    assert!(recovery::claim(&swap, &config.editor.recovery).is_err());
    edit(&mut owner, 0, "recovered\n".len(), "owner edits\n");
    keys(&mut owner, ":preserve<ret>").await?;
    assert_eq!(only_swap(&owner)?, swap);
    assert_eq!(recovery::read(&swap)?.text.to_string(), "owner edits\n");
    assert!(recovery::claim(&swap, &config.editor.recovery).is_err());
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    test_key_sequence(&mut owner, Some(":q!<ret>"), None, true).await?;
    assert!(!swap.exists());
    retained_backup(&owner, &bytes)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_by_default_but_manual_preserve_and_recover_work() -> Result<()> {
    assert!(!Config::default().editor.recovery.enable);
    assert!(!Config::default().editor.recovery.keep_recovered);
    assert_eq!(
        Config::default().editor.recovery.directories[1],
        helix_loader::state_dir().join("recovery")
    );
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
    assert_eq!(fs::read(&swap)?, bytes);
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert!(!swap.exists());
    retained_backup(&app, &bytes)?;
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
    assert_eq!(fs::read(&swap)?, bytes);
    assert_eq!(fs::read_dir(&root)?.count(), 1);
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert!(!swap.exists());
    retained_backup(&app, &bytes)?;
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
        .with_config(config.clone())
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
        doc.recover(recovery::claim(&swap, &config.editor.recovery)?, view)?;
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
    assert!(!swap.exists());
    assert!(!owned_swap.exists());
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
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    edit(&mut app, 0, 0, "local ");
    {
        let (view, doc) = current!(app.editor);
        assert!(doc
            .recover(recovery::claim(&swap, &config.editor.recovery)?, view)
            .is_err());
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
        let error = doc
            .recover(recovery::claim(&swap, &config.editor.recovery)?, view)
            .unwrap_err();
        assert!(error.to_string().contains("pending recovery I/O"));
        assert_eq!(doc.text().to_string(), "disk\n");
        assert!(!doc.recovery_requires_save());
    }
    settle(&mut app).await?;
    {
        let (view, doc) = current!(app.editor);
        doc.readonly = true;
        let error = doc
            .recover(recovery::claim(&swap, &config.editor.recovery)?, view)
            .unwrap_err();
        assert!(error.to_string().contains("readonly"));
        assert_eq!(doc.text().to_string(), "disk\n");
        assert!(!doc.recovery_requires_save());
        doc.readonly = false;
        doc.recover(recovery::claim(&swap, &config.editor.recovery)?, view)?;
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
    // The deliberate saves above may cross a snapshot timestamp second.
    let timestamp = recovery::claim(&swap, &app.editor.config().recovery)?
        .metadata
        .timestamp;
    set_modified(&original, timestamp, 0)?;
    keys(&mut app, &format!(":recover \"{}\"<ret>", swap.display())).await?;
    assert_eq!(current_ref!(app.editor).1.text(), &saved.text);
    assert!(current_ref!(app.editor).1.recovery_requires_save());
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    assert_eq!(fs::read(&swap)?, bytes);
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert!(!swap.exists());
    retained_backup(&app, &bytes)?;
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
    let crashed_bytes = fs::read(&crashed)?;
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
    assert!(!crashed.exists());
    retained_backup(&app, &crashed_bytes)?;
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
        let config = config(&root);
        let swap = crashed_snapshot(&config, &snapshot(&root, Some(original.clone()), after))?;
        let bytes = fs::read(&swap)?;
        let mut app = AppBuilder::new()
            .with_config(config.clone())
            .with_file(&original, None)
            .build()?;
        let (view, doc) = current!(app.editor);
        doc.recover(recovery::claim(&swap, &config.editor.recovery)?, view)?;
        assert_eq!(doc.text().to_string(), after);
        assert_eq!(fs::read(&swap)?, bytes);
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
    assert_eq!(fs::read(&second)?, second_bytes);
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert_eq!(fs::read(&first)?, first_bytes);
    assert!(!second.exists());
    retained_backup(&app, &second_bytes)?;
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
    let bytes = fs::read(&swap)?;
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
    assert!(!swap.exists());
    retained_backup(&app, &bytes)?;
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
        .with_config(config.clone())
        .with_file(&original, None)
        .build()?;
    let owned_swap = {
        let (view, doc) = current!(app.editor);
        doc.recover(recovery::claim(&swap, &config.editor.recovery)?, view)?;
        assert_eq!(doc.text(), &saved.text);
        assert_eq!(doc.encoding().name(), "UTF-16LE");
        assert_eq!(doc.line_ending, LineEnding::Crlf);
        assert_eq!(
            doc.selection(view.id),
            &Selection::new(vec![Range::new(2, 1), Range::new(4, 6)].into(), 1)
        );
        assert_eq!(fs::read(&swap)?, bytes);
        doc.preserve(view.id)?
    };
    assert_eq!(owned_swap, swap);
    assert_eq!(only_swap(&app)?, swap);
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
    settle(&mut app).await?;
    assert_eq!(only_swap(&app)?, swap);
    assert_eq!(recovery::read(&swap)?.text, saved.text);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn intentional_exit_reports_retained_backup_on_stderr() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let original = root.join("original.txt");
    fs::write(&original, "disk\n")?;
    let swap = crashed_snapshot(
        &config(&root),
        &snapshot(&root, Some(original.clone()), "recovered\n"),
    )?;
    let bytes = fs::read(&swap)?;
    let child_root = root.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "test::recovery::recovery_retention_child",
                "--nocapture",
            ])
            .env("HELIX_RECOVERY_RETENTION_CHILD_DIR", child_root)
            .stdin(Stdio::null())
            .output()
    })
    .await??;
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let backups = archives(&root)?;
    assert_eq!(backups.len(), 1);
    let expected = format!("Recovery backup retained: {}", backups[0].display());
    assert!(String::from_utf8_lossy(&output.stderr).contains(&expected));
    assert_eq!(fs::read(&backups[0])?, bytes);
    assert!(!swap.exists());
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "subprocess fixture; launched by intentional_exit_reports_retained_backup_on_stderr"]
async fn recovery_retention_child() -> Result<()> {
    let Some(root) = std::env::var_os("HELIX_RECOVERY_RETENTION_CHILD_DIR") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    ensure!(root.is_absolute(), "child directory must be absolute");
    let mut app = AppBuilder::new()
        .with_config(config(&root))
        .with_file(root.join("original.txt"), None)
        .build()?;
    keys(&mut app, ":recover<ret>").await?;
    assert_eq!(current_ref!(app.editor).1.text().to_string(), "recovered\n");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    for key in parse_macro(":q!<ret>")? {
        tx.send(Ok(Event::Key(KeyEvent::from(key))))?;
    }
    let mut stream = UnboundedReceiverStream::new(rx);
    assert_eq!(
        tokio::time::timeout(TIMEOUT, app.run(&mut stream)).await??,
        0
    );
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
    let recovery_config = config(&root).editor.recovery;
    let live = recovery::discover(&recovery_config, Some(&original), &root)?;
    assert!(live.warnings.is_empty());
    assert_eq!(live.candidates.len(), 1);
    assert_eq!(
        recovery::read(&live.candidates[0])?.text.to_string(),
        "unsaved disk\n"
    );
    assert!(
        recovery::claim(&live.candidates[0], &recovery_config).is_err(),
        "a running child process must retain exclusive swap ownership"
    );
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
    assert_eq!(fs::read(&swap)?, bytes);
    test_key_sequence(&mut app, Some(":q!<ret>"), None, true).await?;
    assert!(!swap.exists());
    assert_eq!(fs::read_to_string(&original)?, "disk\n");
    retained_backup(&app, &bytes)?;
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
