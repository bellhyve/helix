use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::{anyhow, ensure, Result};
use parking_lot::Mutex;
use tokio::{
    task::JoinHandle,
    time::{Instant, Sleep},
};

use super::{Config, Recovery, Snapshot, Swap};

/// One serialized writer and one replaceable pending snapshot per document.
/// Timers and workers are driven exclusively by the editor polling this state.
pub(crate) struct State {
    cwd: PathBuf,
    swap: Arc<Mutex<Swap>>,
    published_path: Option<PathBuf>,
    worker: Option<JoinHandle<Result<Option<PathBuf>>>>,
    worker_generation: u64,
    worker_unnamed: bool,
    generation: u64,
    ignore_through: u64,
    cached: Option<Snapshot>,
    pending: Option<(Snapshot, Config)>,
    changed_chars: usize,
    first_pending: Option<Instant>,
    deadline: Option<Instant>,
    sleep: Option<Pin<Box<Sleep>>>,
    attempted: bool,
    failed: bool,
    unnamed_reported: bool,
    oversized: bool,
    notice: Option<String>,
    recovered_revision: Option<usize>,
    recovered_written: bool,
    closed: bool,
}

impl Default for State {
    fn default() -> Self {
        // Unlike the cached Helix cwd accessor, current_dir can report a deleted
        // cwd without panicking. Never retain a relative fallback in a sidefile.
        let cwd = std::env::current_dir().unwrap_or_else(|error| {
            log::warn!("Cannot determine recovery cwd: {error}; using an absolute fallback");
            std::env::current_exe()
                .ok()
                .filter(|path| path.is_absolute())
                .and_then(|path| path.parent().map(Path::to_owned))
                .unwrap_or_else(|| {
                    if cfg!(windows) {
                        PathBuf::from("C:\\")
                    } else {
                        PathBuf::from("/")
                    }
                })
        });
        Self {
            cwd,
            swap: Arc::new(Mutex::new(Swap::default())),
            published_path: None,
            worker: None,
            worker_generation: 0,
            worker_unnamed: false,
            generation: 0,
            ignore_through: 0,
            cached: None,
            pending: None,
            changed_chars: 0,
            first_pending: None,
            deadline: None,
            sleep: None,
            attempted: false,
            failed: false,
            unnamed_reported: false,
            oversized: false,
            notice: None,
            recovered_revision: None,
            recovered_written: false,
            closed: false,
        }
    }
}

impl State {
    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub(crate) fn path(&self) -> Option<PathBuf> {
        self.published_path.clone()
    }

    pub(crate) fn pending(&self) -> bool {
        self.pending.is_some() || self.worker.is_some() || self.notice.is_some()
    }

    fn clear_pending(&mut self) {
        self.pending = None;
        self.changed_chars = 0;
        self.first_pending = None;
        self.deadline = None;
        self.sleep = None;
    }

    pub(crate) fn disable(&mut self) {
        self.clear_pending();
        self.cached = None;
        self.notice = None;
        self.oversized = false;
        self.ignore_through = self.generation;
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }

    pub(crate) fn recovered(
        &mut self,
        recovered: Recovery,
        mut snapshot: Snapshot,
        revision: usize,
    ) {
        self.disable();
        self.recovered_revision = Some(revision);
        self.recovered_written = false;
        self.cwd = recovered.snapshot.cwd.clone();
        snapshot.cwd.clone_from(&self.cwd);
        self.published_path = Some(recovered.path().to_owned());
        self.swap.lock().adopt(recovered);
        self.cached = Some(snapshot);
    }

    pub(crate) fn mark_written(&mut self, revision: usize) {
        // A save of a revision preceding recovery must not authorize backup deletion.
        if self
            .recovered_revision
            .is_some_and(|recovered| revision >= recovered)
        {
            self.recovered_written = true;
        }
    }

    pub(crate) fn record(&mut self, snapshot: Snapshot, config: Config, changed_chars: usize) {
        if !config.enable {
            self.disable();
            return;
        }
        if self.closed {
            return;
        }
        let oversized =
            config.size_threshold != 0 && snapshot.text.len_bytes() > config.size_threshold;
        self.cached = Some(snapshot.clone());
        if changed_chars != 0 {
            self.failed = false;
        } else if self.failed {
            return;
        }
        if oversized {
            self.clear_pending();
            if !self.oversized {
                self.notice = Some(format!(
                    "Recovery snapshot exceeds size-threshold ({} bytes); previous recovery file retained",
                    config.size_threshold
                ));
            }
            self.oversized = true;
            return;
        }
        self.oversized = false;
        self.notice = None;
        let now = Instant::now();
        self.changed_chars = self.changed_chars.saturating_add(changed_chars);
        // A stream of small edits must not move the first pending edit's deadline.
        let first_pending = *self.first_pending.get_or_insert(now);
        let Some(due) = first_pending.checked_add(Duration::from_secs(config.update_time)) else {
            self.clear_pending();
            self.notice =
                Some("Recovery update-time is too large; previous snapshot retained".into());
            return;
        };
        let deadline = self.deadline.get_or_insert(due);
        *deadline = (*deadline).min(due);
        if !self.attempted || self.changed_chars >= config.update_count {
            *deadline = (*deadline).min(now);
        }
        self.pending = Some((snapshot, config));
    }

    /// Refresh only metadata supplied by the document, keeping its cached safe text.
    pub(crate) fn refresh(
        &mut self,
        path: Option<PathBuf>,
        encoding: String,
        has_bom: bool,
        line_ending: String,
        config: Config,
    ) {
        let Some(mut snapshot) = self.cached.clone() else {
            return;
        };
        snapshot.path = path;
        snapshot.encoding = encoding;
        snapshot.has_bom = has_bom;
        snapshot.line_ending = line_ending;
        if !config.enable {
            self.disable();
            self.cached = Some(snapshot);
            return;
        }
        self.record(snapshot, config, 0);
        if self.pending.is_some() {
            self.deadline = Some(Instant::now());
        }
    }

    pub(crate) fn saved_text(&mut self, text: &helix_core::Rope) {
        if let Some(snapshot) = &mut self.cached {
            snapshot.text = text.clone();
            for (anchor, head) in &mut snapshot.selections {
                *anchor = (*anchor).min(text.len_chars());
                *head = (*head).min(text.len_chars());
            }
        }
    }

    pub(crate) fn reconfigure(&mut self, config: Config) {
        self.failed = false;
        if let Some(snapshot) = self.cached.clone() {
            self.record(snapshot, config, 0);
            if self.pending.is_some() {
                self.deadline = Some(Instant::now());
            }
        }
    }

    pub(crate) fn preserve(&mut self, snapshot: Snapshot, mut config: Config) -> Result<PathBuf> {
        ensure!(!self.closed, "recovery state is closed");
        config.enable = true;
        self.clear_pending();
        self.notice = None;
        self.cached = Some(snapshot.clone());
        self.generation += 1;
        self.ignore_through = self.generation;
        self.attempted = true;
        self.failed = true;
        // The same lock and generation fence serialize explicit writes with
        // workers already running or still queued in the blocking pool.
        let result = self
            .swap
            .lock()
            .write(&snapshot, &config, self.generation)?;
        let path = result.ok_or_else(|| {
            anyhow!(
                "Recovery snapshot exceeds size-threshold ({} bytes)",
                config.size_threshold
            )
        })?;
        self.failed = false;
        self.unnamed_reported |= snapshot.path.is_none();
        self.oversized = false;
        self.published_path = Some(path.clone());
        Ok(path)
    }

    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<String>>> {
        if self.closed {
            return Poll::Pending;
        }
        if let Some(notice) = self.notice.take() {
            return Poll::Ready(Ok(Some(notice)));
        }
        loop {
            if let Some(worker) = &mut self.worker {
                let result = match Pin::new(worker).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result,
                };
                self.worker = None;
                if self.worker_generation > self.ignore_through {
                    let result = result
                        .map_err(anyhow::Error::from)
                        .and_then(|result| result);
                    if result.is_err() && self.changed_chars == 0 {
                        self.clear_pending();
                        self.failed = true;
                    }
                    return Poll::Ready(result.map(|path| {
                        path.and_then(|path| {
                            self.published_path = Some(path.clone());
                            if self.worker_unnamed && !self.unnamed_reported {
                                self.unnamed_reported = true;
                                Some(format!("Recovery file: {}", path.display()))
                            } else {
                                None
                            }
                        })
                    }));
                }
            }
            let Some(deadline) = self.deadline else {
                return Poll::Pending;
            };
            if deadline > Instant::now() {
                let sleep = self
                    .sleep
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
                if sleep.deadline() != deadline {
                    sleep.as_mut().reset(deadline);
                }
                if sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
            }
            let (snapshot, config) = self
                .pending
                .take()
                .expect("deadline without recovery snapshot");
            self.clear_pending();
            self.attempted = true;
            self.generation += 1;
            self.worker_generation = self.generation;
            self.worker_unnamed = snapshot.path.is_none();
            let swap = self.swap.clone();
            let generation = self.generation;
            self.worker = Some(tokio::task::spawn_blocking(move || {
                swap.lock().write(&snapshot, &config, generation)
            }));
            // Poll immediately to register the editor's waker on the new job.
        }
    }

    pub(crate) fn close(&mut self, keep_recovered: bool) -> Result<Vec<PathBuf>> {
        self.closed = true;
        self.disable();
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
        // Abort cannot stop an already-running blocking worker. Swap::close
        // fences it under the same lock and removes whatever it published.
        let retained = self
            .swap
            .lock()
            .close(keep_recovered || !self.recovered_written)?;
        self.published_path = None;
        Ok(retained)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{future::poll_fn, FutureExt};
    use helix_core::Rope;

    fn snapshot(directory: &Path, text: &str) -> Snapshot {
        Snapshot {
            path: None,
            cwd: directory.to_owned(),
            text: Rope::from_str(text),
            encoding: "UTF-8".into(),
            has_bom: false,
            line_ending: "lf".into(),
            selections: vec![(0, 0)],
            primary: 0,
        }
    }

    fn config(directory: &Path) -> Config {
        Config {
            enable: true,
            directories: vec![directory.to_owned()],
            ..Config::default()
        }
    }

    #[test]
    fn maximum_interval_and_character_threshold() {
        let mut state = State::default();
        let cwd = state.cwd.clone();
        let mut config = config(&cwd);
        state.attempted = true;
        state.record(snapshot(&cwd, "a"), config.clone(), 1);
        let deadline = state.deadline.unwrap();
        assert_eq!(
            deadline - state.first_pending.unwrap(),
            Duration::from_secs(4)
        );
        state.record(snapshot(&cwd, "b"), config.clone(), 198);
        assert_eq!(state.deadline, Some(deadline));
        assert_eq!(state.changed_chars, 199);
        state.record(snapshot(&cwd, "c"), config.clone(), 1);
        assert!(state.deadline.unwrap() <= Instant::now());
        let immediate = state.deadline;
        state.record(snapshot(&cwd, "d"), config.clone(), 1);
        assert_eq!(state.deadline, immediate);

        state.clear_pending();
        state.record(snapshot(&cwd, "e"), config.clone(), 1);
        let started = state.first_pending.unwrap();
        config.update_time = 1;
        state.record(snapshot(&cwd, "f"), config, 1);
        assert_eq!(state.deadline, Some(started + Duration::from_secs(1)));
    }

    #[test]
    fn disabled_does_not_retain_snapshots_or_create_timers() {
        let mut state = State::default();
        let snapshot = snapshot(state.cwd(), "a");
        state.record(snapshot, Config::default(), 200);
        assert!(!state.pending());
        assert!(state.cached.is_none());
        assert!(state.sleep.is_none());
        assert!(state.path().is_none());
    }

    #[tokio::test]
    async fn first_edit_is_immediate_and_inflight_edits_coalesce() {
        let directory = tempfile::tempdir().unwrap();
        let config = config(directory.path());
        let mut state = State::default();
        state.record(snapshot(directory.path(), "first"), config.clone(), 1);
        assert!(state.deadline.unwrap() <= Instant::now());
        // Start the job and register its waker without waiting for disk I/O.
        let first = poll_fn(|cx| state.poll(cx)).now_or_never();
        assert_eq!(state.changed_chars, 0);
        state.record(snapshot(directory.path(), "second"), config.clone(), 100);
        let deadline = state.deadline.unwrap();
        state.record(snapshot(directory.path(), "latest"), config, 100);
        assert!(state.deadline.unwrap() <= deadline);
        assert_eq!(state.pending.as_ref().unwrap().0.text.to_string(), "latest");
        let first = match first {
            Some(result) => result,
            None => poll_fn(|cx| state.poll(cx)).await,
        };
        assert!(first.unwrap().unwrap().starts_with("Recovery file:"));
        assert!(poll_fn(|cx| state.poll(cx)).await.unwrap().is_none());
        assert_eq!(
            super::super::read(&state.path().unwrap())
                .unwrap()
                .text
                .to_string(),
            "latest"
        );
        assert!(!state.pending());
        state.close(false).unwrap();
    }

    #[tokio::test]
    async fn failure_waits_for_an_edit_not_metadata_refresh() {
        let mut state = State::default();
        let cwd = state.cwd.clone();
        let mut config = config(&cwd);
        config.directories.clear();
        state.record(snapshot(&cwd, "a"), config.clone(), 1);
        assert!(poll_fn(|cx| state.poll(cx)).await.is_err());
        assert!(!state.pending());
        state.refresh(None, "UTF-8".into(), false, "lf".into(), config.clone());
        assert!(!state.pending());
        assert!(poll_fn(|cx| state.poll(cx)).now_or_never().is_none());
        state.record(snapshot(&cwd, "b"), config, 200);
        assert!(state.pending());
        assert!(poll_fn(|cx| state.poll(cx)).await.is_err());
        assert!(!state.pending());
    }

    #[tokio::test]
    async fn size_limit_warns_once_and_keeps_previous_file() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config(directory.path());
        let mut state = State::default();
        let path = state
            .preserve(snapshot(directory.path(), "a"), config.clone())
            .unwrap();
        config.size_threshold = 1;
        state.record(snapshot(directory.path(), "too large"), config.clone(), 200);
        assert!(poll_fn(|cx| state.poll(cx)).await.unwrap().is_some());
        state.record(
            snapshot(directory.path(), "still too large"),
            config.clone(),
            200,
        );
        assert!(!state.pending());
        assert_eq!(super::super::read(&path).unwrap().text.to_string(), "a");
        state.record(snapshot(directory.path(), "b"), config, 200);
        assert!(poll_fn(|cx| state.poll(cx)).await.unwrap().is_none());
        assert_eq!(state.path(), Some(path.clone()));
        assert_eq!(super::super::read(&path).unwrap().text.to_string(), "b");
        state.close(false).unwrap();
    }

    #[tokio::test]
    async fn manual_preserve_fences_older_worker_even_when_disabled() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config(directory.path());
        let mut state = State::default();
        state.record(snapshot(directory.path(), "old"), config.clone(), 1);
        let _ = poll_fn(|cx| state.poll(cx)).now_or_never();
        config.enable = false;
        let path = state
            .preserve(snapshot(directory.path(), "new"), config)
            .unwrap();
        if let Some(worker) = state.worker.take() {
            worker.await.unwrap().unwrap();
        }
        assert_eq!(super::super::read(&path).unwrap().text.to_string(), "new");
        assert!(!state.pending());
        state.close(false).unwrap();
    }

    #[tokio::test]
    async fn metadata_refresh_keeps_latest_safe_text_and_migrates_on_poll() {
        let directory = tempfile::tempdir().unwrap();
        let config = config(directory.path());
        let mut state = State::default();
        let old_path = state
            .preserve(snapshot(directory.path(), "saved"), config.clone())
            .unwrap();
        state.record(snapshot(directory.path(), "newer edit"), config.clone(), 1);
        let original = Some(directory.path().canonicalize().unwrap().join("renamed.txt"));
        state.refresh(
            original.clone(),
            "UTF-16LE".into(),
            true,
            "crlf".into(),
            config.clone(),
        );
        assert_eq!(state.path(), Some(old_path.clone()));
        assert_eq!(
            super::super::read(&old_path).unwrap().text.to_string(),
            "saved"
        );
        assert!(poll_fn(|cx| state.poll(cx)).await.unwrap().is_none());
        let path = state.path().unwrap();
        assert_ne!(path, old_path);
        assert!(!old_path.exists());
        let saved = super::super::read(&path).unwrap();
        assert_eq!(saved.text.to_string(), "newer edit");
        assert_eq!(saved.path, original);
        assert_eq!(saved.encoding, "UTF-16LE");
        assert!(saved.has_bom);
        assert_eq!(saved.line_ending, "crlf");
        state.refresh(original, "UTF-8".into(), false, "lf".into(), config);
        assert!(poll_fn(|cx| state.poll(cx)).await.unwrap().is_none());
        assert_eq!(state.path(), Some(path));
        state.close(false).unwrap();
    }

    #[tokio::test]
    async fn close_fences_inflight_and_future_writes() {
        let directory = tempfile::tempdir().unwrap();
        let config = config(directory.path());
        let snapshot = snapshot(directory.path(), "a");
        let mut state = State::default();
        let path = state.preserve(snapshot.clone(), config.clone()).unwrap();
        state.record(snapshot.clone(), config.clone(), 200);
        let _ = poll_fn(|cx| state.poll(cx)).now_or_never();
        let swap = state.swap.clone();
        state.close(false).unwrap();
        assert!(!state.pending());
        assert!(!path.exists());
        assert!(swap
            .lock()
            .write(&snapshot, &config, u64::MAX)
            .unwrap()
            .is_none());
        state.record(snapshot, config, 200);
        assert!(!state.pending());
        assert!(poll_fn(|cx| state.poll(cx)).now_or_never().is_none());
    }
}
