# Configuration

To override global configuration parameters, create a `config.toml` file located in your config directory:

- Linux and Mac: `~/.config/helix/config.toml`
- Windows: `%AppData%\helix\config.toml`

> 💡 You can easily open the config file by typing `:config-open` within Helix normal mode.

Example config:

```toml
theme = "onedark"

[editor]
line-number = "relative"
mouse = false

[editor.cursor-shape]
insert = "bar"
normal = "block"
select = "underline"

[editor.file-picker]
hidden = false
```

You can use a custom configuration file by specifying it with the `-c` or
`--config` command line argument, for example `hx -c path/to/custom-config.toml`.
You can reload the config file by issuing the `:config-reload` command. Alternatively, on Unix operating systems, you can reload it by sending the USR1
signal to the Helix process, such as by using the command `pkill -USR1 hx`.

Finally, you can have a `config.toml` and a `languages.toml` local to a project by putting it under a `.helix` directory in your repository.
Its settings will be merged with the configuration directory and the built-in configuration.

## Crash recovery

Crash recovery is opt-in. It preserves full-buffer snapshots in separate swap
files without writing the original files. Both named files and unnamed scratch
buffers are supported. To enable it, set `enable = true` in this section; the
values below are the defaults on an XDG platform without a custom state directory:

```toml
[editor.recovery]
enable = false
keep-recovered = false
directories = [".", "~/.local/state/helix/recovery", "/var/tmp", "/tmp"]
suffix = ".swp"
size-threshold = 0
update-count = 200
update-time = 4
```

| Key | Description |
| --- | --- |
| `enable` | Enable automatic preservation and recovery notifications. |
| `keep-recovered` | Always retain the original recovered snapshot's backup after close. When `false`, remove it only after recovered work is successfully written and the buffer intentionally closed. |
| `directories` | Ordered locations to try when creating a swap file. |
| `suffix` | Swap filename suffix; not a directory or path. |
| `size-threshold` | Maximum current buffer size in UTF-8 bytes; `0` is unlimited. |
| `update-count` | Positive number of inserted plus deleted Unicode characters between snapshots. |
| `update-time` | Positive maximum interval in seconds from the first pending change, not an idle timeout. |

The first real edit schedules preservation immediately, including in insert mode.
Subsequent edits are preserved when either the count or time threshold is met.
Replacing text counts both the removed and inserted characters, not their UTF-8
bytes. A snapshot contains the full text, encoding, byte-order mark (BOM), line
ending, and cursor/selections. It does not contain undo history or editor session
state, and it is not a version history or a substitute for backups.

For a named buffer, `.` means the resolved original file's directory. For an unnamed
buffer, it means the working directory captured when that buffer was created.
Other relative directory entries are resolved against that captured working
directory; `~` expands to your home directory. Fallback checks actual file
creation, not just whether a directory exists. The default second entry is resolved
from Helix's platform state directory, not a literal fixed home-directory path:
on XDG platforms it is `$XDG_STATE_HOME/helix/recovery`, or
`~/.local/state/helix/recovery` when unset. On platforms without a state-directory
location, Helix uses `helix/state/recovery` under the platform cache directory.
Leave `directories` unset to use the platform-resolved defaults.

Helix lazily creates only its own resolved state recovery directory when needed
for a write, with private permissions (`0700` on Unix). Discovery and claiming
snapshots do not create it. Other missing configured directories are not created.
Temporary directories may be cleared on reboot or by the operating system; choose
a persistent directory if you need longer retention.

If a buffer exceeds `size-threshold`, Helix warns and retains its previous
snapshot rather than replacing it with oversized content. The limit measures
the current text's UTF-8 bytes, regardless of the original file encoding.
Preservation errors appear in the status line. Retry after a new real edit or
with `:preserve` after correcting the problem.

### Preserving and recovering

`:preserve` (alias `:pre`) immediately preserves the current buffer and reports
the swap path. It also works with automatic recovery disabled, respects the size
limit, and never writes the original file.

`:recover` (alias `:rec`) recovers the current file when exactly one matching
snapshot exists. You can also use `:recover <original-file>` or
`:recover <swap-file>`; an explicit swap path also supports unnamed buffers.
Quote paths containing spaces. If several snapshots match, Helix reports their
exact filenames: choose one explicitly rather than relying on automatic selection.
Retained backups can also be read with `:recover <backup-file>`, but are never
offered by automatic discovery. These are editor commands, not new command-line
flags.

Recovery refuses modified or readonly targets, pending saves or on-save jobs,
and pending recovery I/O. Wait for pending work to finish before retrying.
If the original file's modification time is newer than the snapshot timestamp,
Helix asks `Warning: Recovery file is older than the saved file! Continue? (y/N)`.
Only `y` or `yes` (case-insensitive) accepts; Enter, `n`, or Escape cancels without
opening, switching, or changing a buffer. The snapshot stays claimed while the
prompt is open. Before applying it, Helix rechecks the current view and buffer,
target state, pending work, and disk metadata; a change while waiting requires
retrying recovery. Snapshot timestamps have whole-second precision, so an equal
modification-time second does not prompt, but the disk recheck uses full precision.
Recovery details, including UTC snapshot time and retention policy, are also
written to the Helix log. Use `:log-open` to read the complete report if a popup
is truncated on a small terminal.

Recovery restores the snapshot's text, encoding, BOM, line ending, and selections
in normal mode. Text and selections are restored as one undoable change; encoding,
BOM and line-ending preferences remain buffer settings and are not undo-tracked.
It restores neither the old undo tree nor the old session. Recovery writes nothing
to the original file or the selected swap and unlinks nothing. Only after successful
recovery does the buffer adopt the selected swap for subsequent preservation and
cleanup. Active swaps use the `.helix-recovery-` filename prefix and the configured
suffix. Before overwriting an adopted swap, or removing an unwritten recovery on
close, Helix archives the original snapshot byte-for-byte as a separate immutable
`.helix-recovered-*.recovered` backup. This copy is lazy: recovery itself does not
create it. The backup preserves the originally recovered bytes, not later edits.
An explicitly recovered backup stays separate from the new active swap created
on subsequent preservation, even if the active suffix is also `.recovered`.
Helix updates or removes only files it owns, leaving unselected orphan snapshots
alone.
Delay and focus-loss autosave are inhibited for the recovered buffer until an
explicit save or a subsequent real edit. Review the result before `:write`.
The recovery report shows the snapshot and original paths, a human-readable UTC
timestamp, text size, disk size/mtime comparison, restored state, and the actual
backup retention policy. Matching metadata does not verify matching content;
missing or unavailable metadata is reported as unknown. Nothing is written to
the original file by recovery.

Helix removes active swaps after a successful intentional buffer close or exit.
For a recovered buffer that has not been successfully written, even `:q!` retains
the original immutable backup while cleaning up the active swap. With
`keep-recovered = false`, successfully writing recovered work and then intentionally
closing removes both active swap and backup. With `keep-recovered = true`, the
backup remains even after a successful save and close. Undoing back before recovery
and closing conservatively retains the backup; saving that older revision does not
authorize backup deletion. Retained paths are shown in a warning and printed to
standard error after the terminal is restored on exit.

Ordinary buffers that were not recovered still have their active swaps cleaned up
by `:q!`. Merely closing one split does not discard a buffer's recovery files.
Crashes and signal-driven exits retain swaps; failed saves and refused closes keep
the recovered source or its original backup too.
Preserved scratch buffers stay open when switching files, even after undoing back
to empty; close them explicitly when you no longer need them. Recovery does not
provide persistent undo history.

### Durability and privacy

Published swaps are exclusively locked by their owner. Other updated Helix instances
can read and discover them, but cannot claim them while they are owned. These locks
are cooperative: exit older Helix instances before upgrading, since older versions
do not participate in locking.

Snapshots are flushed and published using an atomic rename, with directory
synchronization on Unix. These measures do not guarantee survival of hardware
failure, power loss, or reboot-time cleanup. Swap files contain potentially
sensitive document text, as do retained backups. New swaps and backups use mode
`0600` on Unix; select directories you trust. Equivalent ownership or ACL protection
is not promised on Windows.

Recovery reaches the last completed snapshot, not necessarily the last keystroke.
Slow storage can delay completion; full snapshots of large buffers also cost I/O.
