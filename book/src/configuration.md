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
values below are the defaults:

```toml
[editor.recovery]
enable = false
directories = [".", "~/tmp", "/var/tmp", "/tmp"]
suffix = ".swp"
size-threshold = 0
update-count = 200
update-time = 4
```

| Key | Description |
| --- | --- |
| `enable` | Enable automatic preservation and recovery notifications. |
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
creation, not just whether a directory exists. Helix does not create missing
directories. Temporary directories may be cleared on reboot or by the operating
system; choose a persistent directory if you need longer retention.

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
These are editor commands, not new command-line flags.

Recovery refuses modified or readonly targets, pending saves or on-save jobs,
and pending recovery I/O. Wait for pending work to finish before retrying.
Recovery restores the snapshot's text, encoding, BOM, line ending, and selections
in normal mode. Text and selections are restored as one undoable change; encoding,
BOM and line-ending preferences remain buffer settings and are not undo-tracked.
It restores neither the old undo tree nor the old session. Nothing is written to
the original file or the selected swap.
Delay and focus-loss autosave are inhibited for the recovered buffer until an
explicit save or a subsequent real edit. Review the result before `:write`.

Helix removes its own swaps after a successful intentional buffer close or exit,
including deliberate forced closes such as `:q!`. Merely closing one split does
not discard the buffer's recovery file. Crashes and signal-driven exits retain
swaps. Recovery does not adopt or delete the old swap: remove it manually once
you no longer need it, even after saving or deliberately quitting the new session.
Preserved scratch buffers stay open when switching files, even after undoing back
to empty; close them explicitly when you no longer need them.

### Durability and privacy

Snapshots are flushed and published using an atomic rename, with directory
synchronization on Unix. These measures do not guarantee survival of hardware
failure, power loss, or reboot-time cleanup. Swap files contain potentially
sensitive document text. New swaps use mode `0600` on Unix; select directories
you trust. Equivalent ownership or ACL protection is not promised on Windows.

Recovery reaches the last completed snapshot, not necessarily the last keystroke.
Slow storage can delay completion; full snapshots of large buffers also cost I/O.
