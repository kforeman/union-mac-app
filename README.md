# Union Status — macOS menu bar app

![Screenshot](screenshot.png)

A small Mac app that displays the health of your Union.ai cluster as a
menu bar icon. Built against the Flyte v2 SDK (`flyte` package) and [`rumps`](https://github.com/jaredks/rumps).

## Menu bar icon

The Union.ai "U" logo with a small colored dot in the bottom-right corner:

- 🟢 **green** — most recently finished run succeeded
- 🔵 **blue** — at least one run is in progress
- 🟠 **orange** — most recently finished run was aborted
- 🔴 **red** — most recently finished run failed (or timed out)
- 🟣 **purple** — something is queued / waiting for resources
- ○ — no runs in the selected window

The dot always reflects the *filtered* view (selected projects × selected
time window).

## Menu contents

At the top: **Showing: `{project}/{domain}`** — the scope the app is
reading from (taken from your Union config).

Then: one row per task (workflow function) in the window. Each row shows
the task name plus a strip of up to 10 flat colored dots — the status of
the last N runs, oldest on the left, newest on the right.

- Click a row to jump straight to the **latest** run in the Union v2 console.
- Hover the row to expand a submenu of every run in the group; click any
  run to open just that one.

Below the runs, if any apps are deployed and active in that project/domain:

- **Active apps** — one row per active app. Click the row to open the
  app's exposed public endpoint. Expand the submenu for **Open in Union
  console** to inspect it instead.

Below that:

- **Project ▶** — radio-style list of every `{project}/{domain}` on the
  cluster. Click one to switch scope live; the app re-inits against that
  project and the pick is persisted in `~/.config/union-status/config.json`.
- **Time window ▶** — radio-style picker: last 1h / 6h / 12h / 24h / 3d /
  1w / Ever. A run counts if it's currently in progress *or* it started or
  ended inside the window. Default: 24 hours.
- **Refresh now** — force an immediate poll.
- **Open Union UI** — opens the current project in the v2 console.

At the very bottom: when the data was last refreshed.

Auto-refreshes every 60 seconds.

## Authentication and scope

Credentials, endpoint, and the project/domain scope are all read from
`~/.union/config.yaml` — the same file the `union` CLI uses. No keys or
endpoints live in this repo.

On first launch the app scopes itself to the `task.project` and
`task.domain` set in that config. To switch later, use the **Project ▶**
submenu — the choice is saved in `~/.config/union-status/config.json`
(alongside the time-window pick) and overrides the union-config default on
subsequent launches. Clearing that file reverts to the union-config default.

## Install (one line)

Prereq: [`uv`](https://github.com/astral-sh/uv). If you don't have it:
`curl -LsSf https://astral.sh/uv/install.sh | sh`.

Then:

```bash
curl -fsSL https://raw.githubusercontent.com/kforeman/union-mac-app/main/install.sh | sh
```

That installs the `union-status` CLI into uv's tool env *and* registers a
launchd agent so the menu bar icon appears on login (and after crashes).
Re-run to upgrade. Uninstall with the command printed at the end of the
install output.

If you'd rather skip launchd and just run it foreground:

```bash
uvx --from git+https://github.com/kforeman/union-mac-app union-status
```

## Running from a checkout

```bash
uv run python main.py
```

To run detached so it survives terminal close:

```bash
nohup uv run python main.py >/tmp/union-status.log 2>&1 &
```

## Managing the launchd agent

The one-line installer writes `~/Library/LaunchAgents/com.<user>.union-status.plist`.

```bash
LABEL=com.$(whoami).union-status
PLIST=~/Library/LaunchAgents/$LABEL.plist

# Status (PID in first column when running, '-' when stopped)
launchctl list | grep union-status

# Stop / start without rebooting
launchctl unload $PLIST
launchctl load -w $PLIST

# Tail logs
tail -f ~/Library/Logs/union-status.log
```

Re-running the installer automatically unloads/reloads launchd, so upgrades
take effect within a few seconds.

## Uninstall

```bash
LABEL=com.$(whoami).union-status
launchctl unload ~/Library/LaunchAgents/$LABEL.plist 2>/dev/null || true
rm -f ~/Library/LaunchAgents/$LABEL.plist
uv tool uninstall union-status
```

That removes the launchd agent, the plist, and the `union-status` binary.
Also optional — your persisted preferences (window choice, picked project):

```bash
rm -rf ~/.config/union-status
```

The app never touches `~/.union/config.yaml`, so your Union CLI auth stays
put.

## Tunables

A few knobs live at the top of [main.py](main.py):

- `REFRESH_SECONDS` — poll interval (60s by default)
- `RECENT_LIMIT_PER_DOMAIN` — how many runs to fetch per project/domain per
  refresh (default 100, sorted newest-first)
- `TOTAL_SHOWN` — max task groups displayed in the menu
- `DOT_STRIP_MAX` — max dots per group row
- `TIME_WINDOWS` / `DEFAULT_WINDOW_LABEL` — edit the time-window submenu

## License

MIT — see [LICENSE](LICENSE).
