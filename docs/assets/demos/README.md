# Terminal demos

Short recordings of real CLI and SDK workflows. Each tape runs against a new
local daemon with its own state. The five sample-process recordings need no
external services or credentials.

The pane and detach recordings use the default **Alt+U** leader.

| Recording | What it demonstrates | Source |
| --- | --- | --- |
| [npm server health](server-health.gif) · [still image](server-health-poster.png) | Request error while healthy → dependency failure → unhealthy → recovery with retained errors | [server-health.tape](server-health.tape), [sample server](fixtures/server/server.mjs) |
| [CLI overview](overview.gif) · [still image](overview-poster.png) | Start → attach → detach → check progress → reattach → stop | [overview.tape](overview.tape) |
| [Python control](python-control.gif) · [still image](python-control-poster.png) | Start → send input → read application acknowledgement → stop | [python-control.tape](python-control.tape), [control.py](fixtures/control.py) |
| [Wait for interaction](interaction.gif) · [still image](interaction-poster.png) | Observe a confirmation request → send an answer → verify the import | [interaction.tape](interaction.tape), [approval.py](fixtures/approval.py) |
| [Coordinate jobs](orchestration.gif) · [still image](orchestration-poster.png) | Run two jobs concurrently → check both exits → start a dependent report | [orchestration.tape](orchestration.tape), [orchestrate.py](fixtures/orchestrate.py) |

## Check an npm server and find errors

The recording starts in a managed command shell and opens the npm server in a
second native pane. Commands stay on the left; the server's live output stays on
the right. The [sample package](fixtures/server/package.json) runs a small Node.js
HTTP API with `npm start`; there are no dependencies to install.

The runner supplies `PORT` and `APP` for a temporary localhost listener. The
visible launch checks the service's health endpoint:

```sh
uc start api --healthy-when "http:$APP/health" -- npm start
```

| Step | What the recording shows |
| --- | --- |
| `uc wait api --health healthy --timeout 15s`, then `uc ls` | The HTTP check passes; the server is running and healthy. |
| `curl -sS "$APP/orders"` | An actual successful API response. |
| `curl -sS "$APP/orders?demo_error=1"`, then `curl -sS "$APP/orders"` | A deliberately failed request returns HTTP 500; the next ordinary request succeeds. |
| `uc ls`, then `uc logs api --max-bytes 4096` | The service remains **healthy** despite the recorded request error. |
| `curl -sS -X POST "$APP/demo/fail"`, then `curl -sS "$APP/orders"` | A simulated essential dependency failure makes orders return HTTP 503. |
| `uc wait api --health unhealthy --timeout 15s`, then `uc ls` | The process remains **running**, but `/health` now returns 503 and health is **unhealthy**. |
| `curl -sS -X POST "$APP/demo/recover"`, then `uc wait api --health healthy --timeout 15s` | Restore the sample dependency and wait for **healthy**. |
| `curl -sS "$APP/orders"`, then `uc ls` and `uc logs api --max-bytes 4096` | Orders succeed again; the same healthy process retains both earlier errors. |
| `uc stop api` | Stop the npm session and close its pane. |

The HTTP rule requires status **200**. The sample's health endpoint reflects
whether its essential inventory dependency is available. An isolated failed
request does not change that condition. Logs retain what happened; they do not
determine health in this recording. A running process can be unable to serve
its intended work, and a recovered service can still have errors in its history.
See [health-rule syntax](../../reference/cli-commands.md#health-rules) and
[status and output](../../guides/cli/status-and-output.md) for the full behavior.

With Node.js 22+, npm, curl, and the [recording tools](#record-locally) installed:

```sh
python3 scripts/docs/render_demos.py server-health --bin-dir target/debug
```

The runner copies the fixture into a temporary workspace, selects a local port,
verifies HTTP responses and fresh daemon health observations at all four stages,
and checks successful requests and retained errors. It stops the API and command
shell and removes successful temporary state. The sample server also has a
three-minute maximum lifetime.

## Wait for interaction

The sample importer previews four rows from [orders.csv](fixtures/orders.csv)
and waits for `yes` before writing `imported.json`. The runner installs
[ask.yaml](fixtures/ask.yaml) in the temporary home's `profiles/` directory
before starting the daemon. The tape selects that profile explicitly.

`uc wait review --interaction requested --timeout 15s` observes the prompt.
`uc send review yes --raw --newline` supplies the answer; a successful exit and
the expected output file establish that the import finished. The profile only
recognizes this sample's prompt. Adapt and verify the patterns for your program
using the [profile guide](../../guides/configuration/profiles.md).

## Coordinate jobs

The [coordinator](fixtures/orchestrate.py) starts two real
[CSV jobs](fixtures/job.py) through one Python SDK client: `check` validates the
input, and `total` calculates its value. A bounded fixture barrier holds both
processes until the coordinator observes them running. A three-second display
pause lets the viewer read that state.

After both sessions exit with code zero, the coordinator launches `report`,
which reads their JSON artifacts and writes `report.txt`. The runner independently
recomputes the expected report, checks overlapping process lifetimes, and checks
that the report started after both prerequisites finished. No deployment or
external action takes place.

The Python application owns dependency ordering and success criteria;
Undercurrent supplies process supervision, state waits, and retained output.
An unsuccessful prerequisite prevents the report step. A failed or interrupted
recording leaves session cleanup to the runner. See
[client sharing](../../sdk/python/operations.md#share-and-close) and
[wait semantics](../../sdk/python/state.md) when adapting the example.

## Record locally

Use Linux or macOS with the repository's [build prerequisites](../../development/setup.md).
The recording tools are [VHS](https://github.com/charmbracelet/vhs), `ttyd`,
FFmpeg (including `ffprobe`), Chromium, Bash, Git, Python 3.11+, and the
**Noto Sans Mono** font. The recordings were verified on Linux with VHS 0.11.0.
The renderer uses the Python SDK directly from this checkout.
The server recording also needs **Node.js 22+, npm, and curl**; it makes only
localhost HTTP requests and needs no `npm install`.

From the repository root, build the matching CLI, daemon, process host, and bridge:

```sh
cargo build --locked -p undercurrent-cli -p undercurrent-daemon -p undercurrent-python-bridge --bins
```

Then record the five local sample-process demos:

```sh
python3 scripts/docs/render_demos.py all --bin-dir target/debug
```

Use `overview`, `python-control`, `interaction`, `orchestration`, or `server-health` instead of
`all` for one recording.
For a custom Cargo target directory, point `--bin-dir` at its `debug` directory.
If Chromium is not on `PATH`, add `--browser /path/to/chromium`.
Use `--output-dir /path/to/preview` to review an alternate take before replacing
the checked-in assets. No output is uploaded.

## What the runner checks

The [runner](../../../scripts/docs/render_demos.py) creates a private temporary
home and workspace, starts the matching daemon, and supplies the SDK bridge.
It verifies:

- **Detach:** the same worker and session remain running, and the worker's
  progress counter increases while the terminal is detached.
- **Input:** the worker receives exactly the expected message. The SDK example
  waits for an application acknowledgement, not just terminal echo.
- **Interaction:** the importer is running with the expected requested-input
  observation before any import file exists; its final file matches the CSV.
- **Server:** the same process stays healthy after an isolated request error,
  becomes unhealthy during an essential dependency failure, and recovers to
  healthy. Fresh HTTP checks match each stage; ordinary requests succeed before
  and after the isolated error and after recovery. Both errors remain in the logs.
  After stopping the session, its localhost listener refuses connections.
- **Orchestration:** both independent jobs run, both prerequisites succeed,
  and the dependent job produces the expected report in the right order.
- **Completion:** sample processes reach `stopped` or a successful `exited` state.
  Each GIF contains multiple frames and at least ten seconds of footage.

It stops remaining sessions in that temporary home and stops its daemon on both
success and failure. Successful runs save the GIF and poster after
verification and remove temporary state. Logs, output checkpoints, and a JSON
report remain under `target/vhs-demos/`.

Failed runs preserve their temporary directory for diagnosis. Its path and any
cleanup failure appear in `report.json`; resolve a cleanup failure before removing
that directory. The renderer never selects your usual Undercurrent home.

## Maintain the footage

Edit the tapes and [shared theme](theme.tape), then use the runner again.
Review the animation and static poster at README width: commands should remain
readable, tables should fit, and pauses should leave time to understand each step.
Keep each recording focused on one workflow and put setup instructions in the
linked guide. Preserve the GIF's text equivalent and still-image link on its page.

[setup.tape](setup.tape) requires a marker supplied by the runner. Do not invoke
the tapes directly: the runner owns daemon setup, isolation, and cleanup.
Hidden tape commands only prepare the display or collect verification evidence;
the visible CLI and SDK output comes from the running product.

The sample programs, [output helper](fixtures/demo_helpers.py), and
[checkpoint collector](fixtures/checkpoint.py) are recording fixtures. Their
input/barrier waits are bounded; the original worker limits itself to two minutes.
The runner handles failure cleanup. For application
integration, use the [Python quickstart](../../sdk/python/quickstart.md) and its
maintained examples, including their lifecycle and error handling.

[Documentation assets](../README.md)
