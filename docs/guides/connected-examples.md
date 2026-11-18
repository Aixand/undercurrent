# Prepare connected workflow examples

These walkthroughs combine current public APIs. The application owns workflow decisions; Undercurrent owns processes, Carrier owns stored messages, and each manager owns its resources. No model account is needed for the API worker or CSV audit.

## Prepare the tools

[Install Undercurrent](https://projects.aixand.org/undercurrent/docs/installation/) including its matching Python bridge. Use Python 3.11+ (the current manager development range is 3.11–3.14). Hosted CLI installers keep their Python environments private; install SDK dependencies in your own virtual environment.

For a source-based environment, arrange the current checkouts as siblings and run from their parent directory:

```sh
python3 -m venv connected-env
. connected-env/bin/activate
python -m pip install ./aixand-libraries/filesystem ./carrier ./undercurrent/python \
  ./manager-suites/prompt-manager ./manager-suites/skill-manager
```

This uses the current source manifests rather than the archive's package names. The managed Codex example additionally needs `./manager-suites/agent-manager`, provider installation, and authentication; see its separate prerequisites. On Windows, use `connected-env\Scripts\Activate.ps1` and native paths; the shell commands below are for Linux/macOS.

## Create an isolated workspace

Use a fresh directory outside your source repositories. Keep this terminal's environment active for all subsequent commands:

```sh
mkdir connected-demo
cd connected-demo
export UNDERCURRENT_HOME="$PWD/runtime"
uc daemon start
uc daemon status
python -c 'import carrier, undercurrent, prompt_manager, skill_manager'
```

The runtime directory, databases, credentials, and reports stay in this directory. Do not commit them. Copy the same absolute `UNDERCURRENT_HOME` and activate the same Python environment in each additional terminal. The SDK does not start a daemon automatically.

## Choose a workflow

- [Keep Carrier running](https://projects.aixand.org/carrier/examples/persistent-service/): start a named service and preserve its database across restarts.
- [Trigger an HTTP service](https://projects.aixand.org/undercurrent/examples/carrier-api/): receive a request, call a fixed endpoint, and recover its correlated result.
- [Audit a CSV and deliver its report](https://projects.aixand.org/manager-suite/examples/audit-to-inbox/): render a prompt, connect a skill, run its script through Undercurrent, and send a report through Carrier.
- [Let a managed Codex agent call the worker](https://projects.aixand.org/carrier/examples/managed-api-caller/): use the current Carrier adapter with Agent Manager.

## Work with native agents

- [Launch and recover an agent](https://projects.aixand.org/manager-suite/examples/persistent-agents/) with retained execution and input identities.
- [Run two independent reviewers](https://projects.aixand.org/manager-suite/examples/parallel-reviews/) against one project.
- [Coordinate a worker and reviewer](https://projects.aixand.org/carrier/examples/agentic-review/) through Carrier and independently verify their artifact and sign-off.

These paths additionally require Agent Manager and authenticated native providers. Use the current setup in each walkthrough.

## Finish

Stop each walkthrough's worker and service sessions before stopping this dedicated daemon:

```sh
uc ls
uc daemon stop
```

Retain the demo directory to inspect reports, message history, and credentials or to resume work. Closing an SDK connection or timing out a wait does not stop a session. Use each workflow's explicit stop commands before leaving its runtime.
