# Getting help with Undercurrent

Start with the [README](README.md) and [documentation](docs/README.md), then search
[existing issues](https://github.com/Aixand/undercurrent/issues) for the same question or error.

We welcome issues. We are not accepting pull requests because the team lacks
the capacity to review external contributions. See the [participation policy](CONTRIBUTING.md).

Use the [issue forms](https://github.com/Aixand/undercurrent/issues/new/choose) for:

- **Usage questions:** setup, configuration, or how to use the project.
- **Bug reports:** reproducible behavior that differs from the documentation.
- **Documentation:** unclear instructions, missing examples, or broken links.
- **Feature requests:** the problem you want to solve and the expected benefit.

Include the version or source commit, operating system, relevant setup, what you
tried, and the smallest example that explains the issue. Redact credentials,
private file paths, personal information, and confidential data from logs or
screenshots. Public issues are visible to everyone.

Bug reports normally include a reviewed support archive. Follow the
[diagnostic instructions](#required-diagnostics-for-bug-reports)
for collection, privacy review, and alternatives when `uc` cannot run.

Report vulnerabilities privately using [SECURITY.md](SECURITY.md). Report conduct
concerns using [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). For code ownership or
licensing concerns, use the [private reporting route](#report-code-ownership-or-licensing-concerns).

Support is provided by community maintainers as availability permits. Opening an
issue does not create a response-time, resolution, or commercial support
commitment.

## Report code ownership or licensing concerns

AI tools have been used in this project, as disclosed in the
[README](README.md#ai-assisted-development). If you believe the repository
contains proprietary code that we do not have permission to distribute, email
[corporate@aixand.org](mailto:corporate@aixand.org) privately.

Include enough evidence for us to investigate:

- The affected repository file, code block or line numbers, and commit or version.
- The original work, its source, and the specific matching code or other evidence
  supporting your concern.
- The relevant ownership, license terms, or distribution restrictions, and your
  relationship to the rights holder, if applicable.
- A way to contact you for clarification.

Do not post confidential source code in public issues. Send only material you
are authorized to share. If evidence is confidential, describe it first so we
can discuss an appropriate way to review it. No support archive is required.

We do not knowingly include proprietary code without permission. We investigate
credible ownership reports and may request clarification. When the evidence
establishes that we lack distribution rights, we remove or replace the affected
material. For other substantiated licensing problems,
we will take appropriate corrective action, including correcting attribution or
notices. AI assistance does not excuse unauthorized use of someone else's work.

## Required diagnostics for bug reports

Bug reports must include a support archive collected after the failure. Run
the following as the same OS user and with the same `UNDERCURRENT_HOME` as the
failed command, choosing a new output filename in a writable directory:

```sh
uc debug collect --output uc-support.zip
```

If the error printed a `UC-` reference (also available as `incident_id` in JSON
errors), use this command instead, replacing `UC_REFERENCE` with that reference:

```sh
uc debug collect --incident UC_REFERENCE --output uc-support.zip
```

Review the archive's contents and `manifest.json` before attaching it to the
issue. Terminal content and panic backtraces are excluded by default; the
[support-report guide](docs/guides/daemon/diagnostics.md#collect-a-support-report)
explains what is collected and how to review it. Include the original command,
error message and reference if available, approximate failure time and timezone,
reproduction steps, and your OS and Undercurrent version. Remove secrets from
any commands, errors, or extra logs you share.

A partial archive is still useful. Collection can work when the daemon is
stopped or configuration is invalid, and does not start or repair the daemon.
If you cannot collect or share an archive, explain why in the required support
report field and include any collection error. Build or installation failures
that prevent running `uc` should include the failing command and build or
installer output instead. Documentation reports, feature requests, and general
usage questions do not require diagnostics.
