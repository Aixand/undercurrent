# Security policy

## Report a vulnerability privately

Email [corporate@aixand.org](mailto:corporate@aixand.org) with the subject
`Undercurrent security report`. Artificial Intelligence Advancement Initiative
Inc. maintains Undercurrent and receives reports at this address.
If the repository offers GitHub's private vulnerability reporting, you may
use that channel instead. Email remains available.

Do not report a suspected vulnerability through a public issue, discussion, or
pull request. Ordinary bug-report requirements for a support archive do not
apply to security reports. Send only the information needed to explain the
problem; remove credentials, personal information, and unrelated terminal
content. If reproduction requires sensitive material, first ask how to share
it safely instead of attaching it to the initial email.

Include, when available:

- The affected version or source commit, operating system, and configuration.
- The affected component and the potential security impact.
- Minimal reproduction steps or a proof of concept, using synthetic data.
- Any suggested fix and your preferred contact and credit details.

Maintainers will investigate and coordinate remediation and disclosure with
the reporter. Response and fix timing depend on severity and available
maintainer capacity; this policy does not promise a response deadline or a
bounty. Please coordinate public disclosure so affected users can receive a fix
or mitigation. Credit is optional and should use the reporter's preferred name.
Sensitive details are shared only as needed to investigate and fix the issue;
absolute confidentiality cannot be guaranteed.

## Supported versions

Security reports are welcome for any version of Undercurrent. Security fixes
target the current release and current `main` development branch. Earlier
releases and development snapshots are unsupported and receive no backports.
Maintainers may ask you to reproduce an issue on the current build.

Include the output of `uc --version` and, for a source build, the source commit
in your report. Any additional maintained release lines or changes to this
support policy will be documented here.

## Other reports

Use the [issue templates](https://github.com/Aixand/undercurrent/issues/new/choose)
for ordinary bugs and usage questions. See [support guidance](SUPPORT.md)
for diagnostics and [community conduct](CODE_OF_CONDUCT.md) for conduct reports.
The [support guide](SUPPORT.md) summarizes public support channels.
