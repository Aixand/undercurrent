# Maintain repository checks and reporting routes

Artificial Intelligence Advancement Initiative Inc. maintains Undercurrent.
The [participation policy](../../CONTRIBUTING.md),
[security policy](../../SECURITY.md), and [code of conduct](../../CODE_OF_CONDUCT.md)
describe the project's reporting and community requirements. Issues are welcome;
external pull requests are not currently accepted.

The [support guide](../../SUPPORT.md) names public help channels, and the
[changelog](../../CHANGELOG.md) records user-visible changes.

## Maintain shared tooling

The installation helper uses revision 2, and the documentation follows standard
0.1.0. Their contracts are recorded in the [helper revision metadata](../../scripts/dev/template-revision.json)
and [documentation standards](../_standards/README.md).
Review updates alongside product-specific adapters, notices, and reader paths.
Run the [repository checks](verification.md) after updating shared tooling.

## Repository files and secret scanning

Follow the [repository file policy](../../REPOSITORY_FILES.md). Its local commit
hook checks the complete Git index; CI also checks every introduced commit.
Coding-assistant entry points stay ignored. Installable product assets under
`docs/agent/skills/` remain allowed. Run its checker and regression tests from
the repository root:

```sh
python3 scripts/checks/check-repository-files.py
python3 tests/tooling/checks/repository_files_test.py
```

Both commands should exit zero. The hook is installed per clone and preserves
custom hook managers.

The [Secret scan workflow](../../.github/workflows/secret-scan.yml) verifies its
pinned Gitleaks archive and scans all fetched history with default rules and
full redaction. Repository configuration and inline suppression comments cannot
disable findings. The initial public history has no finding exceptions. New
findings require investigation; do not suppress a whole path or commit.

The [dependency advisory workflow](../../.github/workflows/dependency-advisories.yml)
checks the locked Rust graph against RustSec on pull requests, pushes, and a
weekly schedule. Keep Dependabot security-update PRs enabled. Review newly
reported advisories for the selected features and call paths, update affected
dependencies, and refresh the license inventory before accepting changes.

## Main branch protection

Repository administrators should keep the following protection on `main`:

- Use pull requests for maintainer changes. External pull requests are not accepted.
- After successful trial runs, require **Secret scan**,
  **Repository file policy**, **Documentation checks**, **License inventory and
  packaging**, **Dependency advisories**, and all four **Product checks (OS)** matrix jobs.
- Require the branch to be up to date before merging.
- Apply protection to administrators as well as other maintainers.
- Prevent force pushes and branch deletion.

Maintainers review changes, verification evidence, and third-party licensing
before merging. Choose the checks appropriate to each change.

Choose implementation checks using the [verification guide](verification.md); the
[native platform workflow](platform-testing.md) runs for diagnostic tags and
version-tag releases. It
checks that the tagged commit is on `main` before starting native tests.
The [product checks workflow](../../.github/workflows/product-checks.yml) runs
formatting, lint, Rust/Python tests, and Linux shell contracts on pull requests
and pushes to `main`. The separate
[product installer workflow](../../.github/workflows/product-installer.yml) runs
full packaging and installation qualification for releases and manual rehearsals.
Changes to workflows and protection settings require particular
care because they control this enforcement.

GitHub repository settings are separate from the checkout. After changing them,
verify the protected branch and required check in GitHub, including the check's
GitHub Actions source. Do not silently remove protection to merge a failing PR.

The [settings configuration](../../.github/open-source-settings.json) records
repository identity, the maintainer profile, project check names, and security
preferences. It is configuration for maintainer tooling; this repository does
not include a command that applies it to GitHub.

A repository administrator can configure the settings directly in GitHub:

1. Set the default Actions token to read-only permissions. Workflows must request
   any additional permissions explicitly.
2. Land the checked-in workflows on `main` and run a trial pull request.
   Confirm each required check succeeds before adding it to branch protection.
3. Apply the protection requirements above. Match the exact check names in the
   workflows and settings configuration, with GitHub Actions as their source.
   Preserve other protections unless a maintainer deliberately changes them.
4. Keep Dependabot security-update pull requests enabled. Check which security
   features are available for the repository's visibility and account.
5. Verify the resulting settings and protection in GitHub. Recheck security
   features after changing repository visibility.

A local test pass does not supply a successful GitHub Actions run. Editing the
settings configuration alone does not change remote settings or visibility.

## Private reports and release readiness

Security, conduct, and licensing concerns are routed to
[corporate@aixand.org](mailto:corporate@aixand.org). Keep that inbox monitored and
update all policy files and issue-template links if it changes.

Keep email available for private vulnerability reports. For a public
repository, also enable GitHub's private vulnerability reporting. Verify that
the reporting form is available before adding its link to `SECURITY.md` and
the issue chooser. See [GitHub's configuration guide](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/configure-vulnerability-reporting/configure-for-a-repository).

Before publishing a release, complete the
[third-party licensing review](releases.md#review-licenses-before-distribution)
and review the support policy in `SECURITY.md`, documenting any additional
maintained release lines. These repository and community files establish the
workflow; they do not certify all future
distributions or dependency changes.

[Development guides](README.md) · [Issues and pull requests](../../CONTRIBUTING.md)
