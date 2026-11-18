# Change, verify and distribute Undercurrent

Use these reading paths from the source checkout. General owns shared setup and
cross-crate verification. Each crate and Python maintainer category owns local
implementation mechanisms and focused checks.

| Task | Owning guide |
| --- | --- |
| Choose an implementation owner | [Workspace map](workspace-guide.md) |
| Prepare the toolchain | [Development setup](setup.md) |
| Extend a capability | [Extension points](extending.md) |
| Select meaningful evidence | [Verification](verification.md) and [native platforms](platform-testing.md) |
| Find test families | [Test map](testing/suites.md), [architecture gate](testing/architecture.md), [live regressions](testing/end-to-end.md) |
| Maintain developer tools | [Tool map](tooling/developer-tools.md) and [tool self-tests](tooling/self-tests.md) |
| Assemble and publish | [Distribution](distribution/releases.md) and [installer qualification](distribution/installer-validation.md) |
| Maintain repository checks | [Repository maintenance](maintenance/repository-maintenance.md) |
| Write and publish documentation | [Authoring](documentation.md), [coverage](documentation-coverage.md) and [publication](../publication.md) |
| Understand a lasting design choice | [Architecture decisions](../architecture/decisions/README.md) |

[Participation](../../CONTRIBUTING.md) explains the current issue/PR policy.
[Security reports](../../SECURITY.md) use the private reporting route.
