# Developer tooling self-tests

Run from the repository root with Python 3.11 or newer. These self-tests
need no product build or running daemon:

```sh
python3 tests/tooling/run.py
```

Select `--group checks` for repository, documentation, license, release, and
vendor integrity policies, `--group e2e` for
catalog coverage and test selection, or `--group runners` for subprocess
timeouts, interruption, CI logging, and validation reports. Runner tests use
temporary directories and owned short-lived processes; Linux identity tests
and the POSIX CI adapter checks skip on other platforms.

Vendor checks use `scripts/checks/vendor_policy.py`. E2E runner tests use mocks
and temporary directories.

The project validator includes these suites in its `tooling` stage. See the
[live regression guide](../testing/end-to-end.md) for product tests.
