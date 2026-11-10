# Developer tooling self-tests

Run script self-tests without building or starting the product daemon:

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

The project validator includes these suites in its `tooling` stage. Discover
live product regressions separately with `python3 tests/e2e/run.py --list`.
