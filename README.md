# z4j-taskiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiq.svg?v=1.6.7)](https://pypi.org/project/z4j-taskiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiq.svg?v=1.6.7)](https://pypi.org/project/z4j-taskiq/)
[![License](https://img.shields.io/pypi/l/z4j-taskiq.svg?v=1.6.7)](https://github.com/z4jdev/z4j-taskiq/blob/main/LICENSE)

The TaskIQ engine adapter for [z4j](https://z4j.com).

Streams every TaskIQ task lifecycle event from your async workers to
z4j and accepts operator control actions from the dashboard.
Pair with z4j-taskiqscheduler to surface taskiq-scheduler periodic jobs.

## Compatibility

- TaskIQ 0.11+ and <1 (capped below the eventual TaskIQ 1.0 breaking-major)
- Python 3.10+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| Task lifecycle events | enqueued, started, succeeded, failed, retried |
| Task discovery | runtime broker-task registry merge + static scan |
| Submit / retry / cancel | direct against the TaskIQ broker |
| Bulk retry | filter-driven; re-enqueues matching tasks |
| Purge queue | with confirm-token guard |
| Reconcile task | via the configured TaskIQ result backend |

Async-native, uses TaskIQ's middleware hook system.

## Install

```bash
pip install z4j-taskiq z4j-taskiqscheduler
```

Pair with a framework adapter:

```bash
pip install z4j-fastapi z4j-taskiq z4j-taskiqscheduler
pip install z4j-bare    z4j-taskiq z4j-taskiqscheduler   # framework-free worker
```

## Pairs with

- [`z4j-taskiqscheduler`](https://github.com/z4jdev/z4j-taskiqscheduler), schedule adapter for taskiq-scheduler

## Reliability

- No exception from the adapter ever propagates back into TaskIQ
  middleware or your task code.
- Events buffer locally when z4j is unreachable; workers never
  block on network I/O.

## Documentation

Full docs at [z4j.dev/engines/taskiq/](https://z4j.dev/engines/taskiq/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-taskiq/
- Issues: https://github.com/z4jdev/z4j-taskiq/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
