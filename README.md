# z4j-taskiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiq.svg?v=1.8.0)](https://pypi.org/project/z4j-taskiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiq.svg?v=1.8.0)](https://pypi.org/project/z4j-taskiq/)
[![License](https://img.shields.io/pypi/l/z4j-taskiq.svg?v=1.8.0)](https://github.com/z4jdev/z4j-taskiq/blob/main/LICENSE)

The TaskIQ engine adapter for [z4j](https://z4j.com).

Streams every TaskIQ task lifecycle event from your async workers to
z4j and accepts operator control actions from the dashboard.
Pair with z4j-taskiqscheduler to surface taskiq-scheduler periodic jobs.

## Compatibility

- TaskIQ 0.11+ and <1 (capped below the eventual TaskIQ 1.0 breaking-major)
- Python 3.11+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| Task lifecycle events | enqueued, started, succeeded, failed |
| Task discovery | runtime broker task registry (`broker.get_all_tasks()`) |
| Submit task | enqueue a registered task against the TaskIQ broker via `.kiq()` |
| Reconcile task | via the configured TaskIQ result backend |

Async-native, uses TaskIQ's middleware hook system.

Control actions beyond submit (retry, cancel, bulk retry, purge queue)
are not yet supported. TaskIQ's broker-agnostic design means each broker
needs its own implementation, so the dashboard greys these actions out
for TaskIQ engines until per-broker support lands.

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
