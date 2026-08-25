# z4j-taskiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiq.svg)](https://pypi.org/project/z4j-taskiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiq.svg)](https://pypi.org/project/z4j-taskiq/)
[![License](https://img.shields.io/pypi/l/z4j-taskiq.svg)](https://github.com/z4jdev/z4j-taskiq/blob/main/LICENSE)

The TaskIQ engine adapter for [z4j](https://z4j.com).

Streams supported TaskIQ task lifecycle events from your async workers to
z4j and accepts operator control actions from the dashboard.
Pair with z4j-taskiqscheduler to surface taskiq-scheduler periodic jobs.

## Compatibility

- TaskIQ 0.11+ and <1 (capped below the eventual TaskIQ 1.0 breaking-major)
- Python 3.11+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| Task lifecycle events | received, started, succeeded, retried, failed |
| Task discovery | runtime broker task registry (`broker.get_all_tasks()`) |
| Submit task | enqueue a registered task against the TaskIQ broker via `.kiq()`; an omitted queue or z4j's logical `default` queue uses the broker default |
| Reconcile task | via the configured TaskIQ result backend |

Async-native, uses TaskIQ's middleware hook system.

TaskIQ network brokers and result backends belong to the event loop that
started them, while the z4j agent runs on its own background loop. Attach the
middleware before broker startup so the adapter records the broker's owner
loop and marshals submit and reconcile calls back to it:

```python
adapter = TaskiqEngineAdapter(broker=broker)
attach_to_broker(broker, adapter=adapter)
```

Attach before the TaskIQ CLI or your application lifespan starts the broker;
that host owns broker startup and shutdown. Do not start an already host-managed
broker a second time. A standalone manual client must start and shut down its
broker exactly once in its own lifecycle.

If installation itself runs inside a TaskIQ startup callback without the
middleware, pass `broker_loop=asyncio.get_running_loop()` explicitly. Do not
capture a temporary loop created by `asyncio.run()`; that loop closes when the
call returns. Until either startup binding or explicit binding has happened,
submit and result-backend probes fail closed; they never guess that the z4j
agent's background loop owns the broker.

Control actions beyond submit (retry, cancel, bulk retry, purge queue)
are not yet supported. TaskIQ's broker-agnostic design means each broker
needs its own implementation, so the dashboard greys these actions out
for TaskIQ engines until per-broker support lands.

TaskIQ has no portable named-queue, ETA, or priority override. Submit accepts
an omitted queue and the canonical logical queue `default`, both of which use
the broker's configured default. Any other queue name, ETA, or priority is
rejected before enqueue rather than silently misrouted.

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

- Lifecycle-capture failures are isolated from TaskIQ middleware and task code;
  capture hooks make no brain network request inline.
- The in-process event queue and SQLite outbound buffer are bounded. Queue
  overflow drops new events and buffer pressure evicts oldest rows; both losses
  are logged.

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
