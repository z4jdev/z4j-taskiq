# z4j-taskiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiq.svg)](https://pypi.org/project/z4j-taskiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiq.svg)](https://pypi.org/project/z4j-taskiq/)
[![License](https://img.shields.io/pypi/l/z4j-taskiq.svg)](https://github.com/z4jdev/z4j-taskiq/blob/main/LICENSE)


z4j queue-engine adapter for [taskiq](https://github.com/taskiq-python/taskiq).

```python
from taskiq_redis import RedisStreamBroker, RedisAsyncResultBackend
from z4j_taskiq import TaskiqEngineAdapter

broker = RedisStreamBroker(url="redis://redis:6379/0")
broker = broker.with_result_backend(
    RedisAsyncResultBackend(redis_url="redis://redis:6379/1"),
)

# In your z4j-bare bootstrap:
from z4j_bare import install_agent
install_agent(engines=[TaskiqEngineAdapter(broker=broker)])
```

## Capabilities

- ✅ Task discovery (every `@broker.task`)
- ✅ Result-backend reconciliation (`reconcile_task`) - reads
  `broker.result_backend` to classify task state.
- ❌ `cancel_task` - taskiq has no native cancel primitive across
  brokers; deferred to v1.1.
- ❌ `retry_task` - same; needs the original message to re-kick.
- ❌ `bulk_retry`, `purge_queue`, `restart_worker`, `rate_limit` -
  no broker-agnostic primitive.

## Periodic tasks

taskiq has a separate `taskiq.scheduler.TaskiqScheduler` package
for cron schedules. Pair this adapter with `z4j-taskiqscheduler`
to surface them on the Schedules page.

Apache 2.0.

## License

Apache 2.0 - see [LICENSE](LICENSE). This package is deliberately permissively licensed so that proprietary Django / Flask / FastAPI applications can import it without any license concerns.

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Source: <https://github.com/z4jdev/z4j-taskiq>
- Issues: <https://github.com/z4jdev/z4j-taskiq/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
