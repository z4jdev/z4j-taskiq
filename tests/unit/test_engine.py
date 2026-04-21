"""TaskiqEngineAdapter tests against an InMemoryBroker."""

from __future__ import annotations

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker  # noqa: E402

from z4j_taskiq import TaskiqEngineAdapter  # noqa: E402


@pytest.fixture
def broker():
    b = InMemoryBroker()

    @b.task
    async def add(x, y):
        return x + y

    @b.task
    async def boom():
        raise RuntimeError("nope")

    return b


@pytest.fixture
def adapter(broker):
    return TaskiqEngineAdapter(broker=broker)


@pytest.mark.asyncio
async def test_capabilities_unified_in_v1(adapter):
    # v1.0: only the universal submit_task primitive is advertised.
    # Brain polyfills retry / bulk_retry on top of it. Per-broker
    # cancel and other actions land in v1.1.
    assert adapter.capabilities() == {"submit_task"}


@pytest.mark.asyncio
async def test_discover_lists_registered_tasks(adapter):
    defs = await adapter.discover_tasks()
    names = {d.name for d in defs}
    assert any("add" in n for n in names)
    assert any("boom" in n for n in names)
    assert all(d.engine == "taskiq" for d in defs)


@pytest.mark.asyncio
async def test_list_queues_empty(adapter):
    assert await adapter.list_queues() == []


@pytest.mark.asyncio
async def test_list_workers_empty(adapter):
    assert await adapter.list_workers() == []


@pytest.mark.asyncio
async def test_reconcile_unknown_task(adapter):
    res = await adapter.reconcile_task("nonexistent")
    assert res.status == "success"
    # InMemoryBroker has a result backend but no row for unknown id
    # → "pending" (not_ready) is the canonical state.
    assert res.result["engine_state"] in ("pending", "unknown")


@pytest.mark.asyncio
async def test_reconcile_completed_task_returns_success(broker, adapter):
    # Kick the task and wait for it to complete via the in-memory backend.
    add = broker.find_task("add") or list(broker.get_all_tasks().values())[0]
    sent = await add.kiq(2, 3)
    result = await sent.wait_result(timeout=2)
    assert result.return_value == 5

    res = await adapter.reconcile_task(sent.task_id)
    assert res.status == "success"
    assert res.result["engine_state"] == "success"


@pytest.mark.asyncio
async def test_unsupported_actions_return_failed(adapter):
    for coro in [
        adapter.retry_task("id"),
        adapter.cancel_task("id"),
        adapter.bulk_retry({}),
        adapter.purge_queue("q"),
        adapter.requeue_dead_letter("id"),
        adapter.rate_limit("t", "5/s"),
        adapter.restart_worker("w"),
    ]:
        res = await coro
        assert res.status == "failed"
