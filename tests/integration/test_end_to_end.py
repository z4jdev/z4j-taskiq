"""End-to-end smoke for z4j-taskiq.

Uses the InMemoryBroker (taskiq's built-in test broker) to run a
full pipeline: middleware → submit_task → execute → reconcile.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("taskiq")

pytestmark = pytest.mark.integration

from taskiq import InMemoryBroker  # noqa: E402

from z4j_core.models.event import EventKind  # noqa: E402

from z4j_taskiq import TaskiqEngineAdapter, attach_to_broker  # noqa: E402


@pytest.fixture
async def broker_with_middleware():
    broker = InMemoryBroker()

    @broker.task
    async def add(x, y):
        return x + y

    adapter = TaskiqEngineAdapter(broker=broker)
    attach_to_broker(broker, adapter=adapter)
    await broker.startup()
    try:
        yield broker, adapter, add
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_full_lifecycle(broker_with_middleware):
    broker, adapter, add = broker_with_middleware

    # Discovery.
    defs = await adapter.discover_tasks()
    names = {d.name for d in defs}
    assert any("add" in n for n in names)

    # Submit via universal primitive.
    submit_name = next(n for n in names if "add" in n)
    res = await adapter.submit_task(submit_name, args=(2, 3))
    assert res.status == "success"
    submitted_id = res.result["task_id"]

    # Wait for the in-memory broker to run the task end-to-end so
    # the middleware fires its hooks.
    sent = await add.kiq(4, 5)
    result = await sent.wait_result(timeout=2)
    assert result.return_value == 9

    # Drain captured events.
    await asyncio.sleep(0.05)
    kinds: list[EventKind] = []
    while not adapter._event_queue.empty():
        kinds.append(adapter._event_queue.get_nowait().kind)
    assert EventKind.TASK_RECEIVED in kinds
    assert EventKind.TASK_SUCCEEDED in kinds

    # Reconcile the completed task - InMemoryBroker stores results.
    reconcile = await adapter.reconcile_task(sent.task_id)
    assert reconcile.status == "success"
    assert reconcile.result["engine_state"] == "success"

    # The InMemoryBroker auto-runs kicked tasks synchronously so the
    # submitted task may already be in the result store; accept any
    # of the canonical states.
    reconcile2 = await adapter.reconcile_task(submitted_id)
    assert reconcile2.status == "success"
    assert reconcile2.result["engine_state"] in {"pending", "success", "unknown"}
