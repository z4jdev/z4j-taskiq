"""taskiq middleware event capture."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker, TaskiqMessage  # noqa: E402

from z4j_core.models.event import EventKind  # noqa: E402

from z4j_taskiq import (  # noqa: E402
    TaskiqEngineAdapter,
    Z4JTaskiqMiddleware,
    attach_to_broker,
)


@pytest.fixture
def broker():
    b = InMemoryBroker()

    @b.task
    async def add(x, y):
        return x + y

    return b


@pytest.fixture
def adapter(broker):
    return TaskiqEngineAdapter(broker=broker)


@pytest.mark.asyncio
async def test_attach_adds_middleware(broker, adapter):
    middleware = attach_to_broker(broker, adapter=adapter)
    assert isinstance(middleware, Z4JTaskiqMiddleware)
    assert middleware in broker.middlewares


@pytest.mark.asyncio
async def test_pre_send_emits_received(adapter):
    mw = Z4JTaskiqMiddleware(queue=adapter._event_queue)
    msg = TaskiqMessage(
        task_id="t1", task_name="myapp.add", labels={}, labels_types={},
        args=[1, 2], kwargs={},
    )
    await mw.pre_send(msg)
    evt = adapter._event_queue.get_nowait()
    assert evt.kind == EventKind.TASK_RECEIVED
    assert evt.task_id == "t1"
    assert evt.engine == "taskiq"


@pytest.mark.asyncio
async def test_post_execute_success_then_failure(adapter):
    mw = Z4JTaskiqMiddleware(queue=adapter._event_queue)
    msg = TaskiqMessage(
        task_id="t2", task_name="myapp.add", labels={}, labels_types={},
        args=[], kwargs={},
    )

    class _OkResult:
        is_err = False

    class _ErrResult:
        is_err = True

    await mw.post_execute(msg, _OkResult())
    evt1 = adapter._event_queue.get_nowait()
    assert evt1.kind == EventKind.TASK_SUCCEEDED

    await mw.post_execute(msg, _ErrResult())
    evt2 = adapter._event_queue.get_nowait()
    assert evt2.kind == EventKind.TASK_FAILED


@pytest.mark.asyncio
async def test_subscribe_events_yields_from_queue(broker, adapter):
    attach_to_broker(broker, adapter=adapter)
    msg = TaskiqMessage(
        task_id="t3", task_name="myapp.add", labels={}, labels_types={},
        args=[5], kwargs={},
    )
    # Trigger one middleware hook directly so the queue has data.
    await broker.middlewares[-1].pre_send(msg)
    async def _take():
        async for e in adapter.subscribe_events():
            return e
    evt = await asyncio.wait_for(_take(), timeout=0.5)
    assert evt.task_id == "t3"
