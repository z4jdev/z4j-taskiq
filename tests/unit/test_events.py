"""taskiq middleware event capture."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker, TaskiqMessage
from z4j_core.models.event import EventKind
from z4j_taskiq import (
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
        task_id="t1",
        task_name="myapp.add",
        labels={},
        labels_types={},
        args=[1, 2],
        kwargs={},
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
        task_id="t2",
        task_name="myapp.add",
        labels={},
        labels_types={},
        args=[],
        kwargs={},
    )

    class _OkResult:
        is_err = False

    class _ErrResult:
        is_err = True

    await mw.post_execute(msg, _OkResult())
    evt1 = adapter._event_queue.get_nowait()
    assert evt1.kind == EventKind.TASK_SUCCEEDED

    # B21: an errored post_execute emits NOTHING -- on_error is the
    # canonical failure emitter, so mapping this to TASK_FAILED would
    # double-count every failure (post_execute + on_error).
    await mw.post_execute(msg, _ErrResult())
    assert adapter._event_queue.empty()

    # on_error is what surfaces the failure.
    await mw.on_error(msg, _ErrResult(), RuntimeError("boom"))
    evt2 = adapter._event_queue.get_nowait()
    assert evt2.kind == EventKind.TASK_FAILED


@pytest.mark.asyncio
async def test_subscribe_events_yields_from_queue(broker, adapter):
    attach_to_broker(broker, adapter=adapter)
    msg = TaskiqMessage(
        task_id="t3",
        task_name="myapp.add",
        labels={},
        labels_types={},
        args=[5],
        kwargs={},
    )
    # Trigger one middleware hook directly so the queue has data.
    await broker.middlewares[-1].pre_send(msg)

    async def _take():
        async for e in adapter.subscribe_events():
            return e
        return None

    evt = await asyncio.wait_for(_take(), timeout=0.5)
    assert evt.task_id == "t3"


@pytest.mark.asyncio
async def test_pre_send_scrubs_args_with_real_redaction_engine(adapter):
    # 1.7.0 release-validation blocker: the middleware called
    # RedactionEngine.redact_args()/redact_kwargs(), methods that have
    # never existed; the blanket except then silently replaced EVERY
    # task's args/kwargs with empty whenever redaction was configured.
    # Pin the real contract: secrets masked, benign values retained.
    from z4j_core.redaction import RedactionConfig, RedactionEngine

    mw = Z4JTaskiqMiddleware(
        queue=adapter._event_queue,
        redaction=RedactionEngine(RedactionConfig()),
    )
    msg = TaskiqMessage(
        task_id="t-redact",
        task_name="myapp.pay",
        labels={},
        labels_types={},
        args=["visible-value"],
        kwargs={"password": "hunter2", "note": "keep-me"},
    )
    await mw.pre_send(msg)
    evt = adapter._event_queue.get_nowait()
    assert evt.data["args"] == ["visible-value"]
    assert evt.data["kwargs"]["note"] == "keep-me"
    assert evt.data["kwargs"]["password"] != "hunter2"


class TestB13CrossLoopHop:
    """B13: the middleware runs on the taskiq worker loop while the agent
    drains the queue on its own loop, so a raw put_nowait is cross-loop and
    unsafe. When a consumer loop is known and differs from the current one,
    the middleware must hop via call_soon_threadsafe."""

    def test_hops_to_consumer_loop_when_different(self) -> None:
        import asyncio
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from z4j_core.models import Event
        from z4j_core.models.event import EventKind

        q: asyncio.Queue = asyncio.Queue(maxsize=10)
        fake_loop = MagicMock()
        fake_loop.is_closed.return_value = False
        source = SimpleNamespace(_consumer_loop=fake_loop)
        mw = Z4JTaskiqMiddleware(queue=q, loop_source=source)

        evt = Event(
            id=__import__("uuid").uuid4(),
            project_id=__import__("uuid").uuid4(),
            agent_id=__import__("uuid").uuid4(),
            engine="taskiq",
            task_id="t1",
            kind=EventKind.TASK_RECEIVED,
            occurred_at=__import__("datetime").datetime.now(__import__("datetime").UTC),
            data={},
        )
        # Called with no running loop (running is not fake_loop) -> must hop.
        mw._put(evt)
        fake_loop.call_soon_threadsafe.assert_called_once()
        # The queue was NOT written directly on this (wrong) loop.
        assert q.empty()

    def test_direct_enqueue_when_no_consumer_loop(self) -> None:
        import asyncio

        q: asyncio.Queue = asyncio.Queue(maxsize=10)
        mw = Z4JTaskiqMiddleware(queue=q, loop_source=None)
        from z4j_core.models import Event
        from z4j_core.models.event import EventKind

        evt = Event(
            id=__import__("uuid").uuid4(),
            project_id=__import__("uuid").uuid4(),
            agent_id=__import__("uuid").uuid4(),
            engine="taskiq",
            task_id="t2",
            kind=EventKind.TASK_RECEIVED,
            occurred_at=__import__("datetime").datetime.now(__import__("datetime").UTC),
            data={},
        )
        mw._put(evt)
        assert q.qsize() == 1
