"""taskiq middleware event capture."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker, TaskiqEvents, TaskiqMessage
from z4j_core.models.event import EventKind
from z4j_taskiq import (
    TaskiqEngineAdapter,
    Z4JTaskiqMiddleware,
    attach_to_broker,
)


def _simple_retry_middleware_cls():
    from taskiq import middlewares

    middleware_cls = getattr(middlewares, "SimpleRetryMiddleware", None)
    if middleware_cls is None:
        pytest.skip(
            "upstream SimpleRetryMiddleware requires taskiq 0.11.17+; "
            "the version-independent z4j retry contract runs below",
        )
    return middleware_cls


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


def test_reattach_same_broker_and_adapter_is_idempotent(broker, adapter):
    first = attach_to_broker(broker, adapter=adapter)
    handler_counts = {
        event: len(broker.event_handlers[event])
        for event in (
            TaskiqEvents.CLIENT_STARTUP,
            TaskiqEvents.WORKER_STARTUP,
            TaskiqEvents.CLIENT_SHUTDOWN,
            TaskiqEvents.WORKER_SHUTDOWN,
        )
    }
    second = attach_to_broker(broker, adapter=adapter)

    assert second is first
    assert broker.middlewares.count(first) == 1
    assert {event: len(broker.event_handlers[event]) for event in handler_counts} == handler_counts


def test_attach_rejects_different_adapter_for_same_broker(broker, adapter):
    middleware = attach_to_broker(broker, adapter=adapter)
    conflicting = TaskiqEngineAdapter(broker=broker)

    with pytest.raises(RuntimeError, match="different adapter or queue"):
        attach_to_broker(broker, adapter=conflicting)

    assert broker.middlewares == [middleware]


def test_attach_rejects_preexisting_duplicate_z4j_middlewares(broker, adapter):
    first = attach_to_broker(broker, adapter=adapter)
    duplicate = Z4JTaskiqMiddleware(
        queue=adapter._event_queue,
        loop_source=adapter,
        broker_source=broker,
    )
    broker.add_middlewares(duplicate)
    before = list(broker.middlewares)

    with pytest.raises(RuntimeError, match="different adapter or queue"):
        attach_to_broker(broker, adapter=adapter)

    assert broker.middlewares == before
    assert before == [first, duplicate]


@pytest.mark.asyncio
async def test_attach_rejects_raw_queue_when_broker_already_attached(
    broker,
    adapter,
):
    middleware = attach_to_broker(broker, adapter=adapter)

    with pytest.raises(RuntimeError, match="different adapter or queue"):
        attach_to_broker(
            broker,
            queue=asyncio.Queue(),
            consumer_loop=asyncio.get_running_loop(),
        )

    assert broker.middlewares == [middleware]


@pytest.mark.asyncio
async def test_attach_rejects_adapter_when_broker_has_raw_queue(broker):
    middleware = attach_to_broker(
        broker,
        queue=asyncio.Queue(),
        consumer_loop=asyncio.get_running_loop(),
    )

    with pytest.raises(RuntimeError, match="different adapter or queue"):
        attach_to_broker(
            broker,
            adapter=TaskiqEngineAdapter(broker=broker),
        )

    assert broker.middlewares == [middleware]


def test_attach_rejects_adapter_owned_by_another_broker() -> None:
    first = InMemoryBroker()
    second = InMemoryBroker()
    adapter = TaskiqEngineAdapter(broker=first)

    with pytest.raises(ValueError, match="different broker"):
        attach_to_broker(second, adapter=adapter)

    assert second.middlewares == []


def test_attach_rejects_adapter_and_foreign_raw_queue(broker, adapter) -> None:
    with pytest.raises(ValueError, match="adapter or raw queue"):
        attach_to_broker(
            broker,
            adapter=adapter,
            queue=asyncio.Queue(),
        )

    assert broker.middlewares == []


@pytest.mark.asyncio
async def test_middleware_startup_binds_broker_owner_loop(
    broker: Any,
    adapter: TaskiqEngineAdapter,
) -> None:
    middleware = attach_to_broker(broker, adapter=adapter)

    await middleware.startup()

    assert adapter._broker_loop is asyncio.get_running_loop()


@pytest.mark.asyncio
async def test_real_inmemory_lifecycle_binds_and_unbinds_owner(
    broker: Any,
    adapter: TaskiqEngineAdapter,
) -> None:
    attach_to_broker(broker, adapter=adapter)

    await broker.startup()
    assert adapter._broker_loop is asyncio.get_running_loop()

    await broker.shutdown()
    assert adapter._broker_loop is None


def test_middleware_can_restart_on_a_new_loop(broker, adapter) -> None:
    middleware = attach_to_broker(broker, adapter=adapter)

    async def cycle() -> asyncio.AbstractEventLoop:
        owner = asyncio.get_running_loop()
        await middleware.startup()
        assert adapter._broker_loop is owner
        await middleware.shutdown()
        assert adapter._broker_loop is None
        return owner

    first = asyncio.run(cycle())
    second = asyncio.run(cycle())

    assert first is not second


def test_attach_rejects_raw_queue_without_consumer_loop(broker):
    with pytest.raises(ValueError, match="raw queue requires consumer_loop"):
        attach_to_broker(broker, queue=asyncio.Queue())


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
async def test_real_kiq_survives_capture_failure_without_secret_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = InMemoryBroker()
    executed = False

    @broker.task
    async def work() -> str:
        nonlocal executed
        executed = True
        return "done"

    adapter = TaskiqEngineAdapter(broker=broker)
    middleware = attach_to_broker(broker, adapter=adapter)

    def fail_capture(_event: Any) -> None:
        raise RuntimeError("sensitive-capture-detail")

    monkeypatch.setattr(middleware, "_put", fail_capture)
    await broker.startup()
    try:
        with caplog.at_level("ERROR", logger="z4j.adapter.taskiq.events"):
            sent = await work.kiq()
            result = await sent.wait_result(timeout=2)
    finally:
        await broker.shutdown()

    assert executed is True
    assert result.return_value == "done"
    assert "capture failed (RuntimeError)" in caplog.text
    assert "sensitive-capture-detail" not in caplog.text


@pytest.mark.asyncio
async def test_pre_send_isolates_malformed_retry_metadata(
    caplog: pytest.LogCaptureFixture,
) -> None:
    middleware = Z4JTaskiqMiddleware(queue=asyncio.Queue())
    message = TaskiqMessage(
        task_id="malformed-retry",
        task_name="jobs.work",
        labels={"_retries": "not-an-integer"},
        labels_types={},
        args=[],
        kwargs={},
    )

    with caplog.at_level("ERROR", logger="z4j.adapter.taskiq.events"):
        returned = await middleware.pre_send(message)

    assert returned is message
    assert "pre_send capture failed (ValueError)" in caplog.text
    assert "not-an-integer" not in caplog.text


@pytest.mark.asyncio
async def test_consumer_loop_close_race_drops_capture_without_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class RacingLoop:
        def is_closed(self) -> bool:
            return False

        def call_soon_threadsafe(self, *_args: Any) -> None:
            raise RuntimeError("sensitive-loop-race-detail")

    middleware = Z4JTaskiqMiddleware(
        queue=asyncio.Queue(),
        consumer_loop=RacingLoop(),  # type: ignore[arg-type]
    )
    message = TaskiqMessage(
        task_id="loop-race",
        task_name="jobs.work",
        labels={},
        labels_types={},
        args=[],
        kwargs={},
    )

    with caplog.at_level("ERROR", logger="z4j.adapter.taskiq.events"):
        returned = await middleware.pre_send(message)

    assert returned is message
    assert "pre_send capture failed (RuntimeError)" in caplog.text
    assert "sensitive-loop-race-detail" not in caplog.text


@pytest.mark.asyncio
async def test_execute_and_error_hooks_isolate_capture_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from types import SimpleNamespace

    middleware = Z4JTaskiqMiddleware(queue=asyncio.Queue())
    message = TaskiqMessage(
        task_id="hook-failure",
        task_name="jobs.work",
        labels={},
        labels_types={},
        args=[],
        kwargs={},
    )

    def fail_build(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("sensitive-hook-detail")

    monkeypatch.setattr(middleware, "_build", fail_build)
    with caplog.at_level("ERROR", logger="z4j.adapter.taskiq.events"):
        assert await middleware.pre_execute(message) is message
        await middleware.post_execute(message, SimpleNamespace(is_err=False))
        await middleware.on_error(
            message,
            SimpleNamespace(error=RuntimeError("task failed")),
            RuntimeError("task failed"),
        )

    for hook in ("pre_execute", "post_execute", "on_error"):
        assert f"{hook} capture failed (RuntimeError)" in caplog.text
    assert "sensitive-hook-detail" not in caplog.text


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
@pytest.mark.parametrize("no_result_on_retry", [True, False])
async def test_simple_retry_emits_retried_without_terminal_failure(
    no_result_on_retry,
):
    from types import SimpleNamespace

    broker = InMemoryBroker()

    @broker.task
    async def flaky():
        return "recovered"

    task_name = next(iter(broker.get_all_tasks()))
    retry = _simple_retry_middleware_cls()(
        default_retry_count=2,
        default_retry_label=True,
        no_result_on_retry=no_result_on_retry,
    )
    broker.add_middlewares(retry)
    adapter = TaskiqEngineAdapter(broker=broker)
    middleware = attach_to_broker(broker, adapter=adapter)
    assert broker.middlewares[0] is middleware

    message = TaskiqMessage(
        task_id="retry-id",
        task_name=task_name,
        labels={"retry_on_error": True},
        labels_types={},
        args=[],
        kwargs={},
    )
    exception = RuntimeError("transient")
    result = SimpleNamespace(error=exception)
    for item in reversed(broker.middlewares):
        if type(item).on_error is not Z4JTaskiqMiddleware.__mro__[1].on_error:
            await item.on_error(message, result, exception)

    kinds = []
    while not adapter._event_queue.empty():
        kinds.append(adapter._event_queue.get_nowait().kind)
    assert kinds.count(EventKind.TASK_RETRIED) == 1
    assert EventKind.TASK_FAILED not in kinds


@pytest.mark.asyncio
async def test_exhausted_simple_retry_emits_terminal_failure():
    from types import SimpleNamespace

    broker = InMemoryBroker()
    broker.add_middlewares(
        _simple_retry_middleware_cls()(default_retry_count=2, default_retry_label=True),
    )
    adapter = TaskiqEngineAdapter(broker=broker)
    attach_to_broker(broker, adapter=adapter)
    message = TaskiqMessage(
        task_id="failed-id",
        task_name="myapp.flaky",
        labels={"retry_on_error": True, "_retries": 1},
        labels_types={},
        args=[],
        kwargs={},
    )
    exception = RuntimeError("terminal")
    result = SimpleNamespace(error=exception)
    for item in reversed(broker.middlewares):
        await item.on_error(message, result, exception)

    kinds = []
    while not adapter._event_queue.empty():
        kinds.append(adapter._event_queue.get_nowait().kind)
    assert kinds == [EventKind.TASK_FAILED]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("completed_retries", "expected"),
    [(0, EventKind.TASK_RETRIED), (1, EventKind.TASK_FAILED)],
)
async def test_retry_event_contract_is_covered_without_optional_middleware_version(
    completed_retries,
    expected,
):
    from types import SimpleNamespace

    retry_cls = type(
        "SimpleRetryMiddleware",
        (),
        {"__module__": "taskiq.middlewares"},
    )
    retry = retry_cls()
    retry.types_of_exceptions = None
    retry.default_retry_label = True
    retry.default_retry_count = 2
    broker = SimpleNamespace(middlewares=[retry])
    queue = asyncio.Queue()
    middleware = Z4JTaskiqMiddleware(
        queue=queue,
        broker_source=broker,
    )
    message = TaskiqMessage(
        task_id="retry-contract",
        task_name="myapp.flaky",
        labels={"retry_on_error": True, "_retries": completed_retries},
        labels_types={},
        args=[],
        kwargs={},
    )
    exception = RuntimeError("transient")

    await middleware.on_error(
        message,
        SimpleNamespace(error=exception),
        exception,
    )

    assert queue.get_nowait().kind == expected
    assert queue.empty()


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

    def test_raw_queue_hops_to_explicit_consumer_loop(self) -> None:
        from unittest.mock import MagicMock

        q: asyncio.Queue = asyncio.Queue(maxsize=10)
        consumer_loop = MagicMock()
        consumer_loop.is_closed.return_value = False
        mw = Z4JTaskiqMiddleware(queue=q, consumer_loop=consumer_loop)
        from z4j_core.models import Event
        from z4j_core.models.event import EventKind

        evt = Event(
            id=__import__("uuid").uuid4(),
            project_id=__import__("uuid").uuid4(),
            agent_id=__import__("uuid").uuid4(),
            engine="taskiq",
            task_id="t3",
            kind=EventKind.TASK_RECEIVED,
            occurred_at=__import__("datetime").datetime.now(__import__("datetime").UTC),
            data={},
        )
        mw._put(evt)
        consumer_loop.call_soon_threadsafe.assert_called_once_with(mw._enqueue, evt)
        assert q.empty()
