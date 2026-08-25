"""Cross-loop contracts for Taskiq broker and result-backend calls."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable, Generator
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker
from z4j_taskiq import TaskiqEngineAdapter, Z4JTaskiqMiddleware, attach_to_broker


class _OwnerLoopThread:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread_id: int | None = None
        self._started = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self._started.wait(timeout=2), "owner loop thread did not start"

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.thread_id = threading.get_ident()
        self._started.set()
        self.loop.run_forever()
        self.loop.close()

    def close(self) -> None:
        if self.loop.is_closed():
            return
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=2)
        assert not self._thread.is_alive(), "owner loop thread did not stop"


@pytest.fixture
def owner_loop() -> Generator[_OwnerLoopThread, None, None]:
    owner = _OwnerLoopThread()
    try:
        yield owner
    finally:
        owner.close()


class _LoopBoundBackend:
    def __init__(self, owner: _OwnerLoopThread) -> None:
        self._owner = owner
        self.call_threads: list[int] = []

    def _record_owner(self) -> None:
        if asyncio.get_running_loop() is not self._owner.loop:
            raise RuntimeError("result backend called from a different loop")
        self.call_threads.append(threading.get_ident())

    async def is_result_ready(self, _task_id: str) -> bool:
        self._record_owner()
        return True

    async def get_result(self, _task_id: str) -> SimpleNamespace:
        self._record_owner()
        return SimpleNamespace(is_err=False)


class _LoopBoundTask:
    def __init__(self, owner: _OwnerLoopThread) -> None:
        self._owner = owner
        self.call_threads: list[int] = []

    async def kiq(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        if asyncio.get_running_loop() is not self._owner.loop:
            raise RuntimeError("broker called from a different loop")
        self.call_threads.append(threading.get_ident())
        return SimpleNamespace(task_id="owner-loop-task")


class _LoopBoundBroker:
    def __init__(self, owner: _OwnerLoopThread) -> None:
        self.task = _LoopBoundTask(owner)
        self.result_backend = _LoopBoundBackend(owner)

    def find_task(self, name: str) -> _LoopBoundTask | None:
        return self.task if name == "jobs.owner" else None


@pytest.mark.asyncio
async def test_submit_marshals_to_live_broker_owner_loop(
    owner_loop: _OwnerLoopThread,
) -> None:
    broker = _LoopBoundBroker(owner_loop)
    adapter = TaskiqEngineAdapter(broker=broker, broker_loop=owner_loop.loop)

    result = await adapter.submit_task("jobs.owner", args=(1,), kwargs={"two": 2})

    assert result.status == "success"
    assert result.result == {"task_id": "owner-loop-task", "engine": "taskiq"}
    assert broker.task.call_threads == [owner_loop.thread_id]


@pytest.mark.asyncio
async def test_reconcile_marshals_backend_calls_to_owner_loop(
    owner_loop: _OwnerLoopThread,
) -> None:
    broker = _LoopBoundBroker(owner_loop)
    adapter = TaskiqEngineAdapter(broker=broker, broker_loop=owner_loop.loop)

    result = await adapter.reconcile_task("owner-loop-task")

    assert result.status == "success"
    assert result.result is not None
    assert result.result["engine_state"] == "success"
    assert broker.result_backend.call_threads == [
        owner_loop.thread_id,
        owner_loop.thread_id,
    ]


@pytest.mark.asyncio
async def test_get_task_readiness_probe_runs_on_owner_loop(
    owner_loop: _OwnerLoopThread,
) -> None:
    broker = _LoopBoundBroker(owner_loop)
    adapter = TaskiqEngineAdapter(broker=broker, broker_loop=owner_loop.loop)

    assert await adapter.get_task("owner-loop-task") is None
    assert broker.result_backend.call_threads == [owner_loop.thread_id]


@pytest.mark.asyncio
async def test_no_hop_negative_reproduces_submit_and_reconcile_failure(
    owner_loop: _OwnerLoopThread,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _LoopBoundBroker(owner_loop)
    adapter = TaskiqEngineAdapter(broker=broker, broker_loop=owner_loop.loop)

    async def direct(operation: Callable[[], Awaitable[Any]]) -> Any:
        return await operation()

    monkeypatch.setattr(adapter, "_await_on_broker_loop", direct)

    submit = await adapter.submit_task("jobs.owner")
    reconcile = await adapter.reconcile_task("owner-loop-task")

    assert submit.status == "failed"
    assert "different loop" in (submit.error or "")
    assert reconcile.status == "success"
    assert reconcile.result is not None
    assert reconcile.result["engine_state"] == "unknown"
    assert broker.task.call_threads == []
    assert broker.result_backend.call_threads == []


@pytest.mark.asyncio
async def test_same_owner_loop_awaits_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = SimpleNamespace(loop=asyncio.get_running_loop())
    broker = _LoopBoundBroker(owner)  # type: ignore[arg-type]
    adapter = TaskiqEngineAdapter(broker=broker, broker_loop=owner.loop)

    def unexpected_bridge(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("same-loop operation used the cross-thread bridge")

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", unexpected_bridge)

    result = await adapter.submit_task("jobs.owner")

    assert result.status == "success"
    assert broker.task.call_threads == [threading.get_ident()]


@pytest.mark.asyncio
async def test_unbound_owner_fails_before_creating_operation() -> None:
    adapter = TaskiqEngineAdapter(broker=object())
    called = False

    async def operation() -> None:
        nonlocal called
        called = True

    with pytest.raises(RuntimeError, match="event loop is not bound"):
        await adapter._await_on_broker_loop(operation)
    assert called is False


@pytest.mark.asyncio
async def test_unbound_public_operations_fail_closed_without_broker_calls(
    owner_loop: _OwnerLoopThread,
) -> None:
    broker = _LoopBoundBroker(owner_loop)
    adapter = TaskiqEngineAdapter(broker=broker)

    submit = await adapter.submit_task("jobs.owner")
    reconcile = await adapter.reconcile_task("owner-loop-task")
    task = await adapter.get_task("owner-loop-task")

    assert submit.status == "failed"
    assert "event loop is not bound" in (submit.error or "")
    assert reconcile.status == "success"
    assert reconcile.result is not None
    assert reconcile.result["engine_state"] == "unknown"
    assert task is None
    assert broker.task.call_threads == []
    assert broker.result_backend.call_threads == []


@pytest.mark.asyncio
async def test_direct_live_loop_rebind_conflict_disables_operations(
    owner_loop: _OwnerLoopThread,
) -> None:
    adapter = TaskiqEngineAdapter(
        broker=object(),
        broker_loop=owner_loop.loop,
    )
    called = False

    async def operation() -> None:
        nonlocal called
        called = True

    with pytest.raises(RuntimeError, match="different live event loop"):
        adapter.bind_broker_loop(asyncio.get_running_loop())
    with pytest.raises(RuntimeError, match="disabled after a binding conflict"):
        await adapter._await_on_broker_loop(operation)

    assert adapter._broker_loop is owner_loop.loop
    assert called is False


@pytest.mark.asyncio
async def test_broker_startup_bind_conflict_is_nonfatal_and_logs_only_type(
    owner_loop: _OwnerLoopThread,
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = InMemoryBroker()

    @broker.task
    async def should_not_run() -> None:
        raise AssertionError("disabled adapter touched the broker")

    adapter = TaskiqEngineAdapter(
        broker=broker,
        broker_loop=owner_loop.loop,
    )
    middleware = attach_to_broker(broker, adapter=adapter)

    with caplog.at_level("ERROR", logger="z4j.adapter.taskiq.events"):
        await broker.startup()

    task_name = next(iter(broker.get_all_tasks()))
    result = await adapter.submit_task(task_name)

    assert adapter._broker_loop is owner_loop.loop
    assert adapter._broker_loop_disabled is True
    assert result.status == "failed"
    assert "disabled after a binding conflict" in (result.error or "")
    assert "broker loop binding failed (RuntimeError)" in caplog.text
    assert "different live event loop" not in caplog.text

    await broker.shutdown()
    owner_loop.close()
    await middleware.startup()
    assert adapter._broker_loop is asyncio.get_running_loop()
    assert adapter._broker_loop_disabled is False
    await middleware.shutdown()


@pytest.mark.asyncio
async def test_middleware_shutdown_unbinds_and_public_calls_do_not_touch_broker() -> None:
    owner = SimpleNamespace(loop=asyncio.get_running_loop())
    broker = _LoopBoundBroker(owner)  # type: ignore[arg-type]
    adapter = TaskiqEngineAdapter(broker=broker)
    middleware = Z4JTaskiqMiddleware(
        queue=adapter._event_queue,
        loop_source=adapter,
    )
    await middleware.startup()

    await middleware.shutdown()
    submit = await adapter.submit_task("jobs.owner")
    reconcile = await adapter.reconcile_task("owner-loop-task")
    task = await adapter.get_task("owner-loop-task")

    assert adapter._broker_loop is None
    assert submit.status == "failed"
    assert "event loop is not bound" in (submit.error or "")
    assert reconcile.result is not None
    assert reconcile.result["engine_state"] == "unknown"
    assert task is None
    assert broker.task.call_threads == []
    assert broker.result_backend.call_threads == []


@pytest.mark.asyncio
async def test_late_unbind_does_not_clear_new_owner() -> None:
    stale = asyncio.new_event_loop()
    adapter = TaskiqEngineAdapter(broker=object(), broker_loop=stale)
    current = asyncio.get_running_loop()
    try:
        adapter.bind_broker_loop(current)

        assert adapter.unbind_broker_loop(stale) is False
        assert adapter._broker_loop is current
    finally:
        stale.close()


@pytest.mark.asyncio
async def test_queued_operation_refuses_after_owner_unbind(
    owner_loop: _OwnerLoopThread,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = TaskiqEngineAdapter(
        broker=object(),
        broker_loop=owner_loop.loop,
    )
    owner_blocked = threading.Event()
    release_owner = threading.Event()
    scheduled = threading.Event()
    operation_called = False

    def block_owner() -> None:
        owner_blocked.set()
        assert release_owner.wait(timeout=2), "owner loop was not released"

    original_schedule = asyncio.run_coroutine_threadsafe

    def record_schedule(
        coroutine: Awaitable[Any],
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        future = original_schedule(coroutine, loop)
        scheduled.set()
        return future

    async def operation() -> None:
        nonlocal operation_called
        operation_called = True

    owner_loop.loop.call_soon_threadsafe(block_owner)
    assert owner_blocked.wait(timeout=2), "owner loop did not block"
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", record_schedule)
    pending = asyncio.create_task(adapter._await_on_broker_loop(operation))
    try:
        assert await asyncio.to_thread(scheduled.wait, 2), "operation was not scheduled"
        assert adapter.unbind_broker_loop(owner_loop.loop) is True
        release_owner.set()

        with pytest.raises(RuntimeError, match="ownership changed before operation started"):
            await pending
        assert operation_called is False
    finally:
        release_owner.set()
        if not pending.done():
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending


@pytest.mark.asyncio
async def test_closed_owner_loop_fails_before_creating_operation() -> None:
    owner = asyncio.new_event_loop()
    owner.close()
    adapter = TaskiqEngineAdapter(broker=object(), broker_loop=owner)
    called = False

    async def operation() -> None:
        nonlocal called
        called = True

    with pytest.raises(RuntimeError, match="event loop is closed"):
        await adapter._await_on_broker_loop(operation)
    assert called is False


@pytest.mark.asyncio
async def test_stopped_owner_loop_fails_before_creating_operation() -> None:
    owner = asyncio.new_event_loop()
    adapter = TaskiqEngineAdapter(broker=object(), broker_loop=owner)
    called = False

    async def operation() -> None:
        nonlocal called
        called = True

    try:
        with pytest.raises(RuntimeError, match="event loop is not running"):
            await adapter._await_on_broker_loop(operation)
        assert called is False
    finally:
        owner.close()
