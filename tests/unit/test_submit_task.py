"""Tests for ``TaskiqEngineAdapter.submit_task``.

The bare-agent dispatcher's v1.1.0 ``schedule.fire`` path routes
brain-side scheduler ticks to ``engine.submit_task(...)``. taskiq is
the second async-Python option for FastAPI stacks alongside arq, so
this contract MUST hold.

Pinned behaviour:
- ``submit_task`` is in ``capabilities()``.
- The adapter resolves the task via ``broker.find_task(name)`` and
  invokes ``.kiq(*args, **kwargs)``, returning the resulting
  ``task_id``.
- An unknown task name fails cleanly (no traceback bubbles to the
  dispatcher).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker
from z4j_taskiq import TaskiqEngineAdapter


@pytest.fixture
async def broker():
    b = InMemoryBroker()

    @b.task
    async def send_email(to, *, template):
        return (to, template)

    @b.task
    async def boom():
        raise RuntimeError("nope")

    await b.startup()
    yield b
    await b.shutdown()


@pytest.fixture
async def adapter(broker) -> TaskiqEngineAdapter:
    return TaskiqEngineAdapter(
        broker=broker,
        broker_loop=asyncio.get_running_loop(),
    )


def _registered_name(broker, suffix: str) -> str:
    keys = list(broker.get_all_tasks().keys())
    matches = [k for k in keys if k.endswith(suffix)]
    assert matches, f"no task ends with {suffix!r}; have {keys}"
    return matches[0]


@pytest.mark.asyncio
class TestSubmitTask:
    async def test_capability_advertised(self, adapter) -> None:
        assert "submit_task" in adapter.capabilities()

    async def test_known_task_kiqs_and_returns_task_id(
        self,
        adapter,
        broker,
    ) -> None:
        name = _registered_name(broker, ":send_email")
        result = await adapter.submit_task(
            name,
            args=("alice@example.com",),
            kwargs={"template": "welcome"},
        )
        assert result.status == "success", f"got {result.error!r}"
        assert result.result["engine"] == "taskiq"
        assert isinstance(result.result["task_id"], str)
        assert len(result.result["task_id"]) > 0

    async def test_unknown_task_fails_cleanly(self, adapter) -> None:
        result = await adapter.submit_task("not.a.real.task")
        assert result.status == "failed"
        assert "unknown taskiq task" in (result.error or "")

    async def test_kiq_exception_returns_failed(self, adapter, broker) -> None:
        """If ``.kiq`` raises (e.g. broker connection error), the
        adapter returns a clean failed result rather than letting
        the exception escape into the dispatcher loop.
        """
        name = _registered_name(broker, ":send_email")
        fn = broker.find_task(name)

        async def boom(*_a, **_k):
            raise RuntimeError("rabbitmq down")

        original_kiq = fn.kiq
        fn.kiq = boom  # type: ignore[method-assign]
        try:
            result = await adapter.submit_task(name)
        finally:
            fn.kiq = original_kiq  # type: ignore[method-assign]
        assert result.status == "failed"
        assert "rabbitmq down" in (result.error or "")

    async def test_default_queue_enqueues_but_named_queue_does_not(
        self,
        adapter,
        broker,
        monkeypatch,
    ) -> None:
        name = _registered_name(broker, ":send_email")
        fn = broker.find_task(name)
        calls = []

        async def record_call(*args, **kwargs):
            calls.append((args, kwargs))
            return SimpleNamespace(task_id="default-queue-task")

        monkeypatch.setattr(fn, "kiq", record_call)

        default_result = await adapter.submit_task(
            name,
            args=("alice@example.com",),
            kwargs={"template": "welcome"},
            queue="default",
        )
        named_result = await adapter.submit_task(name, queue="urgent")

        assert default_result.status == "success"
        assert default_result.result["task_id"] == "default-queue-task"
        assert named_result.status == "failed"
        assert "queue" in (named_result.error or "")
        assert calls == [(("alice@example.com",), {"template": "welcome"})]

    @pytest.mark.parametrize(
        ("option", "value"),
        [("queue", "urgent"), ("eta", 1_800_000_000.0), ("priority", 9)],
    )
    async def test_unsupported_delivery_option_does_not_enqueue(
        self,
        adapter,
        broker,
        monkeypatch,
        option,
        value,
    ) -> None:
        name = _registered_name(broker, ":send_email")
        fn = broker.find_task(name)
        called = False

        async def record_call(*_args, **_kwargs):
            nonlocal called
            called = True

        monkeypatch.setattr(fn, "kiq", record_call)
        result = await adapter.submit_task(name, **{option: value})

        assert result.status == "failed"
        assert option in (result.error or "")
        assert called is False
