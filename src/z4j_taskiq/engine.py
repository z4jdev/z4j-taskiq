"""The :class:`TaskiqEngineAdapter` - z4j's taskiq queue engine adapter.

Implements :class:`z4j_core.protocols.QueueEngineAdapter` against
any taskiq ``AsyncBroker`` instance.

v0 scope: discovery + reconciliation. Action surface (cancel /
retry / bulk) is intentionally empty in v1 because taskiq's
broker matrix means each broker needs a different implementation;
v1.1 will land per-broker support starting with redis-streams.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.redaction.engine import RedactionEngine
from z4j_core.version import PROTOCOL_VERSION

from z4j_taskiq.capabilities import DEFAULT_CAPABILITIES

logger = logging.getLogger("z4j.adapter.taskiq.engine")

ENGINE_NAME = "taskiq"


class TaskiqEngineAdapter:
    """Queue-engine adapter for taskiq.

    Args:
        broker: A live ``taskiq.AsyncBroker`` (RedisStreamBroker,
                NatsBroker, AioPikaBroker, InMemoryBroker, ...).
                Duck-typed via ``get_all_tasks()`` and
                ``result_backend``.
        redaction: Shared :class:`RedactionEngine`.
    """

    name: str = ENGINE_NAME
    protocol_version: str = PROTOCOL_VERSION

    def __init__(
        self,
        *,
        broker: Any,
        redaction: RedactionEngine | None = None,
    ) -> None:
        self.broker = broker
        self.redaction = redaction or RedactionEngine()
        import asyncio as _aio

        # Event queue populated by Z4JTaskiqMiddleware. Drained by
        # ``subscribe_events``. Empty until the user wires the
        # middleware via ``z4j_taskiq.events.attach_to_broker``.
        self._event_queue: _aio.Queue[Event] = _aio.Queue(maxsize=10_000)
        # The loop on which ``subscribe_events`` drains the queue (the z4j
        # agent runtime loop). Read by the middleware to hand events across
        # from the taskiq worker loop via call_soon_threadsafe (B13).
        self._consumer_loop: _aio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    async def discover_tasks(
        self,
        hints: DiscoveryHints | None = None,
    ) -> list[TaskDefinition]:
        """Return one TaskDefinition per ``@broker.task`` decorator.

        taskiq's ``broker.get_all_tasks()`` returns a dict keyed by
        the registered task name. Anonymous tasks (those without an
        explicit name) are keyed as ``"<module>:<func>"``.
        """
        try:
            tasks = self.broker.get_all_tasks()
        except Exception:
            return []
        return [
            TaskDefinition(
                engine=self.name,
                name=name,
                queue=getattr(self.broker, "queue_name", "taskiq"),
            )
            for name in tasks
        ]

    async def subscribe_registry_changes(
        self,
    ) -> AsyncIterator[TaskRegistryDelta]:
        """Decorator-time only; no runtime change signal."""
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]
        return

    # ------------------------------------------------------------------
    # Observation
    # ------------------------------------------------------------------

    async def subscribe_events(self) -> AsyncIterator[Event]:
        """Drain the internal event queue populated by
        :class:`z4j_taskiq.events.Z4JTaskiqMiddleware`.

        Empty until the user attaches the middleware via
        ``attach_to_broker`` (or instantiates it manually).
        """
        import asyncio as _aio

        # Record the loop we drain on so the middleware (running on the
        # taskiq worker loop) can hand events across safely (B13).
        self._consumer_loop = _aio.get_running_loop()
        while True:
            evt = await self._event_queue.get()
            yield evt

    async def list_queues(self) -> list[Queue]:
        return []

    async def list_workers(self) -> list[Worker]:
        return []

    async def get_task(self, task_id: str) -> Task | None:
        backend = getattr(self.broker, "result_backend", None)
        if backend is None:
            return None
        try:
            ready = await backend.is_result_ready(task_id)
        except Exception:
            return None
        if not ready:
            return None
        return Task(
            engine=self.name,
            task_id=task_id,
            name="",
            state="success",  # refined by reconcile_task
        )

    async def reconcile_task(self, task_id: str) -> CommandResult:
        """Probe the broker's result backend.

        taskiq's result backend has only two readable signals:
        ``is_result_ready(task_id)`` and ``get_result(task_id)``.
        We map "no result" → ``"pending"`` (taskiq doesn't expose
        a separate "started" state across all brokers) and "result
        present" → ``"success"`` / ``"failure"`` from
        ``TaskiqResult.is_err``.
        """
        backend = getattr(self.broker, "result_backend", None)
        if backend is None:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": None,
                },
            )

        try:
            ready = await backend.is_result_ready(task_id)
        except Exception:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": None,
                },
            )

        if not ready:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "pending",
                    "finished_at": None,
                    "exception": None,
                },
            )

        try:
            result = await backend.get_result(task_id)
        except Exception:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": None,
                },
            )

        is_err = bool(getattr(result, "is_err", False))
        exception_text: str | None = None
        if is_err:
            err = getattr(result, "error", None)
            if isinstance(err, BaseException):
                exception_text = f"{type(err).__name__}: {err}"
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "engine_state": "failure" if is_err else "success",
                "finished_at": None,
                "exception": exception_text,
            },
        )

    # ------------------------------------------------------------------
    # Actions - all unimplemented in v1
    # ------------------------------------------------------------------

    async def submit_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        eta: float | None = None,
        priority: int | None = None,
    ) -> CommandResult:
        """Universal enqueue - looks up the registered task by name
        and kicks it via taskiq's normal ``.kiq()`` path.
        """
        try:
            fn = self.broker.find_task(name)
        except Exception as exc:
            return CommandResult(status="failed", error=str(exc))
        if fn is None:
            return CommandResult(
                status="failed",
                error=f"unknown taskiq task {name!r}",
            )
        try:
            sent = await fn.kiq(*args, **(kwargs or {}))
        except Exception as exc:
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": sent.task_id, "engine": self.name},
        )

    async def retry_task(
        self,
        task_id: str,
        *,
        override_args: tuple[Any, ...] | None = None,
        override_kwargs: dict[str, Any] | None = None,
        eta: float | None = None,
        priority: int | None = None,
    ) -> CommandResult:
        # Brain polyfills via submit_task using its captured args.
        return CommandResult(
            status="failed",
            error=(
                "z4j-taskiq has no native retry_task; brain polyfills "
                "via submit_task with original args"
            ),
        )

    async def cancel_task(self, task_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error=(
                "cancel_task not implemented in z4j-taskiq v1; "
                "taskiq has no broker-agnostic cancel primitive"
            ),
        )

    async def bulk_retry(
        self,
        filter: dict[str, Any],  # noqa: A002  public bulk_retry signature
        *,
        max: int = 1000,  # noqa: A002  public bulk_retry signature
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="bulk_retry not implemented in z4j-taskiq v1",
        )

    async def purge_queue(
        self,
        queue_name: str,
        *,
        confirm_token: str | None = None,
        force: bool = False,
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="purge_queue not implemented in z4j-taskiq v1",
        )

    async def requeue_dead_letter(self, task_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error="taskiq DLQ semantics are broker-specific; deferred to v1.1",
        )

    async def rate_limit(
        self,
        task_name: str,
        rate: str,
        *,
        worker_name: str | None = None,
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="rate_limit not supported by taskiq",
        )

    async def restart_worker(self, worker_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error="taskiq workers expose no remote restart",
        )

    # ------------------------------------------------------------------
    # Capabilities
    # ------------------------------------------------------------------

    def capabilities(self) -> set[str]:
        return set(DEFAULT_CAPABILITIES)


__all__ = ["ENGINE_NAME", "TaskiqEngineAdapter"]
