"""The :class:`TaskiqEngineAdapter` - z4j's taskiq queue engine adapter.

Implements :class:`z4j_core.protocols.QueueEngineAdapter` against
any taskiq ``AsyncBroker`` instance.

The adapter supports discovery, reconciliation, and task submission.
Operations on existing tasks, including cancel, retry, and bulk actions,
are not supported because taskiq's broker matrix requires a separate
implementation for each broker.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, TypeVar

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
_T = TypeVar("_T")


class TaskiqEngineAdapter:
    """Queue-engine adapter for taskiq.

    Args:
        broker: A live ``taskiq.AsyncBroker`` (RedisStreamBroker,
                NatsBroker, AioPikaBroker, InMemoryBroker, ...).
                Duck-typed via ``get_all_tasks()`` and
                ``result_backend``.
        broker_loop: Event loop that owns the started broker and result
                backend. z4j's agent runs on a separate background loop, so
                loop-bound broker calls are marshalled back to this loop.
                ``attach_to_broker`` captures it during broker startup; pass
                it explicitly when the adapter is installed from a Taskiq
                startup callback without that middleware. Until an owner is
                bound, async broker operations fail closed instead of running
                on the agent's unrelated background loop.
        redaction: Shared :class:`RedactionEngine`.
    """

    name: str = ENGINE_NAME
    protocol_version: str = PROTOCOL_VERSION

    def __init__(
        self,
        *,
        broker: Any,
        broker_loop: asyncio.AbstractEventLoop | None = None,
        redaction: RedactionEngine | None = None,
    ) -> None:
        self.broker = broker
        self._broker_loop = broker_loop
        self._broker_loop_disabled = False
        self._broker_loop_generation = 0
        self._broker_loop_lock = threading.Lock()
        self.redaction = redaction or RedactionEngine()

        # Event queue populated by Z4JTaskiqMiddleware. Drained by
        # ``subscribe_events``. Empty until the user wires the
        # middleware via ``z4j_taskiq.events.attach_to_broker``.
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10_000)
        # The loop on which ``subscribe_events`` drains the queue (the z4j
        # agent runtime loop). Read by the middleware to hand events across
        # from the taskiq worker loop via call_soon_threadsafe (B13).
        self._consumer_loop: asyncio.AbstractEventLoop | None = None

    def bind_broker_loop(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """Bind async broker operations to the broker's live owner loop.

        Calling this repeatedly with the same loop is idempotent. Rebinding
        away from a different live loop is rejected because using one broker
        concurrently from two event loops corrupts loop-owned transports.
        """
        owner = loop or asyncio.get_running_loop()
        with self._broker_loop_lock:
            previous = self._broker_loop
            if previous is owner:
                if self._broker_loop_disabled:
                    raise RuntimeError(
                        "Taskiq broker event loop ownership remains disabled "
                        "after a binding conflict",
                    )
                return
            if previous is not None and previous.is_running() and not previous.is_closed():
                self._broker_loop_disabled = True
                self._broker_loop_generation += 1
                raise RuntimeError(
                    "Taskiq broker is already bound to a different live event loop",
                )
            self._broker_loop = owner
            self._broker_loop_disabled = False
            self._broker_loop_generation += 1

    def unbind_broker_loop(self, expected: asyncio.AbstractEventLoop) -> bool:
        """Clear loop ownership only when ``expected`` is still the owner.

        Taskiq shutdown can race a later startup. A stale middleware must not
        clear ownership that a newer lifecycle has already established.
        """
        with self._broker_loop_lock:
            if self._broker_loop is not expected:
                return False
            self._broker_loop = None
            self._broker_loop_disabled = False
            self._broker_loop_generation += 1
            return True

    async def _await_on_broker_loop(
        self,
        operation: Callable[[], Awaitable[_T]],
    ) -> _T:
        """Run one broker/backend operation on its owner loop, without retry."""
        with self._broker_loop_lock:
            owner = self._broker_loop
            disabled = self._broker_loop_disabled
            generation = self._broker_loop_generation
        current = asyncio.get_running_loop()
        if disabled:
            raise RuntimeError(
                "Taskiq broker event loop ownership is disabled after a binding conflict",
            )
        if owner is None:
            raise RuntimeError(
                "Taskiq broker event loop is not bound; attach the z4j "
                "middleware before broker startup or pass broker_loop",
            )
        if owner is current:
            with self._broker_loop_lock:
                if (
                    self._broker_loop is not owner
                    or self._broker_loop_disabled
                    or self._broker_loop_generation != generation
                ):
                    raise RuntimeError(
                        "Taskiq broker event loop ownership changed before operation started",
                    )
                direct_operation = operation()
            return await direct_operation
        if owner.is_closed():
            raise RuntimeError("Taskiq broker event loop is closed")
        if not owner.is_running():
            raise RuntimeError("Taskiq broker event loop is not running")

        async def invoke() -> _T:
            with self._broker_loop_lock:
                if (
                    self._broker_loop is not owner
                    or self._broker_loop_disabled
                    or self._broker_loop_generation != generation
                ):
                    raise RuntimeError(
                        "Taskiq broker event loop ownership changed before operation started",
                    )
                owner_operation = operation()
            return await owner_operation

        coroutine = invoke()
        try:
            concurrent_future = asyncio.run_coroutine_threadsafe(coroutine, owner)
        except BaseException:
            coroutine.close()
            raise
        try:
            return await asyncio.wrap_future(concurrent_future)
        except asyncio.CancelledError:
            concurrent_future.cancel()
            raise

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
        # Record the loop we drain on so the middleware (running on the
        # taskiq worker loop) can hand events across safely (B13).
        self._consumer_loop = asyncio.get_running_loop()
        while True:
            evt = await self._event_queue.get()
            yield evt

    async def list_queues(self) -> list[Queue]:
        return []

    async def list_workers(self) -> list[Worker]:
        return []

    async def get_task(self, task_id: str) -> Task | None:
        """Return no task snapshot when only result readiness is available.

        Taskiq's portable result-backend API does not expose the task name,
        queue, arguments, or lifecycle timestamps required to construct a
        truthful :class:`Task`.  ``reconcile_task`` remains the supported
        result-state probe.
        """
        backend = getattr(self.broker, "result_backend", None)
        if backend is None:
            return None
        try:
            ready = await self._await_on_broker_loop(
                lambda: backend.is_result_ready(task_id),
            )
        except Exception:
            return None
        if not ready:
            return None
        return None

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
            ready = await self._await_on_broker_loop(
                lambda: backend.is_result_ready(task_id),
            )
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
            result = await self._await_on_broker_loop(
                lambda: backend.get_result(task_id),
            )
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
    # Actions
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

        The broker-independent Taskiq API has no portable named-queue or
        priority override, and delayed delivery requires a configured schedule
        source.  ``None`` and z4j's logical ``"default"`` queue sentinel both
        select the broker's configured default; refuse real overrides instead
        of silently misrouting them there.
        """
        unsupported = [
            option
            for option, value in (
                ("queue", None if queue in (None, "default") else queue),
                ("eta", eta),
                ("priority", priority),
            )
            if value is not None
        ]
        if unsupported:
            return CommandResult(
                status="failed",
                error=(
                    "z4j-taskiq cannot portably honor submit option(s): " + ", ".join(unsupported)
                ),
            )
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
            sent = await self._await_on_broker_loop(
                lambda: fn.kiq(*args, **(kwargs or {})),
            )
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
        # A safe re-submission needs the task name plus both complete argument
        # collections supplied explicitly; the brain's stored arguments are
        # redacted and cannot be replayed.
        return CommandResult(
            status="failed",
            error=(
                "z4j-taskiq has no native retry_task; re-submit the task "
                "with both complete argument collections explicitly"
            ),
        )

    async def cancel_task(self, task_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error=(
                "cancel_task is not supported by z4j-taskiq; "
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
            error="bulk_retry is not supported by z4j-taskiq",
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
            error="purge_queue is not supported by z4j-taskiq",
        )

    async def requeue_dead_letter(self, task_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error="taskiq DLQ semantics are broker-specific; requeue is not supported",
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
