"""z4j capture as a taskiq middleware.

taskiq's middleware system gives us four useful hooks:
``pre_send`` (broker enqueue), ``pre_execute`` (worker pickup),
``post_execute`` (worker completion success), ``on_error``
(worker completion failure). These run on the taskiq worker's own
asyncio loop; the z4j agent drains the queue on a DIFFERENT loop
(its background-thread runtime), so events are handed across via
``call_soon_threadsafe`` (see ``Z4JTaskiqMiddleware._put``).

Mapping:

| Middleware hook | EventKind         |
|-----------------|-------------------|
| pre_send        | TASK_RECEIVED     |
| pre_execute     | TASK_STARTED      |
| post_execute    | TASK_SUCCEEDED    |
| on_error        | TASK_RETRIED or TASK_FAILED |
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from taskiq import TaskiqEvents, TaskiqMessage, TaskiqMiddleware
from z4j_core.models import Event
from z4j_core.models.event import EventKind

logger = logging.getLogger("z4j.adapter.taskiq.events")

ENGINE_NAME = "taskiq"


class Z4JTaskiqMiddleware(TaskiqMiddleware):
    """Attempt mapped z4j events for Taskiq lifecycle hooks.

    The destination queue is bounded. When it is full, the middleware logs and
    drops the event rather than delaying the host task.

    Args:
        queue: asyncio queue owned by the engine adapter.
        redaction: optional RedactionEngine for args/kwargs scrubbing.
    """

    def __init__(
        self,
        *,
        queue: asyncio.Queue[Event],
        redaction: Any | None = None,
        loop_source: Any | None = None,
        consumer_loop: asyncio.AbstractEventLoop | None = None,
        broker_source: Any | None = None,
    ) -> None:
        super().__init__()
        self._queue = queue
        self._redaction = redaction
        # B13: the queue is drained by the z4j agent runtime on ITS OWN
        # background-thread event loop, while these middleware hooks run on
        # the taskiq worker's loop -- a DIFFERENT loop in the same process.
        # ``asyncio.Queue`` is not thread/loop-safe, so a raw put_nowait
        # from the taskiq loop can corrupt the queue internals and its
        # wakeup future never fires on the consumer loop (events stall
        # until the consumer wakes for another reason). We hop to the
        # consumer loop via call_soon_threadsafe. ``loop_source`` is the
        # engine adapter, which records its draining loop on
        # ``_consumer_loop`` when ``subscribe_events`` starts.
        self._loop_source = loop_source
        self._consumer_loop = consumer_loop
        self._broker_source = broker_source
        self._bound_broker_loop: asyncio.AbstractEventLoop | None = None
        self._binding_disabled = False
        # Official retry middleware reuses the task id. ``pre_send`` records
        # the successful re-enqueue here so the originating ``on_error`` does
        # not also emit a terminal failure.
        self._retry_enqueued_ids: set[str] = set()

    async def startup(self) -> None:
        """Capture the loop that owns the started Taskiq broker."""
        bind_broker_loop = getattr(self._loop_source, "bind_broker_loop", None)
        if self._binding_disabled or not callable(bind_broker_loop):
            return
        owner = asyncio.get_running_loop()
        try:
            bind_broker_loop(owner)
        except Exception as exc:
            # A z4j ownership conflict must not abort Taskiq host startup. The
            # adapter marks itself disabled while retaining the prior owner as
            # a safety lock. Never include the exception body because broker
            # URLs can carry credentials.
            self._binding_disabled = True
            logger.error(  # noqa: TRY400  type-only host-boundary log
                "z4j-taskiq: broker loop binding failed (%s); adapter commands disabled",
                type(exc).__name__,
            )
            return
        self._bound_broker_loop = owner

    async def shutdown(self) -> None:
        """Release only the broker loop captured by this middleware startup."""
        self._binding_disabled = False
        owner = self._bound_broker_loop
        if owner is None:
            return
        if asyncio.get_running_loop() is not owner:
            return
        self._bound_broker_loop = None
        unbind_broker_loop = getattr(
            self._loop_source,
            "unbind_broker_loop",
            None,
        )
        if not callable(unbind_broker_loop):
            return
        try:
            unbind_broker_loop(owner)
        except Exception as exc:  # pragma: no cover - defensive host boundary
            logger.error(  # noqa: TRY400  type-only host-boundary log
                "z4j-taskiq: broker loop shutdown unbind failed (%s)",
                type(exc).__name__,
            )

    def _put(self, evt: Event) -> None:
        consumer_loop = self._consumer_loop or getattr(
            self._loop_source,
            "_consumer_loop",
            None,
        )
        if consumer_loop is not None and not consumer_loop.is_closed():
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is not consumer_loop:
                # Cross-loop: schedule the enqueue on the consumer's loop.
                consumer_loop.call_soon_threadsafe(self._enqueue, evt)
                return
        self._enqueue(evt)

    def _enqueue(self, evt: Event) -> None:
        try:
            self._queue.put_nowait(evt)
        except asyncio.QueueFull:
            logger.warning(
                "z4j-taskiq: event queue full; dropping %s",
                evt.kind,
            )

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        try:
            retries = int(message.labels.get("_retries", 0) or 0)
            if retries > 0:
                self._retry_enqueued_ids.add(message.task_id)
                self._put(self._build(EventKind.TASK_RETRIED, message))
            else:
                self._put(self._build(EventKind.TASK_RECEIVED, message))
        except Exception as exc:
            self._log_capture_failure("pre_send", exc)
        return message

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        try:
            self._put(self._build(EventKind.TASK_STARTED, message))
        except Exception as exc:
            self._log_capture_failure("pre_execute", exc)
        return message

    async def post_execute(self, message: TaskiqMessage, result: Any) -> None:
        # B21: only emit the SUCCESS event here. A failed task fires BOTH
        # post_execute (with is_err=True) AND on_error, so mapping an errored
        # post_execute to TASK_FAILED double-counted every failure (two
        # events, distinct uuids -> brain dedup can't collapse them). on_error
        # is the canonical failure emitter (see below).
        try:
            if bool(getattr(result, "is_err", False)):
                return
            self._put(self._build(EventKind.TASK_SUCCEEDED, message))
        except Exception as exc:
            self._log_capture_failure("post_execute", exc)

    async def on_error(
        self,
        message: TaskiqMessage,
        result: Any,
        exception: BaseException,
    ) -> None:
        try:
            # An observed pre_send is authoritative proof that SimpleRetry
            # already re-enqueued this id. Check it before mirroring the retry
            # decision; some Taskiq versions do not attach ``broker`` to every
            # middleware instance until worker startup.
            if message.task_id in self._retry_enqueued_ids:
                self._retry_enqueued_ids.discard(message.task_id)
                return
            if self._official_retry_will_run(message, exception):
                # SmartRetry with an external schedule source has no broker
                # pre_send hook, so emit its successful retry decision here.
                self._put(self._build(EventKind.TASK_RETRIED, message))
                return
            evt = self._build(EventKind.TASK_FAILED, message, exception=exception)
            self._put(evt)
        except Exception as exc:
            self._log_capture_failure("on_error", exc)

    def _log_capture_failure(self, hook: str, exc: Exception) -> None:
        """Log a redacted capture failure without affecting Taskiq control."""
        logger.error(
            "z4j-taskiq: %s capture failed (%s); dropping event",
            hook,
            type(exc).__name__,
        )

    def _official_retry_will_run(
        self,
        message: TaskiqMessage,
        exception: BaseException,
    ) -> bool:
        """Mirror the supported Taskiq retry middleware decision.

        ``attach_to_broker`` places this middleware first, so Taskiq's reverse
        error-hook order runs SimpleRetry/SmartRetry before this method. A
        successful retry decision therefore has already enqueued or scheduled
        replacement work when this returns true.
        """
        broker = self._broker_source or getattr(self, "broker", None)
        for middleware in getattr(broker, "middlewares", ()):
            if middleware is self or type(middleware).__name__ not in {
                "SimpleRetryMiddleware",
                "SmartRetryMiddleware",
            }:
                continue
            if not type(middleware).__module__.startswith("taskiq"):
                continue
            accepted = getattr(middleware, "types_of_exceptions", None)
            if accepted is not None and not isinstance(exception, tuple(accepted)):
                continue
            if type(exception).__name__ == "NoResultError":
                continue
            enabled = message.labels.get("retry_on_error")
            if isinstance(enabled, str):
                enabled = enabled.lower() == "true"
            if enabled is None:
                enabled = getattr(middleware, "default_retry_label", False)
            if not enabled:
                continue
            retries = int(message.labels.get("_retries", 0) or 0) + 1
            maximum = int(
                message.labels.get(
                    "max_retries",
                    getattr(middleware, "default_retry_count", 0),
                ),
            )
            if retries < maximum:
                return True
        return False

    def _build(
        self,
        kind: EventKind,
        message: TaskiqMessage,
        exception: BaseException | None = None,
    ) -> Event:
        now = datetime.now(UTC)
        data: dict[str, Any] = {"task_name": message.task_name}
        if exception is not None:
            data["exception"] = f"{type(exception).__name__}: {exception}"
        if kind == EventKind.TASK_RECEIVED:
            args = list(message.args or [])
            kwargs = dict(message.kwargs or {})
            if self._redaction is not None:
                # RedactionEngine's API is scrub() (recursive over
                # lists/dicts). The redact_args/redact_kwargs calls
                # this previously made never existed, and the blanket
                # except silently replaced EVERY task's args/kwargs
                # with empty whenever redaction was configured
                # (1.7.0 release-validation blocker).
                try:
                    args = list(self._redaction.scrub(list(args)))
                    kwargs = dict(self._redaction.scrub(dict(kwargs)))
                except Exception:
                    args, kwargs = [], {}
            data["args"] = list(args)
            data["kwargs"] = kwargs
        return Event(
            id=uuid4(),
            project_id=uuid4(),
            agent_id=uuid4(),
            engine=ENGINE_NAME,
            task_id=message.task_id,
            kind=kind,
            occurred_at=now,
            data=data,
        )


def attach_to_broker(
    broker: Any,
    *,
    adapter: Any | None = None,
    queue: asyncio.Queue[Event] | None = None,
    redaction: Any | None = None,
    consumer_loop: asyncio.AbstractEventLoop | None = None,
) -> Z4JTaskiqMiddleware:
    """Add :class:`Z4JTaskiqMiddleware` to ``broker``.

    Pass either an ``adapter`` (the middleware uses
    ``adapter._event_queue`` and discovers its consumer loop) or both a raw
    ``queue`` and the event loop that drains it.  A raw queue without its loop
    is rejected because Taskiq worker hooks may run on a different loop.
    Reattaching the same adapter to the same broker is idempotent and returns
    the existing middleware.
    """
    if adapter is not None and queue is not None:
        raise ValueError(
            "attach_to_broker: provide adapter or raw queue, not both",
        )
    if adapter is not None and getattr(adapter, "broker", broker) is not broker:
        raise ValueError(
            "attach_to_broker: adapter belongs to a different broker",
        )
    if queue is None and adapter is not None:
        queue = getattr(adapter, "_event_queue", None)
    if queue is None:
        raise ValueError(
            "attach_to_broker: provide either adapter or queue",
        )
    if adapter is None and consumer_loop is None:
        raise ValueError(
            "attach_to_broker: raw queue requires consumer_loop",
        )
    existing_z4j = [
        existing
        for existing in getattr(broker, "middlewares", ())
        if isinstance(existing, Z4JTaskiqMiddleware)
    ]
    if existing_z4j:
        if (
            len(existing_z4j) == 1
            and adapter is not None
            and existing_z4j[0]._loop_source is adapter
            and existing_z4j[0]._broker_source is broker
        ):
            return existing_z4j[0]
        raise RuntimeError(
            "attach_to_broker: broker already has z4j middleware for a different adapter or queue",
        )
    if redaction is None and adapter is not None:
        redaction = getattr(adapter, "redaction", None)
    # Pass the adapter as the loop source so the middleware can hand events
    # to the agent's draining loop via call_soon_threadsafe (B13).
    middleware = Z4JTaskiqMiddleware(
        queue=queue,
        redaction=redaction,
        loop_source=adapter,
        consumer_loop=consumer_loop,
        broker_source=broker,
    )
    broker.add_middlewares(middleware)
    # Taskiq calls on_error hooks in reverse middleware order. Keep z4j first
    # so official retry middleware performs (and proves) its re-enqueue before
    # z4j decides between RETRIED and terminal FAILED.
    broker.middlewares.remove(middleware)
    broker.middlewares.insert(0, middleware)

    # Taskiq invokes lifecycle event handlers across the supported 0.11/0.12
    # range, while middleware lifecycle coverage varies by version and broker
    # (InMemoryBroker invokes only CLIENT/WORKER handlers). Register both event
    # roles as the compatibility path. Newer standard brokers may additionally
    # call the middleware methods; both methods are deliberately idempotent.
    async def _z4j_startup(_state: Any) -> None:
        await middleware.startup()

    async def _z4j_shutdown(_state: Any) -> None:
        await middleware.shutdown()

    for event in (TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.WORKER_STARTUP):
        broker.on_event(event)(_z4j_startup)
    for event in (TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN):
        broker.on_event(event)(_z4j_shutdown)
    return middleware


__all__ = ["Z4JTaskiqMiddleware", "attach_to_broker"]
