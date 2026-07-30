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
| on_error        | TASK_FAILED       |
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from taskiq import TaskiqMessage, TaskiqMiddleware
from z4j_core.models import Event
from z4j_core.models.event import EventKind

logger = logging.getLogger("z4j.adapter.taskiq.events")

ENGINE_NAME = "taskiq"


class Z4JTaskiqMiddleware(TaskiqMiddleware):
    """Middleware that emits z4j Events for every task lifecycle hop.

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

    def _put(self, evt: Event) -> None:
        consumer_loop = getattr(self._loop_source, "_consumer_loop", None)
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
        self._put(self._build(EventKind.TASK_RECEIVED, message))
        return message

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        self._put(self._build(EventKind.TASK_STARTED, message))
        return message

    async def post_execute(self, message: TaskiqMessage, result: Any) -> None:
        # B21: only emit the SUCCESS event here. A failed task fires BOTH
        # post_execute (with is_err=True) AND on_error, so mapping an errored
        # post_execute to TASK_FAILED double-counted every failure (two
        # events, distinct uuids -> brain dedup can't collapse them). on_error
        # is the canonical failure emitter (see below).
        if bool(getattr(result, "is_err", False)):
            return
        self._put(self._build(EventKind.TASK_SUCCEEDED, message))

    async def on_error(
        self,
        message: TaskiqMessage,
        result: Any,
        exception: BaseException,
    ) -> None:
        evt = self._build(EventKind.TASK_FAILED, message, exception=exception)
        self._put(evt)

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
) -> Z4JTaskiqMiddleware:
    """Add :class:`Z4JTaskiqMiddleware` to ``broker``.

    Pass either an ``adapter`` (the middleware uses
    ``adapter._event_queue``) or a raw ``queue``.
    """
    if queue is None and adapter is not None:
        queue = getattr(adapter, "_event_queue", None)
    if queue is None:
        raise ValueError(
            "attach_to_broker: provide either adapter or queue",
        )
    if redaction is None and adapter is not None:
        redaction = getattr(adapter, "redaction", None)
    # Pass the adapter as the loop source so the middleware can hand events
    # to the agent's draining loop via call_soon_threadsafe (B13).
    middleware = Z4JTaskiqMiddleware(queue=queue, redaction=redaction, loop_source=adapter)
    broker.add_middlewares(middleware)
    return middleware


__all__ = ["Z4JTaskiqMiddleware", "attach_to_broker"]
