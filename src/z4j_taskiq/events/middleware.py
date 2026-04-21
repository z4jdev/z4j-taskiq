"""z4j capture as a taskiq middleware.

taskiq's middleware system gives us four useful hooks:
``pre_send`` (broker enqueue), ``pre_execute`` (worker pickup),
``post_execute`` (worker completion success), ``on_error``
(worker completion failure). All four run on the worker's own
asyncio loop, so we can put events directly on the queue
without ``call_soon_threadsafe``.

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

logger = logging.getLogger("z4j.agent.taskiq.events")

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
    ) -> None:
        super().__init__()
        self._queue = queue
        self._redaction = redaction

    def _put(self, evt: Event) -> None:
        try:
            self._queue.put_nowait(evt)
        except asyncio.QueueFull:
            logger.warning(
                "z4j-taskiq: event queue full; dropping %s", evt.kind,
            )

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        self._put(self._build(EventKind.TASK_RECEIVED, message))
        return message

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        self._put(self._build(EventKind.TASK_STARTED, message))
        return message

    async def post_execute(self, message: TaskiqMessage, result: Any) -> None:
        # taskiq calls post_execute even on errors when on_error
        # isn't set; check ``result.is_err`` defensively.
        is_err = bool(getattr(result, "is_err", False))
        kind = EventKind.TASK_FAILED if is_err else EventKind.TASK_SUCCEEDED
        self._put(self._build(kind, message))

    async def on_error(
        self, message: TaskiqMessage, result: Any, exception: BaseException,  # noqa: ARG002
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
                try:
                    args = self._redaction.redact_args(tuple(args))
                    kwargs = self._redaction.redact_kwargs(kwargs)
                except Exception:  # noqa: BLE001
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
    middleware = Z4JTaskiqMiddleware(queue=queue, redaction=redaction)
    broker.add_middlewares(middleware)
    return middleware


__all__ = ["Z4JTaskiqMiddleware", "attach_to_broker"]
