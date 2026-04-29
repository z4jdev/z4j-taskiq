"""End-to-end dispatcher integration: real taskiq engine + bare dispatcher."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker  # noqa: E402

from z4j_bare.buffer import BufferStore  # noqa: E402
from z4j_bare.dispatcher import CommandDispatcher  # noqa: E402
from z4j_core.transport.frames import CommandFrame, CommandPayload  # noqa: E402

from z4j_taskiq import TaskiqEngineAdapter  # noqa: E402


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.fixture
async def broker():
    b = InMemoryBroker()

    @b.task
    async def send_email(to, *, template):
        return (to, template)

    await b.startup()
    yield b
    await b.shutdown()


@pytest.mark.asyncio
async def test_schedule_fire_end_to_end_through_dispatcher(
    broker, buf: BufferStore,
) -> None:
    engine = TaskiqEngineAdapter(broker=broker)
    dispatcher = CommandDispatcher(
        engines={"taskiq": engine},
        schedulers={},
        buffer=buf,
    )

    name = next(
        k for k in broker.get_all_tasks() if k.endswith(":send_email")
    )

    frame = CommandFrame(
        id="cmd_e2e_taskiq_01",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "schedule_id": "s1",
                "schedule_name": "nightly-emails",
                "task_name": name,
                "engine": "taskiq",
                "args": ["alice@example.com"],
                "kwargs": {"template": "welcome"},
                "fire_id": "f1",
            },
        ),
        hmac="deadbeef" * 8,
    )

    await dispatcher.handle(frame)

    results = [e for e in buf.drain(10) if e.kind == "command_result"]
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success", (
        f"got {parsed['payload'].get('error')!r}"
    )
    assert parsed["payload"]["result"]["engine"] == "taskiq"
    assert isinstance(parsed["payload"]["result"]["task_id"], str)
    assert len(parsed["payload"]["result"]["task_id"]) > 0
