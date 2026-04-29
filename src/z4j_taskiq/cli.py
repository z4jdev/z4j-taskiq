"""z4j-taskiq CLI: ``z4j-taskiq doctor | check | status | version``."""

from __future__ import annotations

from z4j_bare.cli import make_engine_main

# Taskiq supports multiple backends; broker is configured via
# ``TaskiqBroker`` instances in code, not env vars. The framework's
# doctor checks the resolved broker instead.
main = make_engine_main(
    "taskiq",
    upstream_package="taskiq",
    broker_env=None,
)


__all__ = ["main"]
