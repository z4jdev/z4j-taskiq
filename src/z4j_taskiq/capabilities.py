"""Capability tokens for the taskiq engine adapter.

taskiq's broker-agnostic design means operations on existing tasks need
to be implemented per broker rather than once at the adapter layer.
Task submission is supported; cancel, retry, and bulk operations are not.
"""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset({"submit_task"})
"""Task submission is the only advertised data-plane action.

Reconciliation does not need an entry here - the brain calls
``reconcile_task`` directly without consulting capabilities (see
``z4j_bare.dispatcher`` for the bypass).
"""


__all__ = ["DEFAULT_CAPABILITIES"]
