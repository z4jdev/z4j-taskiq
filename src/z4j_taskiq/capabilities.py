"""Capability tokens for the taskiq engine adapter.

taskiq's broker-agnostic design means most "actions" need to be
implemented per-broker rather than once at the adapter layer. v1
ships discovery + reconciliation; cancel / retry / bulk operations
are deferred to v1.1 once the per-broker matrix is settled.
"""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset({"submit_task"})
"""No data-plane actions in v1.

Reconciliation does not need an entry here - the brain calls
``reconcile_task`` directly without consulting capabilities (see
``z4j_bare.dispatcher`` for the bypass).
"""


__all__ = ["DEFAULT_CAPABILITIES"]
