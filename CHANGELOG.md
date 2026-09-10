# Changelog

## 1.11.0 (2026-09-10)

* Align runtime version metadata and sibling dependency floors with the coordinated 1.11.0 release.

## 1.10.0 (2026-08-28)

* Carried with the coordinated fleet release. No behaviour changed.

## 1.9.1 (2026-08-27)

* Carried with the coordinated fleet release. No adapter behaviour changed.

## 1.9.0 (2026-08-25)

* Submit and result reconciliation now execute on the live TaskIQ broker's
  owning event loop when the z4j agent runs on its separate background loop.
  Middleware wiring captures that loop during broker startup, and callers
  installing from another startup hook can pass `broker_loop` explicitly.
  An adapter whose broker owner has not been bound fails those asynchronous
  operations closed rather than touching loop-owned clients from the agent
  loop.
* Broker lifecycle handlers now bind and conditionally unbind ownership across
  TaskIQ 0.11/0.12 and `InMemoryBroker`. Conflicting attachments fail closed,
  while z4j capture-hook failures are redacted and dropped without blocking
  TaskIQ enqueue or execution.
* Submit now fails before enqueue when a named queue, ETA, or priority override
  cannot be honored by TaskIQ's broker-independent API. An omitted queue and
  z4j's canonical logical `default` queue both continue to use the configured
  broker default.
* Version bumped as part of the coordinated 1.9.0 fleet release, so every
  package in a deployment agrees on its peers.

## 1.8.0 (2026-07-23)

* Events hand across event loops safely and failures are no longer double-counted; applied the low-tier sweep fixes.
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.7.0 (2026-07-07)

* Lifecycle-events capability corrected (the middleware emits received / started / succeeded / failed, not `retried`).
* Declares its `z4j-bare` dependency (the console script imports it).
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: TaskIQ engine adapter. Async-native; middleware hooks.
