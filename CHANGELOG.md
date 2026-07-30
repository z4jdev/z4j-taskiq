# Changelog

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
