# Changelog

All notable changes to `z4j-taskiq` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.2] - 2026-04-28

### Added

- **`z4j-taskiq` console script** + `python -m z4j_taskiq` module
  form. Both work and dispatch to the same code path. Subcommands:
  - `doctor` - check upstream `taskiq` library + adapter import + broker URL
  - `check` - alias for doctor
  - `status` - one-line: package presence + broker URL state
  - `version` - print z4j-taskiq version
  Engines are libraries (no agent runtime to manage), so the CLI is
  intentionally narrower than a framework's: no `run`, no `restart`.
  The framework's doctor (z4j-django, z4j-flask, z4j-fastapi) calls
  into these same probes automatically when taskiq is the detected
  engine.


## [1.0.1] - 2026-04-21

### Changed

- Lowered minimum Python version from 3.13 to 3.11. This package now supports Python 3.11, 3.12, 3.13, and 3.14.
- Documentation polish: standardized on ASCII hyphens across README, CHANGELOG, and docstrings for consistent rendering on PyPI.


## [1.0.0] - 2026-04

### Added

<!--
TODO: describe what ships in this first public release. One bullet per
capability. Examples:
- First public release.
- <Headline feature>
- <Second feature>
- N unit tests.
-->

- First public release.

## Links

- Repository: <https://github.com/z4jdev/z4j-taskiq>
- Issues: <https://github.com/z4jdev/z4j-taskiq/issues>
- PyPI: <https://pypi.org/project/z4j-taskiq/>

[Unreleased]: https://github.com/z4jdev/z4j-taskiq/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/z4jdev/z4j-taskiq/releases/tag/v1.0.0
