# z4j-taskiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiq.svg)](https://pypi.org/project/z4j-taskiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiq.svg)](https://pypi.org/project/z4j-taskiq/)
[![License](https://img.shields.io/pypi/l/z4j-taskiq.svg)](https://github.com/z4jdev/z4j-taskiq/blob/main/LICENSE)

The TaskIQ engine adapter for [z4j](https://z4j.com).

Streams TaskIQ task lifecycle events to the z4j brain and
accepts control actions (retry, cancel, bulk retry, purge) from
the dashboard. Pair with z4j-taskiqscheduler to surface
TaskIQ scheduler jobs.

## Install

```bash
pip install z4j-taskiq z4j-taskiqscheduler
```

## Pairs with

- [`z4j-taskiqscheduler`](https://github.com/z4jdev/z4j-taskiqscheduler) — schedule adapter for taskiq-scheduler

## Documentation

Full docs at [z4j.dev/engines/taskiq/](https://z4j.dev/engines/taskiq/).

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-taskiq/
- Issues: https://github.com/z4jdev/z4j-taskiq/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
