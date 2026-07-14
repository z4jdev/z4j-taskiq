"""z4j-taskiq - taskiq queue engine adapter for z4j."""

from __future__ import annotations

from z4j_taskiq.engine import TaskiqEngineAdapter
from z4j_taskiq.events import Z4JTaskiqMiddleware, attach_to_broker

try:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("z4j-taskiq")
except PackageNotFoundError:  # source checkout, no installed metadata
    from z4j_core.version import __version__  # type: ignore[no-redef]

__all__ = [
    "TaskiqEngineAdapter",
    "Z4JTaskiqMiddleware",
    "__version__",
    "attach_to_broker",
]
