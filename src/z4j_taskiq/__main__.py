"""``python -m z4j_taskiq`` - module entry point for the engine doctor."""

from __future__ import annotations

import sys

from z4j_taskiq.cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
