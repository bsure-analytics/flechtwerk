import sys

from .cli import main

__all__: list[str] = []  # purely internal: `python -m flechtctl` is an entry point, not an API

sys.exit(main())
