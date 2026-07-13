#!/usr/bin/env python3
"""Celtic interlace ornament: four-strand round braid."""

from _braid import run

if __name__ == "__main__":
    run(4, description="four-strand round braid", amplitude_frac=0.34, period_factor=1.5)
