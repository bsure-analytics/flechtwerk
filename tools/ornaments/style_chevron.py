#!/usr/bin/env python3
"""Celtic interlace ornament: three-strand chevron (sharp triangular braid)."""

from _braid import run

if __name__ == "__main__":
    run(3, wave="triangle", description="three-strand chevron braid", amplitude_frac=0.34, period_factor=1.5)
