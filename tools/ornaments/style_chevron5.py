#!/usr/bin/env python3
"""Celtic interlace ornament: five-strand chevron (dense sharp braid)."""

from _braid import run

if __name__ == "__main__":
    run(5, wave="triangle", description="five-strand chevron braid", amplitude_frac=0.32, period_factor=1.7)
