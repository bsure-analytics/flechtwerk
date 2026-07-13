#!/usr/bin/env python3
"""Celtic interlace ornament: six-strand wide flat plait."""

from _braid import run

if __name__ == "__main__":
    run(6, description="six-strand wide flat plait", amplitude_frac=0.28, period_factor=2.0, core_frac=0.085)
