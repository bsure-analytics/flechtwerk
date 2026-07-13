#!/usr/bin/env python3
"""Celtic interlace ornament: twill-weave diagonal lattice."""

from _weave import run

if __name__ == "__main__":
    run("twill", wefts=4, warps=8, description="twill-weave diagonal lattice")
