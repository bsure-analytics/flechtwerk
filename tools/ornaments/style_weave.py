#!/usr/bin/env python3
"""Celtic interlace ornament: plain-weave basket lattice."""

from _weave import run

if __name__ == "__main__":
    run("plain", wefts=4, warps=6, description="plain-weave basket lattice")
