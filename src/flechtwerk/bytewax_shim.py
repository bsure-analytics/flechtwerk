"""Bytewax → fretworx redirect shim.

This file is NOT used at runtime from the fretworx package directly.
It is copied into the Docker image to replace bytewax/run/__main__.py,
making `python -m bytewax.run` invoke fretworx transparently.

See the Dockerfile for the installation step.
"""
import sys

from fretworx.__main__ import main

main()
sys.exit(0)
