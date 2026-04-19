"""Fretworx — async stream processing framework for Kafka."""
from __future__ import annotations

# Load .env before any framework module evaluates a module-level os.getenv().
# fretworx/extractor.py (imported below) reads POLL_INTERVAL_SECONDS at import
# time; without this, dotenv overrides would never take effect.
from dotenv import load_dotenv

load_dotenv()

from .extractor import Extractor  # noqa: E402
from .transformer import Transformer  # noqa: E402
from .types import Config, Event, IncomingMessage, Message, State  # noqa: E402

__all__ = [
    "Config",
    "Event",
    "Extractor",
    "IncomingMessage",
    "Message",
    "State",
    "Transformer",
]
