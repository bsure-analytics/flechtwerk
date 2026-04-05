"""State store port and adapters (RocksDB, in-memory)."""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class StateEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime and set.

    Note: tuples are natively serialized as JSON arrays and round-trip as lists.
    This is acceptable — no business logic depends on the tuple/list distinction.
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        if isinstance(obj, (set, frozenset)):
            return {"__type__": "set", "value": sorted(obj, key=str)}
        return super().default(obj)


def state_decoder_hook(obj: dict) -> Any:
    """JSON object hook that restores datetime and set."""
    type_tag = obj.get("__type__")
    if type_tag == "datetime":
        return datetime.fromisoformat(obj["value"])
    if type_tag == "set":
        return set(obj["value"])
    return obj


class StateStore(ABC):
    """Port: persistent key-value state store."""

    @abstractmethod
    def get(self, key: str) -> dict[str, Any] | None:
        ...

    @abstractmethod
    def put(self, key: str, state: dict[str, Any]) -> None:
        ...

    @abstractmethod
    def delete(self, key: str) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...


class RocksDBStateStore(StateStore):
    """Adapter: RocksDB-backed state store.

    State values are JSON-serialized with custom encoding for datetime/set/tuple.
    Every put() writes to the RocksDB WAL immediately — no periodic snapshots.
    """

    def __init__(self, path: str):
        from rocksdict import Rdict

        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        db_path = str(self.path / "state.db")
        self.db = Rdict(db_path)
        log.info("Opened RocksDB state store at %s", db_path)

    def get(self, key: str) -> dict[str, Any] | None:
        try:
            raw = self.db[key]
        except KeyError:
            return None
        return json.loads(raw, object_hook=state_decoder_hook)

    def put(self, key: str, state: dict[str, Any]) -> None:
        raw = json.dumps(state, cls=StateEncoder, sort_keys=True)
        self.db[key] = raw

    def delete(self, key: str) -> None:
        try:
            del self.db[key]
        except KeyError:
            pass

    def close(self) -> None:
        self.db.close()
        log.info("Closed RocksDB state store at %s", self.path)


class InMemoryStateStore(StateStore):
    """Adapter: in-memory state store for testing."""

    def __init__(self):
        self.store: dict[str, dict[str, Any]] = {}

    def get(self, key: str) -> dict[str, Any] | None:
        raw = self.store.get(key)
        if raw is None:
            return None
        # Round-trip through JSON to match RocksDB behavior (datetime/set encoding)
        serialized = json.dumps(raw, cls=StateEncoder, sort_keys=True)
        return json.loads(serialized, object_hook=state_decoder_hook)

    def put(self, key: str, state: dict[str, Any]) -> None:
        self.store[key] = state

    def delete(self, key: str) -> None:
        self.store.pop(key, None)

    def close(self) -> None:
        pass
