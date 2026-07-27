"""Kafka utilities and changelog restore."""
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

import aiokafka
from aiokafka import ConsumerRecord

from flechtwerk.attribute import Record

from .types import Event, IncomingMessage, InvalidMessageError

if TYPE_CHECKING:
    # `OnInvalidMessageFn` lives on the stage side, beside `ExtractStateKeyFn`
    # — importing it at runtime would close a stage → configs → kafka → stage
    # cycle, so the annotations below are quoted forward references.
    from .stage import OnInvalidMessageFn

log = logging.getLogger(__name__)

# Framework-internal on purpose: these are the shared building blocks of the
# runners and the config machinery, not an application-facing surface.
__all__: list[str] = []


# --- Utilities ---


def encode_json(value: bytes | str | Record) -> bytes:
    """Encode a payload to bytes for Kafka — one rule per accepted type.

    - ``bytes``: passed through untouched (pre-encoded by the caller).
    - ``str``: UTF-8 text, deliberately NOT JSON-quoted — a wire-format
      commitment: ``decode_key``'s exact mirror. JSON-quoting strings would
      remap every partition and state identity across a fleet.
    - ``Record``: canonical JSON — compact separators, sorted keys,
      ensure_ascii=False, allow_nan=False — so equal records produce
      identical bytes.

    Anything else raises TypeError. Application payloads are validated
    earlier, at `Message` construction (see `flechtwerk.types.Payload`);
    this check guards the framework's own call sites (`serialize`, the
    runners).
    """

    match value:
        case Record():
            return json.dumps(
                value.raw,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        case str():
            return value.encode("utf-8")
        case bytes():
            return value
        case _:
            raise TypeError(f"Expected bytes | str | Record, got {type(value).__name__}")


def datetime_to_millis(dt: datetime | None) -> int | None:
    """Convert a datetime to Kafka millisecond epoch, or None."""
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)


def millis_to_datetime(millis: int | None) -> datetime | None:
    """Convert Kafka millisecond epoch to a UTC datetime, or None."""
    if millis is None:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)


def decode_key(key: bytes | str | None) -> str:
    """Decode a Kafka message key to a string; missing keys become ``""``.

    Strict UTF-8: an ill-formed key raises ``UnicodeDecodeError`` (a
    ``ValueError``). Repairing it with ``errors="replace"`` — as this once did
    — would let a broken key flow silently into state identity, bucketing,
    changelog keys, and ``token_for`` ownership, where distinct broken keys
    can even collide on the same replacement characters.
    """
    return (key.decode("utf-8") if isinstance(key, bytes) else key) or ""


def decode_record[R: Record](value: bytes | str | None, cls: type[R]) -> R:
    """Decode a Kafka message value into a ``cls`` record.

    The CALLER names the type, because the wire carries none: the same bytes
    are a `Config` to a config-topic reader and an `Event` to a data-topic
    reader (see `flechtwerk.types.Payload`). Returning one fixed type would
    make every other call site copy the record just to relabel it.

    An empty or absent value decodes to ``cls.wrap({})`` — the one
    unambiguous case, meaning "empty value" and never "garbage". Everything
    undecodable raises a ``ValueError``, so a caller wraps exactly that one
    type into an `InvalidMessageError` for `Stage.on_invalid_message`:
      - non-UTF-8 bytes → ``UnicodeDecodeError``
      - invalid JSON → ``json.JSONDecodeError``
      - valid JSON that is not an object (scalar, array) → ``ValueError``
    """
    # Strict UTF-8 on purpose — json.loads(bytes) would sniff the encoding and
    # decode with errors="surrogatepass", letting ill-formed payloads (CESU-8
    # surrogates, UTF-16) through as lone-surrogate str values that crash
    # encode_json when a stage re-emits them, far from the source.
    raw_value = value.decode("utf-8") if isinstance(value, bytes) else value
    parsed = json.loads(raw_value) if raw_value else {}
    if not isinstance(parsed, dict):
        raise ValueError(f"JSON value decoded to {type(parsed).__name__}, expected an object")
    return cls.wrap(parsed)


def invalid_message_error(
        msg: ConsumerRecord[Any, Any],
        part: Literal["key", "value"],
        cause: ValueError,
) -> InvalidMessageError:
    """Wrap a strict-decode failure for `Stage.on_invalid_message`.

    Chains ``cause`` in as ``__cause__`` so the original
    ``UnicodeDecodeError`` / ``json.JSONDecodeError`` / non-dict
    ``ValueError`` reaches the traceback of a raising handler — the default
    policy, and the only announcement the framework makes.
    """
    error = InvalidMessageError(
        part=part,
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
        key=msg.key,
        value=msg.value,
    )
    error.__cause__ = cause
    return error


def decode_key_mediated(
        msg: ConsumerRecord[Any, Any],
        on_invalid: "OnInvalidMessageFn",
) -> str | None:
    """Strictly decode a record's key, routing a failure through ``on_invalid``.

    Returns ``None`` when the handler skipped the record. A handler that
    returns a ``Record`` gets the teaching ``TypeError``: a key is state
    identity — bucketing, changelog keys, and ``token_for`` ownership all
    derive from it — and identity must not be synthesized for a record whose
    identity is unreadable. Key failures accept raise or skip only.
    """
    try:
        return decode_key(msg.key)
    except ValueError as e:
        error = invalid_message_error(msg, "key", e)
        if on_invalid(error) is not None:
            raise TypeError(
                "on_invalid_message returned a Record for an undecodable key at"
                f" {msg.topic}/{msg.partition}/{msg.offset}: a key is state identity"
                " — bucketing, changelog keys, and token_for ownership all derive"
                " from it — and identity must not be synthesized for a record whose"
                " identity is unreadable. Return None to skip the record, or"
                " re-raise to crash."
            ) from e
        return None


def decode_event_mediated(
        msg: ConsumerRecord[Any, Any],
        on_invalid: "OnInvalidMessageFn",
) -> Event | None:
    """Strictly decode a record's value, routing a failure through ``on_invalid``.

    Returns ``None`` when the handler skipped the record, and the
    handler-supplied substitute — retyped as an `Event`, because the CALL
    SITE assigns the semantic type — when it substituted one.

    `Event` and not ``cls`` on purpose: this helper's product is always an
    `IncomingMessage.value`, which is an `Event` by definition, whatever
    topic the record came from.
    """
    try:
        return decode_record(msg.value, Event)
    except ValueError as e:
        substitute = on_invalid(invalid_message_error(msg, "value", e))
        if substitute is None:
            return None
        if not isinstance(substitute, Record):
            # Teaching error at the decision site (the `Message.__post_init__`
            # style): `Event(raw_dict)` would take the typed-literal path and
            # die inside `Record.__init__` with an opaque AttributeError.
            raise TypeError(
                "on_invalid_message must return a Record or None, got"
                f" {type(substitute).__name__}. Wrap a raw dict in Event.wrap(...) —"
                " the call site assigns the semantic type."
            ) from e
        return Event(substitute)


def parse_message(
        msg: ConsumerRecord[Any, Any],
        on_invalid: "OnInvalidMessageFn",
) -> IncomingMessage | None:
    """Parse an aiokafka ConsumerRecord into an IncomingMessage.

    Decoding is strict and mediated by ``on_invalid`` (see
    `Stage.on_invalid_message`); ``None`` means the handler skipped the
    record, and a raising handler — the default — propagates.

    The key is decoded FIRST and a key failure short-circuits: the value is
    never attempted, so one broken record costs one handler call, not two.
    """
    key = decode_key_mediated(msg, on_invalid)
    if key is None:
        return None
    value = decode_event_mediated(msg, on_invalid)
    if value is None:
        return None
    return IncomingMessage(
        key=key,
        offset=msg.offset,
        partition=msg.partition,
        timestamp=millis_to_datetime(msg.timestamp),
        topic=msg.topic,
        value=value,
    )


# --- Reading topics to their end ---


def is_tombstone(raw: bytes | str | None) -> bool:
    """True for an empty Kafka value or a serialized falsy record.

    Covers ``b""``/``None`` (a real Kafka tombstone) and ``b"{}"`` (a falsy
    Record serialized to JSON). Accepts ``str`` like the decode helpers —
    aiokafka delivers bytes, test doubles may carry strings.
    """
    return not raw or raw in (b"{}", "{}")


async def read_to_end(
    consumer: aiokafka.AIOKafkaConsumer,
    tps: list[aiokafka.TopicPartition],
    apply: Callable[[ConsumerRecord[Any, Any]], Awaitable[None]],
) -> int:
    """Read the given partitions from the beginning to their current end.

    Uses manual partition assignment (no consumer group) on an
    already-started consumer. Reads to the end offsets captured at entry —
    under isolation_level="read_committed" that is the last stable offset,
    so records of in-flight transactions are never applied. Leaves the
    consumer assigned to ``tps`` and positioned at the captured end offsets,
    so the caller can keep polling for later records seamlessly.

    Returns the number of records passed to ``apply``.
    """
    consumer.assign(tps)
    await consumer.seek_to_beginning(*tps)
    end_offsets = await consumer.end_offsets(tps)

    count = 0
    pending = {tp for tp in tps if end_offsets[tp] > 0}
    while pending:
        records = await consumer.getmany(*pending, timeout_ms=2000)
        for msgs in records.values():
            for msg in msgs:
                await apply(msg)
                count += 1
        # An empty poll is not end-of-log — broker stalls and fetch backoff
        # yield empty results too. Only the fetch position reaching the end
        # offset captured at entry terminates a partition's read.
        pending = {tp for tp in pending if await consumer.position(tp) < end_offsets[tp]}
    return count


async def topic_partitions(
    consumer: aiokafka.AIOKafkaConsumer,
    topics: list[str],
) -> list[aiokafka.TopicPartition]:
    """Prime metadata and return every partition of every topic in ``topics``.

    Shared by the config bootstrap (`configs.bootstrap_config_store`) and the
    secrets scan (`secrets.scan_config_topics`), factoring out the
    `consumer._client.set_topics` private-API coupling (no fully public API
    primes the consumer's own metadata cache; the integration tests under
    tests/integration/ lock this down against aiokafka upgrades). `restore_changelog`
    keeps its own copy of the same priming for its single-topic, partition-subset
    case. Unknown topics contribute no partitions; a caller that must fail on a
    missing topic checks the returned set itself.
    """
    await consumer._client.set_topics(list(topics))
    return [
        aiokafka.TopicPartition(topic, partition)
        for topic in topics
        for partition in sorted(consumer.partitions_for_topic(topic) or ())
    ]


# --- Changelog restore ---


async def restore_changelog(
    consumer: aiokafka.AIOKafkaConsumer,
    topic: str,
    put_raw: Callable[[str, bytes], Awaitable[None]],
    delete: Callable[[str], Awaitable[None]],
    partitions: set[int] | None = None,
) -> int:
    """Read a compacted changelog topic (or a subset of its partitions) to rebuild state.

    Uses manual partition assignment (no consumer group). The consumer
    must already be started with group_id=None. Reads to the end offsets
    captured at entry — under isolation_level="read_committed" that is the
    last stable offset, so records of in-flight transactions are never
    restored. Callers restoring a task partition must fence the previous
    owner (InitProducerId via producer.start()) *before* calling this, so
    that owner's pending transaction is aborted and the captured end offset
    is final.

    Args:
        consumer: An already-started AIOKafkaConsumer (group_id=None).
        topic: Changelog topic name.
        put_raw: async callable(key, raw_bytes) to store wire bytes for a key.
            Per-key deduplication happens at the storage layer (RocksDB
            overwrites earlier writes for the same key on disk), so memory
            usage stays bounded by the inner store's cache, not by the topic
            size. Deserialization is deferred to the first `get()` for the
            key — keys that are never read by the running stage never pay
            the deserialize cost.
        delete: async callable(key) to remove a state entry.
        partitions: Restrict the restore to these partition numbers.
            None restores every partition of the topic.

    Returns:
        Number of records processed.
    """
    subset = partitions is not None
    if partitions is None:
        # Prime the consumer's internal cluster metadata for this topic so
        # partitions_for_topic() returns data. No fully public API achieves this:
        # consumer.topics() / fetch_all_metadata() returns a *separate* ClusterMetadata
        # object that doesn't update the consumer's own cache, and assign() requires
        # the partition set we're about to fetch. `_client.set_topics()` is a public
        # method on AIOKafkaClient (the `_client` attribute is the only underscore).
        # The integration tests under tests/integration/ lock down this
        # coupling against aiokafka upgrades.
        await consumer._client.set_topics([topic])
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            log.info("No partitions found for changelog topic %s", topic)
            return 0

    async def apply(msg: ConsumerRecord[Any, Any]) -> None:
        # Tombstones delete; anything else is wire bytes — pass through to
        # the inner store.
        #
        # No `on_invalid_message` mediation here, deliberately: the changelog
        # is framework-owned (every key is written from a `str`), so an
        # undecodable key means foreign writes or corruption — an
        # unrecoverable data error to crash on and then reset, not an
        # application policy question. Same reason `state.deserialize` has no
        # fallback.
        key = decode_key(msg.key)
        if is_tombstone(msg.value):
            await delete(key)
        else:
            await put_raw(key, msg.value)

    tps = [aiokafka.TopicPartition(topic, p) for p in sorted(partitions)]
    count = await read_to_end(consumer, tps, apply)

    log.info("Restored %d state entries from %s%s", count, topic,
             f" partitions {sorted(partitions)}" if subset else "")
    return count
