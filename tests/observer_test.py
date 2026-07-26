"""Tests for flechtwerk.observer.PrometheusObserver."""
from prometheus_client import CollectorRegistry

from flechtwerk.metrics import Metrics
from flechtwerk.observer import Observer, PrometheusObserver


def make_observer() -> tuple[PrometheusObserver, CollectorRegistry]:
    registry = CollectorRegistry()
    metrics = Metrics()
    metrics.max_poll_records = 500
    metrics.metrics_labels = {"datasource": "ds1", "stage": "extractor"}
    metrics.registry = registry

    observer = PrometheusObserver()
    observer.metrics = metrics
    observer.metrics_labels = {"datasource": "ds1", "stage": "extractor"}
    return observer, registry


def test_no_op_observer_does_nothing():
    observer = Observer()
    observer.message_in("t")
    observer.message_in_bytes("t", 128)
    observer.message_out("t")
    observer.message_out_bytes("t", 128)
    observer.state_record_bytes(128)
    observer.transaction_committed()
    observer.active_configs(3)
    with observer.dispatch_scope():
        pass
    with observer.batch_scope(5):
        pass
    with observer.poll_cycle_scope():
        pass


def test_message_in_increments_counter():
    observer, registry = make_observer()
    observer.message_in("topic-a")
    observer.message_in("topic-a")
    observer.message_in("topic-b")
    assert registry.get_sample_value(
        "flechtwerk_messages_in_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "topic-a"},
    ) == 2
    assert registry.get_sample_value(
        "flechtwerk_messages_in_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "topic-b"},
    ) == 1


def test_message_out_increments_counter():
    observer, registry = make_observer()
    observer.message_out("out-topic")
    assert registry.get_sample_value(
        "flechtwerk_messages_out_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "out-topic"},
    ) == 1


# --- Byte histograms and their high-water-mark gauges ---


def test_message_in_bytes_observes_per_topic():
    observer, registry = make_observer()
    observer.message_in_bytes("topic-a", 300)
    observer.message_in_bytes("topic-b", 5_000)
    a = {"datasource": "ds1", "stage": "extractor", "topic": "topic-a"}
    b = {"datasource": "ds1", "stage": "extractor", "topic": "topic-b"}
    assert registry.get_sample_value("flechtwerk_message_in_bytes_sum", a) == 300
    assert registry.get_sample_value("flechtwerk_message_in_bytes_bucket", {**a, "le": "1024.0"}) == 1
    assert registry.get_sample_value("flechtwerk_message_in_bytes_bucket", {**a, "le": "256.0"}) == 0
    assert registry.get_sample_value("flechtwerk_message_in_bytes_sum", b) == 5_000
    assert registry.get_sample_value("flechtwerk_message_in_bytes_bucket", {**b, "le": "16384.0"}) == 1


def test_message_out_bytes_observes_per_topic():
    observer, registry = make_observer()
    observer.message_out_bytes("out-topic", 2_000)
    labels = {"datasource": "ds1", "stage": "extractor", "topic": "out-topic"}
    assert registry.get_sample_value("flechtwerk_message_out_bytes_count", labels) == 1
    assert registry.get_sample_value("flechtwerk_message_out_bytes_sum", labels) == 2_000
    assert registry.get_sample_value("flechtwerk_message_out_bytes_bucket", {**labels, "le": "4096.0"}) == 1
    assert registry.get_sample_value("flechtwerk_message_out_bytes_bucket", {**labels, "le": "1024.0"}) == 0


def test_state_record_bytes_brackets_the_record_ceiling():
    """The ladder must resolve the danger zone: the crash that motivated this
    metric wrote 1 014 623 bytes, which has to read as "past 917 504, under the
    1 MiB ceiling" — not as an anonymous sample in the last decade.

    The `le` for the ceiling is spelled `1.048576e+06`: prometheus_client
    formats bounds Go-style and switches to scientific notation above 1e6. The
    exact string is what an operator types into PromQL, so pin it here.
    """
    observer, registry = make_observer()
    observer.state_record_bytes(1_014_623)
    labels = {"datasource": "ds1", "stage": "extractor"}
    assert registry.get_sample_value("flechtwerk_state_record_bytes_count", labels) == 1
    assert registry.get_sample_value("flechtwerk_state_record_bytes_sum", labels) == 1_014_623
    assert registry.get_sample_value(
        "flechtwerk_state_record_bytes_bucket", {**labels, "le": "917504.0"},
    ) == 0
    assert registry.get_sample_value(
        "flechtwerk_state_record_bytes_bucket", {**labels, "le": "1.048576e+06"},
    ) == 1


def test_state_record_bytes_beyond_the_top_bound_lands_only_in_inf():
    """A raised-limit deployment's oversized record still counts — the ladder's
    top finite bound is 4 MiB, and anything past it falls in +Inf alone."""
    observer, registry = make_observer()
    observer.state_record_bytes(5_000_000)
    labels = {"datasource": "ds1", "stage": "extractor"}
    assert registry.get_sample_value(
        "flechtwerk_state_record_bytes_bucket", {**labels, "le": "4.194304e+06"},
    ) == 0
    assert registry.get_sample_value(
        "flechtwerk_state_record_bytes_bucket", {**labels, "le": "+Inf"},
    ) == 1


def test_state_record_max_bytes_is_a_running_high_water_mark():
    """A last-value gauge would lose every peak between scrape instants, so the
    mark only ever climbs — it is the largest observation since process start."""
    observer, registry = make_observer()
    labels = {"datasource": "ds1", "stage": "extractor"}

    observer.state_record_bytes(100)
    assert registry.get_sample_value("flechtwerk_state_record_max_bytes", labels) == 100

    observer.state_record_bytes(50)
    assert registry.get_sample_value("flechtwerk_state_record_max_bytes", labels) == 100

    observer.state_record_bytes(200)
    assert registry.get_sample_value("flechtwerk_state_record_max_bytes", labels) == 200


def test_message_max_bytes_gauges_track_topics_independently():
    observer, registry = make_observer()
    observer.message_out_bytes("topic-a", 100)
    observer.message_out_bytes("topic-b", 900)
    observer.message_out_bytes("topic-a", 50)
    observer.message_in_bytes("topic-a", 700)
    a = {"datasource": "ds1", "stage": "extractor", "topic": "topic-a"}
    b = {"datasource": "ds1", "stage": "extractor", "topic": "topic-b"}
    assert registry.get_sample_value("flechtwerk_message_out_max_bytes", a) == 100
    assert registry.get_sample_value("flechtwerk_message_out_max_bytes", b) == 900
    # The in/out marks are separate series, not one shared tracker.
    assert registry.get_sample_value("flechtwerk_message_in_max_bytes", a) == 700
    assert registry.get_sample_value("flechtwerk_message_in_max_bytes", b) is None


def test_max_bytes_trackers_are_per_observer_instance():
    """The trackers must not be mutable class attributes — two observers over
    two registries would otherwise suppress each other's first observation."""
    first, first_registry = make_observer()
    second, second_registry = make_observer()
    labels = {"datasource": "ds1", "stage": "extractor"}

    first.state_record_bytes(500)
    first.message_out_bytes("t", 500)
    second.state_record_bytes(10)
    second.message_out_bytes("t", 10)

    assert second_registry.get_sample_value("flechtwerk_state_record_max_bytes", labels) == 10
    assert second_registry.get_sample_value(
        "flechtwerk_message_out_max_bytes", {**labels, "topic": "t"},
    ) == 10
    assert first_registry.get_sample_value("flechtwerk_state_record_max_bytes", labels) == 500


def test_transaction_committed_increments_counter():
    observer, registry = make_observer()
    observer.transaction_committed()
    observer.transaction_committed()
    assert registry.get_sample_value(
        "flechtwerk_transactions_committed_total",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 2


def test_active_configs_sets_gauge():
    observer, registry = make_observer()
    observer.active_configs(7)
    assert registry.get_sample_value(
        "flechtwerk_active_configs",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 7
    observer.active_configs(3)
    assert registry.get_sample_value(
        "flechtwerk_active_configs",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 3


def test_tokens_assigned_sets_gauge():
    observer, registry = make_observer()
    observer.tokens_assigned(8)
    assert registry.get_sample_value(
        "flechtwerk_tokens_assigned",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 8
    observer.tokens_assigned(0)  # revoked — hot standby
    assert registry.get_sample_value(
        "flechtwerk_tokens_assigned",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 0


def test_config_message_in_increments_counter():
    observer, registry = make_observer()
    observer.config_message_in("cfg")
    observer.config_message_in("cfg")
    assert registry.get_sample_value(
        "flechtwerk_config_messages_in_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "cfg"},
    ) == 2


def test_config_store_entries_sets_gauge():
    observer, registry = make_observer()
    observer.config_store_entries(9)
    observer.config_store_entries(4)
    assert registry.get_sample_value(
        "flechtwerk_config_store_entries",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 4


def test_config_store_restored_increments_counter_by_entries():
    observer, registry = make_observer()
    observer.config_store_restored(5)
    assert registry.get_sample_value(
        "flechtwerk_config_store_restored_entries_total",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 5


def test_dispatch_scope_records_histogram():
    observer, registry = make_observer()
    with observer.dispatch_scope():
        pass
    count = registry.get_sample_value(
        "flechtwerk_message_processing_seconds_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    assert count == 1


def test_batch_size_buckets_derive_from_max_poll_records():
    """The ladder ends at cap-1/cap so at-cap batches are one bucket subtraction."""
    observer, registry = make_observer()
    with observer.batch_scope(300):
        pass
    with observer.batch_scope(500):
        pass
    labels = {"datasource": "ds1", "stage": "extractor"}
    below_cap = registry.get_sample_value(
        "flechtwerk_batch_size_bucket", {**labels, "le": "499.0"},
    )
    at_cap = registry.get_sample_value(
        "flechtwerk_batch_size_bucket", {**labels, "le": "500.0"},
    )
    beyond_cap = registry.get_sample_value(
        "flechtwerk_batch_size_bucket", {**labels, "le": "1000.0"},
    )
    assert below_cap == 1
    assert at_cap == 2
    assert beyond_cap is None


def test_batch_scope_records_size_and_duration():
    observer, registry = make_observer()
    with observer.batch_scope(42):
        pass
    size_count = registry.get_sample_value(
        "flechtwerk_batch_size_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    duration_count = registry.get_sample_value(
        "flechtwerk_batch_processing_seconds_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    assert size_count == 1
    assert duration_count == 1


def test_poll_cycle_scope_records_histogram():
    observer, registry = make_observer()
    with observer.poll_cycle_scope():
        pass
    count = registry.get_sample_value(
        "flechtwerk_poll_cycle_seconds_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    assert count == 1


def test_keyring_key_loaded_sets_gauge_per_kid():
    observer, registry = make_observer()
    observer.keyring_key_loaded("prod-2026-07")
    assert registry.get_sample_value(
        "flechtwerk_keyring_keys_loaded",
        {"datasource": "ds1", "stage": "extractor", "kid": "prod-2026-07"},
    ) == 1


def test_secret_plaintext_read_increments_counter_per_scope():
    observer, registry = make_observer()
    observer.secret_plaintext_read("api_key")
    observer.secret_plaintext_read("api_key")
    assert registry.get_sample_value(
        "flechtwerk_secret_plaintext_reads_total",
        {"datasource": "ds1", "stage": "extractor", "scope": "api_key"},
    ) == 2


def test_secret_decrypted_increments_counter_per_scope_and_kid():
    observer, registry = make_observer()
    observer.secret_decrypted("api_key", "k1")
    assert registry.get_sample_value(
        "flechtwerk_secret_decrypts_total",
        {"datasource": "ds1", "stage": "extractor", "scope": "api_key", "kid": "k1"},
    ) == 1


def test_mqtt_connects_and_disconnects_increment_counters():
    observer, registry = make_observer()
    observer.mqtt_connected()
    observer.mqtt_connected()
    observer.mqtt_disconnected()
    assert registry.get_sample_value(
        "flechtwerk_mqtt_connects_total",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 2
    assert registry.get_sample_value(
        "flechtwerk_mqtt_disconnects_total",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 1


def test_mqtt_message_in_increments_counter_per_topic():
    observer, registry = make_observer()
    observer.mqtt_message_in("t/+/events")
    observer.mqtt_message_in("t/+/events")
    assert registry.get_sample_value(
        "flechtwerk_mqtt_messages_in_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "t/+/events"},
    ) == 2


def test_mqtt_message_dropped_increments_counter_per_reason():
    observer, registry = make_observer()
    observer.mqtt_message_dropped("t/+/events", "filtered")
    observer.mqtt_message_dropped("t/+/events", "poison")
    observer.mqtt_message_dropped("t/+/events", "poison")
    assert registry.get_sample_value(
        "flechtwerk_mqtt_messages_dropped_total",
        {"datasource": "ds1", "reason": "filtered", "stage": "extractor", "topic": "t/+/events"},
    ) == 1
    assert registry.get_sample_value(
        "flechtwerk_mqtt_messages_dropped_total",
        {"datasource": "ds1", "reason": "poison", "stage": "extractor", "topic": "t/+/events"},
    ) == 2


def test_mqtt_buffered_sets_gauge():
    observer, registry = make_observer()
    observer.mqtt_buffered("t/+/events", 5)
    observer.mqtt_buffered("t/+/events", 0)
    assert registry.get_sample_value(
        "flechtwerk_mqtt_buffered_messages",
        {"datasource": "ds1", "stage": "extractor", "topic": "t/+/events"},
    ) == 0
