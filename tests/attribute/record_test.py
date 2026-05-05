import copy
import pickle

import pytest

from fretworx.attribute import (
    INT,
    MissingAttributeError,
    OptionalAttribute,
    Record,
    RequiredAttribute,
    STR,
)


COUNT = RequiredAttribute("count", INT)
NAME = RequiredAttribute("name", STR)
LABEL = OptionalAttribute("label", STR)
MAYBE_COUNT = OptionalAttribute("count", INT)


# --- construction ---


def test_construct_empty():
    d = Record()
    assert d.raw == {}


def test_construct_from_mapping():
    d = Record({"count": "42"})
    assert d.raw == {"count": "42"}


def test_construct_copies_input():
    """The input mapping is shallow-copied so external mutation doesn't leak in."""
    raw = {"count": "42"}
    d = Record(raw)
    raw["count"] = "100"
    assert d.raw["count"] == "42"


def test_construct_from_another_dict():
    """A Record can be constructed from another Record instance — the underlying
    `data` dicts are independent (shallow-copied)."""
    a = Record({"count": "42"})
    b = Record(a)
    assert b.raw == a.raw
    a.raw["count"] = "100"
    assert b.raw["count"] == "42"


def test_construct_from_subclass_to_subclass():
    """A subclass can be constructed from another subclass instance."""
    class A(Record):
        pass

    class B(Record):
        pass

    a = A({"x": 1})
    b = B(a)
    assert isinstance(b, B)
    assert b.raw == {"x": 1}


# --- __getitem__ ---


def test_getitem_validates_and_returns():
    d = Record({"count": 42})
    assert d[COUNT] == 42


def test_getitem_raises_on_wire_type_mismatch():
    """If the wire value isn't an instance of V, the type-validating decoder asserts."""
    d = Record({"count": "42"})  # wire is str but COUNT is RequiredAttribute[int]
    with pytest.raises(AssertionError):
        _ = d[COUNT]


def test_getitem_missing_raises():
    d = Record()
    with pytest.raises(MissingAttributeError):
        _ = d[COUNT]


def test_getitem_null_raises():
    d = Record({"count": None})
    with pytest.raises(MissingAttributeError):
        _ = d[COUNT]


# --- __setitem__ ---


def test_setitem_validates_and_stores():
    d = Record()
    d[COUNT] = 42
    assert d.raw == {"count": 42}


def test_setitem_raises_on_value_type_mismatch():
    """The type-validating encoder asserts if the value isn't of the expected type."""
    d = Record()
    with pytest.raises(AssertionError):
        d[COUNT] = "42"  # type: ignore[assignment]


def test_setitem_overwrites_existing():
    d = Record({"count": 1})
    d[COUNT] = 99
    assert d.raw == {"count": 99}


def test_setitem_optional_stores_none_without_encode():
    """`OptionalAttribute` accepts `None` — stored as `null`, encoder is bypassed."""
    d = Record()
    d[LABEL] = None
    assert d.raw == {"label": None}


def test_setitem_required_rejects_none():
    """Writing `None` to a `RequiredAttribute` is a type bug — fail loudly."""
    d = Record()
    with pytest.raises(ValueError, match="cannot assign None to required"):
        d[COUNT] = None  # type: ignore[assignment]


def test_construct_optional_attribute_with_none_stores_null():
    """The `Record(...)` constructor mirrors `__setitem__` for Attribute keys."""
    d = Record({LABEL: None})
    assert d.raw == {"label": None}


def test_construct_required_attribute_with_none_raises():
    d = Record()
    with pytest.raises(ValueError, match="cannot assign None to required"):
        Record({COUNT: None})  # type: ignore[dict-item]


# --- __delitem__ ---


def test_delitem_removes_key():
    d = Record({"count": 42, "name": "x"})
    del d[COUNT.optional]
    assert d.raw == {"name": "x"}


def test_delitem_missing_raises_keyerror():
    d = Record()
    with pytest.raises(KeyError):
        del d[COUNT.optional]


# --- __contains__ ---


def test_contains_true_when_present():
    d = Record({"count": "42"})
    assert COUNT in d


def test_contains_false_when_absent():
    d = Record()
    assert COUNT not in d


# --- __len__, __iter__, __bool__ ---


def test_len():
    d = Record({"a": 1, "b": 2, "c": 3})
    assert len(d) == 3


def test_iter_yields_name_strings():
    """Iteration yields the raw string keys of the wrapped data dict."""
    d = Record({"count": "42", "name": "x"})
    assert list(d) == ["count", "name"]


def test_bool_empty_is_false():
    assert bool(Record()) is False


def test_bool_nonempty_is_true():
    assert bool(Record({"a": 1})) is True


# --- __eq__, __hash__, __repr__ ---


def test_eq_same_data():
    assert Record({"a": 1}) == Record({"a": 1})


def test_eq_different_data():
    assert Record({"a": 1}) != Record({"a": 2})


def test_eq_to_plain_dict_is_not_equal():
    """A Record is its own type; not equal to a plain dict, even with same contents."""
    assert Record({"a": 1}) != {"a": 1}


def test_eq_across_subclasses_is_not_equal():
    """Equality is type-strict: a Record subclass is not equal to a different subclass
    (or to the base Record) even with the same wrapped data."""
    class A(Record):
        pass

    class B(Record):
        pass

    assert A({"x": 1}) != B({"x": 1})
    assert A({"x": 1}) != Record({"x": 1})


def test_unhashable():
    """Like `dict`, `Record` is unhashable (mutable)."""
    with pytest.raises(TypeError):
        hash(Record())


def test_repr_includes_data():
    d = Record({"count": "42"})
    assert "count" in repr(d)
    assert "Record" in repr(d)


# --- copy ---


def test_copy_is_independent():
    d = Record({"count": 42})
    c = copy.copy(d)
    c[COUNT] = 99
    assert d[COUNT] == 42


def test_deepcopy_is_independent():
    d = Record({"nested": {"a": 1}})
    c = copy.deepcopy(d)
    c.raw["nested"]["a"] = 99
    assert d.raw["nested"]["a"] == 1


# --- pickle ---


def test_pickle_round_trip():
    d = Record({"count": "42", "name": "x"})
    restored = pickle.loads(pickle.dumps(d))
    assert restored == d
    assert restored.raw == d.raw


def test_pickle_legacy_dict_subclass_format():
    """Legacy changelog data was pickled when Event/Config/State were `dict`
    subclasses. Those bytes (NEWOBJ + SETITEMS with str keys) must restore
    cleanly into the new Record-based class, picking up the items via the
    str-key compat path in __setitem__.

    Construct the legacy bytes by hand using the pickle opcodes — exactly
    what the old code would have produced for an `Event({"foo": "bar"})`.

    TODO(legacy-pickle-compat): delete this test alongside the
    `Record.__new__` / `Record.__setitem__` shims once all changelog topics in
    every environment have been fully replaced with new-format entries.
    """
    import io
    legacy_bytes = pickle.dumps(LegacyEventDictSubclass({"foo": "bar", "count": 42}))
    # Redirect class lookup at load time: simulate that on disk we have bytes
    # naming "Event" and the module's runtime definition is now Record-based.

    class CompatUnpickler(pickle.Unpickler):
        def find_class(self, module: str, name: str):
            if name == LegacyEventDictSubclass.__name__:
                return Event
            return super().find_class(module, name)

    restored = CompatUnpickler(io.BytesIO(legacy_bytes)).load()
    assert isinstance(restored, Event)
    assert restored.raw == {"foo": "bar", "count": 42}


# Module-level legacy class to enable pickling in `test_pickle_legacy_dict_subclass_format`
# (local classes can't be pickled).
class LegacyEventDictSubclass(dict):
    pass


# --- get() — OptionalAttribute only ---


def test_get_returns_decoded_value():
    d = Record({"count": 42})
    assert d.get(MAYBE_COUNT) == 42


def test_get_returns_default_when_missing():
    d = Record()
    assert d.get(MAYBE_COUNT, 0) == 0


def test_get_returns_default_when_null():
    d = Record({"count": None})
    assert d.get(MAYBE_COUNT, 0) == 0


def test_get_returns_none_default_when_missing():
    d = Record()
    assert d.get(MAYBE_COUNT) is None


# --- pop() — OptionalAttribute only ---


def test_pop_returns_decoded_and_removes():
    d = Record({"count": 42, "name": "x"})
    assert d.pop(MAYBE_COUNT) == 42
    assert d.raw == {"name": "x"}


def test_pop_missing_with_default_returns_default():
    d = Record()
    assert d.pop(MAYBE_COUNT, 0) == 0


def test_pop_missing_without_default_raises():
    d = Record()
    with pytest.raises(KeyError):
        d.pop(MAYBE_COUNT)


def test_pop_returns_none_for_explicit_null_and_removes():
    """A stored `None` pops back as `None`, key is removed (decode is skipped)."""
    d = Record({"count": None, "name": "x"})
    assert d.pop(MAYBE_COUNT) is None
    assert d.raw == {"name": "x"}


# --- update() ---


def test_update_merges_data_from_another_dict():
    a = Record({"count": "1", "name": "x"})
    b = Record({"count": "2", "label": "lbl"})
    a.update(b)
    assert a.raw == {"count": "2", "name": "x", "label": "lbl"}


# --- subclass behavior ---


class Event(Record):
    pass


def test_subclass_preserves_type_via_copy():
    e = Event({"count": "42"})
    c = copy.copy(e)
    assert isinstance(c, Event)


def test_subclass_preserves_type_via_deepcopy():
    e = Event({"count": "42"})
    c = copy.deepcopy(e)
    assert isinstance(c, Event)


def test_subclass_preserves_type_via_pickle():
    e = Event({"count": "42"})
    restored = pickle.loads(pickle.dumps(e))
    assert isinstance(restored, Event)


def test_subclass_repr_uses_subclass_name():
    e = Event({"count": "42"})
    assert "Event" in repr(e)
    assert "Record(" not in repr(e)
