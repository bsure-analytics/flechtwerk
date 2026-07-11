from datetime import datetime, timezone

import pytest

from flechtwerk.attribute import (
    Attribute,
    Codec,
    DATETIME,
    INT,
    LIST,
    STR,
)


def test_attribute_is_concrete_and_required_by_default():
    attr = Attribute("count", INT)
    assert attr.optional is False
    assert attr.required is True


def test_optional_attribute():
    attr = Attribute("count", INT, optional=True)
    assert attr.optional is True
    assert attr.required is False


def test_required_property_inverts_optional():
    assert Attribute("c", INT).required is True
    assert Attribute("c", INT, optional=True).required is False


def test_optional_is_keyword_only():
    """`optional` must be passed by keyword — no positional boolean trap."""
    with pytest.raises(TypeError):
        Attribute("count", INT, True)  # type: ignore[misc]


def test_codec_drives_validation():
    """The supplied codec asserts on type mismatch."""
    attr = Attribute("name", STR)
    assert attr.codec.decode("hello") == "hello"
    with pytest.raises(AssertionError):
        attr.codec.decode(42)
    with pytest.raises(AssertionError):
        attr.codec.encode(42)


def test_int_codec_rejects_bool():
    """Exact-type check: bool is not int even though `isinstance(True, int)` is True."""
    attr = Attribute("count", INT)
    with pytest.raises(AssertionError):
        attr.codec.encode(True)
    with pytest.raises(AssertionError):
        attr.codec.decode(True)


def test_datetime_codec_round_trip():
    attr = Attribute("ts", DATETIME)
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    encoded = attr.codec.encode(dt)
    assert encoded == "2024-06-15T14:30:00Z"
    assert attr.codec.decode(encoded) == dt


def test_name_attribute():
    attr = Attribute("count", INT)
    assert attr.name == "count"


def test_attribute_is_not_a_str():
    """An attribute is a distinct type — it does not subclass str."""
    attr = Attribute("count", INT)
    assert not isinstance(attr, str)


def test_attribute_does_not_compare_equal_to_string():
    attr = Attribute("count", INT)
    assert attr != "count"


def test_equality_is_name_keyed_independent_of_optional_and_codec():
    """Identity is `(type, name)` — `optional` and codec don't affect it, so the
    required and optional handles for one wire key are the same dict/set key."""
    assert Attribute("x", STR) == Attribute("x", STR, optional=True)
    assert hash(Attribute("x", STR)) == hash(Attribute("x", STR, optional=True))
    # Composite codecs are rebuilt per call (LIST(STR) is not LIST(STR) by
    # identity); name-keyed equality keeps "same field" attributes equal anyway.
    assert LIST(STR) != LIST(STR)
    assert Attribute("tags", LIST(STR)) == Attribute("tags", LIST(STR))


def test_required_attribute_repr():
    assert repr(Attribute("count", INT)) == "Attribute('count')"


def test_optional_attribute_repr():
    assert repr(Attribute("count", INT, optional=True)) == "Attribute('count', optional=True)"


# --- sealed hierarchy ---


def test_attribute_cannot_be_subclassed_directly():
    """The hierarchy is sealed — every Attribute lives in the framework module."""
    with pytest.raises(TypeError, match="sealed Attribute hierarchy"):
        class Rogue(Attribute):  # noqa
            ...


# --- per-attribute custom codecs ---


def test_attribute_with_custom_codec():
    """A `Codec` with custom encode/decode is honored end-to-end."""
    attr = Attribute(
        "count",
        Codec(
            encode=lambda v: f"int:{v}",
            decode=lambda v: int(v.split(":")[1]),
        ),
    )
    assert attr.codec.encode(5) == "int:5"
    assert attr.codec.decode("int:5") == 5


def test_attribute_custom_codec_used_via_dict_access():
    """A `Record` uses the attribute's codec for both encode (set) and decode (get)."""
    from flechtwerk.attribute import Record
    attr = Attribute(
        "count",
        Codec(
            encode=lambda v: f"int:{v}",
            decode=lambda v: int(v.split(":")[1]),
        ),
    )
    d = Record()
    d[attr] = 5
    assert d.raw["count"] == "int:5"
    assert d[attr] == 5
