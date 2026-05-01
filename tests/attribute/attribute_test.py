from datetime import datetime, timezone

import pytest

from fretworx.attribute import (
    Attribute,
    Codec,
    DATETIME,
    INT,
    OptionalAttribute,
    RequiredAttribute,
    STR,
)


def test_attribute_is_abstract():
    """Direct instantiation of the base `Attribute` is rejected."""
    with pytest.raises(TypeError):
        Attribute("count", INT)  # type: ignore[abstract]


def test_required_attribute_is_concrete():
    RequiredAttribute("count", INT)


def test_optional_attribute_is_concrete():
    OptionalAttribute("count", INT)


def test_subclasses_inherit_attribute():
    assert issubclass(RequiredAttribute, Attribute)
    assert issubclass(OptionalAttribute, Attribute)


def test_codec_drives_validation():
    """The supplied codec produces an isinstance validator on str."""
    attr = RequiredAttribute("name", STR)
    assert attr.decode("hello") == "hello"
    with pytest.raises(TypeError):
        attr.decode(42)
    with pytest.raises(TypeError):
        attr.encode(42)


def test_int_codec_rejects_bool():
    """Exact-type check: bool is not int even though `isinstance(True, int)` is True."""
    attr = RequiredAttribute("count", INT)
    with pytest.raises(TypeError):
        attr.encode(True)
    with pytest.raises(TypeError):
        attr.decode(True)


def test_datetime_codec_round_trip():
    attr = RequiredAttribute("ts", DATETIME)
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    encoded = attr.encode(dt)
    assert encoded == "2024-06-15T14:30:00.000Z"
    assert attr.decode(encoded) == dt


def test_name_attribute():
    attr = RequiredAttribute("count", INT)
    assert attr.name == "count"


def test_attribute_is_not_a_str():
    """An attribute is a distinct type — it does not subclass str."""
    attr = RequiredAttribute("count", INT)
    assert not isinstance(attr, str)


def test_attribute_does_not_compare_equal_to_string():
    attr = RequiredAttribute("count", INT)
    assert attr != "count"


def test_required_attribute_repr():
    attr = RequiredAttribute("count", INT)
    assert repr(attr) == "RequiredAttribute('count')"


def test_optional_attribute_repr():
    attr = OptionalAttribute("count", INT)
    assert repr(attr) == "OptionalAttribute('count')"


# --- presence-kind conversion ---


def test_optional_required_returns_required_with_same_name_and_codec():
    opt = OptionalAttribute("count", INT)
    req = opt.required
    assert isinstance(req, RequiredAttribute)
    assert req.name == "count"
    assert req.codec is opt.codec
    assert req.decode(42) == 42


def test_required_optional_returns_optional_with_same_name_and_codec():
    req = RequiredAttribute("count", INT)
    opt = req.optional
    assert isinstance(opt, OptionalAttribute)
    assert opt.name == "count"
    assert opt.codec is req.codec
    assert opt.decode(42) == 42


def test_converted_attribute_round_trip_preserves_value_type():
    opt = OptionalAttribute("name", STR)
    assert opt.required.optional == opt
    req = RequiredAttribute("name", STR)
    assert req.optional.required == req


def test_converted_attribute_is_cached():
    """`required` / `optional` cache the converted view on the source instance."""
    opt = OptionalAttribute("name", STR)
    assert opt.required is opt.required
    req = RequiredAttribute("name", STR)
    assert req.optional is req.optional


def test_converted_attribute_works_with_dict_access():
    """An `OPT.required` is accepted by `Record.__getitem__`."""
    from fretworx.attribute import Record
    opt = OptionalAttribute("token", STR)
    d = Record({"token": "abc"})
    assert d[opt.required] == "abc"


# --- per-attribute custom codecs ---


def test_attribute_with_custom_codec():
    """A `Codec` with custom encode/decode is honored end-to-end."""
    attr = RequiredAttribute(
        "count",
        Codec(
            encode=lambda v: f"int:{v}",
            decode=lambda v: int(v.split(":")[1]),
        ),
    )
    assert attr.encode(5) == "int:5"
    assert attr.decode("int:5") == 5


def test_attribute_custom_codec_carries_through_kind_conversion():
    """`OPT.required` (and reverse) inherit the source's codec."""
    codec = Codec(
        encode=lambda v: f"e:{v}",
        decode=lambda v: int(v.split(":")[1]) if isinstance(v, str) else v,
    )
    opt = OptionalAttribute("count", codec)
    req = opt.required
    assert req.encode(5) == "e:5"
    assert req.decode("e:5") == 5


def test_attribute_custom_codec_used_via_dict_access():
    """A `Record` uses the attribute's codec for both encode (set) and decode (get)."""
    from fretworx.attribute import Record
    attr = RequiredAttribute(
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
