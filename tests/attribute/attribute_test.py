from typing import Any

import pytest

from fretworx.attribute import Attribute, OptionalAttribute, RequiredAttribute


def test_attribute_is_abstract():
    """Direct instantiation of the base `Attribute` is rejected."""
    with pytest.raises(TypeError):
        Attribute("count")  # type: ignore[abstract]


def test_required_attribute_is_concrete():
    RequiredAttribute[int]("count")


def test_optional_attribute_is_concrete():
    OptionalAttribute[int]("count")


def test_subclasses_inherit_attribute():
    assert issubclass(RequiredAttribute, Attribute)
    assert issubclass(OptionalAttribute, Attribute)


def test_attribute_takes_only_a_name():
    """The constructor takes only `name`. No `decode=`/`encode=` kwargs — codecs come from the registry."""
    with pytest.raises(TypeError):
        RequiredAttribute[int]("count", decode=int)  # type: ignore[call-arg]


def test_v_subscript_drives_validation_by_default():
    """Without an explicit codec, the `[V]` subscript produces an isinstance validator via the registry."""
    attr = RequiredAttribute[str]("name")
    assert attr.decode("hello") == "hello"
    with pytest.raises(TypeError):
        attr.decode(42)
    with pytest.raises(TypeError):
        attr.encode(42)


def test_missing_v_subscript_raises_on_decode():
    """A bare `RequiredAttribute("name")` with no `[V]` raises on first decode use."""
    attr = RequiredAttribute("count")  # type: ignore[type-arg]
    with pytest.raises(TypeError, match=r"\[V\] type parameter"):
        attr.decode("anything")


def test_missing_v_subscript_raises_on_encode():
    attr = OptionalAttribute("count")  # type: ignore[type-arg]
    with pytest.raises(TypeError, match=r"\[V\] type parameter"):
        attr.encode("anything")


def test_any_subscript_is_rejected():
    """`[Any]` carries no information about the value's shape — using it is an error."""
    attr = RequiredAttribute[Any]("payload")
    with pytest.raises(TypeError, match=r"\[Any\]"):
        attr.decode("anything")
    with pytest.raises(TypeError, match=r"\[Any\]"):
        attr.encode("anything")


def test_name_attribute():
    attr = RequiredAttribute[int]("count")
    assert attr.name == "count"


def test_attribute_is_not_a_str():
    """An attribute is a distinct type — it does not subclass str."""
    attr = RequiredAttribute[int]("count")
    assert not isinstance(attr, str)


def test_attribute_does_not_compare_equal_to_string():
    attr = RequiredAttribute[int]("count")
    assert attr != "count"


def test_required_attribute_repr():
    attr = RequiredAttribute[int]("count")
    assert repr(attr) == "RequiredAttribute('count')"


def test_optional_attribute_repr():
    attr = OptionalAttribute[int]("count")
    assert repr(attr) == "OptionalAttribute('count')"


# --- presence-kind conversion ---


def test_optional_required_returns_required_with_same_name_and_codec():
    opt = OptionalAttribute[int]("count")
    req = opt.required
    assert isinstance(req, RequiredAttribute)
    assert req.name == "count"
    # Codec lookup must work via the copied [V] parametrization.
    assert req.decode(42) == 42


def test_required_optional_returns_optional_with_same_name_and_codec():
    req = RequiredAttribute[int]("count")
    opt = req.optional
    assert isinstance(opt, OptionalAttribute)
    assert opt.name == "count"
    assert opt.decode(42) == 42


def test_converted_attribute_round_trip_preserves_value_type():
    opt = OptionalAttribute[str]("name")
    assert opt.required.optional == opt
    req = RequiredAttribute[str]("name")
    assert req.optional.required == req


def test_converted_attribute_is_cached():
    """`required` / `optional` cache the converted view on the source instance."""
    opt = OptionalAttribute[str]("name")
    assert opt.required is opt.required
    req = RequiredAttribute[str]("name")
    assert req.optional is req.optional


def test_converted_attribute_works_with_dict_access():
    """An `OPT.required` is accepted by `Dict.__getitem__`."""
    from fretworx.attribute import Dict
    opt = OptionalAttribute[str]("token")
    d = Dict({"token": "abc"})
    assert d[opt.required] == "abc"
