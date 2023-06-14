import sys

if sys.version_info >= (3, 11):
    from enum import EnumType

from nomenklatura.dataset import Dataset

from ftmq import util


def test_util_make_dataset():
    ds = util.make_dataset("Test")
    assert isinstance(ds, Dataset)
    assert ds.to_dict() == {
        "name": "Test",
        "title": "Test",
        "resources": [],
        "children": [],
    }


def test_util_str_enum():
    enum = util.StrEnum("Foo", ["a", "b", 2])
    assert enum.a == "a"
    assert str(enum.a) == "a"
    if sys.version_info >= (3, 11):
        assert isinstance(enum, EnumType)
