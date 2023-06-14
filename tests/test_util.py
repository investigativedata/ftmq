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


def test_util_unknown_filters():
    res = (("country", "de", None), ("name", "alice", None))
    args = ("--country", "de", "--name", "alice")
    assert tuple(util.parse_unknown_cli_filters(args)) == res
    args = ("--country=de", "--name=alice")
    assert tuple(util.parse_unknown_cli_filters(args)) == res
    args = ("--country", "de", "--name=alice")
    assert tuple(util.parse_unknown_cli_filters(args)) == res
    args = ()
    assert tuple(util.parse_unknown_cli_filters(args)) == ()

    args = ("--country", "de", "--year__gte", "2023")
    res = (("country", "de", None), ("year", "2023", "gte"))
    assert tuple(util.parse_unknown_cli_filters(args)) == res
