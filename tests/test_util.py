import sys

import cloudpickle

if sys.version_info >= (3, 11):
    from enum import EnumType

from nomenklatura.dataset import Dataset

from ftmq import util
from ftmq.enums import StrEnum


def test_util_make_dataset():
    ds = util.make_dataset("Test")
    assert isinstance(ds, Dataset)
    assert ds.to_dict() == {
        "name": "Test",
        "title": "Test",
        "resources": [],
    }


def test_util_str_enum():
    enum = StrEnum("Foo", ["a", "b", 2])
    assert enum.a == "a"
    assert str(enum.a) == "a"
    if sys.version_info >= (3, 11):
        assert isinstance(enum, EnumType)

        # https://gist.github.com/simonwoerpel/bdb9959de75e550349961677549624fb
        enum = StrEnum("Foo", ["name", "name2"])
        assert "name" in enum.__dict__
        dump = cloudpickle.dumps(enum)
        assert isinstance(dump, bytes)
        enum2 = cloudpickle.loads(dump)
        assert enum2 == enum


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
