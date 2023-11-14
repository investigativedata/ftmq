from pathlib import Path

import orjson
from moto import mock_s3
from nomenklatura.entity import CE, CompositeEntity

from ftmq.io import (
    SmartHandler,
    apply_datasets,
    make_proxy,
    smart_read,
    smart_read_proxies,
    smart_write,
    smart_write_proxies,
)
from ftmq.store import get_store
from tests.conftest import setup_s3


def test_io_read(fixtures_path: Path):
    success = False
    for proxy in smart_read_proxies(fixtures_path / "ec_meetings.ftm.json"):
        assert isinstance(proxy, CompositeEntity)
        success = True
        break
    assert success

    # read from an iterable of uris
    uri = fixtures_path / "eu_authorities.ftm.json"
    uris = [uri, uri]
    proxies = smart_read_proxies(uris, serialize=False)
    assert len([p for p in proxies]) == 302


def test_io_write(tmp_path: Path, proxies: list[CE]):
    path = tmp_path / "proxies.json"
    res = smart_write_proxies(path, proxies[:99], serialize=True)
    assert res == 99
    success = False
    for proxy in smart_read_proxies(path):
        assert isinstance(proxy, CompositeEntity)
        success = True
        break
    assert success


def test_io_write_stdout(capsys, proxies: list[CE]):
    res = smart_write_proxies("-", proxies[:5], serialize=True)
    assert res == 5
    captured = capsys.readouterr()
    proxy = None
    for line in captured.out.split("\n"):
        proxy = make_proxy(orjson.loads(line))
        break
    assert isinstance(proxy, CompositeEntity)


@mock_s3
def test_io_s3(proxies: list[CE]):
    setup_s3()
    uri = "s3://ftmq/entities.json"
    res = smart_write_proxies(uri, proxies[:5], serialize=True)
    assert res == 5
    proxies = []
    proxies.extend(smart_read_proxies(uri))
    assert len(proxies) == 5
    assert isinstance(proxies[0], CompositeEntity)


def test_io_apply_datasets(proxies: list[CE]):
    success = False
    for proxy in apply_datasets(proxies, "foo", "bar"):
        assert "foo" in proxy.datasets
        assert "bar" in proxy.datasets
        assert "default" not in proxy.datasets
        success = True
        break
    assert success

    success = False
    proxies = apply_datasets(proxies, "foo", "bar")
    for proxy in apply_datasets(proxies, "baz", replace=True):
        assert "baz" in proxy.datasets
        assert "foo" not in proxy.datasets
        assert "bar" not in proxy.datasets
        assert "default" not in proxy.datasets
        success = True
        break
    assert success


@mock_s3
def test_io_generic(fixtures_path: Path):
    setup_s3()
    uri = "s3://ftmq/content"
    content = "foo"
    smart_write(uri, content.encode())
    content = smart_read(uri)
    assert isinstance(content, bytes)
    assert content.decode() == "foo"
    content = smart_read(uri, mode="r")
    assert isinstance(content, str)
    assert content == "foo"

    # stream
    tested = False
    for line in smart_read(fixtures_path / "ec_meetings.ftm.json", stream=True):
        assert isinstance(orjson.loads(line), dict)
        tested = True
    assert tested


def test_io_store(tmp_path, eu_authorities):
    uri = f"leveldb://{tmp_path}/level.db"
    store = get_store(uri, dataset="eu_authorities")
    with store.writer() as bulk:
        for proxy in eu_authorities:
            bulk.add_entity(proxy)
            break
    tested = False
    for proxy in smart_read_proxies(uri, dataset="eu_authorities"):
        assert isinstance(proxy, CompositeEntity)
        tested = True
        break
    assert tested

    res = smart_write_proxies(uri, eu_authorities, dataset="eu_authorities")
    assert res == 151
    res = [p for p in smart_read_proxies(uri, dataset="eu_authorities")]
    assert len(res) == 151


@mock_s3
def test_io_smart_handler(fixtures_path: Path):
    with SmartHandler(fixtures_path / "ec_meetings.ftm.json", stream=True) as h:
        line = h.readline()
        assert isinstance(orjson.loads(line), dict)

    setup_s3()
    uri = "s3://ftmq/content"
    content = b"foo"
    with SmartHandler(uri, mode="wb") as h:
        h.write(content)

    assert smart_read(uri) == content
