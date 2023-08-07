from pathlib import Path

import orjson
from moto import mock_s3
from nomenklatura.entity import CE, CompositeEntity

from ftmq.io import (
    apply_datasets,
    make_proxy,
    smart_read,
    smart_read_proxies,
    smart_write,
    smart_write_proxies,
)
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


def test_io_write(test_dir: Path, proxies: list[CE]):
    path = test_dir / "proxies.json"
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
def test_io_generic():
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
