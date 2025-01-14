from pathlib import Path

import orjson
from nomenklatura.entity import CE, CompositeEntity

from ftmq.io import apply_datasets, make_proxy, smart_read_proxies, smart_write_proxies
from ftmq.store import get_store


def test_io_read(fixtures_path: Path):
    success = False
    for proxy in smart_read_proxies(fixtures_path / "eu_authorities.ftm.json"):
        assert isinstance(proxy, CompositeEntity)
        success = True
        break
    assert success

    # read from an iterable of uris
    uri = fixtures_path / "eu_authorities.ftm.json"
    uris = [uri, uri]
    proxies = smart_read_proxies(uris)
    assert len([p for p in proxies]) == 302


def test_io_write(tmp_path: Path, proxies: list[CE]):
    path = tmp_path / "proxies.json"
    res = smart_write_proxies(path, proxies[:99])
    assert res == 99
    success = False
    for proxy in smart_read_proxies(path):
        assert isinstance(proxy, CompositeEntity)
        success = True
        break
    assert success


def test_io_write_stdout(capsys, proxies: list[CE]):
    res = smart_write_proxies("-", proxies[:5])
    assert res == 5
    captured = capsys.readouterr()
    proxy = None
    for line in captured.out.split("\n"):
        proxy = make_proxy(orjson.loads(line))
        break
    assert isinstance(proxy, CompositeEntity)


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
