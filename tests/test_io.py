from pathlib import Path

import orjson
from moto import mock_s3
from nomenklatura.entity import CE, CompositeEntity

from ftmq.io import load_proxy, smart_read_proxies, smart_write_proxies

from .conftest import setup_s3


def test_io_read(fixtures_path: Path):
    success = False
    for proxy in smart_read_proxies(fixtures_path / "ec_meetings.ftm.json"):
        assert isinstance(proxy, CompositeEntity)
        success = True
        break
    assert success


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
        proxy = load_proxy(orjson.loads(line))
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
