from pathlib import Path

import boto3
import pytest

from ftmq.io import smart_read_proxies

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()
FIXTURES = ("ec_meetings.ftm.json", "eu_authorities.ftm.json")


def get_proxies():
    proxies = []
    for f in FIXTURES:
        proxies.extend(smart_read_proxies(FIXTURES_PATH / f))
    return proxies


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def test_dir():
    path = Path(".test").absolute()
    path.mkdir(exist_ok=True)
    return path


@pytest.fixture(scope="module")
def proxies():
    return get_proxies()


# @mock_s3
def setup_s3(with_fixtures: bool | None = False):
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="ftmq")
    if with_fixtures:
        client = boto3.client("s3")
        for f in FIXTURES:
            client.upload_file(FIXTURES_PATH / f, "ftmq", f)
