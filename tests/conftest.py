import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import boto3
import botocore.awsrequest
import botocore.model
import pytest
import requests

from ftmq.io import smart_read_proxies

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()
FIXTURES = ("ec_meetings.ftm.json", "eu_authorities.ftm.json", "donations.ijson")


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def proxies():
    proxies = []
    for f in FIXTURES:
        proxies.extend(smart_read_proxies(FIXTURES_PATH / f))
    return proxies


@pytest.fixture(scope="module")
def eu_authorities():
    return [x for x in smart_read_proxies(FIXTURES_PATH / FIXTURES[1])]


@pytest.fixture(scope="module")
def donations():
    return [x for x in smart_read_proxies(FIXTURES_PATH / FIXTURES[2])]


# https://pawamoy.github.io/posts/local-http-server-fake-files-testing-purposes/
def spawn_and_wait_server():
    process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "-d", FIXTURES_PATH]
    )
    while True:
        try:
            requests.get("http://localhost:8000")
        except Exception:
            time.sleep(1)
        else:
            break
    return process


@pytest.fixture(scope="session", autouse=True)
def http_server():
    process = spawn_and_wait_server()
    yield process
    process.kill()
    process.wait()
    return


# Mock s3 for fsspec
# https://github.com/aio-libs/aiobotocore/issues/755


def setup_s3(with_fixtures: bool | None = False):
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="ftmq")
    if with_fixtures:
        client = boto3.client("s3")
        for f in FIXTURES:
            client.upload_file(FIXTURES_PATH / f, "ftmq", f)


class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    """
    Mocked AWS Response.

    https://github.com/aio-libs/aiobotocore/issues/755
    https://gist.github.com/giles-betteromics/12e68b88e261402fbe31c2e918ea4168
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        self._moto_response = response
        self.status_code = response.status_code
        self.raw = MockHttpClientResponse(response)

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    """
    Mocked HTP Response.

    See <MockAWSResponse> Notes
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        """
        Mocked Response Init.
        """

        async def read(self: MockHttpClientResponse, n: int = -1) -> bytes:
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self.content.read = read
        self.response = response

    @property
    def raw_headers(self) -> Any:
        """
        Return the headers encoded the way that aiobotocore expects them.
        """
        return {
            k.encode("utf-8"): str(v).encode("utf-8")
            for k, v in self.response.headers.items()
        }.items()


@pytest.fixture(scope="session", autouse=True)
def patch_aiobotocore() -> None:
    """
    Pytest Fixture Supporting S3FS Mocks.

    See <MockAWSResponse> Notes
    """

    def factory(original: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
        """
        Response Conversion Factory.
        """

        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel,
        ) -> Any:
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    aiobotocore.endpoint.convert_to_response_dict = factory(
        aiobotocore.endpoint.convert_to_response_dict
    )
