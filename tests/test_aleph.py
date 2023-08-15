from ftmq.aleph import parse_uri


def test_aleph_uri():
    assert parse_uri("http://localhost") == ("http://localhost", None, None)
    assert parse_uri("http+aleph://localhost") == ("http://localhost", None, None)
    assert parse_uri("https+aleph://dataset@localhost") == (
        "https://localhost",
        None,
        "dataset",
    )
    assert parse_uri("https+aleph://dataset:api_key@localhost") == (
        "https://localhost",
        "api_key",
        "dataset",
    )
