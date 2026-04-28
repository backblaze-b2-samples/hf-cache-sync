"""Tests for storage backend user-agent, error handling, and humanized errors."""

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from hf_cache_sync.storage import (
    B2_USER_AGENT,
    DEFAULT_USER_AGENT,
    StorageError,
    _humanize_client_error,
    _humanize_endpoint_error,
    _is_backblaze_endpoint,
    get_user_agent,
)


def test_is_backblaze_endpoint():
    assert _is_backblaze_endpoint("https://s3.us-west-000.backblazeb2.com") is True
    assert _is_backblaze_endpoint("https://s3.eu-central-003.backblazeb2.com") is True
    assert _is_backblaze_endpoint("https://s3.amazonaws.com") is False
    assert _is_backblaze_endpoint("https://minio.local:9000") is False
    assert _is_backblaze_endpoint("") is False


def test_get_user_agent_b2():
    ua = get_user_agent("https://s3.us-west-000.backblazeb2.com")
    assert ua.startswith(B2_USER_AGENT)


def test_get_user_agent_other():
    ua = get_user_agent("https://s3.amazonaws.com")
    assert ua.startswith(DEFAULT_USER_AGENT)


def _make_client_error(code: str, message: str = "boom") -> ClientError:
    return ClientError(
        error_response={"Error": {"Code": code, "Message": message}},
        operation_name="HeadObject",
    )


@pytest.mark.parametrize(
    "code,must_be_auth,must_be_transient,expected_keyword",
    [
        ("InvalidAccessKeyId", True, False, "credentials rejected"),
        ("SignatureDoesNotMatch", True, False, "credentials rejected"),
        ("AccessDenied", True, False, "Access denied"),
        ("NoSuchBucket", False, False, "not found"),
        ("PermanentRedirect", False, False, "not in the configured region"),
        ("ServiceUnavailable", False, True, "unavailable"),
        ("SlowDown", False, True, "unavailable"),
    ],
)
def test_humanize_client_error_codes(code, must_be_auth, must_be_transient, expected_keyword):
    err = _humanize_client_error(_make_client_error(code), bucket="b", endpoint="https://e")
    assert isinstance(err, StorageError)
    assert err.code == code
    assert err.auth_failure is must_be_auth
    assert err.transient is must_be_transient
    assert expected_keyword.lower() in str(err).lower()


def test_humanize_client_error_unknown_code_includes_message():
    err = _humanize_client_error(
        _make_client_error("WeirdCode", "weird message"),
        bucket="b",
        endpoint="https://e",
    )
    assert "WeirdCode" in str(err)
    assert "weird message" in str(err)


def test_humanize_endpoint_error():
    inner = EndpointConnectionError(endpoint_url="https://nope.example.com")
    err = _humanize_endpoint_error(inner, endpoint="https://nope.example.com")
    assert err.code == "EndpointConnectionError"
    assert err.transient is True
    assert "Could not reach" in str(err)


def test_storage_error_chain_preserves_cause():
    """The original boto error must be available via __cause__ for debugging."""
    inner = _make_client_error("AccessDenied")
    try:
        raise _humanize_client_error(inner, bucket="b", endpoint="") from inner
    except StorageError as caught:
        assert caught.__cause__ is inner
