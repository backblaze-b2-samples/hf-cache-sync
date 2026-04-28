"""Tests for storage backend user-agent and error handling."""

from hf_cache_sync.storage import (
    B2_USER_AGENT,
    DEFAULT_USER_AGENT,
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
