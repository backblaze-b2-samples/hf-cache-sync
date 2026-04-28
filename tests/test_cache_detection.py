"""Tests for xet pointer and gated model detection."""

from hf_cache_sync.cache import is_likely_gated, is_xet_pointer


def test_xet_pointer_lfs(tmp_path):
    p = tmp_path / "blob"
    p.write_bytes(b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 100\n")
    assert is_xet_pointer(p) is True


def test_xet_pointer_xet_oid(tmp_path):
    p = tmp_path / "blob"
    p.write_bytes(b"version https://xethub.com/spec/v1\noid xet:abc\nsize 100\n")
    assert is_xet_pointer(p) is True


def test_real_blob_not_xet(tmp_path):
    p = tmp_path / "blob"
    p.write_bytes(b"x" * 2048)
    assert is_xet_pointer(p) is False


def test_small_file_no_markers(tmp_path):
    p = tmp_path / "blob"
    p.write_bytes(b"just some text")
    assert is_xet_pointer(p) is False


def test_xet_nonexistent(tmp_path):
    assert is_xet_pointer(tmp_path / "nope") is False


def test_is_likely_gated_with_license(tmp_path):
    repo = tmp_path / "models--meta-llama--Llama-2-7b"
    snap = repo / "snapshots" / "abc123"
    snap.mkdir(parents=True)
    (snap / "LICENSE.txt").write_text(
        "META LLAMA 2 COMMUNITY LICENSE AGREEMENT\nYou must accept these terms before using."
    )
    assert is_likely_gated(repo) is True


def test_is_likely_gated_permissive_license(tmp_path):
    repo = tmp_path / "models--org--open-model"
    snap = repo / "snapshots" / "abc123"
    snap.mkdir(parents=True)
    (snap / "LICENSE").write_text("MIT License\nPermission is hereby granted...")
    assert is_likely_gated(repo) is False


def test_is_likely_gated_no_snapshots(tmp_path):
    repo = tmp_path / "models--org--model"
    repo.mkdir(parents=True)
    assert is_likely_gated(repo) is False


def test_is_likely_gated_use_policy(tmp_path):
    repo = tmp_path / "models--google--gemma"
    snap = repo / "snapshots" / "def456"
    snap.mkdir(parents=True)
    (snap / "USE_POLICY.md").write_text("Acceptable Use Policy\nYou must accept this agreement.")
    assert is_likely_gated(repo) is True
