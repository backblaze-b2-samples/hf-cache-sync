"""Tests for manifest serialization."""

from hf_cache_sync.manifest import Manifest, ManifestFile


def test_roundtrip():
    m = Manifest(
        repo="org/model",
        revision="abc123",
        files=[
            ManifestFile(path="weights.bin", blob="deadbeef", size=1000),
        ],
    )
    json_str = m.to_json()
    m2 = Manifest.from_json(json_str)
    assert m2.repo == "org/model"
    assert m2.revision == "abc123"
    assert len(m2.files) == 1
    assert m2.files[0].blob == "deadbeef"


def test_remote_key():
    m = Manifest(repo="org/model", revision="abc123", files=[])
    assert m.remote_key == "manifests/org__model@abc123.json"


def test_blob_keys():
    m = Manifest(
        repo="x", revision="y",
        files=[
            ManifestFile(path="a", blob="h1", size=1),
            ManifestFile(path="b", blob="h2", size=2),
        ],
    )
    assert m.blob_keys() == ["blobs/h1", "blobs/h2"]
