"""Tests for manifest serialization."""

from hf_cache_sync.manifest import Manifest, ManifestFile


def test_roundtrip():
    m = Manifest(
        repo="org/model",
        revision="abc123",
        repo_type="model",
        files=[
            ManifestFile(path="weights.bin", blob="deadbeef", size=1000),
        ],
    )
    json_str = m.to_json()
    m2 = Manifest.from_json(json_str)
    assert m2.repo == "org/model"
    assert m2.revision == "abc123"
    assert m2.repo_type == "model"
    assert len(m2.files) == 1
    assert m2.files[0].blob == "deadbeef"


def test_roundtrip_dataset():
    m = Manifest(repo="org/data", revision="rev1", repo_type="dataset", files=[])
    m2 = Manifest.from_json(m.to_json())
    assert m2.repo_type == "dataset"


def test_backward_compat_no_repo_type():
    """Old manifests without repo_type should default to 'model'."""
    import json

    raw = json.dumps({"repo": "org/model", "revision": "abc", "files": []})
    m = Manifest.from_json(raw)
    assert m.repo_type == "model"


def test_remote_key():
    m = Manifest(repo="org/model", revision="abc123", files=[])
    assert m.remote_key == "manifests/org__model@abc123.json"


def test_blob_keys():
    m = Manifest(
        repo="x",
        revision="y",
        files=[
            ManifestFile(path="a", blob="h1", size=1),
            ManifestFile(path="b", blob="h2", size=2),
        ],
    )
    assert m.blob_keys() == ["blobs/h1", "blobs/h2"]
