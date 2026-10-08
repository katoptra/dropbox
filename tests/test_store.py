from __future__ import annotations

import pytest
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError, EndpointConnectionError
from conftest import TEST_ENV

from migrator.paths import WorkPaths
from migrator.store import Store, StoreError

AWS_OVERRIDES = {
    "AWS_ACCESS_KEY_ID": "test-key",
    "AWS_SECRET_ACCESS_KEY": "test-secret",
    "AWS_ENDPOINT_URL_S3": "https://accountid.r2.cloudflarestorage.com",
}


class FakeClient:
    """Records calls; raises botocore ClientError on demand. No network."""

    def __init__(self, *, download_errors=None, list_pages=None, probe_code=None):
        self.calls = []
        self.download_errors = download_errors or {}
        self.list_pages = list_pages if list_pages is not None else []
        self.probe_code = probe_code

    def download_file(self, bucket, key, filename):
        self.calls.append(("download_file", bucket, key, filename))
        code = self.download_errors.get(key)
        if code:
            raise ClientError({"Error": {"Code": code}}, "GetObject")

    def upload_file(self, filename, bucket, key):
        self.calls.append(("upload_file", filename, bucket, key))

    def copy_object(self, **kwargs):
        self.calls.append(("copy_object", kwargs))

    def get_paginator(self, name):
        self.calls.append(("get_paginator", name))
        return self

    def paginate(self, **kwargs):
        self.calls.append(("paginate", kwargs))
        return iter(self.list_pages)

    def list_objects_v2(self, **kwargs):
        self.calls.append(("list_objects_v2", kwargs))
        if self.probe_code:
            raise ClientError({"Error": {"Code": self.probe_code}}, "ListObjectsV2")


class FailingClient:
    """Raises the boto3 and botocore errors that are not ClientError."""

    def upload_file(self, filename, bucket, key):
        raise S3UploadFailedError("upload failed")

    def list_objects_v2(self, **kwargs):
        raise EndpointConnectionError(endpoint_url="https://wrong.example")


def _store(runtime_factory, tmp_path, client, **overrides):
    runtime = runtime_factory(tmp_path, **{**AWS_OVERRIDES, **overrides})
    paths = WorkPaths.from_runtime(runtime)
    paths.ensure()
    return Store(runtime, paths, client=client), paths


def test_get_put_copy_list(runtime_factory, tmp_path):
    client = FakeClient(list_pages=[{"Contents": [{"Key": ".state/history/a.age"}]}])
    store, _ = _store(runtime_factory, tmp_path, client)
    (tmp_path / "x").write_bytes(b"blob")

    assert store.get(".state/x", tmp_path / "got") is True
    store.put(tmp_path / "x", ".state/y")
    store.copy(".state/history/h", ".state/state.sqlite.xz.age")
    assert store.list(".state/history/") == [".state/history/a.age"]

    assert client.calls[0] == (
        "download_file",
        "test-bucket",
        ".state/x",
        str(tmp_path / "got"),
    )
    assert client.calls[1] == (
        "upload_file",
        str(tmp_path / "x"),
        "test-bucket",
        ".state/y",
    )
    assert client.calls[2] == (
        "copy_object",
        {
            "Bucket": "test-bucket",
            "CopySource": {"Bucket": "test-bucket", "Key": ".state/history/h"},
            "Key": ".state/state.sqlite.xz.age",
        },
    )
    paginate_call = ("paginate", {"Bucket": "test-bucket", "Prefix": ".state/history/"})
    assert paginate_call in client.calls


def test_get_missing_returns_false(runtime_factory, tmp_path):
    client = FakeClient(download_errors={".state/missing": "NoSuchKey"})
    store, _ = _store(runtime_factory, tmp_path, client)
    assert store.get(".state/missing", tmp_path / "m") is False


def test_get_other_error_raises(runtime_factory, tmp_path):
    client = FakeClient(download_errors={".state/other": "AccessDenied"})
    store, _ = _store(runtime_factory, tmp_path, client)
    with pytest.raises(StoreError):
        store.get(".state/other", tmp_path / "o")


def test_list_of_absent_prefix_is_empty(runtime_factory, tmp_path):
    client = FakeClient(list_pages=[{"Contents": []}])
    store, _ = _store(runtime_factory, tmp_path, client)
    assert store.list(".state/history/") == []


def test_probe_raises_when_bucket_unreachable(runtime_factory, tmp_path):
    client = FakeClient(probe_code="NoSuchBucket")
    store, _ = _store(runtime_factory, tmp_path, client)
    with pytest.raises(StoreError, match="reachable"):
        store.probe()


def test_probe_succeeds_when_bucket_reachable(runtime_factory, tmp_path):
    client = FakeClient()
    store, _ = _store(runtime_factory, tmp_path, client)
    store.probe()


def test_bucket_required(runtime_factory, tmp_path):
    runtime = runtime_factory(tmp_path, MIRROR_R2_BUCKET="")
    with pytest.raises(StoreError, match="MIRROR_R2_BUCKET"):
        Store(runtime, WorkPaths.from_runtime(runtime))


@pytest.mark.parametrize(
    "unset",
    ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL_S3"],
)
def test_missing_credential_is_unset_secret_error(runtime_factory, tmp_path, unset):
    runtime = runtime_factory(tmp_path, **{**AWS_OVERRIDES, unset: ""})
    with pytest.raises(StoreError, match=unset):
        Store(runtime, WorkPaths.from_runtime(runtime))


def test_the_client_gets_its_endpoint_from_the_environment(runtime_factory, tmp_path):
    # conftest puts the AWS_* values of TEST_ENV in the environment, as op run does.
    runtime = runtime_factory(tmp_path)
    store = Store(runtime, WorkPaths.from_runtime(runtime))
    assert store.client.meta.endpoint_url == TEST_ENV["AWS_ENDPOINT_URL_S3"]


def test_boto3_and_botocore_errors_are_store_errors(runtime_factory, tmp_path):
    store, _ = _store(runtime_factory, tmp_path, FailingClient())
    (tmp_path / "x").write_bytes(b"blob")
    with pytest.raises(StoreError, match="upload failed"):
        store.put(tmp_path / "x", ".state/y")
    with pytest.raises(StoreError, match="reachable"):
        store.probe()
