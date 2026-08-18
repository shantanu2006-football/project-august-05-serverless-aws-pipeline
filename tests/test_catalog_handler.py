"""Integration-style tests for the catalog Lambda, backed by moto (no real AWS calls)."""
import importlib
import sys

import boto3
from moto import mock_aws

CATALOG_TABLE_NAME = "test-file-catalog"


def _fresh_handler(monkeypatch):
    monkeypatch.setenv("CATALOG_TABLE_NAME", CATALOG_TABLE_NAME)
    for mod_name in list(sys.modules):
        if mod_name.startswith("pipeline.catalog"):
            del sys.modules[mod_name]
    import pipeline.catalog.handler as handler

    return importlib.reload(handler)


def _create_table():
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName=CATALOG_TABLE_NAME,
        KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


def _event(detail: dict) -> dict:
    return {
        "version": "0",
        "id": "test-event-id",
        "detail-type": "FileProcessed",
        "source": "data.pipeline.ingest",
        "detail": detail,
    }


def _base_detail(**overrides) -> dict:
    detail = {
        "file_key": "uploads/orders.csv",
        "bucket": "test-source-bucket",
        "size_bytes": 42,
        "checksum_sha256": "abc123",
        "content_type": "text/csv",
        "record_count": 2,
        "columns": ["id", "name", "amount"],
        "transformed": True,
        "output_location": {"bucket": "test-output-bucket", "key": "processed/orders.jsonl"},
        "processed_at": "2026-08-17T00:00:00+00:00",
    }
    detail.update(overrides)
    return detail


@mock_aws
def test_catalogs_processed_file(monkeypatch):
    handler = _fresh_handler(monkeypatch)
    _create_table()

    result = handler.lambda_handler(_event(_base_detail()))
    assert result == {"file_key": "uploads/orders.csv", "status": "PROCESSED"}

    table = boto3.resource("dynamodb", region_name="us-east-1").Table(CATALOG_TABLE_NAME)
    item = table.get_item(Key={"file_key": "uploads/orders.csv"})["Item"]
    assert item["status"] == "PROCESSED"
    assert item["record_count"] == 2
    assert item["columns"] == ["id", "name", "amount"]


@mock_aws
def test_flags_empty_file(monkeypatch):
    handler = _fresh_handler(monkeypatch)
    _create_table()

    detail = _base_detail(
        file_key="uploads/empty.csv",
        record_count=0,
        columns=[],
        transformed=True,
        output_location=None,
    )
    result = handler.lambda_handler(_event(detail))
    assert result["status"] == "EMPTY_FILE_WARNING"


@mock_aws
def test_flags_skipped_transform(monkeypatch):
    handler = _fresh_handler(monkeypatch)
    _create_table()

    detail = _base_detail(
        file_key="uploads/no-output-bucket.csv",
        transformed=False,
        output_location=None,
    )
    result = handler.lambda_handler(_event(detail))
    assert result["status"] == "TRANSFORM_SKIPPED"


@mock_aws
def test_non_csv_file_is_processed_normally(monkeypatch):
    handler = _fresh_handler(monkeypatch)
    _create_table()

    detail = _base_detail(
        file_key="uploads/notes.txt",
        content_type="text/plain",
        record_count=None,
        columns=None,
        transformed=False,
        output_location=None,
    )
    result = handler.lambda_handler(_event(detail))
    assert result["status"] == "PROCESSED"
