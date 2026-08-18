"""Integration-style tests for the ingest Lambda, backed by moto (no real AWS calls)."""
import importlib
import json
import sys

import boto3
from moto import mock_aws

SOURCE_BUCKET = "test-source-bucket"
OUTPUT_BUCKET = "test-output-bucket"
EVENT_BUS_NAME = "test-event-bus"

CSV_CONTENT = b"id,name,amount\n1,widget,9.99\n2,gadget,19.99\n"


def _s3_event(bucket: str, key: str) -> dict:
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


def _fresh_handler(monkeypatch):
    """Re-import the handler with env vars set so its module-level boto3 clients
    are created inside the active moto mock."""
    monkeypatch.setenv("OUTPUT_BUCKET", OUTPUT_BUCKET)
    monkeypatch.setenv("EVENT_BUS_NAME", EVENT_BUS_NAME)
    for mod_name in list(sys.modules):
        if mod_name.startswith("pipeline.ingest"):
            del sys.modules[mod_name]
    import pipeline.ingest.handler as handler

    return importlib.reload(handler)


@mock_aws
def test_process_csv_upload_transforms_and_emits_event(monkeypatch):
    handler = _fresh_handler(monkeypatch)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=OUTPUT_BUCKET)
    s3.put_object(Bucket=SOURCE_BUCKET, Key="uploads/orders.csv", Body=CSV_CONTENT)

    events = boto3.client("events", region_name="us-east-1")
    events.create_event_bus(Name=EVENT_BUS_NAME)

    result = handler.lambda_handler(_s3_event(SOURCE_BUCKET, "uploads/orders.csv"))

    assert result["processed"] == 1
    metadata = result["results"][0]["metadata"]
    assert metadata["record_count"] == 2
    assert metadata["columns"] == ["id", "name", "amount"]
    assert metadata["transformed"] is True
    assert metadata["output_location"] == {
        "bucket": OUTPUT_BUCKET,
        "key": "processed/orders.jsonl",
    }

    output_obj = s3.get_object(Bucket=OUTPUT_BUCKET, Key="processed/orders.jsonl")
    lines = output_obj["Body"].read().decode("utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": "1", "name": "widget", "amount": "9.99"}
    assert json.loads(lines[1]) == {"id": "2", "name": "gadget", "amount": "19.99"}

    put_events_response = result["results"][0]["put_events_response"]
    assert put_events_response["FailedEntryCount"] == 0
    assert put_events_response["Entries"][0]["EventId"]


@mock_aws
def test_non_csv_upload_skips_transform(monkeypatch):
    handler = _fresh_handler(monkeypatch)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=OUTPUT_BUCKET)
    s3.put_object(Bucket=SOURCE_BUCKET, Key="uploads/notes.txt", Body=b"hello world")

    events = boto3.client("events", region_name="us-east-1")
    events.create_event_bus(Name=EVENT_BUS_NAME)

    result = handler.lambda_handler(_s3_event(SOURCE_BUCKET, "uploads/notes.txt"))

    metadata = result["results"][0]["metadata"]
    assert metadata["transformed"] is False
    assert metadata["record_count"] is None
    assert metadata["output_location"] is None

    objects = s3.list_objects_v2(Bucket=OUTPUT_BUCKET).get("Contents", [])
    assert objects == []


@mock_aws
def test_url_encoded_key_is_decoded(monkeypatch):
    handler = _fresh_handler(monkeypatch)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=OUTPUT_BUCKET)
    s3.put_object(Bucket=SOURCE_BUCKET, Key="uploads/my orders.csv", Body=CSV_CONTENT)

    events = boto3.client("events", region_name="us-east-1")
    events.create_event_bus(Name=EVENT_BUS_NAME)

    result = handler.lambda_handler(_s3_event(SOURCE_BUCKET, "uploads/my+orders.csv"))

    metadata = result["results"][0]["metadata"]
    assert metadata["file_key"] == "uploads/my orders.csv"
