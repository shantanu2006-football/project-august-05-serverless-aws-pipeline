"""Ingest Lambda.

Triggered by S3 ObjectCreated events. For each uploaded object it:
  1. Downloads the object and extracts metadata (size, checksum, content type,
     and for CSV files: row count and column names).
  2. Transforms CSV files into newline-delimited JSON and writes the result
     to the output bucket.
  3. Publishes a "FileProcessed" event to EventBridge carrying the metadata,
     for the catalog Lambda to consume.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import boto3

from pipeline.common.metadata import extract_metadata
from pipeline.common.transform import csv_to_jsonl

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
EVENT_BUS_NAME = os.environ.get("EVENT_BUS_NAME", "default")
EVENT_SOURCE = "data.pipeline.ingest"
EVENT_DETAIL_TYPE = "FileProcessed"

s3_client = boto3.client("s3")
events_client = boto3.client("events")


def _output_key(source_key: str) -> str:
    base = source_key.rsplit("/", 1)[-1]
    stem = base.rsplit(".", 1)[0] if "." in base else base
    return f"processed/{stem}.jsonl"


def process_record(record: dict) -> dict:
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read()

    metadata = extract_metadata(bucket, key, content)

    output_location = None
    if key.lower().endswith(".csv") and OUTPUT_BUCKET:
        jsonl_content = csv_to_jsonl(content)
        output_key = _output_key(key)
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=output_key,
            Body=jsonl_content,
            ContentType="application/x-ndjson",
        )
        metadata.transformed = True
        output_location = {"bucket": OUTPUT_BUCKET, "key": output_key}

    detail = metadata.to_dict()
    detail["output_location"] = output_location
    detail["processed_at"] = datetime.now(timezone.utc).isoformat()

    put_response = events_client.put_events(
        Entries=[
            {
                "Source": EVENT_SOURCE,
                "DetailType": EVENT_DETAIL_TYPE,
                "Detail": json.dumps(detail),
                "EventBusName": EVENT_BUS_NAME,
            }
        ]
    )
    logger.info("Published %s event for s3://%s/%s", EVENT_DETAIL_TYPE, bucket, key)

    return {"metadata": detail, "put_events_response": put_response}


def lambda_handler(event: dict, context: Any = None) -> dict:
    results = [process_record(record) for record in event.get("Records", [])]
    return {"processed": len(results), "results": results}
