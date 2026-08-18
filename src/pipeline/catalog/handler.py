"""Catalog Lambda.

Triggered by EventBridge "FileProcessed" events emitted by the ingest Lambda.
Writes a catalog record to DynamoDB and derives a status:
  - EMPTY_FILE_WARNING: CSV upload had zero data rows.
  - TRANSFORM_SKIPPED: CSV upload was not transformed (no output bucket configured).
  - PROCESSED: everything else.
"""
from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

CATALOG_TABLE_NAME = os.environ.get("CATALOG_TABLE_NAME", "FileCatalog")

dynamodb_resource = boto3.resource("dynamodb")


def _status_for(detail: dict) -> str:
    if detail.get("record_count") == 0:
        return "EMPTY_FILE_WARNING"
    if detail.get("content_type") == "text/csv" and not detail.get("transformed"):
        return "TRANSFORM_SKIPPED"
    return "PROCESSED"


def _to_item(detail: dict) -> dict:
    item = {
        "file_key": detail["file_key"],
        "bucket": detail["bucket"],
        "size_bytes": detail["size_bytes"],
        "checksum_sha256": detail["checksum_sha256"],
        "content_type": detail["content_type"],
        "transformed": detail.get("transformed", False),
        "processed_at": detail.get("processed_at"),
        "status": _status_for(detail),
    }
    if detail.get("record_count") is not None:
        item["record_count"] = Decimal(str(detail["record_count"]))
    if detail.get("columns"):
        item["columns"] = detail["columns"]
    if detail.get("output_location"):
        item["output_location"] = detail["output_location"]
    return item


def lambda_handler(event: dict, context: Any = None) -> dict:
    detail = event["detail"]
    table = dynamodb_resource.Table(CATALOG_TABLE_NAME)
    item = _to_item(detail)
    table.put_item(Item=item)
    logger.info("Cataloged %s with status %s", item["file_key"], item["status"])
    return {"file_key": item["file_key"], "status": item["status"]}
