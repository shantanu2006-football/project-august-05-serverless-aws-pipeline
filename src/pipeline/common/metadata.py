"""Metadata extraction for files uploaded to the pipeline's source bucket."""
from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from typing import Optional

_CONTENT_TYPES = {
    "csv": "text/csv",
    "json": "application/json",
    "txt": "text/plain",
}


@dataclass
class FileMetadata:
    file_key: str
    bucket: str
    size_bytes: int
    checksum_sha256: str
    content_type: str
    record_count: Optional[int] = None
    columns: Optional[list] = None
    transformed: bool = False

    def to_dict(self) -> dict:
        return {
            "file_key": self.file_key,
            "bucket": self.bucket,
            "size_bytes": self.size_bytes,
            "checksum_sha256": self.checksum_sha256,
            "content_type": self.content_type,
            "record_count": self.record_count,
            "columns": self.columns,
            "transformed": self.transformed,
        }


def infer_content_type(key: str) -> str:
    ext = key.rsplit(".", 1)[-1].lower() if "." in key else ""
    return _CONTENT_TYPES.get(ext, "application/octet-stream")


def checksum_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def extract_csv_stats(content: bytes) -> tuple:
    """Return (record_count, columns) for CSV content, excluding the header row."""
    if not content:
        return 0, []
    text = content.decode("utf-8")
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return 0, []
    header, *data_rows = rows
    return len(data_rows), header


def extract_metadata(bucket: str, key: str, content: bytes) -> FileMetadata:
    content_type = infer_content_type(key)
    record_count = None
    columns = None
    if key.lower().endswith(".csv"):
        record_count, columns = extract_csv_stats(content)
    return FileMetadata(
        file_key=key,
        bucket=bucket,
        size_bytes=len(content),
        checksum_sha256=checksum_sha256(content),
        content_type=content_type,
        record_count=record_count,
        columns=columns,
    )
