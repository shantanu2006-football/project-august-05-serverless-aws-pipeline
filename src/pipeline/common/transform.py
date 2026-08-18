"""Transforms raw uploaded content into a normalized output format."""
from __future__ import annotations

import csv
import io
import json


def csv_to_jsonl(content: bytes) -> bytes:
    """Convert CSV bytes (with a header row) into newline-delimited JSON bytes."""
    if not content:
        return b""
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    lines = [json.dumps(row, ensure_ascii=False) for row in reader]
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")
