import json

from pipeline.common.transform import csv_to_jsonl


def test_csv_to_jsonl_converts_rows():
    content = b"id,name\n1,widget\n2,gadget\n"
    lines = csv_to_jsonl(content).decode("utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": "1", "name": "widget"}
    assert json.loads(lines[1]) == {"id": "2", "name": "gadget"}


def test_csv_to_jsonl_handles_header_only():
    assert csv_to_jsonl(b"id,name\n") == b""


def test_csv_to_jsonl_handles_empty_content():
    assert csv_to_jsonl(b"") == b""


def test_csv_to_jsonl_preserves_unicode():
    content = "id,name\n1,widgét\n".encode("utf-8")
    lines = csv_to_jsonl(content).decode("utf-8").strip().splitlines()
    assert json.loads(lines[0]) == {"id": "1", "name": "widgét"}
