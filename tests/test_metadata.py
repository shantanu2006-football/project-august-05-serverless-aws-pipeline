from pipeline.common.metadata import extract_csv_stats, extract_metadata, infer_content_type

CSV_CONTENT = b"id,name,amount\n1,widget,9.99\n2,gadget,19.99\n"


def test_extract_csv_stats_counts_rows_and_columns():
    count, columns = extract_csv_stats(CSV_CONTENT)
    assert count == 2
    assert columns == ["id", "name", "amount"]


def test_extract_csv_stats_handles_empty_content():
    count, columns = extract_csv_stats(b"")
    assert count == 0
    assert columns == []


def test_extract_csv_stats_handles_header_only():
    count, columns = extract_csv_stats(b"id,name,amount\n")
    assert count == 0
    assert columns == ["id", "name", "amount"]


def test_infer_content_type_known_and_unknown_extensions():
    assert infer_content_type("uploads/orders.csv") == "text/csv"
    assert infer_content_type("uploads/orders.json") == "application/json"
    assert infer_content_type("uploads/notes.txt") == "text/plain"
    assert infer_content_type("uploads/archive.zip") == "application/octet-stream"
    assert infer_content_type("uploads/no_extension") == "application/octet-stream"


def test_extract_metadata_for_csv():
    metadata = extract_metadata("bucket", "uploads/orders.csv", CSV_CONTENT)
    assert metadata.record_count == 2
    assert metadata.columns == ["id", "name", "amount"]
    assert metadata.content_type == "text/csv"
    assert metadata.size_bytes == len(CSV_CONTENT)
    assert len(metadata.checksum_sha256) == 64


def test_extract_metadata_for_non_csv_skips_row_stats():
    metadata = extract_metadata("bucket", "uploads/notes.txt", b"hello")
    assert metadata.record_count is None
    assert metadata.columns is None
    assert metadata.content_type == "text/plain"


def test_extract_metadata_checksum_is_deterministic():
    first = extract_metadata("bucket", "uploads/a.csv", CSV_CONTENT)
    second = extract_metadata("bucket", "uploads/a.csv", CSV_CONTENT)
    assert first.checksum_sha256 == second.checksum_sha256

    different = extract_metadata("bucket", "uploads/a.csv", CSV_CONTENT + b"3,thing,1.00\n")
    assert different.checksum_sha256 != first.checksum_sha256
