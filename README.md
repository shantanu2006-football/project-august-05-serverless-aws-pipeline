# Serverless AWS Data Pipeline

*Project August — Day 5 of 19.*

A serverless data pipeline defined as Infrastructure-as-Code (AWS SAM): a file
lands in S3, a Lambda extracts metadata and transforms it, and a second
Lambda consumes the resulting event and catalogs the result — no servers, no
polling, fully event-driven.

## Problem statement

Teams that ingest files (CSV exports, batch drops, partner uploads) need a
reliable way to know **what landed, whether it's usable, and where the
processed version is** — without standing up a server to poll S3 or babysit
a queue. This project builds that as an event-driven pipeline:

1. A file is uploaded to an S3 **source bucket**.
2. An **ingest Lambda** fires automatically, extracts metadata (size,
   checksum, content type, row/column stats for CSV) and — for CSV files —
   transforms the data into newline-delimited JSON (JSONL) written to an
   **output bucket**.
3. The ingest Lambda publishes a `FileProcessed` event onto a custom
   **EventBridge bus**.
4. A **catalog Lambda** subscribed to that event writes a status-tagged
   record into a **DynamoDB** table, so downstream systems (or a human) can
   query "what's been processed and is it healthy?" without touching S3 or
   the ingest Lambda at all.

## Architecture

```
                 ObjectCreated
   ┌──────────┐  ─────────────▶  ┌────────────────┐
   │  Source   │                 │  Ingest Lambda  │
   │  S3 Bucket│                 │  (pipeline/     │
   └──────────┘                 │   ingest)       │
                                  └───────┬────────┘
                                          │ 1. GetObject (source)
                                          │ 2. extract metadata
                                          │ 3. CSV -> JSONL, PutObject (output)
                                          │ 4. PutEvents("FileProcessed")
                          ┌───────────────┴───────────────┐
                          ▼                                ▼
                 ┌──────────────┐               ┌────────────────────┐
                 │ Output S3     │               │  EventBridge        │
                 │ Bucket        │               │  custom bus         │
                 │ (JSONL files) │               └──────────┬──────────┘
                 └──────────────┘                            │ rule match:
                                                               │ source=data.pipeline.ingest
                                                               │ detail-type=FileProcessed
                                                               ▼
                                                     ┌──────────────────┐
                                                     │  Catalog Lambda   │
                                                     │  (pipeline/       │
                                                     │   catalog)        │
                                                     └─────────┬─────────┘
                                                               │ PutItem
                                                               ▼
                                                     ┌──────────────────┐
                                                     │  DynamoDB         │
                                                     │  FileCatalog table│
                                                     └──────────────────┘
```

### Design decisions

- **Two decoupled Lambdas, not one.** The ingest Lambda's job (parse/transform
  a file) and the catalog Lambda's job (record pipeline state) have different
  failure modes, scaling needs, and change cadence. EventBridge is the seam:
  the ingest Lambda doesn't know or care who consumes `FileProcessed`, so a
  third consumer (alerting, a search index, etc.) can subscribe later without
  touching the ingest code — a deliberate fan-out point.
- **EventBridge over direct Lambda-to-Lambda invocation.** Direct invocation
  would couple the two functions' deploy lifecycles and make the ingest
  Lambda responsible for the catalog Lambda's error handling. EventBridge
  gives at-least-once delivery, a durable event bus, and a pattern-matched
  rule instead of hardcoded ARNs.
- **CSV is the one format actually transformed.** Rather than half-supporting
  many formats, CSV-to-JSONL is fully implemented and tested end to end.
  Non-CSV uploads still get metadata extraction (size, checksum, content
  type) and flow through the same event/catalog path, just with
  `transformed: false` — a real, working code path, not a stub.
- **Business logic lives in the catalog Lambda's status derivation.** An
  empty CSV (header row only, zero data rows) is cataloged as
  `EMPTY_FILE_WARNING` rather than silently marked "processed" — the kind of
  check that would otherwise require someone to notice a suspiciously small
  file days later.
- **`src/` package layout, not a script.** `pipeline.common` holds pure,
  boto3-free logic (`metadata.py`, `transform.py`) that's trivially unit
  tested; `pipeline.ingest` and `pipeline.catalog` hold the thin Lambda
  handlers that wire that logic to AWS. This is also exactly how AWS SAM
  expects Lambda code to be laid out (`CodeUri: src/`,
  `Handler: pipeline.ingest.handler.lambda_handler`).
- **moto over LocalStack.** moto mocks S3, EventBridge, and DynamoDB in-process
  at the boto3 client level — no Docker daemon, no network calls, tests run
  in ~1.5 seconds. That's a deliberate trade for a project with no real AWS
  account to deploy to: the goal is proving Lambda *logic* is correct, and
  moto does that with far less setup than LocalStack for the services this
  pipeline uses.

## Project structure

```
.
├── template.yaml                  # AWS SAM IaC: buckets, event bus, table, both functions
├── src/pipeline/
│   ├── common/
│   │   ├── metadata.py            # size/checksum/content-type/row-column extraction
│   │   └── transform.py           # CSV -> JSONL
│   ├── ingest/handler.py          # S3-triggered Lambda
│   └── catalog/handler.py         # EventBridge-triggered Lambda
├── tests/
│   ├── test_metadata.py           # pure unit tests
│   ├── test_transform.py          # pure unit tests
│   ├── test_ingest_handler.py     # moto-backed: S3 + EventBridge
│   └── test_catalog_handler.py    # moto-backed: DynamoDB
├── .github/workflows/ci.yml       # installs deps, runs pytest + cfn-lint on every push/PR
├── pyproject.toml                 # package metadata, pinned runtime dependency
├── requirements.txt                # pinned deps for local dev/test/CI
└── LICENSE                        # MIT
```

## Setup & run instructions

### Prerequisites

- Python 3.11+

### Install and test locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .

pytest -v
```

All tests run against [moto](https://github.com/getmoto/moto), which mocks
AWS S3, EventBridge, and DynamoDB in-process — **no AWS account, credentials,
or network access are required** to develop or test this pipeline.

### Deploying for real (not done in this project — no AWS account available)

This repo ships the IaC and passes tests against mocked AWS services, but has
not been deployed to a real AWS account. To deploy it:

```bash
# 1. Install the AWS SAM CLI: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html

# 2. Build (packages src/ for each Lambda per template.yaml's CodeUri)
sam build

# 3. Deploy (guided mode prompts for stack name, region, confirmation)
sam deploy --guided

# Subsequent deploys:
sam deploy
```

`sam deploy --guided` will create the CloudFormation stack described in
`template.yaml`: the source/output S3 buckets, the `data-pipeline-bus-<env>`
EventBridge bus, the `FileCatalog-<env>` DynamoDB table, and both Lambda
functions with the S3 and EventBridge triggers already wired up — no manual
console steps needed after deploy.

To try it end to end after deploying:

```bash
aws s3 cp orders.csv s3://<SourceBucketName from stack outputs>/uploads/orders.csv
aws s3 ls s3://<OutputBucketName>/processed/
aws dynamodb scan --table-name FileCatalog-dev
```

To tear down: `sam delete`.

## Example output

Uploading a 3-row CSV (`id,name,amount`) and running it through both
Lambdas locally (moto-mocked AWS) produces:

**Ingest Lambda result** (`pipeline.ingest.handler.lambda_handler`):

```json
{
  "file_key": "uploads/orders.csv",
  "bucket": "data-pipeline-source-dev",
  "size_bytes": 57,
  "checksum_sha256": "f1ef48d9d9b4f66f7eee8b1aa18a80cf26aa08d7649c75e8d67fbf6c00300f2c",
  "content_type": "text/csv",
  "record_count": 3,
  "columns": ["id", "name", "amount"],
  "transformed": true,
  "output_location": {
    "bucket": "data-pipeline-output-dev",
    "key": "processed/orders.jsonl"
  },
  "processed_at": "2026-08-18T06:15:49.582386+00:00"
}
```

**Transformed output** (`processed/orders.jsonl` in the output bucket):

```
{"id": "1", "name": "widget", "amount": "9.99"}
{"id": "2", "name": "gadget", "amount": "19.99"}
{"id": "3", "name": "gizmo", "amount": "4.50"}
```

**Catalog Lambda result** (`pipeline.catalog.handler.lambda_handler`,
triggered by the `FileProcessed` event above):

```json
{ "file_key": "uploads/orders.csv", "status": "PROCESSED" }
```

**Resulting DynamoDB item** (`FileCatalog-dev` table):

```json
{
  "file_key": "uploads/orders.csv",
  "bucket": "data-pipeline-source-dev",
  "size_bytes": 57,
  "checksum_sha256": "f1ef48d9d9b4f66f7eee8b1aa18a80cf26aa08d7649c75e8d67fbf6c00300f2c",
  "content_type": "text/csv",
  "transformed": true,
  "processed_at": "2026-08-18T06:15:58.788167+00:00",
  "status": "PROCESSED",
  "record_count": 3,
  "columns": ["id", "name", "amount"],
  "output_location": {
    "bucket": "data-pipeline-output-dev",
    "key": "processed/orders.jsonl"
  }
}
```

An empty CSV (header row, no data) instead produces
`{"status": "EMPTY_FILE_WARNING"}` — see `tests/test_catalog_handler.py`.

## Future work

Cut from this session's scope to keep depth over breadth; noted here rather
than left as broken/stub code:

- **JSON input support.** Only CSV is transformed today; JSON uploads get
  metadata-only treatment (`transformed: false`). Adding a JSON→JSONL
  normalizer would follow the same pattern as `csv_to_jsonl`.
- **Dead-letter queue for the catalog Lambda.** A malformed `FileProcessed`
  event or a DynamoDB throttle currently just fails the Lambda invocation;
  production use would want an SQS DLQ on the EventBridge rule target plus a
  CloudWatch alarm.
- **Idempotency.** Re-uploading the same key re-processes and re-catalogs it
  (overwriting the DynamoDB item, which is *usually* fine) rather than
  detecting a duplicate via the checksum already stored.
- **LocalStack-based end-to-end test.** moto proves the Lambda logic in
  isolation; a LocalStack docker-compose test that exercises the actual S3
  event notification → Lambda → EventBridge rule → Lambda wiring would catch
  IaC misconfiguration that moto's per-service mocking can't.
