# Cloud2OnpremData

A Python CLI tool for migrating Elasticsearch index data from Elastic Cloud to an on-prem Elasticsearch cluster via intermediate NDJSON files. Designed for air-gapped environments where the two clusters have no direct network connectivity.

## Prerequisites

- Python 3.8+
- Access credentials for both Elastic Cloud (source) and on-prem Elasticsearch (target)

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Connection details can be provided via CLI arguments, environment variables, or a `.env` file.

Copy the example and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `ES_CLOUD_ID` | Elastic Cloud deployment ID |
| `ES_CLOUD_URL` | Elasticsearch endpoint URL (alternative to Cloud ID) |
| `ES_CLOUD_API_KEY` | API key for Elastic Cloud |
| `ES_CLOUD_USERNAME` | Username (alternative to API key) |
| `ES_CLOUD_PASSWORD` | Password (alternative to API key) |
| `ES_ONPREM_HOST` | On-prem ES URL, e.g. `https://localhost:9200` |
| `ES_ONPREM_USERNAME` | Username for on-prem ES |
| `ES_ONPREM_PASSWORD` | Password for on-prem ES |
| `ES_ONPREM_API_KEY` | API key for on-prem ES (alternative to user/pass) |
| `ES_ONPREM_CA_CERT` | Path to CA certificate for TLS verification |

## Usage

### Step 1 — Export from Elastic Cloud

Run from a machine that can reach Elastic Cloud:

```bash
# Using Cloud ID
python es_migrate.py export \
    --cloud-id "my-deployment:dXMt..." \
    --api-key "base64key" \
    --index my-index

# Or using the Elasticsearch endpoint URL directly
python es_migrate.py export \
    --url "https://my-deployment.es.us-central1.gcp.cloud.es.io:443" \
    --api-key "base64key" \
    --index my-index
```

Important: when using `--url`, provide the Elasticsearch endpoint (typically a host containing `.es.`), not the Kibana endpoint (typically `.kb.`), or the request may be redirected and fail.

This produces a gzip-compressed file like `my-index_20260507_143000.ndjson.gz`.

You can specify a custom output path:

```bash
python es_migrate.py export --cloud-id "..." --api-key "..." \
    --index my-index --output my-data.ndjson.gz
```

To export only a subset of documents, pass a query filter:

```bash
python es_migrate.py export --cloud-id "..." --api-key "..." \
    --index my-index \
    --query '{"query": {"term": {"status": "active"}}}'
```

During export, the tool prints download milestones every 10,000 documents:

```text
Downloaded: 0 docs
Downloaded: 10,000 docs
Downloaded: 20,000 docs
...
```

### Step 2 — Transfer the file

Copy the `.ndjson.gz` file to the on-prem machine using whatever method is available (USB drive, SCP, file share, etc.).

### Step 3 — Import to on-prem Elasticsearch

Run from the on-prem machine:

```bash
python es_migrate.py import \
    --host https://onprem-es:9200 \
    --username elastic --password changeme \
    --index my-index \
    --input my-index_20260507_143000.ndjson.gz
```

The target index must already exist on the on-prem cluster. If you want the tool to create it with default settings, add `--create-index`:

```bash
python es_migrate.py import \
    --host https://onprem-es:9200 \
    --username elastic --password changeme \
    --index my-index \
    --input my-data.ndjson.gz \
    --create-index
```

During import, the tool prints upload milestones every 10,000 documents:

```text
Uploaded: 0 docs
Uploaded: 10,000 docs
Uploaded: 20,000 docs
...
```

## CLI Reference

### `export`

| Argument | Default | Description |
|----------|---------|-------------|
| `--cloud-id` | `ES_CLOUD_ID` env | Elastic Cloud deployment ID |
| `--url` | `ES_CLOUD_URL` env | Elasticsearch endpoint URL (alternative to `--cloud-id`) |
| `--api-key` | `ES_CLOUD_API_KEY` env | API key for authentication |
| `--username` | `ES_CLOUD_USERNAME` env | Username (alternative to API key) |
| `--password` | `ES_CLOUD_PASSWORD` env | Password (alternative to API key) |
| `--index` | *(required)* | Index name to export |
| `--output`, `-o` | `<index>_<date>.ndjson.gz` | Output file path |
| `--batch-size` | `1000` | Number of docs per scroll batch |
| `--query` | `None` | JSON query filter string |

### `import`

| Argument | Default | Description |
|----------|---------|-------------|
| `--host` | `ES_ONPREM_HOST` env | On-prem ES URL |
| `--api-key` | `ES_ONPREM_API_KEY` env | API key for authentication |
| `--username` | `ES_ONPREM_USERNAME` env | Username |
| `--password` | `ES_ONPREM_PASSWORD` env | Password |
| `--ca-cert` | `ES_ONPREM_CA_CERT` env | CA certificate path for TLS |
| `--no-verify-certs` | `false` | Disable TLS cert verification |
| `--index` | *(required)* | Target index name |
| `--input`, `-i` | *(required)* | Input NDJSON file path |
| `--batch-size` | `500` | Number of docs per bulk request |
| `--create-index` | `false` | Create index if it doesn't exist |

## Features

- **Gzip compression** — output files are compressed automatically when using `.gz` extension
- **Streaming** — documents are never fully loaded into memory, supporting large indices
- **Progress bar** — real-time document count and ETA
- **Milestone status updates** — explicit `Downloaded` / `Uploaded` status lines at 10,000-document increments
- **Retry logic** — bulk imports retry up to 3 times on transient failures
- **Query filtering** — export a subset of documents with an Elasticsearch query
- **Flexible auth** — supports API keys or username/password, via CLI args, env vars, or `.env`

## Troubleshooting

### Export fails with `ApiError(302, 'None')`

If export fails with `ApiError(302, 'None')`, the URL passed to `--url` is usually a Kibana endpoint (host contains `.kb.`), which redirects.

Use the Elasticsearch endpoint instead (host contains `.es.`).

Example:

```bash
# Incorrect (Kibana endpoint)
python es_migrate.py export --url "https://my-deployment.kb.us-east-1.aws.found.io" --api-key "..." --index my-index

# Correct (Elasticsearch endpoint)
python es_migrate.py export --url "https://my-deployment.es.us-east-1.aws.found.io" --api-key "..." --index my-index
```
