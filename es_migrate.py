#!/usr/bin/env python3
"""
Elasticsearch Index Migration Tool

Exports index data from Elastic Cloud to an NDJSON file,
then imports from that file into an on-prem Elasticsearch cluster.

Usage:
    # Export from Elastic Cloud
    python es_migrate.py export \
        --cloud-id "my-deployment:dXMt..." \
        --api-key "base64key" \
        --index my-index \
        --output my-index-data.ndjson.gz

    # Import to on-prem
    python es_migrate.py import \
        --host https://onprem-es:9200 \
        --username elastic --password changeme \
        --index my-index \
        --input my-index-data.ndjson.gz

Connection details can also be set via .env file or environment variables.
"""

import argparse
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Client builders
# ---------------------------------------------------------------------------

def create_cloud_client(cloud_id=None, url=None, api_key=None,
                        username=None, password=None):
    """Create an Elasticsearch client for Elastic Cloud.

    Provide either *cloud_id* or *url* (the Elasticsearch endpoint URL).
    """
    kwargs = {"request_timeout": 120}

    if cloud_id:
        kwargs["cloud_id"] = cloud_id
    elif url:
        kwargs["hosts"] = [url]
    else:
        raise SystemExit("Provide --cloud-id or --url (or set ES_CLOUD_ID / ES_CLOUD_URL).")

    if api_key:
        kwargs["api_key"] = api_key
    elif username and password:
        kwargs["basic_auth"] = (username, password)
    else:
        raise SystemExit("Elastic Cloud requires --api-key or --username/--password.")

    client = Elasticsearch(**kwargs)
    info = client.info()
    print(f"Connected to Elastic Cloud  cluster: {info['cluster_name']}  "
          f"version: {info['version']['number']}")
    return client


def create_onprem_client(host, api_key=None, username=None, password=None,
                         verify_certs=True, ca_cert=None):
    """Create an Elasticsearch client for an on-prem cluster."""
    kwargs = {"hosts": [host], "request_timeout": 120, "verify_certs": verify_certs}

    if ca_cert:
        kwargs["ca_certs"] = ca_cert
    if api_key:
        kwargs["api_key"] = api_key
    elif username and password:
        kwargs["basic_auth"] = (username, password)

    client = Elasticsearch(**kwargs)
    info = client.info()
    print(f"Connected to on-prem  cluster: {info['cluster_name']}  "
          f"version: {info['version']['number']}")
    return client


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_index(client, index, output_path, batch_size=1000, query=None):
    """
    Scroll through every document in *index* and write to an NDJSON file.
    If *output_path* ends with '.gz', output is gzip-compressed.
    """

    # Verify the index exists
    if not client.indices.exists(index=index):
        raise SystemExit(f"Index '{index}' does not exist on the source cluster.")

    # Get doc count for progress bar
    count_resp = client.count(index=index)
    total_docs = count_resp["count"]
    print(f"Index '{index}' contains {total_docs:,} documents.")

    if total_docs == 0:
        print("Nothing to export.")
        return

    body = query if query else {"query": {"match_all": {}}}

    opener = gzip.open if output_path.endswith(".gz") else open
    written = 0
    errors = 0
    start = time.time()

    with opener(output_path, "wt", encoding="utf-8") as fh:
        with tqdm(total=total_docs, unit="docs", desc="Exporting") as pbar:
            next_milestone = 10000
            pbar.write("Downloaded: 0 docs")
            for doc in helpers.scan(
                client,
                index=index,
                query=body,
                scroll="5m",
                size=batch_size,
                preserve_order=False,
            ):
                record = {"_id": doc["_id"], "_source": doc["_source"]}
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                written += 1
                pbar.update(1)

                if written >= next_milestone:
                    pbar.write(f"Downloaded: {next_milestone:,} docs")
                    next_milestone += 10000

    elapsed = time.time() - start
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\nExport complete: {written:,} docs written to {output_path} "
          f"({file_size_mb:,.1f} MB) in {elapsed:.1f}s")
    if errors:
        print(f"  {errors:,} errors encountered during export.")


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

def _doc_generator(input_path, index):
    """Yield bulk actions from an NDJSON file."""
    opener = gzip.open if input_path.endswith(".gz") else open

    with opener(input_path, "rt", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  Skipping malformed line {line_no}: {exc}")
                continue

            action = {
                "_index": index,
                "_source": record["_source"],
            }
            if "_id" in record:
                action["_id"] = record["_id"]
            yield action


def _count_lines(path):
    """Count non-empty lines in a file (supports .gz)."""
    opener = gzip.open if path.endswith(".gz") else open
    count = 0
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def import_index(client, index, input_path, batch_size=500,
                 create_index=False):
    """
    Read an NDJSON file and bulk-upload documents into *index*.
    """

    if not os.path.isfile(input_path):
        raise SystemExit(f"Input file not found: {input_path}")

    print("Counting documents in file...")
    total_docs = _count_lines(input_path)
    print(f"File contains {total_docs:,} documents.")

    if total_docs == 0:
        print("Nothing to import.")
        return

    # Verify target index exists (unless --create-index)
    if not client.indices.exists(index=index):
        if create_index:
            print(f"Creating index '{index}' with default settings...")
            client.indices.create(index=index)
        else:
            raise SystemExit(
                f"Index '{index}' does not exist on the target cluster.\n"
                f"Create it first, or re-run with --create-index."
            )

    success_count = 0
    error_count = 0
    start = time.time()

    with tqdm(total=total_docs, unit="docs", desc="Importing") as pbar:
        next_milestone = 10000
        pbar.write("Uploaded: 0 docs")
        for ok, result in helpers.streaming_bulk(
            client,
            actions=_doc_generator(input_path, index),
            chunk_size=batch_size,
            max_retries=3,
            initial_backoff=2,
            raise_on_error=False,
        ):
            if ok:
                success_count += 1
            else:
                error_count += 1
                if error_count <= 10:
                    print(f"  Error: {result}")
            pbar.update(1)

            processed = success_count + error_count
            if processed >= next_milestone:
                pbar.write(f"Uploaded: {next_milestone:,} docs")
                next_milestone += 10000

    elapsed = time.time() - start
    print(f"\nImport complete: {success_count:,} docs indexed, "
          f"{error_count:,} errors in {elapsed:.1f}s")
    if error_count:
        print("Review errors above. Common causes: mapping conflicts, "
              "field type mismatches.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        description="Migrate Elasticsearch index data via NDJSON files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- export -----------------------------------------------------------
    exp = sub.add_parser("export", help="Export index data from Elastic Cloud to file.")
    exp.add_argument("--cloud-id", default=os.getenv("ES_CLOUD_ID"),
                     help="Elastic Cloud deployment ID (or set ES_CLOUD_ID).")
    exp.add_argument("--url", default=os.getenv("ES_CLOUD_URL"),
                     help="Elasticsearch endpoint URL, e.g. https://my-deploy.es.cloud.es.io:443 "
                          "(alternative to --cloud-id; or set ES_CLOUD_URL).")
    exp.add_argument("--api-key", default=os.getenv("ES_CLOUD_API_KEY"),
                     help="API key for Elastic Cloud (or set ES_CLOUD_API_KEY).")
    exp.add_argument("--username", default=os.getenv("ES_CLOUD_USERNAME"),
                     help="Username for Elastic Cloud (or set ES_CLOUD_USERNAME).")
    exp.add_argument("--password", default=os.getenv("ES_CLOUD_PASSWORD"),
                     help="Password for Elastic Cloud (or set ES_CLOUD_PASSWORD).")
    exp.add_argument("--index", required=True,
                     help="Name of the index to export.")
    exp.add_argument("--output", "-o",
                     help="Output file path. Defaults to <index>_<date>.ndjson.gz")
    exp.add_argument("--batch-size", type=int, default=1000,
                     help="Scroll batch size (default: 1000).")
    exp.add_argument("--query", type=str, default=None,
                     help='Optional query filter as JSON string, e.g. \'{"query":{"term":{"status":"active"}}}\'')

    # --- import -----------------------------------------------------------
    imp = sub.add_parser("import", help="Import index data from file to on-prem ES.")
    imp.add_argument("--host", default=os.getenv("ES_ONPREM_HOST"),
                     help="On-prem ES URL, e.g. https://localhost:9200 (or set ES_ONPREM_HOST).")
    imp.add_argument("--api-key", default=os.getenv("ES_ONPREM_API_KEY"),
                     help="API key for on-prem ES (or set ES_ONPREM_API_KEY).")
    imp.add_argument("--username", default=os.getenv("ES_ONPREM_USERNAME"),
                     help="Username for on-prem ES (or set ES_ONPREM_USERNAME).")
    imp.add_argument("--password", default=os.getenv("ES_ONPREM_PASSWORD"),
                     help="Password for on-prem ES (or set ES_ONPREM_PASSWORD).")
    imp.add_argument("--ca-cert", default=os.getenv("ES_ONPREM_CA_CERT"),
                     help="Path to CA certificate for TLS (or set ES_ONPREM_CA_CERT).")
    imp.add_argument("--no-verify-certs", action="store_true",
                     help="Disable TLS certificate verification (not recommended).")
    imp.add_argument("--index", required=True,
                     help="Target index name on the on-prem cluster.")
    imp.add_argument("--input", "-i", required=True,
                     help="Input NDJSON file to import.")
    imp.add_argument("--batch-size", type=int, default=500,
                     help="Bulk batch size (default: 500).")
    imp.add_argument("--create-index", action="store_true",
                     help="Create the target index if it doesn't exist.")

    return parser


def main():
    load_dotenv()  # Load .env file if present
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "export":
        if not args.cloud_id and not args.url:
            parser.error("--cloud-id or --url is required "
                         "(or set ES_CLOUD_ID / ES_CLOUD_URL in .env)")

        client = create_cloud_client(
            cloud_id=args.cloud_id,
            url=args.url,
            api_key=args.api_key,
            username=args.username,
            password=args.password,
        )

        output = args.output
        if not output:
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            output = f"{args.index}_{date_str}.ndjson.gz"

        query = None
        if args.query:
            try:
                query = json.loads(args.query)
            except json.JSONDecodeError as exc:
                parser.error(f"Invalid --query JSON: {exc}")

        export_index(client, args.index, output,
                     batch_size=args.batch_size, query=query)

    elif args.command == "import":
        if not args.host:
            parser.error("--host is required (or set ES_ONPREM_HOST in .env)")

        client = create_onprem_client(
            host=args.host,
            api_key=args.api_key,
            username=args.username,
            password=args.password,
            verify_certs=not args.no_verify_certs,
            ca_cert=args.ca_cert,
        )

        import_index(client, args.index, args.input,
                     batch_size=args.batch_size,
                     create_index=args.create_index)


if __name__ == "__main__":
    main()
