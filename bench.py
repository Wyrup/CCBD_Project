# Authors : Tanguy Godat & Tim Gouvernon --Variant 3

import argparse
import csv
import os
import shutil
import socket
import time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs
from botocore.config import Config

from dataset_gen import SIZE_TO_ROWS, write_small_files
from compact import compact_dataset, count_parquet_files
from upload import upload_directory
from download import download_prefix, list_objects
from visualisation import visu


def ensure_empty_dir(path, create_new= True):
    if os.path.exists(path):
        # if the directory exists, delete it
        shutil.rmtree(path)
    if create_new:
        # re-create the deleted directory
        os.makedirs(path, exist_ok=True)


def ensure_parent_dir(path):
    '''create the parent directory for a path
    if it does not already exist'''
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def dir_size_bytes(directory):
    total = 0
    for root, _, files in os.walk(directory):
        for name in files:
            total += os.path.getsize(os.path.join(root, name))
    return total


def build_boto3_s3_client(endpoint_url, region_name, access_key=None, secret_key=None):
    kwargs = {
        "endpoint_url": endpoint_url.rstrip("/"),
        "region_name": region_name,
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    }

    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key

    return boto3.client("s3", **kwargs)

def build_pyarrow_s3_filesystem(endpoint_url, region_name, access_key=None, secret_key=None):
    endpoint = endpoint_url.replace("http://", "").replace("https://", "")
    scheme = "https"
    if endpoint_url.startswith("http://"):
        scheme = "http"

    kwargs = {
        "region": region_name,
        "scheme": scheme,
        "endpoint_override": endpoint,
    }
    if access_key and secret_key:
        kwargs["access_key"] = access_key
        kwargs["secret_key"] = secret_key

    return fs.S3FileSystem(**kwargs)


def delete_prefix(s3_client, bucket, prefix):
    keys = list_objects(s3_client, bucket, prefix)
    if not keys:
        return 0, 0.0

    start = time.time()
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
    elapsed = time.time() - start
    return len(keys), elapsed


def measure_listing(s3_client, bucket, prefix):
    start = time.time()
    keys = list_objects(s3_client, bucket, prefix)
    elapsed = time.time() - start

    total_bytes = 0
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3_client.list_objects_v2(**kwargs)
        total_bytes += sum(
            obj.get("Size", 0)
            for obj in resp.get("Contents", [])
            if not obj["Key"].endswith("/")
        )

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return len(keys), total_bytes, elapsed


def normalize_query_timestamp(ts):
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return pa.scalar(ts, type=pa.timestamp("ms"))


def run_fixed_query_on_s3(pyarrow_s3fs, bucket, prefix, region, start_ts, end_ts):
    dataset_path = f"{bucket}/{prefix}"
    dataset = ds.dataset(dataset_path, filesystem=pyarrow_s3fs, format="parquet")

    start_scalar = normalize_query_timestamp(start_ts)
    end_scalar = normalize_query_timestamp(end_ts)

    filt = (
        (ds.field("region") == region)
        & (ds.field("ts") >= start_scalar)
        & (ds.field("ts") < end_scalar)
    )

    start = time.time()
    table = dataset.to_table(columns=["event_type", "value"], filter=filt)
    grouped = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean"),
    ])
    elapsed = time.time() - start

    return {
        "query_elapsed_s": elapsed,
        "query_filtered_rows": table.num_rows,
        "query_grouped_rows": grouped.num_rows,
    }


def append_results(results_csv, rows):
    ensure_parent_dir(results_csv)
    write_header = not os.path.exists(results_csv)
    fieldnames = list(rows[0].keys())

    with open(results_csv, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def benchmark_one_layout(
    boto3_s3,
    pyarrow_s3fs,
    bucket,
    dataset_id,
    layout_name,
    local_source_dir,
    s3_prefix,
    download_base_dir,
    query_region,
    query_start,
    query_end,
    metadata,
    cleanup_prefix=True,
):
    '''benchmark the upload, listing, query and download'''
    if not os.path.exists(local_source_dir):
        raise FileNotFoundError(f"Local source directory does not exist: {local_source_dir}")

    if cleanup_prefix:
        deleted_existing_objects, delete_elapsed = delete_prefix(
            s3_client=boto3_s3,
            bucket=bucket,
            prefix=s3_prefix,
        )
    else:
        deleted_existing_objects, delete_elapsed = 0, 0.0

    local_file_count = count_parquet_files(local_source_dir)
    local_total_bytes = dir_size_bytes(local_source_dir)

    # upload
    upload_bytes, upload_file_count, upload_elapsed = upload_directory(
        s3_client=boto3_s3,
        bucket=bucket,
        local_dir=local_source_dir,
        s3_prefix=s3_prefix,
    )
    visu.update("upload", upload_elapsed)

    # listing
    listing_object_count, listing_total_bytes, listing_elapsed = measure_listing(
        s3_client=boto3_s3,
        bucket=bucket,
        prefix=s3_prefix,
    )
    visu.update("listing", listing_elapsed)

    # query
    query_metrics = run_fixed_query_on_s3(
        pyarrow_s3fs=pyarrow_s3fs,
        bucket=bucket,
        prefix=s3_prefix,
        region=query_region,
        start_ts=query_start,
        end_ts=query_end,
    )
    visu.update("query", query_metrics["query_elapsed_s"])

    download_dir = os.path.join(download_base_dir, dataset_id, layout_name)
    ensure_empty_dir(download_dir)

    # download
    download_bytes, download_file_count, download_elapsed = download_prefix(
        s3_client=boto3_s3,
        bucket=bucket,
        prefix=s3_prefix,
        local_dir=download_dir,
    )
    visu.update("download", download_elapsed)

    upload_throughput_mb_s = (
        (upload_bytes / (1024 * 1024)) / upload_elapsed if upload_elapsed > 0 else 0.0
    )
    download_throughput_mb_s = (
        (download_bytes / (1024 * 1024)) / download_elapsed if download_elapsed > 0 else 0.0
    )

    # to create storage space
    ensure_empty_dir(local_source_dir, False)

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "dataset_id": dataset_id,
        "size": metadata["size"],
        "layout": layout_name,
        "compression": metadata["compression"],
        "seed": metadata["seed"],
        "rows_per_file_small": metadata["rows_per_file_small"],
        "requested_compact_ratio": metadata["requested_compact_ratio"],
        "total_rows_expected": metadata["total_rows_expected"],
        "query_region": query_region,
        "query_start_ts": query_start.isoformat(),
        "query_end_ts": query_end.isoformat(),
        "local_source_dir": local_source_dir,
        "s3_prefix": s3_prefix,
        "local_file_count": local_file_count,
        "local_total_bytes": local_total_bytes,
        "deleted_existing_objects": deleted_existing_objects,
        "delete_prefix_elapsed_s": round(delete_elapsed, 6),
        "upload_file_count": upload_file_count,
        "upload_total_bytes": upload_bytes,
        "upload_elapsed_s": round(upload_elapsed, 6),
        "upload_throughput_mb_s": round(upload_throughput_mb_s, 6),
        "listing_object_count": listing_object_count,
        "listing_total_bytes": listing_total_bytes,
        "listing_elapsed_s": round(listing_elapsed, 6),
        "query_elapsed_s": round(query_metrics["query_elapsed_s"], 6),
        "query_filtered_rows": query_metrics["query_filtered_rows"],
        "query_grouped_rows": query_metrics["query_grouped_rows"],
        "download_dir": download_dir,
        "download_file_count": download_file_count,
        "download_total_bytes": download_bytes,
        "download_elapsed_s": round(download_elapsed, 6),
        "download_throughput_mb_s": round(download_throughput_mb_s, 6),
    }


def parse_layout_arg(layout_arg):
    parts = layout_arg.split("::")
    if len(parts) != 3:
        raise ValueError(
            "Each --layout must be: layout_name::local_source_dir::s3_prefix"
        )
    return {
        "layout_name": parts[0],
        "local_source_dir": parts[1],
        "s3_prefix": parts[2],
    }

def to_bench(dataset_id: str, bucket: str, endpoint_url: str, size: int, layout_:list[str],
             generate_small = True, small_output_dir= None, rows_per_file= 10_000,
             seed= 67, compact_from = None, compact_to= None, compact_output_ratio= 25,
             region_name= "us-east-1", access_key= None, secret_key= None,
             query_start= "2025-01-15T00:00:00+00:00", query_end= "2025-02-15T00:00:00+00:00",
             download_base_dir= "bench_downloads", query_region= "tollgate_a1_geneva",
             cleanup_prefix= True, results_csv= "results/results.csv"):
    '''generate the small file layout and/or compacted layout then call
    benchmark_one_layout for the rest of the benchmark'''

    total_rows = SIZE_TO_ROWS[size]
    generation_elapsed = None
    generated_file_count = None
    compaction_elapsed = None
    compact_total_rows = None
    compact_file_count = None
    rows_per_compact_file = None

    if generate_small:
        if not small_output_dir:
            raise ValueError("--small-output-dir is required with --generate-small")

        print("\n=== Step 1: generate locally ===")
        ensure_empty_dir(small_output_dir)
        generated_file_count, generation_elapsed = write_small_files(
            output_dir=small_output_dir,
            total_rows=total_rows,
            rows_per_file=rows_per_file,
            seed=seed
        )
        visu.update("generation", generation_elapsed)

    if compact_from or compact_to:
        if not compact_from or not compact_to:
            raise ValueError("--compact-from and --compact-to must be provided together")

        print("\n=== Step 2: compact locally ===")
        ensure_empty_dir(compact_to, False)

        compact_total_rows, compact_file_count, compaction_elapsed, rows_per_compact_file = compact_dataset(
            input_dir=compact_from,
            output_dir=compact_to,
            output_compact_ratio=compact_output_ratio
        )
        visu.update("compact", compaction_elapsed)

    if not layout_:
        raise ValueError("Provide at least one --layout entry")

    layouts = [parse_layout_arg(x) for x in layout_]

    boto3_s3 = build_boto3_s3_client(
        endpoint_url=endpoint_url,
        region_name=region_name,
        access_key=access_key,
        secret_key=secret_key,
    )

    pyarrow_s3fs = build_pyarrow_s3_filesystem(
        endpoint_url=endpoint_url,
        region_name=region_name,
        access_key=access_key,
        secret_key=secret_key,
    )

    query_start = datetime.fromisoformat(query_start)
    query_end = datetime.fromisoformat(query_end)

    metadata = {
        "size": size,
        "compression": "none",
        "seed": seed,
        "rows_per_file_small": rows_per_file,
        "requested_compact_ratio": compact_output_ratio, #changed from compact_output_file_count to compact_output_ratio
        "total_rows_expected": total_rows,
    }

    result_rows = []

    for layout in layouts:
        print(f"\n=== Benchmarking layout: {layout['layout_name']} ===")
        row = benchmark_one_layout(
            boto3_s3=boto3_s3,
            pyarrow_s3fs=pyarrow_s3fs,
            bucket=bucket,
            dataset_id=dataset_id,
            layout_name=layout["layout_name"],
            local_source_dir=layout["local_source_dir"],
            s3_prefix=layout["s3_prefix"],
            download_base_dir=download_base_dir,
            query_region=query_region,
            query_start=query_start,
            query_end=query_end,
            metadata=metadata,
            cleanup_prefix=cleanup_prefix,
        )

        row["generation_elapsed_s"] = round(generation_elapsed, 6) if generation_elapsed is not None else ""
        row["generated_small_file_count"] = generated_file_count if generated_file_count is not None else ""
        row["compaction_elapsed_s"] = round(compaction_elapsed, 6) if compaction_elapsed is not None else ""
        row["compact_total_rows"] = compact_total_rows if compact_total_rows is not None else ""
        row["compact_actual_file_count"] = compact_file_count if compact_file_count is not None else ""
        row["compact_rows_per_output_file"] = rows_per_compact_file if rows_per_compact_file is not None else ""

        result_rows.append(row)

    append_results(results_csv, result_rows)

    print("\n=== Benchmark complete ===")
    print(f"Results CSV: {results_csv}")
    for row in result_rows:
        print(
            f"layout={row['layout']} "
            f"objects={row['listing_object_count']} "
            f"upload_mb_s={row['upload_throughput_mb_s']} "
            f"listing_s={row['listing_elapsed_s']} "
            f"query_s3_s={row['query_elapsed_s']} "
            f"download_mb_s={row['download_throughput_mb_s']}"
        )


def build_parser():
    
    parser = argparse.ArgumentParser(
        description="Generic Variant 3 benchmark: local generation/compaction, MinIO upload/listing/direct-S3-query/download, CSV output on VM",
        fromfile_prefix_chars="@"
    )

    parser.add_argument("--dataset-id", required=True, help="Logical dataset id, e.g. tollgate_s")
    parser.add_argument("--size", required=True, type=str, choices=["S", "M", "L"], help="Dataset size preset")
    parser.add_argument("--download-base-dir", default="bench_downloads", help="Base directory for downloaded benchmark copies")
    parser.add_argument("--results-csv", default="results/results.csv", help="CSV output path on the VM")
    parser.add_argument("--rows-per-file", type=int, default=10_000, help="Rows per small Parquet file")
    parser.add_argument("--compact-output-ratio", type=int, default=25, help="Compacting ratio desired")
    parser.add_argument("--seed", type=int, default=67, help="Random seed")

    parser.add_argument("--bucket", required=True, help="MinIO/S3 bucket")
    parser.add_argument("--endpoint-url", required=True, help="MinIO/S3 endpoint URL")
    parser.add_argument("--region-name", default="us-east-1", help="S3 region name")
    parser.add_argument("--access-key", default=None, help="S3 access key")
    parser.add_argument("--secret-key", default=None, help="S3 secret key")

    parser.add_argument("--query-region", default="tollgate_a1_geneva", help="Region filter for benchmark query")
    parser.add_argument("--query-start", default="2025-01-15T00:00:00+00:00", help="Inclusive query start ISO-8601")
    parser.add_argument("--query-end", default="2025-02-15T00:00:00+00:00", help="Exclusive query end ISO-8601")

    parser.add_argument("--generate-small", type=bool, default=True, help="Generate a local small-files dataset")
    parser.add_argument("--small-output-dir", default=None, help="Local output dir for generated small dataset")
    parser.add_argument("--compact-from", default=None, help="Local input dir to compact")
    parser.add_argument("--compact-to", default=None, help="Local output dir for compacted dataset")
    parser.add_argument("--cleanup-prefix", type=bool, default= True, help="Delete existing objects under each tested S3 prefix before upload")

    parser.add_argument(
        "--layout",
        action="append",
        default=[],
        help="Layout definition: layout_name::local_source_dir::s3_prefix (repeatable)",
    )

    return parser

def main():

    parser_ = build_parser()
    args = parser_.parse_args()
    visu.init()

    to_bench(args.dataset_id, args.bucket, args.endpoint_url, args.size, args.layout,
             args.generate_small, args.small_output_dir, args.rows_per_file, args.seed,
             args.compact_from, args.compact_to, args.compact_output_ratio,
             args.region_name, args.access_key, args.secret_key, args.query_start,
             args.query_end, args.download_base_dir, args.query_region, args.cleanup_prefix,
             args.results_csv)

    visu.end()


if __name__ == "__main__":
    main()
