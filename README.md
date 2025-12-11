import os
import json
import itertools
from typing import List, Optional, Any

from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound

app = FastAPI(title="Nested JSON -> BigQuery Uploader (preserve nested structs)")

# Optional defaults via env
DEFAULT_GCP_PROJECT = os.environ.get("GCP_PROJECT")
# chunk size for streaming inserts
INSERT_CHUNK = int(os.environ.get("BQ_INSERT_CHUNK", "500"))


def get_bq_client(project: Optional[str] = None) -> bigquery.Client:
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client(project=DEFAULT_GCP_PROJECT)


def parse_json_bytes(content: bytes) -> List[dict]:
    """
    Parse bytes that might be:
      - single JSON object
      - JSON array
      - NDJSON (newline delimited JSON objects)
    Returns list of dict rows.
    """
    text = content.decode("utf-8").strip()
    if not text:
        return []
    # JSON array
    if text.startswith("["):
        parsed = json.loads(text)
        if not isinstance(parsed, list):
            raise ValueError("Top-level JSON array expected.")
        return parsed
    # NDJSON or single object
    lines = text.splitlines()
    if len(lines) > 1:
        rows = []
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            rows.append(json.loads(ln))
        return rows
    # single object / single-line
    obj = json.loads(text)
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return [obj]
    raise ValueError("Unsupported top-level JSON type.")


def python_value_to_bq_type(value: Any) -> str:
    """Map a python value to BigQuery simple type string."""
    if value is None:
        return "STRING"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, dict):
        return "RECORD"
    if isinstance(value, list):
        # inspect first element non-null if possible
        for el in value:
            if el is not None:
                return python_value_to_bq_type(el)
        # fallback
        return "STRING"
    return "STRING"


def infer_bq_schema_from_obj(obj: dict, prefix: str = "") -> List[bigquery.SchemaField]:
    """
    Recursively infer BigQuery SchemaField list for a dict object, preserving nested STRUCTs.
    Arrays of objects will be mapped to REPEATED RECORD fields.
    """
    fields = []
    for k, v in obj.items():
        # sanitize field name (BigQuery prefers letters/numbers/underscores, but allow as-is)
        field_name = k.replace(" ", "_")
        bq_type = python_value_to_bq_type(v)
        if bq_type == "RECORD":
            # Nested dict or list-of-dict
            # determine if repeated (list) or nullable (dict)
            if isinstance(v, list):
                # if list of dicts find a sample element
                sample = None
                for el in v:
                    if isinstance(el, dict):
                        sample = el
                        break
                if sample is None:
                    # empty list or not list-of-dict -> store as repeated STRING
                    fields.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
                else:
                    nested_fields = infer_bq_schema_from_obj(sample, prefix=prefix + field_name + ".")
                    fields.append(bigquery.SchemaField(field_name, "RECORD", mode="REPEATED", fields=nested_fields))
            else:
                # plain nested object
                nested_fields = infer_bq_schema_from_obj(v, prefix=prefix + field_name + ".")
                fields.append(bigquery.SchemaField(field_name, "RECORD", mode="NULLABLE", fields=nested_fields))
        else:
            # scalar or list of scalars
            if isinstance(v, list):
                # repeated scalar
                fields.append(bigquery.SchemaField(field_name, bq_type, mode="REPEATED"))
            else:
                # single scalar or None
                fields.append(bigquery.SchemaField(field_name, bq_type, mode="NULLABLE"))
    return fields


def ensure_table_with_schema(client: bigquery.Client, table_id: str, sample_obj: dict):
    """
    Ensure table exists. If not, create it with an inferred nested schema from sample_obj.
    If table exists, it will NOT be altered here.
    """
    try:
        client.get_table(table_id)
        return  # exists
    except NotFound:
        # create table
        schema = infer_bq_schema_from_obj(sample_obj)
        table = bigquery.Table(table_id, schema=schema)
        # default to partitioning or clustering - left out for simplicity
        created = client.create_table(table)
        return created


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


@app.post("/upload/")
async def upload_json_files(
    files: List[UploadFile] = File(...),
    project: Optional[str] = Query(None, description="GCP project override (optional)"),
    dataset: str = Query(..., description="BigQuery dataset name"),
    table: str = Query(..., description="BigQuery table name"),
    create_table_if_missing: bool = Query(True, description="Create table with inferred nested schema if missing")
):
    """
    Upload multiple JSON files, preserving nested structure in BigQuery.

    Query params:
    - dataset (required)
    - table (required)
    - project (optional): if not provided uses environment/default ADC
    - create_table_if_missing: if true, will create the table using inferred nested schema from first object's structure
    """
    client = get_bq_client(project)
    # build table_id: project.dataset.table
    table_id = f"{client.project}.{dataset}.{table}"

    overall_results = {"files": []}

    # We'll keep a single representative sample for schema inference (first parsed object across files)
    schema_sample = None

    for uploaded in files:
        filename = uploaded.filename
        try:
            raw = await uploaded.read()
            rows = parse_json_bytes(raw)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse {filename}: {e}")

        if not rows:
            overall_results["files"].append({"filename": filename, "status": "skipped", "reason": "no rows parsed"})
            continue

        # pick sample if not yet picked
        if schema_sample is None:
            schema_sample = rows[0]

        # Ensure table exists
        try:
            # optionally create table with inferred schema
            if create_table_if_missing:
                ensure_table_with_schema(client, table_id, schema_sample)
            else:
                # just verify the table exists
                client.get_table(table_id)
        except GoogleAPIError as e:
            raise HTTPException(status_code=502, detail=f"BigQuery table/schema error for {filename}: {e}")

        # Insert using insert_rows_json which supports nested dictionaries for RECORD columns
        try:
            insert_errors = []
            for chunk in chunked_iterable(rows, INSERT_CHUNK):
                errors = client.insert_rows_json(table=table_id, json_rows=chunk)
                if errors:
                    insert_errors.extend(errors)
            if insert_errors:
                overall_results["files"].append({
                    "filename": filename,
                    "status": "partial_failure",
                    "insert_errors_count": len(insert_errors),
                    "errors_sample": insert_errors[:5]
                })
            else:
                overall_results["files"].append({"filename": filename, "status": "ok", "rows_inserted": len(rows)})
        except GoogleAPIError as e:
            raise HTTPException(status_code=502, detail=f"BigQuery insert failed for {filename}: {e}")

    return overall_results
 
