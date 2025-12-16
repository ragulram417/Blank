from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import storage, bigquery
import json
import os
import io
from datetime import datetime

app = FastAPI(title="Prod GCS → BigQuery Ingestion API")

# -------------------------
# Request Model
# -------------------------
class IngestRequest(BaseModel):
    bucket_name: str
    prefix: str
    dataset_id: str
    staging_table: str
    target_table: str


# -------------------------
# Transform Logic
# (Same intent as old insert_json.py)
# -------------------------
def transform_json(data: dict) -> dict:
    return {
        "DCN_Number": data.get("Document Number"),
        "Depots_Affected": data.get("Depots Affected"),
        "DCN_Type": data.get("DCN Type"),
        "Change_Type": data.get("Change Type"),
        "Issue": data.get("Issue", {}).get("text"),
        "Issue_details_text": data.get("Issue Details", {}).get("text"),
    }


# -------------------------
# Main Endpoint
# -------------------------
@app.post("/ingest")
def ingest(req: IngestRequest):
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        raise HTTPException(500, "GOOGLE_CLOUD_PROJECT not set")

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    bucket = storage_client.bucket(req.bucket_name)
    blobs = bucket.list_blobs(prefix=req.prefix)

    staging_rows = []
    processed_files = 0

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        # Prevent duplicate file ingestion
        if _file_already_processed(bq_client, project, req.dataset_id, blob):
            continue

        raw = json.loads(blob.download_as_text())
        transformed = transform_json(raw)

        transformed.update({
            "operation": "UPSERT",
            "source_file": blob.name,
            "ingested_at": datetime.utcnow().isoformat()
        })

        staging_rows.append(transformed)
        _mark_file_processed(bq_client, project, req.dataset_id, blob)
        processed_files += 1

    if staging_rows:
        _load_to_bq(
            bq_client,
            project,
            req.dataset_id,
            req.staging_table,
            staging_rows
        )
        _merge_tables(
            bq_client,
            project,
            req.dataset_id,
            req.staging_table,
            req.target_table
        )

    return {
        "status": "success",
        "files_ingested": processed_files
    }


# -------------------------
# Helpers
# -------------------------
def _load_to_bq(client, project, dataset, table, rows):
    buffer = io.StringIO()
    for r in rows:
        buffer.write(json.dumps(r) + "\n")
    buffer.seek(0)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND"
    )

    client.load_table_from_file(
        buffer,
        f"{project}.{dataset}.{table}",
        job_config=job_config
    ).result()


def _merge_tables(client, project, dataset, staging, target):
    query = f"""
    MERGE `{project}.{dataset}.{target}` T
    USING `{project}.{dataset}.{staging}` S
    ON T.DCN_Number = S.DCN_Number
    WHEN MATCHED THEN
      UPDATE SET
        Depots_Affected = S.Depots_Affected,
        DCN_Type = S.DCN_Type,
        Change_Type = S.Change_Type,
        Issue = S.Issue,
        Issue_details_text = S.Issue_details_text
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    client.query(query).result()


def _file_already_processed(client, project, dataset, blob):
    query = f"""
    SELECT 1 FROM `{project}.{dataset}.processed_files`
    WHERE file_name = @name AND file_generation = @gen
    LIMIT 1
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", blob.name),
                bigquery.ScalarQueryParameter("gen", "STRING", str(blob.generation))
            ]
        )
    )
    return list(job.result())


def _mark_file_processed(client, project, dataset, blob):
    table = f"{project}.{dataset}.processed_files"
    row = {
        "file_name": blob.name,
        "file_generation": str(blob.generation),
        "processed_at": datetime.utcnow().isoformat()
    }
    client.insert_rows_json(table, [row])
