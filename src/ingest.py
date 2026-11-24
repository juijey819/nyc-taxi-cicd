
import os
import json
import argparse

from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError


# =====================================================
# 1. CONFIGURATION
# =====================================================

TRIPS_ID = os.getenv(
    "NYC_TRIPS_DATASET_ID",
    "4b4i-vvec"
)

ZONES_ID = os.getenv(
    "NYC_ZONES_DATASET_ID",
    "8meu-9t5y"
)

TRIPS_URL = (
    f"https://data.cityofnewyork.us/resource/{TRIPS_ID}.json"
)

ZONES_URL = (
    f"https://data.cityofnewyork.us/resource/{ZONES_ID}.json"
)

REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "trip_distance",
    "fare_amount"
}


# =====================================================
# 2. HTTP SESSION AND RETRY
# =====================================================

def create_session():

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True
    )

    session = requests.Session()

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)

    return session


SESSION = create_session()


# =====================================================
# 3. FETCH DATA FROM NYC OPEN DATA
# =====================================================

def fetch_json(url, params):

    headers = {}

    app_token = os.getenv("SOCRATA_APP_TOKEN")

    if app_token:
        headers["X-App-Token"] = app_token

    response = SESSION.get(
        url,
        params=params,
        headers=headers,
        timeout=120
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError("API returned an invalid response.")

    return data


def fetch_trips(offset, limit):

    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": ":id"
    }

    return fetch_json(TRIPS_URL, params)


def fetch_zones():

    params = {
        "$limit": 300,
        "$order": "locationid"
    }

    return fetch_json(ZONES_URL, params)


# =====================================================
# 4. VALIDATE DATA
# =====================================================

def validate_trip_rows(rows):

    if not isinstance(rows, list):
        raise TypeError("Trip data must be a list.")

    if not rows:
        return 0

    if not isinstance(rows[0], dict):
        raise ValueError("Invalid trip record format.")

    missing = REQUIRED_COLUMNS - set(rows[0].keys())

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    return len(rows)


# =====================================================
# 5. TRANSFORM DATA
# =====================================================

def transform_trips(rows, ingested_at):

    df = pd.DataFrame(rows)

    numeric_columns = [
        "vendorid",
        "passenger_count",
        "trip_distance",
        "pulocationid",
        "dolocationid",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type"
    ]

    datetime_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    for col in numeric_columns:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    for col in datetime_columns:

        if col in df.columns:

            df[col] = pd.to_datetime(
                df[col],
                errors="coerce"
            )

    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"]
        - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    df["ingested_at_utc"] = ingested_at

    return df


# =====================================================
# 6. CONVERT DATA TO CSV
# =====================================================

def rows_to_csv(rows):

    df = pd.DataFrame(rows)

    return df.to_csv(
        index=False
    ).encode("utf-8")


# =====================================================
# 7. CONNECT TO AZURE STORAGE
# =====================================================

def get_blob_service():

    account_name = os.getenv(
        "AZURE_STORAGE_ACCOUNT_NAME"
    )

    if not account_name:
        raise ValueError(
            "AZURE_STORAGE_ACCOUNT_NAME is missing."
        )

    credential = DefaultAzureCredential()

    account_url = (
        f"https://{account_name}.blob.core.windows.net"
    )

    return BlobServiceClient(
        account_url=account_url,
        credential=credential
    )


# =====================================================
# 8. UPLOAD BLOB
# =====================================================

def upload_blob(
    service,
    container,
    blob_name,
    data,
    content_type
):

    blob_client = service.get_blob_client(
        container=container,
        blob=blob_name
    )

    blob_client.upload_blob(
        data,
        overwrite=True,
        content_settings=ContentSettings(
            content_type=content_type
        )
    )


# =====================================================
# 9. CHECKPOINT MANAGEMENT
# =====================================================

def load_checkpoint(service):

    blob = service.get_blob_client(
        container="metadata",
        blob="checkpoint.json"
    )

    try:

        content = blob.download_blob().readall()

        checkpoint = json.loads(content)

    except ResourceNotFoundError:

        checkpoint = {
            "dataset_id": TRIPS_ID,
            "offset": 0,
            "completed": False
        }

    if checkpoint.get("dataset_id") != TRIPS_ID:

        raise ValueError(
            "Checkpoint dataset ID does not match."
        )

    return checkpoint


def save_checkpoint(service, checkpoint):

    upload_blob(
        service,
        "metadata",
        "checkpoint.json",
        json.dumps(
            checkpoint,
            indent=4
        ).encode("utf-8"),
        "application/json"
    )


# =====================================================
# 10. LOCAL API TEST
# =====================================================

def run_dry_test(limit):

    print("Starting local ingestion test...")

    trips = fetch_trips(
        offset=0,
        limit=limit
    )

    zones = fetch_zones()

    validate_trip_rows(trips)

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    Path("data/sample").mkdir(
        parents=True,
        exist_ok=True
    )

    pd.DataFrame(trips).to_csv(
        "data/sample/trips_raw_sample.csv",
        index=False
    )

    processed = transform_trips(
        trips,
        timestamp
    )

    processed.to_csv(
        "data/sample/trips_processed_sample.csv",
        index=False
    )

    with open(
        "data/sample/taxi_zones.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            zones,
            file,
            indent=2
        )

    print(f"Trips downloaded: {len(trips)}")
    print(f"Zones downloaded: {len(zones)}")
    print("Local ingestion test completed.")


# =====================================================
# 11. MAIN INGESTION PIPELINE
# =====================================================

def run_pipeline(limit, reset_checkpoint=False):

    service = get_blob_service()

    if reset_checkpoint:

        checkpoint = {
            "dataset_id": TRIPS_ID,
            "offset": 0,
            "completed": False
        }

        save_checkpoint(service, checkpoint)

    else:

        checkpoint = load_checkpoint(service)

    if checkpoint.get("completed"):

        print("Dataset ingestion already completed.")

        return

    offset = int(checkpoint.get("offset", 0))

    print(f"Starting batch at offset: {offset}")

    # Fetch trip records
    trips = fetch_trips(
        offset=offset,
        limit=limit
    )

    # Check end of dataset
    if not trips:

        checkpoint["completed"] = True

        save_checkpoint(service, checkpoint)

        print("Dataset ingestion completed.")

        return

    row_count = validate_trip_rows(trips)

    # Fetch reference data
    zones = fetch_zones()

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    run_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    # Generate blob paths
    prefix = (
        f"trips/{TRIPS_ID}/"
        f"offset_{offset:012d}"
    )

    raw_blob = prefix + ".csv"

    processed_blob = prefix + "_processed.csv"

    # RAW DATA
    raw_data = rows_to_csv(trips)

    upload_blob(
        service,
        "raw",
        raw_blob,
        raw_data,
        "text/csv"
    )

    # PROCESSED DATA
    processed = transform_trips(
        trips,
        timestamp
    )

    processed_data = processed.to_csv(
        index=False
    ).encode("utf-8")

    upload_blob(
        service,
        "processed",
        processed_blob,
        processed_data,
        "text/csv"
    )

    # REFERENCE DATA
    upload_blob(
        service,
        "reference",
        "taxi_zones.json",
        json.dumps(
            zones,
            indent=2
        ).encode("utf-8"),
        "application/json"
    )

    # MANIFEST
    next_offset = offset + row_count

    completed = row_count < limit

    manifest = {
        "source": "NYC Open Data",
        "trips_dataset": TRIPS_ID,
        "zones_dataset": ZONES_ID,
        "offset": offset,
        "next_offset": next_offset,
        "rows_ingested": row_count,
        "zones_count": len(zones),
        "ingested_at_utc": timestamp,
        "completed": completed,
        "raw_blob": raw_blob,
        "processed_blob": processed_blob
    }

    upload_blob(
        service,
        "metadata",
        f"runs/{run_id}_offset_{offset}.json",
        json.dumps(
            manifest,
            indent=4
        ).encode("utf-8"),
        "application/json"
    )

    upload_blob(
        service,
        "metadata",
        "last_run.json",
        json.dumps(
            manifest,
            indent=4
        ).encode("utf-8"),
        "application/json"
    )

    # Update checkpoint only after uploads succeed
    checkpoint = {
        "dataset_id": TRIPS_ID,
        "offset": next_offset,
        "completed": completed,
        "last_successful_run": timestamp
    }

    save_checkpoint(service, checkpoint)

    print("----------------------------------")
    print("INGESTION SUCCESS")
    print(f"Rows processed: {row_count}")
    print(f"Previous offset: {offset}")
    print(f"Next offset: {next_offset}")
    print(f"Zones downloaded: {len(zones)}")
    print(f"Dataset completed: {completed}")
    print("----------------------------------")


# =====================================================
# 12. ENTRY POINT
# =====================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("BATCH_SIZE", "25000"))
    )

    parser.add_argument(
        "--dry-run",
        action="store_true"
    )

    parser.add_argument(
        "--reset-checkpoint",
        action="store_true"
    )

    args = parser.parse_args()

    if not 1 <= args.limit <= 50000:

        parser.error(
            "--limit must be between 1 and 50000"
        )

    if args.dry_run:

        run_dry_test(args.limit)

    else:

        run_pipeline(
            args.limit,
            args.reset_checkpoint
        )
