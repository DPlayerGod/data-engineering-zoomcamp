import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

# =========================
# CONFIG
# =========================
BUCKET_NAME = "dezoomcamp_hw4_2026_dplayergod"
CREDENTIALS_FILE = "kestra-486404-c5877e8c6618.json"

BASE_URLS = {
    "yellow": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow",
    "green":  "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"
}

YEARS = [2019, 2020]
MONTHS = list(range(1, 13))

DOWNLOAD_DIR = "./nyc_taxi_csv_gz"
GCS_PREFIX = "nyc_taxi"
MAX_WORKERS = 2
MAX_RETRIES = 5
CHUNK_SIZE = 8 * 1024 * 1024

# =========================

def csv_filename(color, year, month):
    return f"{color}_tripdata_{year}-{month:02d}.csv.gz"

def csv_url(color, year, month):
    return f"{BASE_URLS[color]}/{csv_filename(color, year, month)}"

def create_storage_client():
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"Missing credentials file: {CREDENTIALS_FILE}")
    return storage.Client.from_service_account_json(CREDENTIALS_FILE)

def ensure_bucket(client):
    try:
        bucket = client.get_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' exists.")
        return bucket
    except NotFound:
        bucket = client.create_bucket(BUCKET_NAME)
        print(f"Created bucket '{BUCKET_NAME}'")
        return bucket
    except Forbidden:
        print("Bucket exists but not accessible. Change bucket name.")
        sys.exit(1)

def download_file(color, year, month):
    url = csv_url(color, year, month)
    filename = csv_filename(color, year, month)
    path = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(path) and os.path.getsize(path) > 0:
        print(f"[SKIP] {filename}")
        return path

    headers = {"User-Agent": "Mozilla/5.0"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Downloading {filename} (attempt {attempt})")
            with requests.get(url, stream=True, headers=headers, timeout=120) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

            # basic sanity check
            if os.path.getsize(path) == 0:
                raise RuntimeError("Downloaded file is empty")

            return path
        except Exception as e:
            print(f"Retry {attempt} failed: {e}")
            time.sleep(3)

    raise Exception(f"Failed to download {filename}")

def upload_to_gcs(bucket, local_path, color):
    filename = os.path.basename(local_path)
    blob_name = f"{GCS_PREFIX}/{color}/{filename}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    if blob.exists():
        print(f"[SKIP UPLOAD] {filename}")
        return

    print(f"Uploading {filename}...")
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_name}")

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    client = create_storage_client()
    bucket = ensure_bucket(client)

    tasks = []
    for color in ["yellow", "green"]:
        for y in YEARS:
            for m in MONTHS:
                tasks.append((color, y, m))

    print(f"Total files: {len(tasks)} (CSV.GZ)")

    downloaded = []

    # Download
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_file, *t) for t in tasks]
        for future in as_completed(futures):
            downloaded.append(future.result())

    # Upload (wait for completion)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        upload_futures = []
        for path in downloaded:
            color = "yellow" if "yellow" in os.path.basename(path) else "green"
            upload_futures.append(executor.submit(upload_to_gcs, bucket, path, color))

        for f in as_completed(upload_futures):
            f.result()

    print("DONE ✅")

if __name__ == "__main__":
    main()
