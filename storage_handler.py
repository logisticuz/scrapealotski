import importlib

from config import (
    ENABLE_UPLOADS,
    USE_DROPBOX,
    DROPBOX_ACCESS_TOKEN,
    GCS_BUCKET_NAME,
    GCS_CREDENTIALS_PATH,
)


dbx = None
gcs_client = None
dropbox_module = None
storage_module = None


def upload_file(local_path, cloud_path):
    if not ENABLE_UPLOADS:
        print("Skipping upload (uploads disabled).")
        return
    if USE_DROPBOX:
        global dbx, dropbox_module
        if not DROPBOX_ACCESS_TOKEN:
            raise RuntimeError("DROPBOX_ACCESS_TOKEN is not set.")
        if dropbox_module is None:
            try:
                dropbox_module = importlib.import_module("dropbox")
            except ImportError as exc:
                raise RuntimeError("dropbox package is not installed.") from exc
        if dbx is None:
            dbx = dropbox_module.Dropbox(DROPBOX_ACCESS_TOKEN)
        with open(local_path, "rb") as handle:
            dbx.files_upload(
                handle.read(),
                cloud_path,
                mode=dropbox_module.files.WriteMode("overwrite"),
            )
        print(f"Uploaded to Dropbox: {cloud_path}")
        return

    global gcs_client, storage_module
    if gcs_client is None:
        if storage_module is None:
            try:
                storage_module = importlib.import_module("google.cloud.storage")
            except ImportError as exc:
                raise RuntimeError("google-cloud-storage package is not installed.") from exc
        if not GCS_CREDENTIALS_PATH:
            print("Skipping upload (no cloud storage configured).")
            return
        gcs_client = storage_module.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
    if not GCS_BUCKET_NAME:
        print("Skipping upload (no cloud storage configured).")
        return
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(cloud_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to Google Cloud Storage: {cloud_path}")
