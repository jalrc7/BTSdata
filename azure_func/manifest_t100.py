# azure_func/build_manifest_timer.py
import os
import logging
from datetime import date

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from function_app import app
import count_rowst100

def _get_blob_service():
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set")
    return BlobServiceClient.from_connection_string(conn)

def _list_curated_csvs_for_year(bc, year: int) -> list[str]:
    return [
        b.name
        for b in bc.list_blobs(name_starts_with=f"{year}/curated/")
        if b.name.endswith(".csv")
    ]

# manifest_t100.py  (your _loader)
def _loader(bc):
    def _load(name: str) -> bytes:
        blob = bc.get_blob_client(name)
        # Read the whole file into memory (safe for manifest build)
        return blob.download_blob().readall()
    return _load


@app.function_name(name="BuildManifestTimer")
@app.schedule(
    schedule="0 10 0 1 * *",   # first of month 00:10 UTC
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def BuildManifestTimer(myTimer: func.TimerRequest):
    container = os.getenv("BTS_CONTAINER", "bts-t100")
    start_year = int(os.getenv("BTS_START_YEAR", "1990"))
    end_year = date.today().year

    bsc = _get_blob_service()
    bc = bsc.get_container_client(container)

    years = list(range(start_year, end_year + 1))
    manifest = count_rowst100.build_manifest_from_provider(
        years=years,
        list_files_for_year=lambda y: _list_curated_csvs_for_year(bc, y),
        load_file_bytes=_loader(bc),
    )

    bc.upload_blob(
        "manifests/index.json",
        count_rowst100.manifest_to_bytes(manifest),
        overwrite=True,
    )
    logging.info("Manifest written to manifests/index.json")
