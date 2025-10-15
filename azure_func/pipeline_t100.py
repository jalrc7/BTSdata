# azure_func/pipeline_t100.py file

import os
import logging
from datetime import date
import azure.functions as func
from .pipeline.t100.fetch import handle_year
from azure.storage.blob import BlobServiceClient
from .function_app import app


def _get_blob_service():
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set")
    return BlobServiceClient.from_connection_string(conn)

def _ensure_container(container: str):
    bsc = _get_blob_service()
    try:
        bsc.create_container(container)
    except Exception:
        pass  # already exists

def _blob_exists(container: str, name: str) -> bool:
    bsc = _get_blob_service()
    bc = bsc.get_container_client(container)
    try:
        bc.get_blob_client(name).get_blob_properties()
        return True
    except Exception:
        return False

def _touch_blob(container: str, name: str):
    _ensure_container(container)
    bsc = _get_blob_service()
    bc = bsc.get_container_client(container)
    bc.upload_blob(name, b"", overwrite=True)

@app.function_name(name="BtsPipelineTimer")
@app.schedule(
    schedule="0 0 0 1 * *",   # 1st of month @ 00:00 (UTC)
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def BtsPipelineTimer(myTimer: func.TimerRequest):
    geo = os.getenv("BTS_GEO", "All")
    period = os.getenv("BTS_PERIOD", "All")
    container = os.getenv("BTS_CONTAINER", "bts-t100")
    backfill_flag = os.getenv("BTS_BACKFILL_ALL", "0")  # "1" to enable one-time backfill
    marker_blob = os.getenv("BTS_BACKFILL_MARKER", "markers/backfill.done")
    ym = date.today().strftime("%Y-%m")
    month_marker = f"markers/monthly/{ym}.done"

    if _blob_exists(container, month_marker):
        logging.info(f"✅ Monthly marker {month_marker} exists; skipping duplicate run.")
        return
    # One-time backfill (guarded by marker blob)
    if backfill_flag == "1":
        if not _blob_exists(container, marker_blob):
            logging.info("Starting one-time BTS backfill for all years (resume-safe)...")
            _ensure_container(container)
            bsc = _get_blob_service()
            bc = bsc.get_container_client(container)

            start_year = 1990
            end_year = date.today().year

            for y in range(start_year, end_year + 1):
                # what the pipeline uploads for curated file
                blob_name = f"{y}/curated/T_T100_SEGMENT_ALL_CARRIER__{y}__with_metrics.csv"
                try:
                    bc.get_blob_client(blob_name).get_blob_properties()
                    logging.info(f"✅ {y}: already uploaded — skipping")
                    continue
                except Exception:
                    logging.info(f"🚀 {y}: not found — running handle_year")

                # run only the missing year
                handle_year(str(y), geography=geo, period=period)

            _touch_blob(container, marker_blob)
            logging.info("Backfill complete; marker written. Future runs will skip backfill.")
        else:
            logging.info("Backfill marker found; skipping one-time backfill.")


    # Always process the current year incrementally (BTS updates through the year)
    cy = str(date.today().year)
    logging.info(f"Running monthly incremental for current year {cy}...")
    handle_year(cy, geography=geo, period=period)
    _touch_blob(container, month_marker)
    logging.info("Monthly incremental complete.")
