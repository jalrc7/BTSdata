# azure_func/pipeline/pipeline_timer_db1b.py
import os, logging
from datetime import date, timedelta
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings
from .function_app import app
from .pipeline.db1bmarket.fetch import handle_year


# ----- helpers --------------------------------
#-------------------------------

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
    bc.upload_blob(name, b"", overwrite=True, content_settings=ContentSettings(content_type="application/octet-stream"))

# ...imports as you have...

# ---- storage helpers (unchanged) ----
# _get_blob_service, _ensure_container, _blob_exists, _touch_blob


@app.timer_trigger(
    schedule="0 0 6 1 * *",
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)

def Db1bMarketTimer(mytimer: func.TimerRequest) -> None:
    logger = logging.getLogger("db1b.timer")
    geo = os.getenv("DB1B_GEO", "All")
    container = os.getenv("DB1B_CONTAINER", "bts-db1b")
    backfill_flag = os.getenv("DB1B_BACKFILL_ALL", "1")  # "1" to enable one-time backfill
    marker_blob = os.getenv("BTS_BACKFILL_MARKER", "markers/backfill.done")
    today = date.today()
    current_q = ((today.month - 1) // 3) + 1              # 1..4
    # optional override like DB1B_QUARTER=All or 1..4
    quarter_cfg = os.getenv("DB1B_QUARTER", str(current_q)).strip()
    if quarter_cfg.lower() in {"all", "*"}:
        quarters = [1, 2, 3, 4]
    else:
        try:
            qn = int(quarter_cfg)
            if qn not in (1, 2, 3, 4):
                raise ValueError
            quarters = [qn]
        except Exception:
            logger.warning("Invalid DB1B_QUARTER=%r; defaulting to current quarter Q%s",
                           quarter_cfg, current_q)
            quarters = [current_q]

    if backfill_flag == "1":
        if not _blob_exists(container, marker_blob):
            logging.info("Starting one-time DB1B backfill for all years (resume-safe)...")
            _ensure_container(container)
            bsc = _get_blob_service()
            bc = bsc.get_container_client(container)

            start_year = 1993
            end_year = date.today().year

            for y in range(start_year, end_year + 1):

                handle_year(str(y), geography=geo, quarter="All")

            _touch_blob(container, marker_blob)
            logger.info("🚀 %s: running handle_year for ALL quarters (geo=%s)", y, geo)
            logging.info("Backfill complete; marker written. Future runs will skip backfill.")
        else:
            logging.info("Backfill marker found; skipping one-time backfill.")


    # Always process the current year incrementally (BTS updates through the year)
    cy = str(date.today().year)
    for q in quarters:
        quarter_marker = f"markers/quarterly/{cy}-Q{q}.done"
        if _blob_exists(container, quarter_marker):
            logging.info(f"✅ Quarter marker {quarter_marker} exists; skipping.")
            continue
        logger.info("Processing current year %s Q%s (geo=%s)...", cy, q, geo)
        handle_year(year=cy, geography=geo, quarter=str(q))
        _touch_blob(container, quarter_marker)
        logger.info("Wrote marker %s", quarter_marker)