# download_t100.py
from concurrent.futures import ThreadPoolExecutor, as_completed
import azure.functions as func
import os, io, csv, json, uuid, datetime, logging
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from .function_app import app

EXCEL_MAX_ROWS = 1_000_000
CONTAINER = os.getenv("BTS_CONTAINER", "bts-t100")

MANIFEST_BLOB = "manifests/index.json"
COUNTS_MANIFEST_BLOB = MANIFEST_BLOB

TMP_PREFIX = "tmp-downloads"

# ---------- Blob clients ----------
def _bsc():
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set")
    return BlobServiceClient.from_connection_string(conn)

def _bc():
    return _bsc().get_container_client(CONTAINER)

def _download_small(name: str) -> bytes:
    return _bc().get_blob_client(name).download_blob().readall()

# ---------- Streaming CSV from blobs ----------
def _stream_blob_lines(blob_name: str, *, encoding="utf-8", max_retries=3):
    blob = _bc().get_blob_client(blob_name)
    attempt = 0
    while attempt < max_retries:
        try:
            downloader = blob.download_blob(max_concurrency=4)
            buf = b""
            for chunk in downloader.chunks():
                buf += chunk
                parts = buf.split(b"\n")
                for line in parts[:-1]:
                    yield line.decode(encoding, errors="replace")
                buf = parts[-1]
            if buf:
                yield buf.decode(encoding, errors="replace")
            return
        except Exception as e:
            attempt += 1
            logging.warning(f"[stream_blob_lines] attempt {attempt}/{max_retries} failed for {blob_name}: {e}")
            if attempt >= max_retries:
                logging.error(f"[stream_blob_lines] giving up after {max_retries} attempts on {blob_name}")
                raise
            import time, random
            time.sleep(random.uniform(2, 5))

def _iter_csv_dictrows(blob_name: str):
    line_iter = _stream_blob_lines(blob_name)
    try:
        header_line = next(line_iter)
    except StopIteration:
        return
    headers = next(csv.reader([header_line]))
    for line in line_iter:
        for row_values in csv.reader([line]):
            if len(row_values) != len(headers):
                row_values = (row_values + [""] * len(headers))[:len(headers)]
            yield dict(zip(headers, row_values))

# ---------- Inputs & preflight ----------
def _parse_quarters(qstr: str):
    if not qstr or qstr.upper() == "ALL":
        return {"1", "2", "3", "4"}
    return {q.strip() for q in qstr.split(",") if q.strip() in {"1","2","3","4"}}

def _sas_url(blob_name: str, hours=24):
    bc = _bc()
    expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=hours)
    sas = generate_blob_sas(
        account_name=bc.account_name,
        container_name=bc.container_name,
        blob_name=blob_name,
        account_key=_bsc().credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry
    )
    return f"https://{bc.account_name}.blob.core.windows.net/{bc.container_name}/{blob_name}?{sas}"

def _load_counts_manifest():
    try:
        raw = _download_small(COUNTS_MANIFEST_BLOB)
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None

def _estimate_rows(counts_manifest, year_from: int, year_to: int, quarters_set: set[str], origin: str):
    if not counts_manifest:
        return None
    by_year = { yrec["year"]: yrec for yrec in counts_manifest.get("years", []) }
    total = 0
    want_all = (origin == "ALL")
    for y in range(year_from, year_to + 1):
        yrec = by_year.get(y)
        if not yrec:
            continue
        airports = yrec.get("airports", {})
        if want_all:
            for a in airports.values():
                for q in quarters_set:
                    total += int(a["quarters"].get(q, 0))
        else:
            a = airports.get(origin.upper())
            if not a:
                continue
            for q in quarters_set:
                total += int(a["quarters"].get(q, 0))
    return total

# ---------- HTTP function ----------
@app.function_name(name="DownloadT100")
@app.route(route="download", auth_level=func.AuthLevel.FUNCTION)
def download(req: func.HttpRequest) -> func.HttpResponse:
    # parse inputs
    try:
        yf = int(req.params.get("year_from"))
        yt = int(req.params.get("year_to"))
    except Exception:
        return func.HttpResponse("Provide ?year_from=&year_to=", status_code=400)

    quarters = _parse_quarters(req.params.get("quarters") or "ALL")
    origin = (req.params.get("origin") or "ALL").upper()
    cache_name = f"prebuilt/{yf}_{yt}_{origin}_{quarters}.csv"

    try:
        if _bc().get_blob_client(cache_name).exists():
            url = _sas_url(cache_name, hours=24)
            payload = {
                "download_url": url,
                "rows": None,               # unknown without reading; optional
                "years": [yf, yt],
                "quarters": sorted(list(quarters)),
                "origin": origin,
                "cached": True
            }
            return func.HttpResponse(json.dumps(payload), mimetype="application/json", status_code=200)
    except Exception:
        # If existence check fails, just fall through to build path
        logging.info(f"Cache check failed for {cache_name}; building on the fly.")
    # guardrail via counts manifest
    counts_manifest = _load_counts_manifest()
    est = _estimate_rows(counts_manifest, yf, yt, quarters, origin)
    if est is not None and est > EXCEL_MAX_ROWS:
        return func.HttpResponse(
            f"Your selection is ~{est:,} rows (> Excel limit). Please narrow years, quarters, or origin.",
            status_code=400
        )

    # discover CSVs
    bc = _bc()
    selected_files = []
    for year in range(yf, yt + 1):
        for b in bc.list_blobs(name_starts_with=f"{year}/curated/"):
            if b.name.endswith(".csv"):
                selected_files.append(b.name)

    if not selected_files:
        return func.HttpResponse("No files match your filters.", status_code=404)

    # compose CSV with row filtering (QUARTER, ORIGIN)
    out = io.StringIO()
    writer = None
    count = 0

    # local helper so we can pass only what's needed
    def iter_filtered_rows(blob_name, quarters, origin):
        for row in _iter_csv_dictrows(blob_name):
            qcol = "QUARTER" if "QUARTER" in row else ("Quarter" if "Quarter" in row else None)
            if qcol and str(row.get(qcol, "")).strip() not in quarters:
                continue
            if origin != "ALL":
                ocol = "ORIGIN" if "ORIGIN" in row else ("Origin" if "Origin" in row else None)
                if ocol and row.get(ocol, "").upper() != origin:
                    continue
            yield row

    # 🚀 Parallelize blob reading (results merged on main thread)
    try:
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [
                ex.submit(lambda b=b: list(iter_filtered_rows(b, quarters, origin)))
                for b in selected_files
            ]
            for fut in as_completed(futures):
                rows = fut.result()
                for row in rows:
                    if writer is None:
                        writer = csv.DictWriter(out, fieldnames=list(row.keys()), lineterminator="\n")
                        writer.writeheader()
                    writer.writerow(row)
                    count += 1
                    if count > EXCEL_MAX_ROWS:
                        return func.HttpResponse(
                            "Selection exceeds Excel's row limit; narrow filters.",
                            status_code=400
                        )
    except Exception:
        logging.exception("Failed while processing blobs")
        return func.HttpResponse("Failed to read data", status_code=502)

    # upload composed CSV & return SAS link
    tmp_name = f"{TMP_PREFIX}/{uuid.uuid4()}.csv"
    try:
        _bc().upload_blob(tmp_name, out.getvalue().encode("utf-8"), overwrite=True)
    except Exception:
        logging.exception("Failed to upload composed CSV")
        return func.HttpResponse("Failed to upload composed CSV.", status_code=502)
    try:
        _bc().upload_blob(cache_name, out.getvalue().encode("utf-8"), overwrite=True)
    except Exception:
        logging.warning(f"Could not write cache blob {cache_name}; continuing without cache.")

    url = _sas_url(cache_name, hours=24)
    payload = {
        "download_url": url,
        "rows": count,
        "years": [yf, yt],
        "quarters": sorted(list(quarters)),
        "origin": origin,
        "cached": False
    }
    return func.HttpResponse(json.dumps(payload), mimetype="application/json", status_code=200)
