import os, io, json, math, zipfile
from datetime import datetime
from typing import Iterable, List, Dict, Tuple

import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient

from function_app import app

# ----- Config -----
AIRPORT_COL = "ORIGIN"
QUARTER_COL = "QUARTER"
EXCEL_ROW_LIMIT = 1_048_576

CONTAINER = os.getenv("BTS_CONTAINER", "bts-t100")
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# curated CSV name pattern (you already use this)
def curated_name_for_year(y: int) -> str:
    return f"{y}/curated/T_T100_SEGMENT_ALL_CARRIER__{y}__with_metrics.csv"

def _bsc() -> BlobServiceClient:
    if not CONN_STR:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set")
    return BlobServiceClient.from_connection_string(CONN_STR)

# ----- Manifest helpers -----
def _load_manifest() -> dict:
    bc = _bsc().get_container_client(CONTAINER)
    try:
        data = bc.get_blob_client("manifests/index.json").download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except Exception:
        # No manifest yet
        return {"years": []}

def _estimate_rows(manifest: dict, years: Iterable[int],
                   airports: List[str] | None,
                   quarters: List[str] | None) -> Tuple[int, Dict[int, int]]:
    """
    Returns (total_rows, per_year_rows) based on manifest.
    """
    airports_set = set(a.upper() for a in airports) if airports else None
    quarters_set = set(quarters) if quarters else None

    per_year: Dict[int, int] = {}
    total = 0

    years_set = set(years)
    for yinfo in manifest.get("years", []):
        y = int(yinfo.get("year"))
        if y not in years_set:
            continue
        if not airports_set and not quarters_set:
            # whole year
            cnt = int(yinfo.get("total_rows", 0))
            per_year[y] = cnt
            total += cnt
            continue

        # filter by airport and quarters
        airports_map = yinfo.get("airports", {})
        ysum = 0
        for a, vals in airports_map.items():
            if airports_set and a.upper() not in airports_set:
                continue
            if not quarters_set:
                ysum += int(vals.get("total", 0))
            else:
                qmap = vals.get("quarters", {})
                ysum += sum(int(qmap.get(q, 0)) for q in quarters_set)
        per_year[y] = ysum
        total += ysum

    return total, per_year

# ----- CSV building -----
def _iter_filtered_chunks(csv_bytes: bytes,
                          airports: List[str] | None,
                          quarters: List[str] | None) -> Iterable[pd.DataFrame]:
    # Normalize filters
    airports_set = set(a.upper() for a in airports) if airports else None
    quarters_set = set(quarters) if quarters else None

    usecols = None  # read all columns so the download is “full fidelity”
    # If you prefer smaller files, set something like:
    # usecols = [AIRPORT_COL, QUARTER_COL, "DEST", "PASSENGERS", "FREIGHT", ...]

    for chunk in pd.read_csv(io.BytesIO(csv_bytes), chunksize=200_000, dtype={QUARTER_COL: "Int64"}, usecols=usecols):
        # Drop rows missing the filter columns (defensive)
        for col in (AIRPORT_COL, QUARTER_COL):
            if col not in chunk.columns:
                raise ValueError(f"Input CSV missing '{col}'")
        chunk = chunk.dropna(subset=[AIRPORT_COL, QUARTER_COL])
        # Normalize types for filtering
        chunk[QUARTER_COL] = chunk[QUARTER_COL].astype("int64").astype(str)
        chunk[AIRPORT_COL] = chunk[AIRPORT_COL].astype(str).str.upper()

        if airports_set:
            chunk = chunk[chunk[AIRPORT_COL].isin(airports_set)]
        if quarters_set:
            chunk = chunk[chunk[QUARTER_COL].isin(quarters_set)]

        if not chunk.empty:
            yield chunk

def _download_year_csv(year: int) -> bytes:
    name = curated_name_for_year(year)
    bc = _bsc().get_container_client(CONTAINER)
    return bc.get_blob_client(name).download_blob().readall()

def _build_single_csv(years: List[int], airports: List[str] | None, quarters: List[str] | None) -> bytes:
    frames = []
    for y in years:
        try:
            data = _download_year_csv(y)
        except Exception:
            continue
        for chunk in _iter_filtered_chunks(data, airports, quarters):
            frames.append(chunk)
    if not frames:
        return b""
    out = pd.concat(frames, ignore_index=True)
    buf = io.StringIO()
    out.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

def _build_split_zip(plan: List[List[int]], airports: List[str] | None, quarters: List[str] | None,
                     start_year: int, end_year: int, airports_slug: str, quarters_slug: str) -> bytes:
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for i, chunk_years in enumerate(plan, start=1):
            csv_bytes = _build_single_csv(chunk_years, airports, quarters)
            fname = f"t100_{airports_slug}_{quarters_slug}_{min(chunk_years)}-{max(chunk_years)}.csv"
            zf.writestr(fname, csv_bytes)
    mem.seek(0)
    return mem.read()

def _compute_split(per_year_rows: Dict[int, int], limit: int) -> List[List[int]]:
    """
    Greedy pack consecutive years into bundles that stay under the limit.
    Returns e.g. [[1990,1991,1992],[1993,1994],...]
    """
    years_sorted = sorted(per_year_rows.keys())
    out: List[List[int]] = []
    current: List[int] = []
    running = 0
    for y in years_sorted:
        cnt = per_year_rows[y]
        if cnt == 0:
            # safe to include in current bundle
            current.append(y)
            continue
        if running + cnt > limit and current:
            out.append(current)
            current = [y]
            running = cnt
        else:
            current.append(y)
            running += cnt
        # Handle pathological case: a single year exceeds the limit
        if running > limit and len(current) == 1:
            out.append(current)
            current = []
            running = 0
    if current:
        out.append(current)
    return out

# ----- HTTP endpoint -----
@app.route(route="export", methods=["GET"])
def export(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Parse inputs
        airports = req.params.get("airports")  # comma sep
        airports_list = [a.strip().upper() for a in airports.split(",")] if airports else None

        quarters = req.params.get("quarters")
        quarters_list = [q.strip() for q in quarters.split(",")] if quarters else None
        # Default to all quarters if not supplied
        if quarters_list:
            for q in quarters_list:
                if q not in {"1", "2", "3", "4"}:
                    return func.HttpResponse("quarters must be 1..4 (comma-separated)", status_code=400)

        start_year = int(req.params.get("start_year") or req.params.get("year") or 1990)
        end_year = int(req.params.get("end_year") or start_year)
        if end_year < start_year:
            return func.HttpResponse("end_year must be >= start_year", status_code=400)

        years = list(range(start_year, end_year + 1))
        dry_run = (req.params.get("dry_run", "false").lower() in ("1", "true", "yes"))

        manifest = _load_manifest()
        total_est, per_year = _estimate_rows(manifest, years, airports_list, quarters_list)

        # For UI/dry-run inspection
        if dry_run:
            plan = _compute_split(per_year, EXCEL_ROW_LIMIT)
            body = {
                "airports": airports_list or "ALL",
                "quarters": quarters_list or ["1","2","3","4"],
                "start_year": start_year,
                "end_year": end_year,
                "estimate_rows": total_est,
                "excel_row_limit": EXCEL_ROW_LIMIT,
                "per_year": per_year,
                "split_plan": plan
            }
            return func.HttpResponse(json.dumps(body, indent=2), mimetype="application/json")

        airports_slug = (",".join(airports_list) if airports_list else "ALL")
        quarters_slug = (",".join(quarters_list) if quarters_list else "Q1-4")

        if total_est <= EXCEL_ROW_LIMIT and total_est > 0:
            csv_bytes = _build_single_csv(years, airports_list, quarters_list)
            fname = f't100_{airports_slug}_{quarters_slug}_{start_year}-{end_year}.csv'
            return func.HttpResponse(
                csv_bytes,
                mimetype="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{fname}"'}
            )

        # Need to split to stay Excel-friendly
        plan = _compute_split(per_year, EXCEL_ROW_LIMIT)
        if not plan or all(len(grp) == 0 for grp in plan):
            # Nothing to export
            return func.HttpResponse("No matching rows for the selection.", status_code=404)

        zip_bytes = _build_split_zip(plan, airports_list, quarters_list,
                                     start_year, end_year, airports_slug, quarters_slug)
        zip_name = f't100_{airports_slug}_{quarters_slug}_{start_year}-{end_year}.zip'
        return func.HttpResponse(
            zip_bytes,
            mimetype="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{zip_name}"'}
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {e}", status_code=500)
