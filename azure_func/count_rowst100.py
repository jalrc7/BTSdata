# azure_func/row_count.py
import io
import json
from datetime import date
import pandas as pd
from .function_app import app


AIRPORT_COL = "ORIGIN"
QUARTER_COL = "QUARTER"  # values 1..4

def airport_quarter_counts_from_bytes(csv_bytes: bytes, *, source_name: str = "<memory>") -> dict:
    """
    Returns a dict of counts like:
      { "JFK": {"total": 123, "quarters": {"1": 30, "2": 31, "3": 29, "4": 33}}, ... }
    """
    acc: dict[str, dict] = {}

    for chunk in pd.read_csv(
        io.BytesIO(csv_bytes),
        usecols=[AIRPORT_COL, QUARTER_COL],
        chunksize=200_000,
        dtype={QUARTER_COL: "Int64"},
    ):
        if AIRPORT_COL not in chunk.columns or QUARTER_COL not in chunk.columns:
            raise ValueError(f"Missing '{AIRPORT_COL}' or '{QUARTER_COL}' in {source_name}")

        chunk = chunk.dropna(subset=[AIRPORT_COL, QUARTER_COL])
        chunk[QUARTER_COL] = chunk[QUARTER_COL].astype("int64").astype(str)

        gb = chunk.groupby([AIRPORT_COL, QUARTER_COL]).size()
        for (airport, q), cnt in gb.items():
            if airport not in acc:
                acc[airport] = {"total": 0, "quarters": {"1": 0, "2": 0, "3": 0, "4": 0}}
            acc[airport]["quarters"][q] += int(cnt)
            acc[airport]["total"] += int(cnt)

    return acc


def build_manifest_from_provider(
    years: list[int],
    list_files_for_year,     # callable: (year) -> list[str]
    load_file_bytes,         # callable: (name) -> bytes
) -> dict:
    """
    Build the manifest using storage-agnostic callbacks.
    list_files_for_year(year) should return CSV keys/paths.
    load_file_bytes(name) should return the file contents as bytes.
    """
    manifest = {"generated_at": date.today().isoformat(), "key": AIRPORT_COL, "years": []}

    for y in years:
        files = list_files_for_year(y)
        if not files:
            continue

        year_map: dict[str, dict] = {}
        year_total = 0

        for name in files:
            data = load_file_bytes(name)
            per_blob = airport_quarter_counts_from_bytes(data, source_name=name)

            for airport, vals in per_blob.items():
                if airport not in year_map:
                    year_map[airport] = {"total": 0, "quarters": {"1": 0, "2": 0, "3": 0, "4": 0}}
                year_map[airport]["total"] += vals["total"]
                for q in ("1", "2", "3", "4"):
                    year_map[airport]["quarters"][q] += vals["quarters"].get(q, 0)
                year_total += vals["total"]

        manifest["years"].append({
            "year": y,
            "total_rows": year_total,
            "airports": year_map,
        })
    return manifest


def manifest_to_bytes(manifest: dict) -> bytes:
    """Serialize manifest to JSON bytes for uploading/writing."""
    return json.dumps(manifest, indent=2).encode("utf-8")
