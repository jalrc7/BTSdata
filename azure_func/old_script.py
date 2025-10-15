# import io, re, zipfile, argparse, requests, tempfile, os, logging
# from bs4 import BeautifulSoup
# from pathlib import Path
# from datetime import date
# from ..paths import dataset_out
# from typing import Optional
# from .transform_helper import add_columnsz
# from ..datasets import ds_upload
# from urllib.parse import urlencode
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import threading, time, random
# MAX_WORKERS  = int(os.getenv("DB1B_MAX_WORKERS",  "4"))
# FILE_WORKERS = int(os.getenv("DB1B_FILE_WORKERS", "4"))
# POST_LIMIT   = int(os.getenv("DB1B_POST_LIMIT",   "2"))
# POST_GATE    = threading.Semaphore(POST_LIMIT)
# JITTER       = (0.25, 0.75)

# logger = logging.getLogger("db1b.fetch")
# logger.setLevel(logging.DEBUG)
# from requests.adapters import HTTPAdapter, Retry
# def make_session() -> requests.Session:
#     s = requests.Session()
#     retries = Retry(
#         total=2,                # up to 5 total attempts
#         connect=2,              # retry on connection errors
#         read=2,                 # retry on read timeouts
#         backoff_factor=1.0,     # exponential delay (1.5s, 3s, 6s, etc.)
#         status_forcelist=(500, 502, 503, 504),
#         allowed_methods=frozenset(["GET", "POST"]),
#         respect_retry_after_header=True
#     )
#     adapter = HTTPAdapter(max_retries=retries)
#     s.mount("https://", adapter)
#     s.mount("http://", adapter)
#     s.headers.update(HEADERS)
#     return s


# # --- constants: keep your imports as-is ---

# DATASET = "db1bmarket"
# PAGE = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FHK&QO_fu146_anzr=b4vtv0+n0q+Qr56v0n6v10+f748rB"
# HEADERS = {
#     "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
#                    "AppleWebKit/537.36 (KHTML, like Gecko) "
#                    "Chrome/140.0.0.0 Safari/537.36"),
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#     "Accept-Language": "en-US,en;q=0.9",
#     # updated below so it points to the correct page
#     "Referer": "https://www.transtats.bts.gov/",
#     "Origin": "https://www.transtats.bts.gov",
#     "Connection": "keep-alive",
#     "Upgrade-Insecure-Requests": "1",
    
# }
# FIELDS = [
#     "YEAR", "QUARTER",
#     "ORIGIN_AIRPORT_ID", "ORIGIN_CITY_MARKET_ID", "ORIGIN",
#     "DEST_AIRPORT_ID", "DEST_CITY_MARKET_ID", "DEST",
#     "PASSENGERS", "MARKET_FARE",
#     "MARKET_DISTANCE", "MARKET_MILES_FLOWN", "NONSTOP_MILES",
# ]

# # --- add near the top (imports already include BeautifulSoup etc.) ---
# def _available_years_periods(html: str):
#     """Return (years_set, periods_set) from the BTS form HTML."""
#     soup = BeautifulSoup(html, "html.parser")
#     def opts(name):
#         sel = soup.find("select", {"name": name})
#         if not sel:
#             return set()
#         return {o.get("value", "").strip() for o in sel.find_all("option") if o.get("value")}
#     years = {y for y in opts("cboYear") if y.isdigit()}
#     periods = {p for p in opts("cboPeriod") if p.isdigit()}
#     return years, periods

# def _local_already_done(year: str, q: int) -> bool:
#     """True if updated/*__with_metrics.csv exists OR the local marker exists."""
#     if _local_quarter_done(year, q) or _marker_exists(year, q):
#         return True
#     # Also skip if the curated file(s) are already present in blob paths locally
#     updated_dir = dataset_out(DATASET, f"year={year}", f"Q{q}", "updated")
#     return any(updated_dir.glob("*__with_metrics.csv"))

# # ---- get_tokens(): GET with GET_PARAMS (no other changes) ----
# def get_tokens(session: requests.Session) -> dict:
#    # session.get("https://www.transtats.bts.gov/", timeout=30)
#     r = session.get(PAGE, timeout=30, allow_redirects=True)  
#    # print("History:", [ (h.status_code, h.headers.get("Location")) for h in r.history ])
#     logger.info("History: %s", [(h.status_code, h.headers.get("Location")) for h in r.history])
#     logger.info("Landed at: %s", r.url)
#     print("GET", r.status_code, r.url, "bytes:", len(r.content))
#     logger.debug("GET %s (%d) bytes=%d", r.url, r.status_code, len(r.content))
#     r.raise_for_status()

#     #txt = r.text.lower()
#     if "__VIEWSTATE" not in r.text or "__EVENTVALIDATION" not in r.text:
#         Path("page_debug.html").write_text(r.text, encoding="utf-8")
#         raise RuntimeError("Did not receive ASP.NET form. Saved to page_debug.html")

#     soup = BeautifulSoup(r.text, "html.parser")
#     def val(name):
#         tag = soup.find("input", {"name": name})
#         if not tag:
#             raise KeyError(f"Missing hidden field: {name}")
#         return tag["value"]
#     return {
#         "__VIEWSTATE": val("__VIEWSTATE"),
#         "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
#         "__EVENTVALIDATION"
#         : val("__EVENTVALIDATION"),
#          "_HTML": r.text
#     }


# def run(year="2025", geography="All", quarter="1") -> Path:
#     y = str(year).strip()
#     periods = [1, 2, 3, 4] if str(quarter).lower() == "all" else [int(quarter)]
#     logger.debug("[run] args year=%s geo=%s quarter=%s -> periods=%s", year, geography, quarter, periods)
#     with make_session() as s:
#         s.headers.update({"Referer": PAGE})
#         tokens = get_tokens(s)
#         avail_years, avail_periods = _available_years_periods(tokens["_HTML"])
#         if y not in avail_years:
#             raise RuntimeError(f"Year {y} is not available on BTS form (have: {sorted(avail_years)[:5]} ...).")
#         for q in periods:
#             if str(q) not in avail_periods:
#                 raise RuntimeError(f"Quarter {q} not available on BTS form (have: {sorted(avail_periods)}).")
#         last_outdir = None
#         for q in periods:
#             if _local_already_done(y, q):
#                 logger.info("[run] ⏭️ skip year=%s Q%s (already done locally)", y, q)
#                 last_outdir = dataset_out("db1bmarket", f"year={y}", f"Q{q}", "download")
#                 last_outdir.mkdir(parents=True, exist_ok=True)  # ensure path exists for callers
#                 continue
#             outdir = dataset_out("db1bmarket", f"year={y}", f"Q{q}", "download")
#             logger.debug("[run] q=%s outdir(local)=%s", q, outdir)

#             outdir.mkdir(parents=True, exist_ok=True)
#             outdir = dataset_out("db1bmarket", f"year={year}", f"Q{q}", "download")
#             logger.debug("[run] q=%s outdir(local)=%s", q, outdir)
#             outdir.mkdir(parents=True, exist_ok=True)
#             payload = {
#                 **tokens,
#                 "__EVENTTARGET": "", "__EVENTARGUMENT": "", "__LASTFOCUS": "",
#                 "txtSearch": "",
#                 "cboGeography": geography,
#                 "cboYear": str(year),
#                 "cboPeriod": str(q),          # <-- always a concrete quarter
#                 "btnDownload": "Download",
#             }
#             for f in FIELDS:
#                 payload[f] = "on"

#             dr = s.post(PAGE, data=payload, timeout=(15, 90), allow_redirects=True)
#             print("POST", dr.status_code, "CT:", dr.headers.get("Content-Type"))
#             logger.debug("[run] POST q=%s payload.cboPeriod=%s", q, payload["cboPeriod"])

#             logger.debug("[run] POST status=%s CT=%s", dr.status_code, dr.headers.get("Content-Type"))
#             dr.raise_for_status()

#             ct = (dr.headers.get("Content-Type") or "").lower()
#             if "zip" not in ct:
#                 Path("error_not_zip.html").write_text(dr.text, encoding="utf-8")
#                 raise RuntimeError(f"Expected ZIP, got {ct} for Q{q}")

#             # unzip & rename with quarter suffix so they don't overwrite
#             zf = zipfile.ZipFile(io.BytesIO(dr.content))
#             zf.extractall(outdir)
#             logger.debug("[run] extracted %d files into %s", len(zf.namelist()), outdir.resolve())
#             for name in zf.namelist():
#                 p = (outdir / name)
#                 if p.suffix.lower() == ".csv":
#                     newp = outdir / f"{p.stem}__{year}Q{q}{p.suffix}"
#                     if newp.exists():
#                         newp.unlink()
#                     p.rename(newp)
#                     logger.debug("[run] renamed CSV -> %s", newp.name)

#             logger.info("[run] ✅ year=%s Q%s ready in %s", y, q, outdir.resolve())
#             last_outdir = outdir

#     return last_outdir

# def _local_quarter_done(year: str, q: int) -> bool:
#     """Check if we already created curated files for this quarter locally."""
#     updated_dir = dataset_out(DATASET, f"year={year}", f"Q{q}", "updated")
#     if not updated_dir.exists():
#         return False
#     return any(updated_dir.glob("*__with_metrics.csv"))

# def _write_done_marker(year: str, q: int) -> None:
#     """Optional local marker file to persist progress even if code stops midway."""
#     marker_dir = dataset_out(DATASET, "markers")
#     marker_dir.mkdir(parents=True, exist_ok=True)
#     (marker_dir / f"{year}-Q{q}.done").write_text("ok", encoding="utf-8")

# def _marker_exists(year: str, q: int) -> bool:
#     """Check if a local marker file already exists."""
#     marker_file = dataset_out(DATASET, "markers") / f"{year}-Q{q}.done"
#     return marker_file.exists()


# def handle_year(year: str, geography: str = "All", quarter: str = "All") -> None:
#     quarters = [1, 2, 3, 4] if str(quarter).lower() in {"all", "*"} else [int(quarter)]

#     for q in quarters:
#         # ✅ skip early if already processed locally
#         if _local_quarter_done(year, q) or _marker_exists(year, q):
#             logger.info("[handle_year] ⏭️ skipping year=%s Q%s (already done)", year, q)
#             continue

#         logger.debug("[handle_year] ▶️ start year=%s q=%s", year, q)
#         try:
#             outdir = run(year=year, geography=geography, quarter=q)
#         except Exception as e:
#             logger.exception("[handle_year] ❌ download failed for year=%s Q%s: %s", year, q, e)
#             continue

#         outdir_updated = dataset_out(DATASET, f"year={year}", f"Q{q}", "updated")
#         outdir_updated.mkdir(parents=True, exist_ok=True)

#         for initial_file in sorted(outdir.glob("*.csv")):
#             raw_blob = f"{year}/Q{q}/raw/{initial_file.name}"
#             ds_upload(DATASET, "raw", str(initial_file), raw_blob, content_type="text/csv")

#             updated_file = outdir_updated / initial_file.name.replace(".csv", "__with_metrics.csv")
#             add_columns(initial_file, updated_file)

#             curated_blob = f"{year}/Q{q}/curated/{updated_file.name}"
#             ds_upload(DATASET, "curated", str(updated_file), curated_blob, content_type="text/csv", overwrite=False)

#         _write_done_marker(year, q)
#         logger.info("[handle_year] ✅ done year=%s Q%s (marker written)", year, q)

# def run_all_years(start: int = 1993, end: Optional[int] = None,
#     geo: str = "All", quarter: str = "All") -> None:
#     if end is None:
#         end = date.today().year
#     for y in range(start, end + 1):
#         handle_year(str(y), geography=geo, quarter=quarter)

# #for backfill
# if __name__ == "__main__":
#     logging.basicConfig(
#         level=logging.DEBUG,
#         format="%(asctime)s %(levelname)s %(name)s: %(message)s",
#         force=True,
#     )
#     ap = argparse.ArgumentParser(description="Download BTS DB1B Market CSV")
#     ap.add_argument("--year", default="2025")
#     ap.add_argument("--geo",  default="All")
#     ap.add_argument("--quarter", default="All", help="1..4")
#     args = ap.parse_args()
#     handle_year(year=args.year, geography=args.geo, quarter=args.quarter)
   