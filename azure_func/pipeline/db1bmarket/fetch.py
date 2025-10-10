import io, zipfile, argparse, requests, os, logging, time, random, shutil
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import date
from ..paths import dataset_out
from typing import Optional, Tuple, Iterable
from .transform_helper import add_columns
from ..datasets import ds_upload
from urllib.parse import urlencode
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading, time, random
import shutil 
from requests.adapters import HTTPAdapter, Retry
from http.client import RemoteDisconnected
from pipeline.blob_utils import _blob_curated_exists, _blob_marker_exists


logger = logging.getLogger("db1b.fetch")
logger.setLevel(logging.DEBUG)

# ------------------------------- Tunables ------------------------------------

CONNECT_TO  = int(os.getenv("DB1B_CONNECT_TIMEOUT", "15"))
READ_TO     = int(os.getenv("DB1B_READ_TIMEOUT",    "180"))
MAX_RETRIES = int(os.getenv("DB1B_MAX_RETRIES",     "5"))
TIMEOUT     = (CONNECT_TO, READ_TO)

# Patient fallback knobs
PATIENT_MODE = os.getenv("DB1B_PATIENT_MODE", "1") not in {"0", "false", "no"}
PATIENT_READ_TO = int(os.getenv("DB1B_READ_TIMEOUT_PATIENT", "420"))  # seconds for final attempt
CHUNK_SIZE = int(os.getenv("DB1B_STREAM_CHUNK_SIZE", "65536"))        # 64 KiB
CHUNK_STALL_SECONDS = int(os.getenv("DB1B_CHUNK_STALL_SECONDS", "45"))# no-progress watchdog


# Hard wall-clock stop per quarter to avoid function timeout
PER_QUARTER_DEADLINE_S = int(os.getenv("DB1B_PER_QUARTER_DEADLINE_S", "900"))

# How many quarters to do per invocation (for range runners below)
MAX_Q_PER_INVOC = int(os.getenv("DB1B_MAX_Q_PER_INVOC", "1"))

KEEP_LOCAL = os.getenv("DB1B_KEEP_LOCAL", "0").lower() not in {"0", "false", "no"}

def _jitter(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))

# ------------------------------- Constants -----------------------------------

DATASET = "db1bmarket"
PAGE = (
    "https://www.transtats.bts.gov/"
    "DL_SelectFields.aspx?gnoyr_VQ=FHK&QO_fu146_anzr=b4vtv0+n0q+Qr56v0n6v10+f748rB"
)
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/140.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PAGE,
    "Origin": "https://www.transtats.bts.gov",
    # avoid flaky keep-alive reuse that caused RemoteDisconnected
    "Connection": "close",
    "Upgrade-Insecure-Requests": "1",
}
FIELDS = [
    "YEAR", "QUARTER",
    "ORIGIN_AIRPORT_ID", "ORIGIN_CITY_MARKET_ID", "ORIGIN",
    "DEST_AIRPORT_ID", "DEST_CITY_MARKET_ID", "DEST",
    "PASSENGERS", "MARKET_FARE",
    "MARKET_DISTANCE", "MARKET_MILES_FLOWN", "NONSTOP_MILES",
]

# ------------------------------- HTTP Utils ----------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False  # ignore any proxy env vars that may hang
    retries = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=1, pool_maxsize=1)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s

def _available_years_periods(html: str):
    soup = BeautifulSoup(html, "html.parser")
    def opts(name):
        sel = soup.find("select", {"name": name})
        if not sel:
            return set()
        return {o.get("value", "").strip() for o in sel.find_all("option") if o.get("value")}
    years = {y for y in opts("cboYear") if y.isdigit()}
    periods = {p for p in opts("cboPeriod") if p.isdigit()}
    return years, periods

def get_tokens(session: requests.Session) -> dict:
    attempts = MAX_RETRIES
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            _jitter(0.25, 0.9)
            # Use patient read timeout on the last attempt
            rt = PATIENT_READ_TO if (PATIENT_MODE and attempt == attempts) else READ_TO
            r = session.get(PAGE, timeout=(CONNECT_TO, rt), allow_redirects=True)
            logger.info("History: %s", [(h.status_code, h.headers.get("Location")) for h in r.history])
            logger.info("Landed at: %s", r.url)
            logger.debug("GET %s (%d) bytes=%d", r.url, r.status_code, len(r.content))
            r.raise_for_status()

            if "__VIEWSTATE" not in r.text or "__EVENTVALIDATION" not in r.text:
                Path("page_debug.html").write_text(r.text, encoding="utf-8")
                raise RuntimeError("Did not receive ASP.NET form. Saved to page_debug.html")

            soup = BeautifulSoup(r.text, "html.parser")
            def val(name):
                tag = soup.find("input", {"name": name})
                if not tag:
                    raise KeyError(f"Missing hidden field: {name}")
                return tag["value"]

            return {
                "__VIEWSTATE": val("__VIEWSTATE"),
                "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
                "__EVENTVALIDATION": val("__EVENTVALIDATION"),
                "_HTML": r.text,
            }

        except (requests.ReadTimeout, requests.ConnectTimeout, requests.ConnectionError, RemoteDisconnected) as e:
            last_exc = e
            if attempt >= attempts:
                break
            backoff = min(60.0, (2 ** attempt) + random.random())
            logger.warning(
                "%s on GET; retrying in %.1fs (attempt %d/%d)",
                type(e).__name__, backoff, attempt + 1, attempts
            )
            time.sleep(backoff)
        except requests.HTTPError as e:
            # retry only on 5xx
            if attempt < attempts and 500 <= e.response.status_code < 600:
                backoff = min(60.0, (2 ** attempt) + random.random())
                logger.warning("HTTP %d on GET; retrying in %.1fs (attempt %d/%d)",
                               e.response.status_code, backoff, attempt + 1, attempts)
                time.sleep(backoff)
                continue
            raise
    raise RuntimeError(f"GET form failed after {attempts} attempts: {last_exc}")



def _local_quarter_done(year: str, q: int) -> bool:
    updated_dir = dataset_out(DATASET, f"year={year}", f"Q{q}", "updated")
    return updated_dir.exists() and any(updated_dir.glob("*__with_metrics.csv"))

def _write_done_marker_local(year: str, q: int) -> None:
    marker_dir = dataset_out(DATASET, "markers")
    marker_dir.mkdir(parents=True, exist_ok=True)
    (marker_dir / f"{year}-Q{q}.done").write_text("ok", encoding="utf-8")

def _marker_exists_local(year: str, q: int) -> bool:
    return (dataset_out(DATASET, "markers") / f"{year}-Q{q}.done").exists()

def _already_done_anywhere(year: int, q: int) -> bool:
    """
    Unified check: local curated or marker, OR blob curated or marker.
    """
    try_local = _local_quarter_done(str(year), q) or _marker_exists_local(str(year), q)
    try_blob  = _blob_curated_exists(year, q) or _blob_marker_exists(year, q)
    return try_local or try_blob

# ------------------------------- Core Runner ----------------------------------

def _post_and_get_zip(session: requests.Session, payload: dict) -> bytes:
    """
    POST the BTS form and return ZIP bytes.
    - Fast attempts first (buffered response).
    - Final attempt (if enabled) switches to 'patient streaming' to survive slow servers.
    """
    start = time.monotonic()
    last_exc: Optional[BaseException] = None

    total_attempts = MAX_RETRIES
    for attempt in range(1, total_attempts + 1):
        # per-quarter wall clock guard
        if time.monotonic() - start > PER_QUARTER_DEADLINE_S:
            raise TimeoutError(f"Per-quarter deadline ({PER_QUARTER_DEADLINE_S}s) exceeded")

        # Decide whether to use streaming "patient" mode
        use_patient = (
            PATIENT_MODE and attempt == total_attempts
        )

        try:
            _jitter(0.25, 0.9)

            if not use_patient:
                # Normal (fast) attempt: regular buffered POST
                dr = session.post(PAGE, data=payload, timeout=TIMEOUT, allow_redirects=True)
                logger.debug("POST -> %s", dr.status_code)
                dr.raise_for_status()
                ct = (dr.headers.get("Content-Type") or "").lower()
                if ("zip" not in ct) and ("application/octet-stream" not in ct):
                    Path("error_not_zip.html").write_text(dr.text, encoding="utf-8")
                    raise RuntimeError(f"Expected ZIP, got Content-Type={ct}")
                return dr.content

            else:
                # Patient final attempt: longer read timeout + streaming
                logger.warning(
                    "Switching to patient streaming mode (attempt %d/%d, read_to=%ss)...",
                    attempt, total_attempts, PATIENT_READ_TO
                )
                # We don’t mutate the original session adapter; just override timeout here.
                with session.post(
                    PAGE,
                    data=payload,
                    allow_redirects=True,
                    stream=True,
                    timeout=(CONNECT_TO, PATIENT_READ_TO),
                ) as resp:
                    resp.raise_for_status()
                    ct = (resp.headers.get("Content-Type") or "").lower()
                    if ("zip" not in ct) and ("application/octet-stream" not in ct):
                        # try to read a bit of the body to dump HTML for diagnosis
                        try:
                            sample = resp.raw.read(4096, decode_content=True)
                        except Exception:
                            sample = b""
                        Path("error_not_zip.html").write_bytes(sample or b"")
                        raise RuntimeError(f"Expected ZIP, got Content-Type={ct}")

                    # Stream the body into memory with a stall watchdog
                    buf = io.BytesIO()
                    last_progress = time.monotonic()
                    for chunk in resp.iter_content(CHUNK_SIZE):
                        if chunk:
                            buf.write(chunk)
                            last_progress = time.monotonic()
                        # Stall watchdog
                        if (time.monotonic() - last_progress) > CHUNK_STALL_SECONDS:
                            raise requests.ReadTimeout(f"No data for {CHUNK_STALL_SECONDS}s while streaming")

                    return buf.getvalue()

        except (requests.ReadTimeout, requests.ConnectTimeout, requests.ConnectionError, RemoteDisconnected) as e:
            last_exc = e
            if attempt >= total_attempts:
                break
            backoff = min(60.0, (2 ** attempt) + random.random())
            logger.warning(
                "%s; retrying in %.1fs (attempt %d/%d)",
                type(e).__name__, backoff, attempt + 1, total_attempts
            )
            time.sleep(backoff)
        except requests.HTTPError as e:
            if attempt < total_attempts and 500 <= e.response.status_code < 600:
                backoff = min(60.0, (2 ** attempt) + random.random())
                logger.warning(
                    "HTTP %d; retrying in %.1fs (attempt %d/%d)",
                    e.response.status_code, backoff, attempt + 1, total_attempts
                )
                time.sleep(backoff)
                continue
            raise

    raise RuntimeError(f"POST failed after {total_attempts} attempts: {last_exc}")

def run(year="2025", geography="All", quarter="1") -> Path:
    """
    Download one or more quarters for a given year (or All), rename CSVs, return last outdir.
    Skips any quarter that's already done locally or in Blob (curated/marker).
    """
    y = str(year).strip()
    periods = [1, 2, 3, 4] if str(quarter).lower() == "all" else [int(quarter)]
    logger.debug("[run] args year=%s geo=%s quarter=%s -> periods=%s", year, geography, quarter, periods)

    last_outdir: Optional[Path] = None
    with make_session() as s:
        tokens = get_tokens(s)
        avail_years, avail_periods = _available_years_periods(tokens["_HTML"])
        if y not in avail_years:
            raise RuntimeError(f"Year {y} is not available on BTS form (have e.g.: {sorted(avail_years)[:5]} …).")
        for q in periods:
            if str(q) not in avail_periods:
                raise RuntimeError(f"Quarter {q} not available on BTS form (have: {sorted(avail_periods)}).")

        for q in periods:
            if _already_done_anywhere(int(y), q):
                logger.info("[run] ⏭️ skip year=%s Q%s (already done locally/blob)", y, q)
                outdir = dataset_out(DATASET, f"year={y}", f"Q{q}", "download")
                outdir.mkdir(parents=True, exist_ok=True)
                last_outdir = outdir
                continue

            quarter_start = time.monotonic()
            outdir = dataset_out(DATASET, f"year={y}", f"Q{q}", "download")
            outdir.mkdir(parents=True, exist_ok=True)
            logger.debug("[run] q=%s outdir(local)=%s", q, outdir)

            payload = {
                **tokens,
                "__EVENTTARGET": "", "__EVENTARGUMENT": "", "__LASTFOCUS": "",
                "txtSearch": "",
                "cboGeography": geography,
                "cboYear": y,
                "cboPeriod": str(q),
                "btnDownload": "Download",
            }
            for f in FIELDS:
                payload[f] = "on"

            zip_bytes = _post_and_get_zip(s, payload)
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                zf.extractall(outdir)
                logger.debug("[run] extracted %d files into %s", len(zf.namelist()), outdir.resolve())
                for name in zf.namelist():
                    p = (outdir / name)
                    if p.suffix.lower() != ".csv":
                        continue
                    newp = outdir / f"{p.stem}__{y}Q{q}{p.suffix}"
                    if newp.exists():
                        newp.unlink()
                    p.rename(newp)
                    logger.debug("[run] renamed CSV -> %s", newp.name)

            last_outdir = outdir
            _jitter(0.6, 1.8)

            elapsed = time.monotonic() - quarter_start
            if elapsed > PER_QUARTER_DEADLINE_S:
                raise TimeoutError(
                    f"Quarter year={y} Q{q} exceeded allotted time ({elapsed:.1f}s > {PER_QUARTER_DEADLINE_S}s)"
                )

    return last_outdir or dataset_out(DATASET, f"year={y}", f"Q{periods[-1]}", "download")

# ------------------------------- Orchestration --------------------------------

def handle_year(year: str, geography: str = "All", quarter: str = "All") -> None:
    quarters = [1, 2, 3, 4] if str(quarter).lower() in {"all", "*"} else [int(quarter)]

    for q in quarters:
        if _already_done_anywhere(int(year), q):
            logger.info("[handle_year] ⏭️ skipping year=%s Q%s (already done)", year, q)
            continue

        outdir = None
        outdir_updated = None
        logger.debug("[handle_year] ▶️ start year=%s q=%s", year, q)

        try:
            outdir = run(year=year, geography=geography, quarter=q)

            outdir_updated = dataset_out(DATASET, f"year={year}", f"Q{q}", "updated")
            outdir_updated.mkdir(parents=True, exist_ok=True)

            for initial_file in sorted(outdir.glob("*.csv")):
                # upload RAW (optional—keep if you need audit)
                raw_blob = f"{year}/Q{q}/raw/{initial_file.name}"
                ds_upload(DATASET, "raw", str(initial_file), raw_blob, content_type="text/csv")

                # build curated
                updated_file = outdir_updated / initial_file.name.replace(".csv", "__with_metrics.csv")
                add_columns(initial_file, updated_file)

                # upload CURATED
                curated_blob = f"{year}/Q{q}/curated/{updated_file.name}"
                ds_upload(DATASET, "curated", str(updated_file), curated_blob,
                          content_type="text/csv", overwrite=False)

                if not KEEP_LOCAL:
                    try: updated_file.unlink()
                    except FileNotFoundError: pass
                    try: initial_file.unlink()
                    except FileNotFoundError: pass

            # local marker (tiny); blob curated presence already acts as a global marker
            _write_done_marker_local(year, q)
            logger.info("[handle_year] ✅ done year=%s Q%s (marker written)", year, q)

        except Exception as e:
            logger.exception("[handle_year] ❌ failed year=%s Q%s: %s", year, q, e)

        finally:
            if not KEEP_LOCAL:
                try: shutil.rmtree(outdir, ignore_errors=True)
                except Exception: pass
                try: shutil.rmtree(outdir_updated, ignore_errors=True)
                except Exception: pass

# ----------------------- Range runner: “start from last missing” --------------

def _quarters_in_range(start_year: int, end_year: int) -> Iterable[Tuple[int, int]]:
    for y in range(start_year, end_year + 1):
        for q in (1, 2, 3, 4):
            yield (y, q)

def process_next_quarters_by_cloud_progress(start_year: int,
                                            end_year:   int,
                                            geography:  str = "All",
                                            max_quarters_per_invocation: int = None) -> int:
    """
    Look at COMPLETED quarters (local OR blob), then process the *next* missing
    quarter(s) within [start_year, end_year]. Returns how many were processed.
    """
    if max_quarters_per_invocation is None:
        max_quarters_per_invocation = MAX_Q_PER_INVOC

    done_this_run = 0
    for (y, q) in _quarters_in_range(start_year, end_year):
        if _already_done_anywhere(y, q):
            continue
        # Process exactly this one quarter (resume-safe)
        handle_year(year=str(y), geography=geography, quarter=str(q))
        done_this_run += 1
        if done_this_run >= max_quarters_per_invocation:
            break

    if done_this_run == 0:
        logger.info("[process_next_quarters_by_cloud_progress] ✅ %d-%d already complete.",
                    start_year, end_year)
    else:
        logger.info("[process_next_quarters_by_cloud_progress] ✅ processed %d quarter(s) in %d-%d",
                    done_this_run, start_year, end_year)
    return done_this_run

# ------------------------------- CLI / Script ---------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    ap = argparse.ArgumentParser(description="Download BTS DB1B Market CSV")
    ap.add_argument("--year", default=None, help="Year or omit to use range mode")
    ap.add_argument("--geo",  default="All")
    ap.add_argument("--quarter", default=None, help="1..4 or All")
    ap.add_argument("--range", nargs=2, metavar=("START", "END"),
                    help="Use range mode: START END (e.g. 1993 2010)")
    ap.add_argument("--maxq", type=int, default=None,
                    help="Max quarters to process this run (default from env DB1B_MAX_Q_PER_INVOC)")
    args = ap.parse_args()

    if args.range:
        start, end = int(args.range[0]), int(args.range[1])
        process_next_quarters_by_cloud_progress(start, end, geography=args.geo,
                                                max_quarters_per_invocation=args.maxq)
    else:
        # Single year mode (backward compatible)
        year = args.year or str(date.today().year)
        quarter = args.quarter or "All"
        handle_year(year=year, geography=args.geo, quarter=quarter)
   