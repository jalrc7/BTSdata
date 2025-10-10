import io, re, zipfile, argparse, requests, tempfile, os
from bs4 import BeautifulSoup
from pathlib import Path 
from pipeline.storage_helper import upload_file
from transform_helper import add_columns
from datetime import date

BASE_OUT = Path(os.getenv("BASE_OUT", Path("out")))  
if os.getenv("FUNCTIONS_WORKER_RUNTIME"):           
    BASE_OUT = Path(os.getenv("BASE_OUT", tempfile.gettempdir())) / "out"
BASE_OUT.mkdir(parents=True, exist_ok=True)

PAGE = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMG&QO_fu146_anzr=Nv4+Pn44vr45"
PARAMS = {"gnoyr_VQ": "FMG", "QO_fu146_anzr": "Nv4 Pn44vr45"} 

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/119.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.transtats.bts.gov/",
    "Origin": "https://www.transtats.bts.gov",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

FIELDS = [
    "DEPARTURES_SCHEDULED","DEPARTURES_PERFORMED","PAYLOAD","SEATS","PASSENGERS","FREIGHT",
    "MAIL","DISTANCE","RAMP_TO_RAMP","AIR_TIME","UNIQUE_CARRIER","AIRLINE_ID",
    "UNIQUE_CARRIER_NAME","UNIQUE_CARRIER_ENTITY","REGION","CARRIER","CARRIER_NAME",
    "CARRIER_GROUP","CARRIER_GROUP_NEW","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID","ORIGIN_CITY_MARKET_ID","ORIGIN",
    "ORIGIN_CITY_NAME","ORIGIN_STATE_ABR","ORIGIN_STATE_FIPS","ORIGIN_STATE_NM","ORIGIN_COUNTRY",
    "ORIGIN_COUNTRY_NAME","ORIGIN_WAC","DEST_AIRPORT_ID","DEST_AIRPORT_SEQ_ID","DEST_CITY_MARKET_ID","DEST",
    "DEST_CITY_NAME","DEST_STATE_ABR","DEST_STATE_FIPS","DEST_STATE_NM","DEST_COUNTRY",
    "DEST_COUNTRY_NAME","DEST_WAC","AIRCRAFT_GROUP","AIRCRAFT_TYPE","AIRCRAFT_CONFIG",
    "YEAR","QUARTER","MONTH","DISTANCE_GROUP","CLASS","DATA_SOURCE"
]

def get_tokens(session):
    session.get("https://www.transtats.bts.gov/", timeout=30)
    r = session.get(PAGE, timeout=30)  
    print("History:", [ (h.status_code, h.headers.get("Location")) for h in r.history ])
    print("GET", r.status_code, r.url, "bytes:", len(r.content))
    r.raise_for_status()

    txt = r.text.lower()
    if "__viewstate" not in txt or "__eventvalidation" not in txt:
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
    }

def run(year="2025", geography="All", period="All"):
    outdir = BASE_OUT / f"year={year}"
    outdir.mkdir(parents=True, exist_ok=True)
    with requests.Session() as s:
        s.headers.update(HEADERS)
        tokens = get_tokens(s)
        payload = {
            **tokens,
            "__EVENTTARGET":"", "__EVENTARGUMENT":"", "__LASTFOCUS":"",
            "cboGeography": geography,
            "cboYear": str(year),
            "cboPeriod": "All" if str(period).lower()=="all" else str(int(period)),
            "chkAllVars":"on",
            "btnDownload":"Download",
        }
        for f in FIELDS:
            payload[f] = "on"

        dr = s.post(PAGE, headers=HEADERS, data=payload, timeout=300)
        print("POST", dr.status_code, "bytes:", dr.headers.get("Content-Length"))
        dr.raise_for_status()

        disp = dr.headers.get("Content-Disposition","")
        m = re.search(r'filename="?([^";]+)"?', disp)
        zip_name = m.group(1) if m else "bts_t100.zip"
        zf = zipfile.ZipFile(io.BytesIO(dr.content))
        print("ZIP:", zip_name, "| entries:", zf.namelist())
        zf.extractall(outdir)
        for name in zf.namelist():
                p = outdir / name
                if p.suffix.lower() == ".csv":
                    p.rename(outdir / f"{p.stem}__{year}{p.suffix}")
        print(f"Extracted CSVs to {outdir.resolve()}")

def handle_year(year: str, geography: str = "All", period: str = "All"):
    run(year=year, geography=geography, period=period)
    outdir = BASE_OUT / f"year={year}"          # <-- use BASE_OUT, not Path("out")
    outdir_updated = outdir / "updated"
    outdir_updated.mkdir(parents=True, exist_ok=True)
    for initial_file in sorted(outdir.glob("*.csv")):
        # Add ASM & RPM columns
        updated_file = outdir_updated / initial_file.name.replace(".csv", "__with_metrics.csv")
        add_columns(initial_file, updated_file)

        # Upload both raw and with-metrics versions
        upload_file(str(initial_file), f"{year}/raw/{initial_file.name}", content_type="text/csv")
        upload_file(str(updated_file), f"{year}/curated/{updated_file.name}", content_type="text/csv")

def run_all_years(start=1990, end=None, geo="All", period="All"):
    if end is None:
        end = date.today().year
    for y in range(start, end + 1):
        handle_year(str(y), geography=geo, period=period)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Download BTS T-100 Segment CSV")
    ap.add_argument("--year", default="2025")
    ap.add_argument("--geo",  default="All")
    ap.add_argument("--period", default="All", help='All or 1..12')
    args = ap.parse_args()
    handle_year(year=args.year, geography=args.geo, period=args.period)