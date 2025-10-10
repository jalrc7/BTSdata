import azure.functions as func
import json, os
from azure.storage.blob import BlobServiceClient
from function_app import app

CONTAINER = os.getenv("BTS_CONTAINER", "bts-t100")
MANIFEST_BLOB = "manifests/index.json"


def _bc():
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    bsc = BlobServiceClient.from_connection_string(conn)
    return bsc.get_container_client(CONTAINER)

@app.function_name(name="ListT100")
@app.route(route="list", auth_level=func.AuthLevel.FUNCTION)
def list_t100(req: func.HttpRequest) -> func.HttpResponse:
    # 1. Download manifest
    blob_client = _bc().get_blob_client(MANIFEST_BLOB)
    raw = blob_client.download_blob().readall()
    manifest = json.loads(raw.decode("utf-8"))


    years = [entry["year"] for entry in manifest["years"]]

    origins = {"ALL"}
    for entry in manifest["years"]:
        origins.update(entry.get("airports", {}).keys())


    quarters = ["1", "2", "3", "4"]

    payload = {
        "years": sorted(years),
        "quarters": quarters,
        "origins": sorted(origins)
    }
    return func.HttpResponse(json.dumps(payload), mimetype="application/json")