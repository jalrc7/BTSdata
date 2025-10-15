# azure_func/pipeline/blob_utils.py
import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

DB1B_CONTAINER = os.getenv("DB1B_CONTAINER", "bts-db1b")

def _get_blob_service() -> BlobServiceClient:
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set")
    return BlobServiceClient.from_connection_string(conn)

def _blob_curated_exists(year: int, q: int) -> bool:
    """True if any curated blob exists at {year}/Q{q}/curated/..."""
    bsc = _get_blob_service()
    cc = bsc.get_container_client(DB1B_CONTAINER)
    try:
        for _ in cc.list_blobs(name_starts_with=f"{year}/Q{q}/curated/"):
            return True
    except ResourceNotFoundError:
        return False
    return False

def _blob_marker_exists(year: int, q: int) -> bool:
    """True if markers/{year}-Q{q}.done exists."""
    bsc = _get_blob_service()
    cc = bsc.get_container_client(DB1B_CONTAINER)
    name = f"markers/{year}-Q{q}.done"
    try:
        cc.get_blob_client(name).get_blob_properties()
        return True
    except Exception:
        return False

# Optional generic helpers that you already had in db1b_timer:
def _ensure_container(container: str):
    bsc = _get_blob_service()
    try:
        bsc.create_container(container)
    except Exception:
        pass

def blob_exists(container: str, name: str) -> bool:
    bsc = _get_blob_service()
    bc = bsc.get_container_client(container)
    # fast HEAD; no 404 exceptions when using .exists()
    return bc.get_blob_client(name).exists()
