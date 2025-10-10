# azure_func/pipeline/storage_helper.py
import logging
import os
from pathlib import Path
from typing import Iterable, Optional
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError 
import logging
logger = logging.getLogger("db1b.storage")
logger.setLevel(logging.DEBUG)

def _get_conn_str() -> str:
    try:
        return os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    except KeyError as e:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING is not set. "
            "Export it locally or add it to your Function App settings."
        ) from e

_SVC: Optional[BlobServiceClient] = None
def _service() -> BlobServiceClient:
    global _SVC
    if _SVC is None:
        _SVC = BlobServiceClient.from_connection_string(_get_conn_str())
    return _SVC

def get_container_client(container: str):
    """Return a ContainerClient, creating the container if it doesn't exist."""
    cc = _service().get_container_client(container)
    try:
        cc.create_container()
    except Exception:
        # already exists or can't create due to perms; safe to ignore here
        pass
    return cc

def _content_settings_for(name: str, explicit: Optional[str]) -> Optional[ContentSettings]:
    if explicit:
        return ContentSettings(content_type=explicit)
    ext = Path(name).suffix.lower()
    if ext == ".csv":
        return ContentSettings(content_type="text/csv")
    if ext in (".parquet", ".pq"):
        return ContentSettings(content_type="application/octet-stream")
    if ext == ".json":
        return ContentSettings(content_type="application/json")
    if ext == ".zip":
        return ContentSettings(content_type="application/zip")
    return None

# --- public API ---------------------------------------------------------------

def upload_file(local_path: str, *, container: str, blob_path: str,
                overwrite: bool = False, content_type: Optional[str] = None) -> None:
    """Upload a local file to `container` at `blob_path`.
       If overwrite=False and blob exists, we log and skip (idempotent)."""
    cc = get_container_client(container)

    # Build content settings (no kwargs)
    cs = _content_settings_for(blob_path, content_type)
    extra = {}
    if cs:
        extra["content_settings"] = cs

    with open(local_path, "rb") as f:
        try:
            cc.upload_blob(name=blob_path, data=f, overwrite=overwrite, **extra)
            logger.info("[upload_file] ✅ uploaded %s/%s overwrite=%s",
                        container, blob_path, overwrite)
        except ResourceExistsError:
            if overwrite:
                logger.exception("[upload_file] overwrite=True but got BlobAlreadyExists for %s/%s",
                                 container, blob_path)
                raise
            logger.info("[upload_file] ⏭️ exists, skipping (overwrite=False): %s/%s",
                        container, blob_path)
            
def upload_bytes(content: bytes, *, container: str, blob_path: str,
                 overwrite: bool = False, content_type: Optional[str] = None) -> None:
    cc = get_container_client(container)

    cs = _content_settings_for(blob_path, content_type)
    extra = {}
    if cs:
        extra["content_settings"] = cs

    try:
        cc.upload_blob(name=blob_path, data=content, overwrite=overwrite, **extra)
        logger.info("[upload_bytes] ✅ uploaded %s/%s overwrite=%s (bytes=%d)",
                    container, blob_path, overwrite, len(content))
    except ResourceExistsError:
        if overwrite:
            logger.exception("[upload_bytes] overwrite=True but got BlobAlreadyExists for %s/%s",
                             container, blob_path)
            raise
        logger.info("[upload_bytes] ⏭️ exists, skipping (overwrite=False): %s/%s",
                    container, blob_path)

def download_blob(blob_path: str, *, container: str) -> bytes:
    """Download a blob and return its bytes."""
    cc = get_container_client(container)
    return cc.download_blob(blob_path).readall()

def list_blob_names(prefix: str = "", *, container: str) -> Iterable[str]:
    """List blob names in `container` (optionally under `prefix`)."""
    cc = get_container_client(container)
    for b in cc.list_blobs(name_starts_with=prefix):
        yield b.name
