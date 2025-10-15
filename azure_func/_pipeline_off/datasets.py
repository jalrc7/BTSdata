from asyncio.log import logger
import logging

DATASETS = {
    "t100":       "bts-t100",
    "db1bmarket": "bts-db1b",
}

from .storage_helper import upload_file, upload_bytes

logger = logging.getLogger("db1b.datasets")
logger.setLevel(logging.DEBUG)

def ds_upload(dataset: str, tier: str, local_path: str, blob_path: str, **kw):
    container = DATASETS[dataset]
    logging.info(f"Uploading {tier} file to {container}/{blob_path}")
    logger.debug("[ds_upload] dataset=%s tier=%s local=%s -> %s/%s overwrite=%s",
                  dataset, tier, local_path, container, blob_path, kw.get("overwrite", True))
    logger.info(f"Uploading {tier} file to {container}/{blob_path}")
    return upload_file(local_path, blob_path=blob_path, container=container, **kw)

def ds_upload_bytes(dataset: str, tier: str, content: bytes, blob_path: str, **kw):
    container = DATASETS[dataset]
    logging.info(f"Uploading {tier} bytes to {container}/{blob_path}")
    logger.debug("[ds_upload_bytes] dataset=%s tier=%s bytes=len(%s) -> %s/%s overwrite=%s",
                 dataset, tier, len(content), container, blob_path, kw.get("overwrite", True))
    logger.info(f"Uploading {tier} bytes to {container}/{blob_path}")
    return upload_bytes(content, blob_path=blob_path, container=container, **kw)