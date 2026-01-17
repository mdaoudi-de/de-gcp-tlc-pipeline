from __future__ import annotations

import hashlib
from pathlib import Path

import requests

from de_pipeline.common.logging import get_logger

logger = get_logger(__name__)


def download_file(url: str, dest_path: Path, timeout_sec: int, chunk_size: int) -> dict:
    """
    Download a file with streaming to disk.
    Returns metadata: bytes, md5.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading: {url}")
    logger.info(f"To: {dest_path}")

    md5 = hashlib.md5()
    total_bytes = 0

    with requests.get(url, stream=True, timeout=timeout_sec) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                md5.update(chunk)
                total_bytes += len(chunk)

    meta = {"url": url, "path": str(dest_path), "bytes": total_bytes, "md5": md5.hexdigest()}
    logger.info(f"Done: {total_bytes} bytes, md5={meta['md5']}")
    return meta
