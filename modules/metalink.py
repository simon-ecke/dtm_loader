#!/usr/bin/env python3
"""
metalink.py  –  async downloader for Metalink 4 feeds
========================================================================

Features
--------
* Parses every `<url>` mirror in the *.meta4* file.
* **Works whether the `<hash>` element is present or not**  
  – files without checksums are downloaded but not verified.
* Tries mirrors in random order, doubles the timeout after each failure.
* Resumes / skips already-verified files.
* Proxy support (`proxy=` arg or HTTPS_PROXY env-var).
* Notebook-safe wrapper (`download_meta4`) that returns an `asyncio.Task`
  when an event loop is already running.

Dependencies
------------
conda install -c conda-forge aiohttp aiofiles tqdm
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import random
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Any

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm


# ─────────────────────────── Metalink parser ──────────────────────────────
def load_meta4(meta4_path: str | Path) -> List[Dict[str, Any]]:
    """
    Return a list of dicts::

        { "name": <filename>,
          "sha":  <sha256 or None>,
          "urls": [<mirror-url>, …] }

    * Accepts both `<hash type="sha256">` and bare `<hash>`.
    * If no `<hash>` is present, ``sha`` is **None** and the caller must
      skip verification for that file.
    """
    ns = {"ml": "urn:ietf:params:xml:ns:metalink"}
    tree = ET.parse(meta4_path)
    items = []

    for f in tree.findall(".//ml:file", ns):
        urls = [u.text for u in f.findall(".//ml:url", ns)]

        # hash may be missing in some meta-files
        h = f.find(".//ml:hash[@type='sha256']", ns) or f.find(".//ml:hash", ns)
        sha = h.text if h is not None else None

        items.append({"name": f.attrib["name"], "sha": sha, "urls": urls})

    return items


# ──────────────────────── download helpers ────────────────────────────────
async def _try_one_url(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    dest: Path,
    url: str,
    sha_expected: str | None,
    timeout: aiohttp.ClientTimeout,
) -> None:
    """Download *url* → *dest*; verify SHA-256 if *sha_expected* given."""
    async with sem, session.get(url, timeout=timeout) as r:
        r.raise_for_status()
        async with aiofiles.open(dest, "wb") as f:
            async for chunk in r.content.iter_chunked(1 << 16):
                await f.write(chunk)

    # verify only when we actually have a checksum
    if sha_expected:
        sha_actual = hashlib.sha256(dest.read_bytes()).hexdigest()
        if sha_actual != sha_expected:
            dest.unlink(missing_ok=True)
            raise ValueError(f"{dest.name}: checksum mismatch")


async def _fetch_one(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    item: Dict[str, Any],
    out_dir: Path,
) -> str:
    """
    Try every mirror until one succeeds, with exponential back-off.
    Returns the filename.
    """
    name, sha, urls = item["name"], item["sha"], item["urls"]
    dest = out_dir / name

    # Already present & verified?
    if sha and dest.exists() and hashlib.sha256(dest.read_bytes()).hexdigest() == sha:
        return name  # skip

    random.shuffle(urls)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=30)

    for i, url in enumerate(urls, 1):
        try:
            await _try_one_url(session, sem, dest, url, sha, timeout)
            return name  # success
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if i == len(urls):
                raise  # all mirrors failed
            timeout = aiohttp.ClientTimeout(
                total=None,
                sock_connect=timeout.sock_connect * 2,
                sock_read=timeout.sock_read * 2,
            )


# ─────────────────────── top-level async worker ───────────────────────────
async def fetch_meta4(
    meta4: str | Path,
    out_dir: str | Path,
    workers: int = 8,
    auth: aiohttp.BasicAuth | None = None,
    proxy: str | None = None,
) -> None:
    """
    Download every file listed in *meta4* into *out_dir* using up to *workers*
    parallel connections.  Skips already verified files.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    items = load_meta4(meta4)
    sem = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers, ttl_dns_cache=600)

    sess_kwargs: dict[str, Any] = {"auth": auth, "connector": connector, "raise_for_status": True}
    if proxy:
        sess_kwargs["proxy"] = proxy

    async with aiohttp.ClientSession(**sess_kwargs) as session:
        tasks = [_fetch_one(session, sem, it, out_dir) for it in items]
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="tiles"):
            await coro


# ───────────────────────── notebook-safe wrapper ──────────────────────────
def download_meta4(
    meta4: str | Path,
    out_dir: str | Path,
    workers: int = 8,
    username: str | None = None,
    password: str | None = None,
    proxy: str | None = os.environ.get("HTTPS_PROXY"),
):
    """
    Wrapper that works both in scripts and inside Jupyter notebooks.

    * If an event loop is already running (Jupyter), returns an `asyncio.Task`
      that the notebook can await.
    * Otherwise starts a fresh loop with `asyncio.run()`.
    """
    auth = aiohttp.BasicAuth(username, password) if username else None

    async def _runner():
        await fetch_meta4(meta4, out_dir, workers, auth, proxy)

    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():              # notebook / anyio context
            return asyncio.create_task(_runner())
    except RuntimeError:
        pass                               # no loop yet (plain script)

    asyncio.run(_runner())
