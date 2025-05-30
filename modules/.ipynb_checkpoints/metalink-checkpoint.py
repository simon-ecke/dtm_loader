
# ---------------------------------------------------------------------------
# Metalink downloader (async, resumable, checksum-verified)
# ---------------------------------------------------------------------------
import asyncio, os, hashlib, xml.etree.ElementTree as ET
from pathlib import Path
import aiohttp, aiofiles
from tqdm.asyncio import tqdm

# ------------ helper to read the .meta4 -------------------------------------------------
def load_meta4(meta4_path):
    ns = {"ml": "urn:ietf:params:xml:ns:metalink"}
    tree = ET.parse(meta4_path)
    files = []
    for f in tree.findall(".//ml:file", ns):
        url = f.find(".//ml:url", ns).text
        sha = f.find(".//ml:hash", ns).text
        files.append((url, sha, f.attrib["name"]))
    return files

# ------------ async download core ------------------------------------------------------
async def _fetch(session, sem, out_dir, url, sha_expected, name):
    dest = Path(out_dir) / name
    # resume if already OK
    if dest.exists() and hashlib.sha256(dest.read_bytes()).hexdigest() == sha_expected:
        return name
    async with sem, session.get(url) as r:
        r.raise_for_status()
        async with aiofiles.open(dest, "wb") as f:
            async for chunk in r.content.iter_chunked(1 << 16):
                await f.write(chunk)
    # verify
    if hashlib.sha256(dest.read_bytes()).hexdigest() != sha_expected:
        dest.unlink(missing_ok=True)
        raise ValueError(f"{name}: checksum mismatch")
    return name

async def fetch_meta4(meta4, out_dir, workers=8, auth=None):
    out_dir = Path(out_dir);  out_dir.mkdir(parents=True, exist_ok=True)
    sem    = asyncio.Semaphore(workers)
    items  = load_meta4(meta4)
    connector = aiohttp.TCPConnector(limit=workers)
    async with aiohttp.ClientSession(auth=auth, connector=connector) as sess:
        tasks = [_fetch(sess, sem, out_dir, *it) for it in items]
        for t in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="tiles"):
            await t

def download_meta4(meta4, out_dir, workers=8, username=None, password=None):
    auth = aiohttp.BasicAuth(username, password) if username else None
    asyncio.run(fetch_meta4(meta4, out_dir, workers, auth))
