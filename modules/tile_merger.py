#!/usr/bin/env python3
"""
tile_merger.py  –  merger for DTM tiles
--------------------------------------------------------------------------

Works on Python 3.9 and later. 

Functions
---------
merge_streaming(tile_dir, out_tif)
    Streams every *.tif under *tile_dir* into *out_tif* (BigTIFF, LZW-compressed)

"""
from __future__ import annotations

from pathlib import Path
from typing import Union

import rasterio
from rasterio.windows import from_bounds
from tqdm import tqdm


# ───────────────────────── helpers ─────────────────────────────────────────
def _block(size_px: int) -> int:
    """Largest multiple of 16 ≤ size_px, capped at 512."""
    return max(16, min(512, (size_px // 16) * 16))


def _safe_unlink(path: Path) -> None:
    """Remove *path* if it exists (ignore FileNotFoundError)."""
    try:
        path.unlink()
    except FileNotFoundError:
        pass


# ──────────────────────── main merge routine ───────────────────────────────
def merge_streaming(tile_dir: Union[str, Path], out_tif: Union[str, Path]) -> None:
    """
    Merge all DTM tiles beneath *tile_dir* into one BigTIFF.

    * Streams tile-by-tile → constant RAM
    * Adaptive block size obeys GeoTIFF rules even for small AOIs
    * Overwrites any half-written output from earlier crashes
    * Shows a tqdm progress bar
    """
    tile_dir, out_tif = Path(tile_dir), Path(out_tif)

    # delete corrupt leftovers so StripOffsets errors never reappear
    _safe_unlink(out_tif)

    src_files = list(tile_dir.rglob("*.tif"))
    if not src_files:
        raise RuntimeError(f"No *.tif files found in {tile_dir}")

    # use first tile as template
    with rasterio.open(src_files[0]) as ref:
        dx = ref.transform.a          # pixel size X
        dy = -ref.transform.e         # pixel size Y (negative)
        dtype   = ref.dtypes[0]
        nodata  = ref.nodata
        crs     = ref.crs

    # overall extent
    bounds = [rasterio.open(fp).bounds for fp in src_files]
    left   = min(b.left   for b in bounds)
    bottom = min(b.bottom for b in bounds)
    right  = max(b.right  for b in bounds)
    top    = max(b.top    for b in bounds)

    width  = int(round((right - left) / dx))
    height = int(round((top   - bottom) / dy))

    meta = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype=dtype,
        nodata=nodata,
        crs=crs,
        transform=rasterio.transform.from_origin(left, top, dx, dy),
        tiled=True,
        blockxsize=_block(width),
        blockysize=_block(height),
        compress="lzw",
        BIGTIFF="YES",
    )

    with rasterio.open(out_tif, "w", **meta) as dst, tqdm(src_files, desc="tiles") as bar:
        for fp in bar:
            with rasterio.open(fp) as src:
                win = from_bounds(*src.bounds, transform=dst.transform)
                dst.write(src.read(1), window=win, indexes=1)

    print(f"✅  Mosaic written → {out_tif}")
