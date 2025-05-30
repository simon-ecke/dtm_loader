
from pathlib import Path
import rasterio
from rasterio.windows import from_bounds
from tqdm import tqdm

def merge_streaming(tile_dir, out_tif):
    """
    Rasterio-only mosaic that writes directly to disk, window by window.
    Much lower RAM use than rasterio.merge(), but slower than GDAL.
    """
    tile_dir = Path(tile_dir)
    src_files = list(tile_dir.rglob("*.tif"))
    if not src_files:
        raise RuntimeError("No tiles found")

    # Read first tile to get crs, dtype, etc.
    with rasterio.open(src_files[0]) as ref:
        meta = ref.meta.copy()
        blockx, blocky = ref.block_shapes[0]  # assume all equal
        dtype = ref.dtypes[0]

    # Calculate overall bounds
    bounds = [rasterio.open(f).bounds for f in src_files]
    left   = min(b.left   for b in bounds)
    bottom = min(b.bottom for b in bounds)
    right  = max(b.right  for b in bounds)
    top    = max(b.top    for b in bounds)

    width  = int(round((right - left) / meta["transform"].a))
    height = int(round((top - bottom) / -meta["transform"].e))

    meta.update(
        driver="GTiff",
        width=width,
        height=height,
        compress="lzw",
        tiled=True,
        blockxsize=blockx,
        blockysize=blocky,
        BIGTIFF="YES",
        dtype=dtype,
    )

    with rasterio.open(out_tif, "w", **meta) as dst:
        for tif in tqdm(src_files, desc="mosaicking"):
            with rasterio.open(tif) as src:
                window = from_bounds(*src.bounds, transform=dst.transform)
                data   = src.read(1)  # single-band DEM
                dst.write(data, window=window, indexes=1)

    print("✅ mosaic written:", out_tif)
