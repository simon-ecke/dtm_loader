
import rasterio
import numpy as np
from rasterio.warp import reproject, Resampling

def add_geoid(dtm_path, geoid_path):
    """Return an in-memory array with ellipsoidal heights, plus profile."""
    with rasterio.open(dtm_path) as dtm_src, rasterio.open(geoid_path) as geoid_src:
        geoid_resampled = np.empty(dtm_src.shape, dtype=np.float32)
        reproject(
            source=rasterio.band(geoid_src, 1),
            destination=geoid_resampled,
            src_transform=geoid_src.transform,
            src_crs=geoid_src.crs,
            dst_transform=dtm_src.transform,
            dst_crs=dtm_src.crs,
            resampling=Resampling.bilinear,
        )
        dtm = dtm_src.read(1).astype(np.float32)
        nodata = dtm_src.nodata if dtm_src.nodata is not None else np.nan
        mask = np.isnan(dtm) if np.isnan(nodata) else (dtm == nodata)
        ellip = np.where(mask, nodata, dtm + geoid_resampled)
        profile = dtm_src.profile
        profile.update(dtype=rasterio.float32, nodata=nodata)
        return ellip, profile
