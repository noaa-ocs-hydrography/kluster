import xarray as xr
import numpy as np
from pyproj import Transformer, CRS, ProjError

from HSTB.kluster.xarray_helpers import stack_nan_array, reform_nan_array


def distrib_run_georeference(dat: list):
    """
    Convenience function for mapping build_beam_pointing_vectors across cluster.  Assumes that you are mapping this
    function with a list of data.

    Parameters
    ----------
    dat
        [sv_data, navigation, heading, heave, waterline, vert_ref, xyz_crs, z_offset]

    Returns
    -------
    xr.DataArray
        alongtrack offset (time, beam)
    xr.DataArray
        acrosstrack offset (time, beam)
    xr.DataArray
        down offset (time, beam)
    xr.DataArray
        corrected heave for TX - RP lever arm, all zeros if in 'ellipse' mode (time)
    xr.DataArray
        corrected altitude for TX - RP lever arm, all zeros if in 'vessel' or 'waterline' mode (time)
    """

    x, y, z, hve, alt = georef_by_worker(dat[0], dat[1], dat[2], dat[3], dat[4], dat[5], dat[6], dat[7])
    return x, y, z, hve, alt


def georef_by_worker(sv_corr: list, nav: xr.Dataset, hdng: xr.DataArray, heave: xr.DataArray,
                     wline: float, vert_ref: str, xyz_crs: CRS, z_offset: float):
    """
    Use the raw attitude/navigation to transform the vessel relative along/across/down offsets to georeferenced
    soundings.  Will support transformation to geographic and projected coordinate systems and with a vertical
    reference that you select.

    Parameters
    ----------
    sv_corr
        [x, y, z] offsets generated with sv_correct
    nav
        1d (time) Dataset containing altitude, latitude and longitude
    hdng
        1d (time) heading in degrees
    heave
        1d (time) heave in degrees
    wline
        waterline offset from reference point
    vert_ref
        vertical reference point, one of ['ellipse', 'vessel', 'waterline']
    xyz_crs
        pyproj CRS object, coordinate reference system information for this run
    z_offset
        lever arm from reference point to transmitter

    Returns
    -------
    xr.DataArray
        alongtrack offset (time, beam)
    xr.DataArray
        acrosstrack offset (time, beam)
    xr.DataArray
        down offset (time, beam)
    xr.DataArray
        corrected heave for TX - RP lever arm, all zeros if in 'ellipse' mode (time)
    xr.DataArray
        corrected altitude for TX - RP lever arm, all zeros if in 'vessel' or 'waterline' mode (time)
    """

    g = xyz_crs.get_geod()

    alongtrack = sv_corr[0]
    acrosstrack = sv_corr[1]
    depthoffset = sv_corr[2] + z_offset

    corr_dpth = None
    corr_heave = None
    corr_altitude = None
    if vert_ref == 'ellipse':
        corr_altitude = - nav['altitude']
        corr_heave = xr.zeros_like(corr_altitude)
        corr_dpth = (depthoffset + corr_altitude.values[:, None]).astype(np.float32)
    elif vert_ref == 'vessel':
        corr_heave = heave
        corr_altitude = xr.zeros_like(corr_heave)
        corr_dpth = (depthoffset + corr_heave.values[:, None]).astype(np.float32)
    elif vert_ref == 'waterline':
        corr_heave = heave
        corr_altitude = xr.zeros_like(corr_heave)
        corr_dpth = (depthoffset + corr_heave.values[:, None] - wline).astype(np.float32)

    at_idx, alongtrack_stck = stack_nan_array(alongtrack, stack_dims=('time', 'beam'))
    ac_idx, acrosstrack_stck = stack_nan_array(acrosstrack, stack_dims=('time', 'beam'))

    bm_azimuth = np.rad2deg(np.arctan2(acrosstrack_stck, alongtrack_stck)) + np.float32(hdng[at_idx[0]].values)
    bm_radius = np.sqrt(acrosstrack_stck ** 2 + alongtrack_stck ** 2)
    pos = g.fwd(nav['longitude'][at_idx[0]].values, nav['latitude'][at_idx[0]].values,
                bm_azimuth.values, bm_radius.values)

    if xyz_crs.is_projected:
        georef_transformer = Transformer.from_crs(xyz_crs.geodetic_crs, xyz_crs)
        try:  # this appears to be valid when using CRS from proj4 string
            newpos = georef_transformer.transform(pos[0], pos[1], errcheck=True)  # longitude / latitude order (x/y)
        except ProjError:  # this appears to be valid when using CRS from epsg
            newpos = georef_transformer.transform(pos[1], pos[0], errcheck=True)  # latitude / longitude order (y/x)
    else:
        newpos = pos

    x = reform_nan_array(np.around(newpos[0], 3), at_idx, alongtrack.shape, alongtrack.coords, alongtrack.dims)
    y = reform_nan_array(np.around(newpos[1], 3), ac_idx, acrosstrack.shape, acrosstrack.coords, acrosstrack.dims)
    z = np.around(corr_dpth, 3)
    return x, y, z, corr_heave, corr_altitude
