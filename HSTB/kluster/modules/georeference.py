import xarray as xr
import numpy as np
from pyproj import Transformer, CRS

from HSTB.kluster.xarray_helpers import stack_nan_array, reform_nan_array


def distrib_run_georeference(dat: list):
    """
    Convenience function for mapping build_beam_pointing_vectors across cluster.  Assumes that you are mapping this
    function with a list of data.

    distrib functions also return a processing status array, here a beamwise array = 4, which states that all
    processed beams are at the 'georeference' status level

    Parameters
    ----------
    dat
        [sv_data, altitude, longitude, latitude, heading, heave, waterline, vert_ref, xyz_crs, z_offset]

    Returns
    -------
    list
        [xr.DataArray alongtrack offset (time, beam), xr.DataArray acrosstrack offset (time, beam),
         xr.DataArray down offset (time, beam), xr.DataArray corrected heave for TX - RP lever arm, all zeros if in 'ellipse' mode (time),
         xr.DataArray corrected altitude for TX - RP lever arm, all zeros if in 'vessel' or 'waterline' mode (time),
         processing_status]
    """

    ans = georef_by_worker(dat[0], dat[1], dat[2], dat[3], dat[4], dat[5], dat[6], dat[7], dat[8], dat[9], dat[10])
    # return processing status = 4 for all affected soundings
    processing_status = xr.DataArray(np.full_like(dat[0][0], 4, dtype=np.uint8),
                                     coords={'time': dat[0][0].coords['time'],
                                             'beam': dat[0][0].coords['beam']},
                                     dims=['time', 'beam'])
    ans.append(processing_status)
    return ans


def georef_by_worker(sv_corr: list, alt: xr.DataArray, lon: xr.DataArray, lat: xr.DataArray, hdng: xr.DataArray,
                     heave: xr.DataArray, wline: float, vert_ref: str, input_crs: CRS, xyz_crs: CRS, z_offset: float):
    """
    Use the raw attitude/navigation to transform the vessel relative along/across/down offsets to georeferenced
    soundings.  Will support transformation to geographic and projected coordinate systems and with a vertical
    reference that you select.

    Parameters
    ----------
    sv_corr
        [x, y, z] offsets generated with sv_correct
    alt
        1d (time) altitude in meters
    lon
        1d (time) longitude in degrees
    lat
        1d (time) latitude in degrees
    hdng
        1d (time) heading in degrees
    heave
        1d (time) heave in degrees
    wline
        waterline offset from reference point
    vert_ref
        vertical reference point, one of ['ellipse', 'vessel', 'waterline']
    input_crs
        pyproj CRS object, input coordinate reference system information for this run
    xyz_crs
        pyproj CRS object, destination coordinate reference system information for this run
    z_offset
        lever arm from reference point to transmitter

    Returns
    -------
    list
        [xr.DataArray alongtrack offset (time, beam), xr.DataArray acrosstrack offset (time, beam),
         xr.DataArray down offset (time, beam), xr.DataArray corrected heave for TX - RP lever arm, all zeros if in 'ellipse' mode (time),
         xr.DataArray corrected altitude for TX - RP lever arm, all zeros if in 'vessel' or 'waterline' mode (time)]
    """

    g = xyz_crs.get_geod()

    # unpack the sv corrected data output
    alongtrack = sv_corr[0]
    acrosstrack = sv_corr[1]
    depthoffset = sv_corr[2] + z_offset

    # generate the corrected depth offset depending on the desired vertical reference
    corr_dpth = None
    corr_heave = None
    corr_altitude = None
    if vert_ref == 'ellipse':
        corr_altitude = alt
        corr_heave = xr.zeros_like(corr_altitude)
        corr_dpth = (depthoffset - corr_altitude.values[:, None]).astype(np.float32)
    elif vert_ref == 'vessel':
        corr_heave = heave
        corr_altitude = xr.zeros_like(corr_heave)
        corr_dpth = (depthoffset + corr_heave.values[:, None]).astype(np.float32)
    elif vert_ref == 'waterline':
        corr_heave = heave
        corr_altitude = xr.zeros_like(corr_heave)
        corr_dpth = (depthoffset + corr_heave.values[:, None] - wline).astype(np.float32)

    # get the sv corrected alongtrack/acrosstrack offsets stacked without the NaNs (arrays have NaNs for beams that do not exist in that sector)
    at_idx, alongtrack_stck = stack_nan_array(alongtrack, stack_dims=('time', 'beam'))
    ac_idx, acrosstrack_stck = stack_nan_array(acrosstrack, stack_dims=('time', 'beam'))

    # determine the beam wise offsets
    bm_azimuth = np.rad2deg(np.arctan2(acrosstrack_stck, alongtrack_stck)) + np.float32(hdng[at_idx[0]].values)
    bm_radius = np.sqrt(acrosstrack_stck ** 2 + alongtrack_stck ** 2)
    pos = g.fwd(lon[at_idx[0]].values, lat[at_idx[0]].values, bm_azimuth.values, bm_radius.values)

    if xyz_crs.is_projected:
        # Transformer.transform input order is based on the CRS, see CRS.geodetic_crs.axis_info
        # - lon, lat - this appears to be valid when using CRS from proj4 string
        # - lat, lon - this appears to be valid when using CRS from epsg
        # use the always_xy option to force the transform to expect lon/lat order
        georef_transformer = Transformer.from_crs(input_crs, xyz_crs, always_xy=True)
        newpos = georef_transformer.transform(pos[0], pos[1], errcheck=True)  # longitude / latitude order (x/y)
    else:
        newpos = pos

    x = reform_nan_array(np.around(newpos[0], 3), at_idx, alongtrack.shape, alongtrack.coords, alongtrack.dims)
    y = reform_nan_array(np.around(newpos[1], 3), ac_idx, acrosstrack.shape, acrosstrack.coords, acrosstrack.dims)
    z = np.around(corr_dpth, 3)
    return [x, y, z, corr_heave, corr_altitude]
