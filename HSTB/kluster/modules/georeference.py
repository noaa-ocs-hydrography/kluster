import os
import xarray as xr
import numpy as np
from pyproj import Transformer, CRS
from typing import Union
import geohash
from shapely import geometry
import queue

from HSTB.kluster.xarray_helpers import stack_nan_array, reform_nan_array
from HSTB.kluster import kluster_variables

try:
    from vyperdatum.points import VyperPoints
    from vyperdatum.core import VyperCore
    vyperdatum_found = True
except ModuleNotFoundError:
    vyperdatum_found = False


def distrib_run_georeference(dat: list):
    """
    Convenience function for mapping build_beam_pointing_vectors across cluster.  Assumes that you are mapping this
    function with a list of data.

    distrib functions also return a processing status array, here a beamwise array = 4, which states that all
    processed beams are at the 'georeference' status level

    Parameters
    ----------
    dat
        [sv_data, altitude, longitude, latitude, heading, heave, waterline, vert_ref, horizontal_crs, z_offset, vdatum_directory]

    Returns
    -------
    list
        [xr.DataArray alongtrack offset (time, beam), xr.DataArray acrosstrack offset (time, beam),
         xr.DataArray down offset (time, beam), xr.DataArray corrected heave for TX - RP lever arm, all zeros if in 'ellipse' mode (time),
         xr.DataArray corrected altitude for TX - RP lever arm, all zeros if in 'vessel' or 'waterline' mode (time),
         processing_status]
    """

    ans = georef_by_worker(dat[0], dat[1], dat[2], dat[3], dat[4], dat[5], dat[6], dat[7], dat[8], dat[9], dat[10], dat[11])
    # return processing status = 4 for all affected soundings
    processing_status = xr.DataArray(np.full_like(dat[0][0], 4, dtype=np.uint8),
                                     coords={'time': dat[0][0].coords['time'],
                                             'beam': dat[0][0].coords['beam']},
                                     dims=['time', 'beam'])
    ans.append(processing_status)
    return ans


def georef_by_worker(sv_corr: list, alt: xr.DataArray, lon: xr.DataArray, lat: xr.DataArray, hdng: xr.DataArray,
                     heave: xr.DataArray, wline: float, vert_ref: str, input_crs: CRS, horizontal_crs: CRS,
                     z_offset: float, vdatum_directory: str = None):
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
    horizontal_crs
        pyproj CRS object, destination coordinate reference system information for this run
    z_offset
        lever arm from reference point to transmitter
    vdatum_directory
            if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here

    Returns
    -------
    list
        [xr.DataArray easting (time, beam), xr.DataArray northing (time, beam), xr.DataArray depth (time, beam),
         xr.DataArray corrected heave for TX - RP lever arm, all zeros if in 'ellipse' mode (time),
         xr.DataArray corrected altitude for TX - RP lever arm, all zeros if in 'vessel' or 'waterline' mode (time),
         xr.DataArray VDatum uncertainty if using a VDatum vertical reference, all zeros otherwise,
         xr.DataArray computed geohash as string encoded base32]
    """

    g = horizontal_crs.get_geod()

    # unpack the sv corrected data output
    alongtrack = sv_corr[0]
    acrosstrack = sv_corr[1]
    depthoffset = sv_corr[2] + z_offset
    # generate the corrected depth offset depending on the desired vertical reference
    corr_dpth = None
    corr_heave = None
    corr_altitude = None
    if vert_ref in kluster_variables.ellipse_based_vertical_references:
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

    z = np.around(corr_dpth, 3)
    if vert_ref in ['NOAA MLLW', 'NOAA MHW']:
        z_stck = z.values[ac_idx]  # get the depth values where there are valid acrosstrack results (i.e. svcorrect worked)
        if vert_ref == 'NOAA MLLW':
            sep, vdatum_unc = transform_vyperdatum(pos[0], pos[1], np.zeros_like(z_stck), input_crs.to_epsg(), 'mllw', vdatum_directory=vdatum_directory)
        else:
            sep, vdatum_unc = transform_vyperdatum(pos[0], pos[1], np.zeros_like(z_stck), input_crs.to_epsg(), 'mhw', vdatum_directory=vdatum_directory)
        z_stck = z_stck - sep
        vdatum_unc = reform_nan_array(vdatum_unc, ac_idx, z.shape, z.coords, z.dims)
        z = reform_nan_array(z_stck, ac_idx, z.shape, z.coords, z.dims)
    else:
        vdatum_unc = xr.zeros_like(z)

    # compute the geohash for each beam return, the base32 encoded cell that the beam falls within, used for spatial indexing
    ghash = compute_geohash(pos[1], pos[0], precision=kluster_variables.geohash_precision)

    if horizontal_crs.is_projected:
        # Transformer.transform input order is based on the CRS, see CRS.geodetic_crs.axis_info
        # - lon, lat - this appears to be valid when using CRS from proj4 string
        # - lat, lon - this appears to be valid when using CRS from epsg
        # use the always_xy option to force the transform to expect lon/lat order
        georef_transformer = Transformer.from_crs(input_crs, horizontal_crs, always_xy=True)
        newpos = georef_transformer.transform(pos[0], pos[1], errcheck=True)  # longitude / latitude order (x/y)
    else:
        newpos = pos

    x = reform_nan_array(np.around(newpos[0], 3), at_idx, alongtrack.shape, alongtrack.coords, alongtrack.dims)
    y = reform_nan_array(np.around(newpos[1], 3), ac_idx, acrosstrack.shape, acrosstrack.coords, acrosstrack.dims)
    ghash = reform_nan_array(ghash, ac_idx, acrosstrack.shape, acrosstrack.coords, acrosstrack.dims)

    return [x, y, z, corr_heave, corr_altitude, vdatum_unc, ghash]


def transform_vyperdatum(x: np.array, y: np.array, z: np.array, source_datum: Union[str, int] = 'nad83',
                         final_datum: str = 'mllw', vdatum_directory: str = None):
    """
    When we specify a NOAA vertical datum (NOAA Mean Lower Low Water, NOAA Mean High Water) in Kluster, we use
    vyperdatum/VDatum to transform the points to the appropriate vertical datum.

    Parameters
    ----------
    x
        easting for each point in source_datum coordinate system
    y
        northing for each point in source_datum coordinate system
    z
        depth offset for each point in source_datum coordinate system
    source_datum
        The horizontal coordinate system of the xyz provided, should be a string identifier ('nad83') or an EPSG code
        specifying the horizontal coordinate system
    final_datum
        The desired final_datum vertical datum as a string (one of 'mllw', 'mhw')
    vdatum_directory
            if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here

    Returns
    -------
    xr.DataArray
        original z array with vertical transformation applied, this new z is at final_datum
    xr.DataArray
        uncertainty associated with the vertical transformation between the source and destination datum
    """

    if vdatum_directory:
        vp = VyperPoints(vdatum_directory=vdatum_directory, silent=True)
    else:
        vp = VyperPoints(silent=True)

    if not os.path.exists(vp.vdatum.vdatum_path):
        raise EnvironmentError('Unable to find path to VDatum folder: {}'.format(vp.vdatum.vdatum_path))
    vp.transform_points(source_datum, final_datum, x, y, z=z)

    return np.around(vp.z, 3), np.around(vp.unc, 3)


def datum_to_wkt(datum_identifier: str, min_lon: float, min_lat: float, max_lon: float, max_lat: float):
    """
    Translate the provided datum to vypercrs wkt string

    Parameters
    ----------
    datum_identifier
        one of 'mllw', 'mhw', etc
    min_lon
        minimum longitude of the survey
    min_lat
        minimum latitude of the survey
    max_lon
        maximum longitude of the survey
    max_lat
        maximum latitude of the survey

    Returns
    -------
    str
        vypercrs wkt string
    """

    vc = VyperCore()
    vc.set_region_by_bounds(min_lon, min_lat, max_lon, max_lat)
    vc.set_output_datum(datum_identifier)
    return vc.out_crs.to_wkt()


def set_vyperdatum_vdatum_path(vdatum_path: str):
    """
    Set the vyperdatum VDatum path, required to use the VDatum grids to do the vertical transformations

    Parameters
    ----------
    vdatum_path
        path to the vdatum folder
    """
    # first time setting vdatum path sets the settings file with the correct path
    vc = VyperCore(vdatum_directory=vdatum_path)
    vc = None
    # vdatum path should be autoloaded now on instantiating the VyperCore
    vc = VyperCore()
    assert os.path.exists(vc.vdatum.vdatum_path)
    assert vc.vdatum.grid_files
    assert vc.vdatum.polygon_files
    assert vc.vdatum.uncertainties


def _new_geohash(latitude: float, longitude: float, precision: int):
    """
    compute new geohash for given latitude longitude

    Parameters
    ----------
    latitude
        latitude as float
    longitude
        longitude as float
    precision
        precision as integer

    Returns
    -------
    bytes
        geohash string encoded as bytes
    """

    return geohash.encode(latitude, longitude, precision=precision).encode()


def compute_geohash(latitude: np.array, longitude: np.array, precision: int):
    """
    Geohash is a geocoding method to encode a specific latitude/longitude into a string representing an area of a given
    precision.  String is a custom base32 implementation encoded string.  We use the python-geohash library to do this.
    The result is a string of length = precision encoding the position.  Higher precision will give you a more
    accurate geohash, i.e. smaller tile.

    Parameters
    ----------
    latitude
        numpy array of latitude values
    longitude
        numpy array of longitude values
    precision
        integer precision, the length of the returned string, higher precision generates a smaller cell area code

    Returns
    -------
    np.array
        array of bytestrings dtype='SX' where X is the precision you have given
    """

    vectorhash = np.vectorize(_new_geohash)
    return vectorhash(latitude, longitude, precision)


def decode_geohash(ghash: Union[str, bytes]):
    """
    Take the given geohash and return the centroid of the geohash cell

    Parameters
    ----------
    ghash
        string geohash or bytestring geohash
    Returns
    -------
    float
        latitude
    float
        longitude
    """

    if isinstance(ghash, str):
        return geohash.decode(ghash)
    else:
        return geohash.decode(ghash.decode())


def geohash_to_polygon(ghash: Union[str, bytes]):
    """
    Take a geohash string and return the shapely polygon object that represents the geohash cell

    Parameters
    ----------
    ghash
        string geohash or bytestring geohash

    Returns
    -------
    geometry.Polygon
        Polygon object for the geohash cell
    """

    if isinstance(ghash, str):
        lat_centroid, lng_centroid, lat_offset, lng_offset = geohash.decode_exactly(ghash)
    else:
        lat_centroid, lng_centroid, lat_offset, lng_offset = geohash.decode_exactly(ghash.decode())

    corner_1 = (lat_centroid - lat_offset, lng_centroid - lng_offset)[::-1]
    corner_2 = (lat_centroid - lat_offset, lng_centroid + lng_offset)[::-1]
    corner_3 = (lat_centroid + lat_offset, lng_centroid + lng_offset)[::-1]
    corner_4 = (lat_centroid + lat_offset, lng_centroid - lng_offset)[::-1]

    return geometry.Polygon([corner_1, corner_2, corner_3, corner_4, corner_1])


def polygon_to_geohashes(polygon: Union[np.array, geometry.Polygon], precision):
    """
    Take a polygon and return a list of all of the geohash codes/cells that are completely inside and those that are
    intersecting

    Parameters
    ----------
    polygon
        polygon as an existing shapely polygon object or a numpy array of coordinates (lat, lon order)
    precision
        length of the geohash string

    Returns
    -------
    list
        list of bytestrings for the geohash cells that are completely inside the polygon
    list
        list of bytestrings for the geohash cells that only intersect the polygon
    """

    if not isinstance(polygon, geometry.Polygon):
        polygon = geometry.Polygon(polygon)

    intersect_geohashes = set()
    inner_geohashes = set()
    outer_geohashes = set()

    envelope = polygon.envelope
    centroid = polygon.centroid

    testing_geohashes = queue.Queue()
    testing_geohashes.put(_new_geohash(centroid.y, centroid.x, precision))

    while not testing_geohashes.empty():
        current_geohash = testing_geohashes.get()

        if current_geohash not in inner_geohashes and current_geohash not in outer_geohashes and current_geohash not in intersect_geohashes:
            current_polygon = geohash_to_polygon(current_geohash)

            if envelope.intersects(current_polygon):
                if polygon.contains(current_polygon):
                    inner_geohashes.add(current_geohash)
                if polygon.intersects(current_polygon):
                    intersect_geohashes.add(current_geohash)
                else:
                    outer_geohashes.add(current_geohash)

                for neighbor in geohash.neighbors(current_geohash.decode()):
                    neighbor = neighbor.encode()
                    if neighbor not in inner_geohashes and neighbor not in outer_geohashes and neighbor not in intersect_geohashes:
                        testing_geohashes.put(neighbor)

    return list(inner_geohashes), list(intersect_geohashes)
