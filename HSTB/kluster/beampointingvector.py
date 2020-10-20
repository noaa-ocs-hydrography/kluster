import xarray as xr
import numpy as np
import pandas as pd

from HSTB.kluster.xarray_helpers import interp_across_chunks


def distrib_run_build_beam_pointing_vector(dat: list):
    """
    Convenience function for mapping build_beam_pointing_vectors across cluster.  Assumes that you are mapping this
    function with a list of data.

    Parameters
    ----------
    dat
        [hdng, bpa, tiltangle, tx_vecs, rx_vecs, tstmp, tx_reversed, rx_reversed]

    Returns
    -------
    list
        [relative azimuth, beam pointing angle]

    """
    ans = build_beam_pointing_vectors(dat[0], dat[1], dat[2], dat[3][0], dat[3][1], dat[4], dat[5], dat[6])
    return ans


def build_beam_pointing_vectors(hdng: xr.DataArray, bpa: xr.DataArray, tiltangle: xr.DataArray, tx_vecs: xr.DataArray,
                                rx_vecs: xr.DataArray, tstmp: xr.DataArray, tx_reversed: bool = False,
                                rx_reversed: bool = False):
    """
    Beam pointing vector is the beam specific vector that arises from the intersection of the tx ping and rx cone
    of sensitivity.  Points at that area.  Is in the geographic coordinate system, built using the tx/rx at time of
    ping/receive.

    Two components are returned.  Relative azimuth, the angle relative to vessel heading that points at the beam
    endpoint.  Beam pointing angle, the roll corrected angle relative to the horizontal that points down at the beam
    endpoint.

    Parameters
    ----------
    hdng
        1d (time) heading in degrees
    bpa
        2d (time, beam) receiver beam pointing angle
    tiltangle
        2d (time, beam) transmitter tiltangle on ping
    tx_vecs
        2 dim (time, xyz) representing tx 3d orientation in space across time
    rx_vecs
        3 dim (time, beam, xyz) representing rx 3d orientation in space across time/beam
    tstmp
        1 dim ping times from the DataSet
    tx_reversed
        if true, the transmitter was installed 180° offset in yaw (i.e. backwards)
    rx_reversed
        if true, the receiver was installed 180° offset in yaw (i.e. backwards)

    Returns
    -------
    xr.DataArray
        2dim (time, beam), beam-wise beam azimuth values relative to vessel heading at time of ping
    xr.DataArray
        2 dim (time, beam) values for beampointingangle at each beam

    """
    hdng = interp_across_chunks(hdng, tstmp)

    # main vec (primary head) is accessed using the primary system selection
    rx_angle = np.deg2rad(bpa)
    tx_angle = np.deg2rad(tiltangle)

    if tx_reversed:
        tx_angle = -tx_angle
    if rx_reversed:
        rx_angle = -rx_angle

    beamvecs = construct_array_relative_beamvector(tx_vecs, rx_vecs, tx_angle, rx_angle)
    rotgeo = return_array_geographic_rotation(tx_vecs, rx_vecs)
    bv_geo = build_geographic_beam_vectors(rotgeo, beamvecs)

    rel_azimuth = compute_relative_azimuth(bv_geo, hdng)
    new_pointing_angle = compute_geo_beam_pointing_angle(bv_geo, rx_angle)

    return rel_azimuth, new_pointing_angle


def construct_array_relative_beamvector(maintx: xr.DataArray, mainrx: xr.DataArray, tx_angle: xr.DataArray,
                                        rx_angle: xr.DataArray):
    """
    Given the orientation vectors representing the transmitter/receiver at time of ping/receive (maintx, mainrx) and
    the TX/RX steering angles (tx_angle, rx_angle), determine new 3d beam vector components at the midpoint between
    the TX and RX.  This would be the 'actual' array relative beam vector.

    This is a simplification of the actual scenario, adding error in the xyz due to the difference in path length/
    direction of the actual ray from tx-seafloor and seafloor-rx and this co-located assumption (tx-seafloor and
    rx-seafloor are the same is the assumption)

    x = +FORWARD, y=+STARBOARD, z=+DOWN

    Parameters
    ----------
    maintx
        orientation vector for transmitter at time of transmit, 2dim of shape (time, xyz)
    mainrx
        orientation vector for receiver at time of receive, 2dim of shape (time, xyz)
    tx_angle
        transmitter tiltangle for each ping time
    rx_angle
        receiver beam pointing angle for each ping time

    Returns
    -------
    xr.DataArray
        3d beam vector in co-located array ref frame.  Of shape (xyz, time, beam), with 10 times and 200 beams,
        beamvecs shape would be (3, 10, 200)

        <xarray.DataArray 'tiltangle' (xyz: 3, time: 10, beam: 200)>
        dask.array<concatenate, shape=(3, 10, 200), dtype=float64, chunksize=(1, 10, 200), chunktype=numpy.ndarray>
        Coordinates:
          * time     (time) float64 1.496e+09 1.496e+09 ...
          * beam     (beam) int32 0 1 2 3 4 5 6 7 8 ... 194 195 196 197 198 199 200
          * xyz      (xyz) object 'x' 'y' 'z'

    """
    # delta - alignment angle between tx/rx vecs
    delt = np.arccos(xr.dot(maintx, mainrx, dims=['xyz'])) - np.pi / 2
    ysub1 = -np.sin(rx_angle)

    # solve for components of 3d beam vector
    ysub1 = ysub1 / np.cos(delt)
    ysub2 = np.sin(tx_angle) * np.tan(delt)
    radial = np.sqrt((ysub1 + ysub2) ** 2 + np.sin(tx_angle) ** 2)
    x = np.sin(tx_angle)
    y = ysub1 + ysub2
    z = np.sqrt(1 - radial ** 2)

    # generate new dataarray object for beam vectors
    newx, _ = xr.broadcast(x, y)  # broadcast to duplicate x along beam dimension
    beamvecs = xr.concat([newx, y, z], pd.Index(list('xyz'), name='xyz'))
    return beamvecs


def return_array_geographic_rotation(maintx: xr.DataArray, mainrx: xr.DataArray):
    """
    Use the transmitter/receiver array orientations to build a rotation matrix between the geographic/array rel
    reference frame.

    Parameters
    ----------
    maintx
        orientation vector for transmitter at time of transmit, 2dim of shape (time, xyz)
    mainrx
        orientation vector for receiver at time of receive, 2dim of shape (time, xyz)

    Returns
    -------
    xr.DataArray
        rotation matrices at each time/beam, of shape (beam, rot_i, time, xyz)

        <xarray.DataArray 'getitem-82dd48467b1f4e8b4f56bbe5e841cc9f' (beam: 182, rot_i: 3, time: 2, xyz: 3)>
        dask.array<transpose, shape=(182, 3, 2, 3), dtype=float64, chunksize=(182, 3, 2, 1), chunktype=numpy.ndarray>
        Coordinates:
          * rot_i    (rot_i) int32 0 1 2
          * time     (time) float64 1.496e+09 1.496e+09
          * beam     (beam) int32 0 1 2 3 4 5 6 7 8 ... 174 175 176 177 178 179 180 181
          * xyz      (xyz) <U1 'x' 'y' 'z'

    """
    # build rotation matrix for going from locally level to geographic coord sys
    x_prime = maintx
    z_prime = cross(x_prime, mainrx, 'xyz')
    y_prime = cross(z_prime, x_prime, 'xyz')
    rotgeo = xr.concat([x_prime, y_prime, z_prime], pd.Index([0, 1, 2], name='rot_j')).T
    # to do the dot product correctly, you need to align the right dimension in both matrices by giving
    # them the same name (xyz for rotgeo and bv_geo in this case)
    rotgeo = rotgeo.rename({'xyz': 'rot_i'})
    rotgeo.coords['rot_i'] = [0, 1, 2]
    rotgeo = rotgeo.rename({'rot_j': 'xyz'})
    rotgeo.coords['xyz'] = ['x', 'y', 'z']
    return rotgeo


def cross(a: xr.DataArray, b: xr.DataArray, spatial_dim: str, output_dtype: np.dtype = None):
    """
    Xarray-compatible cross product.  Compatible with dask, parallelization uses a.dtype as output_dtype

    Parameters
    ----------
    a
        xarray DataArray object with a spatial_dim
    b
        xarray DataArray object with a spatial_dim
    spatial_dim
        dimension name to be mulitplied through
    output_dtype
        dtype of output

    Returns
    -------
    xr.DataArray
        cross product of a and b along spatial_dim

    """
    for d in (a, b):
        if spatial_dim not in d.dims:
            raise ValueError('dimension {} not in {}'.format(spatial_dim, d))
        if d.sizes[spatial_dim] != 3:
            raise ValueError('dimension {} has not length 3 in {}'.format(spatial_dim, d))

    if output_dtype is None:
        output_dtype = a.dtype
    c = xr.apply_ufunc(np.cross, a, b,
                       input_core_dims=[[spatial_dim], [spatial_dim]],
                       output_core_dims=[[spatial_dim]],
                       dask='parallelized', output_dtypes=[output_dtype]
                       )
    return c


def build_geographic_beam_vectors(rotgeo: xr.DataArray, beamvecs: xr.DataArray):
    """
    Apply rotation matrix to bring transducer rel. beam vectors to geographic ref frame

    Parameters
    ----------
    rotgeo
        rotation matrices at each time/beam, of shape (beam, rot_i, time, xyz), see return_array_geographic_rotation
    beamvecs
        3d beam vector in co-located array ref frame (xyz, time, beam), see construct_array_relative_beamvector

    Returns
    -------
    xr.DataArray
        beam vectors in geographic ref frame, of shape (time, beam, bv_xyz)

    """
    bv_geo = xr.dot(rotgeo, beamvecs, dims='xyz')
    bv_geo = bv_geo.rename({'rot_i': 'bv_xyz'})
    bv_geo.coords['bv_xyz'] = ['x', 'y', 'z']
    bv_geo = bv_geo.transpose('time', 'beam', 'bv_xyz')
    return bv_geo


def compute_relative_azimuth(bv_geo: xr.DataArray, heading: xr.DataArray):
    """
    Compute the relative azimuth from array to end of beam vector in geographic ref frame

    Parameters
    ----------
    bv_geo
        beam vectors in geographic ref frame, of shape (time, beam, bv_xyz), see build_geographic_beam_vectors
    heading
        1 dim array of heading values, coords=time

    Returns
    -------
    xr.DataArray
        2dim (time, beam), beam-wise beam azimuth values relative to vessel heading at time of ping

    """
    # derive azimuth/angle from the newly created geographic beam vectors
    bv_azimuth = np.rad2deg(np.arctan2(bv_geo.sel(bv_xyz='y'), bv_geo.sel(bv_xyz='x')))
    rel_azimuth = np.deg2rad((bv_azimuth - heading + 360) % 360)
    return rel_azimuth


def compute_geo_beam_pointing_angle(bv_geo: xr.DataArray, rx_angle: xr.DataArray):
    """
    Build new beam pointing angle (rel to the vertical) and with the correct sign (+ to starboard) in the geographic
    ref frame.

    Parameters
    ----------
    bv_geo
        beam vectors in geographic ref frame, of shape (time, beam, bv_xyz), see build_geographic_beam_vectors
    rx_angle
        receiver beam pointing angle for each ping time

    Returns
    -------
    xr.DataArray
        2 dim (time, beam) values for beampointingangle at each beam

    """
    bvangle_divisor = np.sqrt(np.square(bv_geo.sel(bv_xyz='x')) + np.square(bv_geo.sel(bv_xyz='y')))
    # new pointing angle is equal to pi/2 - depression angle (depression angle relative to horiz, pointing
    #    angle is the incidence angle relative to vertical)
    new_pointing_angle = (np.pi / 2) - np.arctan(bv_geo.sel(bv_xyz='z') / bvangle_divisor)
    # flip the sign where the azimuth is pointing to port, allows us to maintain which side the angle is on
    newindx = np.ones_like(new_pointing_angle)
    newindx = np.negative(newindx, out=newindx, where=rx_angle < 0)
    new_pointing_angle = new_pointing_angle * newindx
    return new_pointing_angle
