import xarray as xr
import numpy as np

from HSTB.kluster.rotations import return_mounting_rotation_matrix, combine_rotation_matrix, \
    return_attitude_rotation_matrix
from HSTB.kluster.xarray_helpers import interp_across_chunks, reform_nan_array, stack_nan_array


def distrib_run_build_orientation_vectors(dat: list):
    """
    Convenience function for mapping build_orientation_vectors across cluster.  Assumes that you are mapping this
    function with a list of data.

    Parameters
    ----------
    dat
        [raw_att, twtt, fut_tx_tstmp_idx, tx_orientation, rx_orientation, latency]

    Returns
    -------
    list
        [tx_vectors, rx_vectors]

    """
    ans = build_orientation_vectors(dat[0], dat[1], dat[2], dat[3], dat[4], dat[5])
    return ans


def build_orientation_vectors(raw_att: xr.Dataset, twtt: xr.DataArray, tx_tstmp_idx: xr.DataArray,
                              tx_orientation: list, rx_orientation: list, latency: float = 0):
    """
    Using attitude angles, mounting angles, build the tx/rx vectors that represent the orientation of the tx/rx at
    time of transmit/receive.  Transmitter vectors end up as the [x, y, z] for the transmitter orientation at the time
    of ping.  Receiver vectors have an additional beam dimension, and represent the slight changes in receiver
    orientation during ping receive

    (this ended up being more important than I thought, saw up to half a meter difference in some datasets)

    Parameters
    ----------
    raw_att
        raw attitude Dataset including roll, pitch, yaw
    twtt
        2dim (time/beam) array of timestamps representing time traveling through water for each beam
    tx_tstmp_idx
        1D ping times from the DataSet
    tx_orientation
        [numpy array with 3 elements (x,y,z) representing the ideal tx orientation, tx roll mounting angle,
        tx pitch mounting angle, tx yaw mounting angle, timestamp]
    rx_orientation
        [numpy array with 3 elements (x,y,z) representing the ideal rx orientation, rx roll mounting angle,
        rx pitch mounting angle, rx yaw mounting angle, timestamp]
    latency
        if included is added as motion latency (in milliseconds)

    Returns
    -------
    xr.DataArray
        2 dim (time, xyz) representing tx 3d orientation in space across time
    xr.DataArray
        3 dim (time, beam, xyz) representing rx 3d orientation in space across time/beam

    """
    # generate rotation matrices for the transmit array at time of ping
    txatt = interp_across_chunks(raw_att, tx_tstmp_idx + latency)
    tx_att_times, tx_attitude_rotation = return_attitude_rotation_matrix(txatt)

    # generate rotation matrices for receive array at time of receive, ping + twtt
    rx_tstmp_idx, inv_idx, rx_interptimes = get_receive_times(tx_tstmp_idx + latency, twtt)
    rxatt = interp_across_chunks(raw_att, rx_interptimes.compute())
    rx_att_times, rx_attitude_rotation = return_attitude_rotation_matrix(rxatt, time_index=inv_idx)

    # Build orientation matrices for mounting angles
    tx_mount_rotation = return_mounting_rotation_matrix(tx_orientation[1], tx_orientation[2], tx_orientation[3],
                                                        tx_orientation[4])
    rx_mount_rotation = return_mounting_rotation_matrix(rx_orientation[1], rx_orientation[2], rx_orientation[3],
                                                        rx_orientation[4])

    final_tx_rot = combine_rotation_matrix(tx_mount_rotation, tx_attitude_rotation)
    final_rx_rot = combine_rotation_matrix(rx_mount_rotation, rx_attitude_rotation)

    # the final vectors are just the rotations applied to the starter vectors.
    # you will see a np.float32 cast for the starter vectors to avoid ending up with float64 (mem hog)
    final_tx_vec = xr.DataArray(final_tx_rot.data @ np.float32(tx_orientation[0]),
                                coords={'time': tx_att_times - latency, 'xyz': ['x', 'y', 'z']},
                                dims=['time', 'xyz'])
    final_rx_vec = xr.DataArray(final_rx_rot.data @ np.float32(rx_orientation[0]),
                                coords={'time': rx_att_times - latency, 'xyz': ['x', 'y', 'z']},
                                dims=['time', 'xyz'])
    final_rx_vec = reform_nan_array(final_rx_vec, rx_tstmp_idx, twtt.shape + (3,),
                                    [twtt.coords['time'], twtt.coords['beam'], final_rx_vec['xyz']],
                                    twtt.dims + ('xyz',))

    # generate tx/rx orientation vectors at time of transmit/receive in local coord system
    # ensure chunks include the whole xyz vector, to avoid operations 'across core dimension', i.e. with
    #    workers having different parts of the same vector
    tx_vecs = final_tx_vec.chunk({'xyz': 3})
    rx_vecs = final_rx_vec.chunk({'xyz': 3})

    return tx_vecs, rx_vecs


def get_receive_times(pingtime: xr.DataArray, twtt: xr.DataArray):
    """
    Given ping time and two way travel time, return the time of receive for each beam.  Provides unique times, as
    by just adding ping time and twtt, you might end up with duplicate times.  To get back to beamwise times, an
    index is provided that you can select by.

    Parameters
    ----------
    pingtime
        1dim array of timestamps representing time of ping
    twtt
        2dim (time/beam) array of timestamps representing time traveling through water for each beam

    Returns
    -------
    tuple
        tuple of numpy arrays, 2d indices of original rx_tstmp Dataarray, used to reconstruct array
    np.array
        1d indices of time of receive that can be used to reconstruct non-unique timestamps
    xr.DataArray
        1 dim array of timestamps for unique times of receive

    """
    rx_tstmp = pingtime + twtt
    rx_tstmp_idx, rx_tstmp_stck = stack_nan_array(rx_tstmp, stack_dims=('time', 'beam'))
    unique_rx_times, inv_idx = np.unique(rx_tstmp_stck.values, return_inverse=True)
    rx_interptimes = xr.DataArray(unique_rx_times, coords=[unique_rx_times], dims=['time']).chunk()
    return rx_tstmp_idx, inv_idx, rx_interptimes
