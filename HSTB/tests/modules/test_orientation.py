import unittest
import numpy as np
import pytest
from xarray import load_dataset

from HSTB.kluster.modules.orientation import build_orientation_vectors
from HSTB.tests.modules.module_test_arrays import expected_tx_vector, expected_rx_vector


class TestOrientation(unittest.TestCase):

    def test_orientation_module(self):
        dset = load_dataset('009_20170523_12119_FA2806.all') #load_dataset(RealFqpr())
        raw_attitude = dset.raw_att
        # expand_dims required to maintain the time dimension metadata when you select only one value
        multibeam = dset.raw_ping[0].isel(time=0).expand_dims('time')
        traveltime = multibeam.traveltime
        delay = multibeam.delay
        timestamps = multibeam.time

        installation_params_time = list(dset.xyzrph['tx_r'].keys())[0]
        tx_orientation = [np.array([1, 0, 0]),  # starting vector for the tx transducer (points forward)
                          dset.xyzrph['tx_r'][installation_params_time],  # roll mounting angle for tx
                          dset.xyzrph['tx_p'][installation_params_time],  # pitch mounting angle for tx
                          dset.xyzrph['tx_h'][installation_params_time],  # yaw mounting angle for tx
                          installation_params_time]  # time stamp for the installation parameters record
        rx_orientation = [np.array([0, 1, 0]),  # same but for the receiver
                          dset.xyzrph['rx_r'][installation_params_time],
                          dset.xyzrph['rx_p'][installation_params_time],
                          dset.xyzrph['rx_h'][installation_params_time],
                          installation_params_time]
        latency = 0  # no latency applied for this test

        calc_tx_vector, calc_rx_vector = build_orientation_vectors(raw_attitude, traveltime, delay, timestamps,
                                                                   tx_orientation, rx_orientation, latency)

        try:
            assert np.array_equal(calc_tx_vector.values, expected_tx_vector)
        except AssertionError:
            print('Falling back to approx, should only be seen in TravisCI environment in my experience')
            # use approx here, I get ever so slightly different answers in the Travis CI environment
            assert calc_tx_vector.values == pytest.approx(expected_tx_vector, 0.000001)
        try:
            assert np.array_equal(calc_rx_vector.values, expected_rx_vector)
        except AssertionError:
            print('Falling back to approx, should only be seen in TravisCI environment in my experience')
            # use approx here, I get ever so slightly different answers in the Travis CI environment
            assert calc_rx_vector.values == pytest.approx(expected_rx_vector, 0.000001)