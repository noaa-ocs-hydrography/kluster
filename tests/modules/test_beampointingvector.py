import unittest

import pytest
import xarray as xr
import numpy as np

from HSTB.kluster.modules.beampointingvector import build_beam_pointing_vectors
try:  # when running from pycharm console
    from hstb_kluster.tests.test_datasets import RealFqpr, load_dataset
    from hstb_kluster.tests.modules.module_test_arrays import expected_tx_vector, expected_rx_vector, expected_beam_azimuth, expected_corrected_beam_angles
except ImportError:  # relative import as tests directory can vary in location depending on how kluster is installed
    from ..test_datasets import RealFqpr, load_dataset
    from ..modules.module_test_arrays import expected_tx_vector, expected_rx_vector, expected_beam_azimuth, expected_corrected_beam_angles


# or for a file-like stream:
# template = pkg_resources.open_text(resources, 'temp_file')

class TestBeamPointingVector(unittest.TestCase):

    def test_beampointingvector_module(self):
        dset = load_dataset(RealFqpr())
        raw_attitude = dset.raw_att
        heading = raw_attitude.heading

        multibeam = dset.raw_ping[0].isel(time=0).expand_dims('time')
        beampointingangle = multibeam.beampointingangle
        tiltangle = multibeam.tiltangle
        ping_time_heading = heading.interp_like(beampointingangle)

        tx_vecs = xr.DataArray(data=expected_tx_vector, dims=['time', 'beam', 'xyz'],
                               coords={'time': multibeam.time.values,
                                       'beam': multibeam.beam.values,
                                       'xyz': ['x', 'y', 'z']})
        rx_vecs = xr.DataArray(data=expected_rx_vector, dims=['time', 'beam', 'xyz'],
                               coords={'time': multibeam.time.values,
                                       'beam': multibeam.beam.values,
                                       'xyz': ['x', 'y', 'z']})

        tx_reversed = False
        rx_reversed = False

        beam_azimuth, corrected_beam_angle = build_beam_pointing_vectors(ping_time_heading, beampointingangle,
                                                                         tiltangle,
                                                                         tx_vecs,
                                                                         rx_vecs, tx_reversed, rx_reversed)

        try:
            assert np.array_equal(beam_azimuth.values, expected_beam_azimuth)
        except AssertionError:
            print('Falling back to approx, should only be seen in TravisCI environment in my experience')
            # use approx here, I get ever so slightly different answers in the Travis CI environment
            assert beam_azimuth.values == pytest.approx(expected_beam_azimuth, 0.000001)
        try:
            assert np.array_equal(corrected_beam_angle.values, expected_corrected_beam_angles)
        except AssertionError:
            print('Falling back to approx, should only be seen in TravisCI environment in my experience')
            # use approx here, I get ever so slightly different answers in the Travis CI environment
            assert corrected_beam_angle.values == pytest.approx(expected_corrected_beam_angles, 0.000001)
