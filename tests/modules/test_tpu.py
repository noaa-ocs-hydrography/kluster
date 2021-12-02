import unittest
import xarray as xr
import numpy as np

from HSTB.kluster.modules.tpu import calculate_tpu
try:  # when running from pycharm console
    from hstb_kluster.tests.test_datasets import RealFqpr, load_dataset
    from hstb_kluster.tests.modules.module_test_arrays import expected_corrected_beam_angles, expected_alongtrack, expected_depth, \
        expected_thu, expected_tvu
except ImportError:  # relative import as tests directory can vary in location depending on how kluster is installed
    from ..test_datasets import RealFqpr, load_dataset
    from ..modules.module_test_arrays import expected_corrected_beam_angles, expected_alongtrack, expected_depth, \
        expected_thu, expected_tvu


class TestTPU(unittest.TestCase):

    def test_tpu(self):
        dset = load_dataset(RealFqpr())
        multibeam = dset.raw_ping[0].isel(time=0).expand_dims('time')
        qf = multibeam.qualityfactor
        tpu = dset.return_tpu_parameters(list(dset.xyzrph['waterline'].keys())[0])

        surface_ss = multibeam.soundspeed
        beampointingangle = multibeam.beampointingangle
        corr_beam_angle = xr.DataArray(data=expected_corrected_beam_angles, dims=['time', 'beam'],
                                       coords={'time': multibeam.time.values, 'beam': multibeam.beam.values})
        x = xr.DataArray(data=expected_alongtrack, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                                  'beam': multibeam.beam.values})
        z = xr.DataArray(data=expected_depth, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                             'beam': multibeam.beam.values})

        raw_attitude = dset.raw_att
        roll = raw_attitude['roll'].interp_like(beampointingangle)

        tvu, thu = calculate_tpu(roll, beampointingangle, corr_beam_angle, x, z, surface_ss, tpu_dict=tpu,
                                 quality_factor=qf, vert_ref='waterline')
        assert np.array_equal(thu, expected_thu)
        assert np.array_equal(tvu, expected_tvu)