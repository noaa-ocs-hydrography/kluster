import unittest
import os
import numpy as np
import xarray as xr

from HSTB.kluster.xarray_conversion import return_xyzrph_from_mbes, _xarr_is_bit_set, _build_serial_mask, \
    _return_xarray_mintime, _return_xarray_timelength, _divide_xarray_indicate_empty_future, \
    _return_xarray_constant_blocks, _merge_constant_blocks, _assess_need_for_split_correction, _correct_for_splits, \
    _closest_prior_key_value, _closest_key_value, simplify_soundvelocity_profile, batch_read_configure_options
from HSTB.kluster import kluster_variables


class TestXArrayConversion(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.tstone = xr.DataArray(np.arange(100), coords={'time': np.arange(100)}, dims=['time'])
        cls.tsttwo = xr.Dataset(data_vars={'tstone': cls.tstone}, coords={'time': np.arange(100)})

    def test_return_xyzrph_from_mbes(self):
        testfile = os.path.join(os.path.dirname(__file__), 'resources', '0009_20170523_181119_FA2806.all')
        assert os.path.exists(testfile)

        xyzrph, _sonarmodel, _serialnum = return_xyzrph_from_mbes(testfile)
        assert xyzrph['beam_opening_angle']['1495563079'] == 1.0
        assert xyzrph['heading_patch_error']['1495563079'] == 0.5
        assert xyzrph['heading_sensor_error']['1495563079'] == 0.02
        assert xyzrph['heave_error']['1495563079'] == 0.05
        assert xyzrph['horizontal_positioning_error']['1495563079'] == 1.5
        assert xyzrph['imu_h']['1495563079'] == 0.400
        assert xyzrph['latency']['1495563079'] == 0.000
        assert xyzrph['imu_p']['1495563079'] == -0.180
        assert xyzrph['imu_r']['1495563079'] == -0.160
        assert xyzrph['imu_x']['1495563079'] == 0.000
        assert xyzrph['imu_y']['1495563079'] == 0.000
        assert xyzrph['imu_z']['1495563079'] == 0.000
        assert xyzrph['latency_patch_error']['1495563079'] == 0.0
        assert xyzrph['pitch_patch_error']['1495563079'] == 0.1
        assert xyzrph['pitch_sensor_error']['1495563079'] == 0.001
        assert xyzrph['roll_patch_error']['1495563079'] == 0.1
        assert xyzrph['roll_sensor_error']['1495563079'] == 0.001
        assert xyzrph['rx_h']['1495563079'] == 0.000
        assert xyzrph['rx_p']['1495563079'] == 0.000
        assert xyzrph['rx_r']['1495563079'] == 0.000
        assert xyzrph['rx_x']['1495563079'] == -0.100
        assert xyzrph['rx_x_0']['1495563079'] == 0.011
        assert xyzrph['rx_x_1']['1495563079'] == 0.011
        assert xyzrph['rx_x_2']['1495563079'] == 0.011
        assert xyzrph['rx_y']['1495563079'] == -0.304
        assert xyzrph['rx_y_0']['1495563079'] == 0.0
        assert xyzrph['rx_y_1']['1495563079'] == 0.0
        assert xyzrph['rx_y_2']['1495563079'] == 0.0
        assert xyzrph['rx_z']['1495563079'] == -0.016
        assert xyzrph['rx_z_0']['1495563079'] == -0.006
        assert xyzrph['rx_z_1']['1495563079'] == -0.006
        assert xyzrph['rx_z_2']['1495563079'] == -0.006
        assert xyzrph['separation_model_error']['1495563079'] == 0.0
        assert xyzrph['surface_sv_error']['1495563079'] == 0.5
        assert xyzrph['timing_latency_error']['1495563079'] == 0.001
        assert xyzrph['tx_h']['1495563079'] == 0.000
        assert xyzrph['tx_p']['1495563079'] == 0.000
        assert xyzrph['tx_r']['1495563079'] == 0.000
        assert xyzrph['tx_to_antenna_x']['1495563079'] == 0.000
        assert xyzrph['tx_to_antenna_y']['1495563079'] == 0.000
        assert xyzrph['tx_to_antenna_z']['1495563079'] == 0.000
        assert xyzrph['tx_x']['1495563079'] == 0.000
        assert xyzrph['tx_x_0']['1495563079'] == 0.0
        assert xyzrph['tx_x_1']['1495563079'] == 0.0
        assert xyzrph['tx_x_2']['1495563079'] == 0.0
        assert xyzrph['tx_y']['1495563079'] == 0.000
        assert xyzrph['tx_y_0']['1495563079'] == -0.0554
        assert xyzrph['tx_y_1']['1495563079'] == 0.0131
        assert xyzrph['tx_y_2']['1495563079'] == 0.0554
        assert xyzrph['tx_z']['1495563079'] == 0.000
        assert xyzrph['tx_z_0']['1495563079'] == -0.012
        assert xyzrph['tx_z_1']['1495563079'] == -0.006
        assert xyzrph['tx_z_2']['1495563079'] == -0.012
        assert xyzrph['vertical_positioning_error']['1495563079'] == 1.0
        assert xyzrph['vessel_speed_error']['1495563079'] == 0.1
        assert xyzrph['waterline']['1495563079'] == -0.640
        assert xyzrph['waterline_error']['1495563079'] == 0.02
        assert xyzrph['x_offset_error']['1495563079'] == 0.2
        assert xyzrph['y_offset_error']['1495563079'] == 0.2
        assert xyzrph['z_offset_error']['1495563079'] == 0.2

    def test_xarr_is_bit_set(self):
        tst = xr.DataArray(np.arange(10))
        # 4 5 6 7 all have the third bit set
        ans = np.array([False, False, False, False, True, True, True, True, False, False])
        assert np.array_equal(_xarr_is_bit_set(tst, 3), ans)

    def test_build_serial_mask(self):
        rec = {'ping': {'serial_num': np.array([40111, 40111, 40112, 40112, 40111, 40111])}}
        ids, msk = _build_serial_mask(rec)
        assert ids == ['40111', '40112']
        assert np.array_equal(msk[0], np.array([0, 1, 4, 5]))
        assert np.array_equal(msk[1], np.array([2, 3]))

    def test_return_xarray_mintime(self):
        assert _return_xarray_mintime(self.tstone) == 0
        assert _return_xarray_mintime(self.tsttwo) == 0

    def test_return_xarray_timelength(self):
        assert _return_xarray_timelength(self.tsttwo) == 100

    def test_divide_xarray_indicate_empty_future(self):
        assert _divide_xarray_indicate_empty_future(None) is False
        assert _divide_xarray_indicate_empty_future(xr.Dataset({'tst': []}, coords={'time': []})) is False
        assert _divide_xarray_indicate_empty_future(xr.Dataset({'tst': [1, 2, 3]}, coords={'time': [1, 2, 3]})) is True

    def test_return_xarray_constant_blocks(self):
        x1 = self.tsttwo.isel(time=slice(0, 33))
        x2 = self.tsttwo.isel(time=slice(33, 66))
        x3 = self.tsttwo.isel(time=slice(66, 100))
        chunks, totallen = _return_xarray_constant_blocks([33, 33, 34], [x1, x2, x3], 10)

        assert totallen == 100
        assert chunks == [[[0, 10, x1]], [[10, 20, x1]], [[20, 30, x1]], [[30, 33, x1], [0, 7, x2]], [[7, 17, x2]],
                          [[17, 27, x2]], [[27, 33, x2], [0, 4, x3]], [[4, 14, x3]], [[14, 24, x3]], [[24, 34, x3]]]

        chunkdata = [x[2] for y in chunks for x in y]
        expected_data = [x1*4, x2 * 4, x3 *4]
        assert all([(c == chk).all() for c, chk in zip(chunkdata, expected_data)])

    def test_merge_constant_blocks(self):
        newblocks = [[0, 3, self.tsttwo], [10, 13, self.tsttwo], [20, 23, self.tsttwo]]
        merged = _merge_constant_blocks(newblocks)
        assert np.array_equal(np.array([0, 1, 2, 10, 11, 12, 20, 21, 22]), merged.tstone.values)

    def test_assess_need_for_split_correction(self):
        tstthree = xr.Dataset(data_vars={'tstone': self.tstone}, coords={'time': np.arange(99, 199)})
        assert _assess_need_for_split_correction(self.tsttwo, tstthree) is True

        tstfour = xr.Dataset(data_vars={'tstone': self.tstone}, coords={'time': np.arange(150, 250)})
        assert _assess_need_for_split_correction(self.tsttwo, tstfour) is False

    def test_correct_for_splits(self):
        assert _correct_for_splits(self.tsttwo, True).tstone.values[0] == 1
        assert _correct_for_splits(self.tsttwo, False).tstone.values[0] == 0

    def test_closest_prior_key_value(self):
        assert _closest_prior_key_value([100.0, 1000.0, 10000.0, 100000.0, 1000000.0], 80584.3) == 10000.0

    def test_closest_key_value(self):
        assert _closest_key_value([100.0, 1000.0, 10000.0, 100000.0, 1000000.0], 80584.3) == 100000.0

    def test_simplify_soundvelocity_profile(self):
        dpths = np.linspace(0, 12000, 500)
        vals = np.linspace(0, 12000, 500)
        prof = np.dstack([dpths, vals])[0]
        newprof = simplify_soundvelocity_profile(prof)
        assert newprof.shape == (kluster_variables.max_profile_length, 2)

    def test_batch_read_configure_options(self):
        assert batch_read_configure_options() == {
            'ping': {'chunksize': kluster_variables.ping_chunk_size, 'chunks': kluster_variables.ping_chunks,
                     'combine_attributes': True, 'output_arrs': [], 'time_arrs': [], 'final_pths': None,
                     'final_attrs': None},
            'attitude': {'chunksize': kluster_variables.attitude_chunk_size, 'chunks': kluster_variables.att_chunks,
                         'combine_attributes': False, 'output_arrs': [], 'time_arrs': [], 'final_pths': None,
                         'final_attrs': None}}

