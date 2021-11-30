import pytest
import xarray as xr
import numpy as np
import unittest

from HSTB.kluster.rotations import build_rot_mat, build_mounting_angle_mat, combine_rotation_matrix, \
    return_attitude_rotation_matrix, return_mounting_rotation_matrix


class TestRotations(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.roll = xr.DataArray([0.1, 0.2, 0.3], coords={'time': [0, 1, 2]}, dims=['time'])
        cls.pitch = xr.DataArray([0.01, 0.02, -0.04], coords={'time': [0, 1, 2]}, dims=['time'])
        cls.yaw = xr.DataArray([359.5, 1.2, 2.3], coords={'time': [0, 1, 2]}, dims=['time'])

    @staticmethod
    def make_matrix(roll, pitch, yaw, dtype=np.float32):
        r = np.deg2rad(roll, dtype=dtype)
        p = np.deg2rad(pitch, dtype=dtype)
        y = np.deg2rad(yaw, dtype=dtype)

        rcos = np.cos(r)
        pcos = np.cos(p)
        ycos = np.cos(y)
        rsin = np.sin(r)
        psin = np.sin(p)
        ysin = np.sin(y)

        return [[ycos * pcos, ycos * psin * rsin - ysin * rcos, ycos * psin * rcos + ysin * rsin],
                [ysin * pcos, ysin * psin * rsin + ycos * rcos, ysin * psin * rcos - ycos * rsin],
                [-psin, pcos * rsin, pcos * rcos]]

    @staticmethod
    def assert_matrix(act_mat, expected_mat):
        for data in act_mat.data:
            for d_row, e_row in zip(data, expected_mat):
                assert pytest.approx(d_row, 0.00000001) == e_row

    @staticmethod
    def assert_matrix_row(act_mat, expected_mat):
        for d_row, e_row in zip(act_mat, expected_mat):
            assert pytest.approx(d_row, 0.00000001) == e_row

    def test_build_rot_mat(self):
        rotmat = build_rot_mat(self.roll, self.pitch, self.yaw, order='rpy', degrees=True)
        assert pytest.approx(rotmat.isel(time=0).values[0, :], 0.00000001) == np.array(
            [0.9999619078338804, 0.00872682681276807, 0.00015929534287439177])
        assert pytest.approx(rotmat.isel(time=0).values[1, :], 0.00000001) == np.array(
            [-0.0087265353654609, 0.9999603973772001, -0.0017467849745820114])
        assert pytest.approx(rotmat.isel(time=0).values[2, :], 0.00000001) == np.array(
            [-0.0001745329243133368, 0.0017453283393154377, 0.99999846168244])

        assert pytest.approx(rotmat.isel(time=1).values[0, :], 0.00000001) == np.array(
            [0.9997806225647237, -0.020941074095018365, 0.00042208984884417513])
        assert pytest.approx(rotmat.isel(time=1).values[1, :], 0.00000001) == np.array(
            [0.02094241860747179, 0.9997746179864385, -0.00348257561876403])
        assert pytest.approx(rotmat.isel(time=1).values[2, :], 0.00000001) == np.array(
            [-0.00034906584331009674, 0.0034906512025610886, 0.9999938467346782])

        assert pytest.approx(rotmat.isel(time=2).values[0, :], 0.00000001) == np.array(
            [0.9991941516168411, -0.040134894863113696, -0.000487431049527475])
        assert pytest.approx(rotmat.isel(time=2).values[1, :], 0.00000001) == np.array(
            [0.040131782752685655, 0.9991805517074703, -0.005259762603623477])
        assert pytest.approx(rotmat.isel(time=2).values[2, :], 0.00000001) == np.array(
            [0.0006981316440875792, 0.005235962555446998, 0.9999860485568414])

        # should be able to do this manually too, and get the right answer
        manual_mat = self.make_matrix(0.1, 0.01, 359.5, dtype= None)
        assert pytest.approx(rotmat.isel(time=0).values[0, :], 0.00000001) == manual_mat[0]
        assert pytest.approx(rotmat.isel(time=0).values[1, :], 0.00000001) == manual_mat[1]
        assert pytest.approx(rotmat.isel(time=0).values[2, :], 0.00000001) == manual_mat[2]

    def test_build_mounting_angle_mat(self):
        rollval = 0.142
        pitchval = -0.241
        yawval = 0.314
        rotmat = build_mounting_angle_mat(rollval, pitchval, yawval, '1616590165')

        assert rotmat.time.values[0] == 1616590165.0
        self.assert_matrix(rotmat, self.make_matrix(rollval, pitchval, yawval))

    def test_return_mounting_rotation_matrix(self):
        rollval = 0.142
        pitchval = -0.241
        yawval = 0.314
        rotmat = return_mounting_rotation_matrix(rollval, pitchval, yawval, '1616590165')

        assert rotmat.time.values[0] == 1616590165.0
        self.assert_matrix(rotmat, self.make_matrix(rollval, pitchval, yawval))

    def test_combine_rotation_matrix(self):
        mounting_angle_mat = build_mounting_angle_mat(0.142, -0.241, 0.314, '1616590165')
        attitude_rotmat = build_rot_mat(self.roll, self.pitch, self.yaw, order='rpy', degrees=True)

        comb_mat = combine_rotation_matrix(mounting_angle_mat, attitude_rotmat)
        expected_mat = [[0.9999865621818694, 0.0032365578874080946, -0.004054948042329317],
                        [-0.00325363346219585, 0.9999858044596385, -0.004211457694367961],
                        [0.00404126048033365, 0.004224594309995787, 0.9999829067856274]]

        self.assert_matrix_row(comb_mat.isel(time=0), expected_mat)

    def test_return_attitude_rotation_matrix(self):
        dset = xr.Dataset(data_vars={'roll': self.roll, 'pitch': self.pitch, 'heading': self.yaw},
                          coords={'time': [0, 1, 2]})
        times, rotmat = return_attitude_rotation_matrix(dset)

        assert np.array_equal(times.values, np.array([0, 1, 2]))
        expected = [[0.9999619078338804, 0.00872682681276807, 0.00015929534287439177],
                    [-0.0087265353654609, 0.9999603973772001, -0.0017467849745820114],
                    [-0.0001745329243133368, 0.0017453283393154377, 0.99999846168244]]
        self.assert_matrix_row(rotmat.isel(time=0), expected)
