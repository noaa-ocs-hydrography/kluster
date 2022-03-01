import unittest

import pyproj.exceptions
import pytest
from pyproj import CRS
import xarray as xr
import numpy as np

try:  # when running from pycharm console
    from hstb_kluster.tests.test_datasets import RealFqpr, load_dataset
    from hstb_kluster.tests.modules.module_test_arrays import expected_alongtrack, expected_acrosstrack, expected_depth, \
        expected_georef_x, expected_georef_y, expected_georef_z
except ImportError:  # relative import as tests directory can vary in location depending on how kluster is installed
    from ..test_datasets import RealFqpr, load_dataset
    from ..modules.module_test_arrays import expected_alongtrack, expected_acrosstrack, expected_depth, \
        expected_georef_x, expected_georef_y, expected_georef_z

from HSTB.kluster.modules.georeference import *


class TestGeoReference(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dset = load_dataset(RealFqpr())

    def test_georeference_module(self):
        multibeam = self.dset.raw_ping[0].isel(time=0).expand_dims('time')
        x = xr.DataArray(data=expected_alongtrack, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                                  'beam': multibeam.beam.values})
        y = xr.DataArray(data=expected_acrosstrack, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                                   'beam': multibeam.beam.values})
        z = xr.DataArray(data=expected_depth, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                             'beam': multibeam.beam.values})
        sv_corr = [x, y, z]

        raw_attitude = self.dset.raw_att
        heading = raw_attitude.heading.interp_like(z)
        heave = raw_attitude.heave.interp_like(z)

        altitude = self.dset.raw_ping[0].altitude
        longitude = self.dset.raw_ping[0].longitude
        latitude = self.dset.raw_ping[0].latitude

        installation_params_time = list(self.dset.xyzrph['tx_r'].keys())[0]
        waterline = float(self.dset.xyzrph['waterline'][installation_params_time])
        vert_ref = 'waterline'

        input_datum = CRS.from_epsg(7911)
        output_datum = CRS.from_epsg(26910)

        z_offset = 1.0

        georef_x, georef_y, georef_z, corrected_heave, corrected_altitude, vdatumunc, geohashes = georef_by_worker(
            sv_corr, altitude,
            longitude, latitude,
            heading, heave,
            waterline, vert_ref,
            input_datum,
            output_datum,
            z_offset)
        assert np.array_equal(georef_x, expected_georef_x)
        assert np.array_equal(georef_y, expected_georef_y)
        assert np.array_equal(georef_z, expected_georef_z)

    def test_georef_with_nan(self):
        multibeam = self.dset.raw_ping[0].isel(time=0).expand_dims('time')
        at_with_nan = expected_alongtrack.copy()
        at_with_nan[0][10] = np.nan
        at_with_nan[0][20] = np.nan
        ac_with_nan = expected_acrosstrack.copy()
        ac_with_nan[0][10] = np.nan
        ac_with_nan[0][20] = np.nan

        x = xr.DataArray(data=at_with_nan, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                          'beam': multibeam.beam.values})
        y = xr.DataArray(data=ac_with_nan, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                          'beam': multibeam.beam.values})
        z = xr.DataArray(data=expected_depth, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                             'beam': multibeam.beam.values})
        sv_corr = [x, y, z]

        raw_attitude = self.dset.raw_att
        heading = raw_attitude.heading.interp_like(z)
        heave = raw_attitude.heave.interp_like(z)

        altitude = self.dset.raw_ping[0].altitude
        longitude = self.dset.raw_ping[0].longitude
        latitude = self.dset.raw_ping[0].altitude

        installation_params_time = list(self.dset.xyzrph['tx_r'].keys())[0]
        waterline = float(self.dset.xyzrph['waterline'][installation_params_time])
        vert_ref = 'waterline'

        input_datum = CRS.from_epsg(7911)
        output_datum = CRS.from_epsg(26910)

        z_offset = 1.0

        georef_x, georef_y, georef_z, corrected_heave, corrected_altitude, vdatumunc, geohashes = georef_by_worker(
            sv_corr, altitude,
            longitude, latitude,
            heading, heave,
            waterline, vert_ref,
            input_datum,
            output_datum,
            z_offset)
        assert np.isnan(georef_x.values[0][10])
        assert np.isnan(georef_x.values[0][20])
        assert np.isnan(georef_y.values[0][10])
        assert np.isnan(georef_y.values[0][20])
        assert np.array_equal(georef_z, expected_georef_z)

    def test_georef_with_depth_nan(self):
        multibeam = self.dset.raw_ping[0].isel(time=0).expand_dims('time')
        dpth_with_nan = expected_depth.copy()
        dpth_with_nan[0][10] = np.nan
        dpth_with_nan[0][20] = np.nan

        x = xr.DataArray(data=expected_alongtrack, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                                  'beam': multibeam.beam.values})
        y = xr.DataArray(data=expected_acrosstrack, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                                   'beam': multibeam.beam.values})
        z = xr.DataArray(data=dpth_with_nan, dims=['time', 'beam'], coords={'time': multibeam.time.values,
                                                                            'beam': multibeam.beam.values})
        sv_corr = [x, y, z]

        raw_attitude = self.dset.raw_att
        heading = raw_attitude.heading.interp_like(z)
        heave = raw_attitude.heave.interp_like(z)

        altitude = self.dset.raw_ping[0].altitude
        longitude = self.dset.raw_ping[0].longitude
        latitude = self.dset.raw_ping[0].latitude

        installation_params_time = list(self.dset.xyzrph['tx_r'].keys())[0]
        waterline = float(self.dset.xyzrph['waterline'][installation_params_time])
        vert_ref = 'waterline'

        input_datum = CRS.from_epsg(7911)
        output_datum = CRS.from_epsg(26910)

        z_offset = 1.0

        georef_x, georef_y, georef_z, corrected_heave, corrected_altitude, vdatumunc, geohashes = georef_by_worker(
            sv_corr, altitude,
            longitude, latitude,
            heading, heave,
            waterline, vert_ref,
            input_datum,
            output_datum,
            z_offset)
        assert np.array_equal(georef_x, expected_georef_x)
        assert np.array_equal(georef_y, expected_georef_y)
        assert np.isnan(georef_z.values[0][10])
        assert np.isnan(georef_z.values[0][20])

    def test_geohash(self):
        newhash_vector = compute_geohash(np.array([43.123456789, 43.123456789, 43.123456789]),
                                         np.array([-73.123456789, -73.123456789, -73.123456789]), precision=7)
        newhash = new_geohash(43.123456789, -73.123456789, precision=7)
        assert newhash == newhash_vector[0]
        assert newhash == b'drsj243'
        lat, lon = decode_geohash(newhash)
        assert lat == pytest.approx(43.12339782714844, abs=0.00000001)
        assert lon == pytest.approx(-73.12294006347656, abs=0.00000001)

    def test_geohash_polygon(self):
        polygon_test = np.array([[-70.1810536, 42.0519741], [-70.178872, 42.0501041], [-70.1813097, 42.0471989],
                                 [-70.1835136, 42.0490578], [-70.1810536, 42.0519741]])
        innerhash, intersecthash = polygon_to_geohashes(polygon_test, precision=7)
        assert innerhash == [b'drqp4yz']  # this cell is completely within the polygon
        assert sorted(intersecthash) == sorted(
            [b'drqp4yv', b'drqp4zp', b'drqp4zn', b'drqp5nc', b'drqp4yz', b'drqp4yx', b'drqp5nb', b'drqp5p0',
             b'drqp4yr', b'drqp5n8', b'drqp5p2', b'drqp4yw', b'drqp4zr', b'drqp4yy', b'drqp5p1'])

    def test_geohash_to_polygon(self):
        assert geohash_to_polygon(b'drqp4yv').bounds == (-70.18478393554688, 42.048797607421875, -70.18341064453125, 42.0501708984375)
        assert geohash_to_polygon(b'drqp4zp').bounds == (-70.18203735351562, 42.0501708984375, -70.1806640625, 42.051544189453125)
        assert geohash_to_polygon(b'drqp5p1').bounds == (-70.17929077148438, 42.0501708984375, -70.17791748046875, 42.051544189453125)
