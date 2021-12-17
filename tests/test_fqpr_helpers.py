import os
import unittest
import numpy as np

from HSTB.kluster import kluster_variables
from HSTB.kluster.fqpr_helpers import epsg_determinator, return_files_from_path, seconds_to_formatted_string, haversine


class TestFqprHelper(unittest.TestCase):

    def test_epsg_determinator(self):
        assert epsg_determinator('nad83(2011)', 12, 'N') == 6341
        assert epsg_determinator('NAD83(2011)', 5, 'N') == 6334
        try:
            epsg_determinator('nad83(2011)', 12, 'S')
        except ValueError:  # this is the expected result since this is out of bounds for nad83
            assert True

        assert epsg_determinator('wgs84', 12, 'N') == 32612
        assert epsg_determinator('wgs84', 12, 'S') == 32712
        assert epsg_determinator('wgs84', 25, 'N') == 32625
        assert epsg_determinator('wgs84', 25, 'S') == 32725

    def test_return_files_from_path(self):
        fil = os.path.join(os.path.dirname(__file__), 'resources', '0009_20170523_181119_FA2806.all')

        assert return_files_from_path(fil) == [[fil]]
        fil = ['a'] * 3
        assert return_files_from_path(fil) == [fil]
        fil = ['a'] * (kluster_variables.converted_files_at_once + 1)
        assert return_files_from_path(fil) == [['a'] * kluster_variables.converted_files_at_once, ['a']]
        fil = ['a'] * (kluster_variables.converted_files_at_once * 3 + 1)
        assert return_files_from_path(fil) == [['a'] * kluster_variables.converted_files_at_once,
                                               ['a'] * kluster_variables.converted_files_at_once,
                                               ['a'] * kluster_variables.converted_files_at_once,
                                               ['a']]

    def test_seconds_to_formatted_string(self):
        assert seconds_to_formatted_string(666) == '11 minutes, 6 seconds'
        assert seconds_to_formatted_string(66666) == '18 hours, 31 minutes, 6 seconds'
        assert seconds_to_formatted_string(6666666) == '1851 hours, 51 minutes, 6 seconds'
        assert seconds_to_formatted_string(0) == '0 seconds'
        assert seconds_to_formatted_string(-1) == '0 seconds'

    def test_haversine(self):
        assert haversine(128.1234, 45.1234, 128.5678, 45.5678) == 60.398765789070794
        assert haversine(-78.1234, -12.1234, -78.5678, -12.5678) == 69.08002250085612

        vectorized = haversine(np.array([128.1234, -78.1234]), np.array([45.1234, -12.1234]),
                               np.array([128.5678, -78.5678]), np.array([45.5678, -12.5678]))
        assert vectorized[0] == 60.398765789070794
        assert vectorized[1] == 69.08002250085612
