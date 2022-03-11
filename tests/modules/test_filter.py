# Import qt first, to resolve the backend issues youll get in matplotlib if you dont import this first, as it prefers PySide2
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled, found_path
if qgis_enabled:
    from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui

import matplotlib
matplotlib.use('qt5agg')

import os
import shutil
import unittest
import tempfile
import numpy as np

from HSTB.kluster.modules import filter
from HSTB.kluster.fqpr_convenience import process_multibeam, convert_multibeam, reload_data
from HSTB.kluster import kluster_variables


class TestFilter(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.expected_output = os.path.join(tempfile.tempdir, 'TestFilter')
        cls.test_filter = 'test_filter_file'
        cls.external_filter_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources')
        cls.testfile = os.path.join(cls.external_filter_directory, '0009_20170523_181119_FA2806.all')

        try:
            os.mkdir(cls.expected_output)
        except FileExistsError:
            shutil.rmtree(cls.expected_output)
            os.mkdir(cls.expected_output)
        cls.datapath = tempfile.mkdtemp(dir=cls.expected_output)

    def setUp(self) -> None:
        self._access_processed_data()
        self.fm = filter.FilterManager(self.out, external_filter_directory=self.external_filter_directory)

    def tearDown(self) -> None:
        try:
            self.out.close()
            self.fm = None
        except:
            pass

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.datapath)
        shutil.rmtree(cls.expected_output)
        resources_folder = cls.external_filter_directory
        data_folders = [os.path.join(resources_folder, fldr) for fldr in os.listdir(resources_folder) if
                        fldr[:9] == 'converted']
        [shutil.rmtree(fold) for fold in data_folders]

    def _access_processed_data(self):
        """
        Either reload (if data has already been processed once here) or process the test line
        """
        try:
            self.out = reload_data(self.datapath)
            if not self.out:
                self.out = process_multibeam(convert_multibeam(self.testfile, outfold=self.datapath),
                                             coord_system='NAD83')
            print('reload')
        except:
            self.out = process_multibeam(convert_multibeam(self.testfile, outfold=self.datapath), coord_system='NAD83')
            print('process')

    def test_filter_manager_setup(self):
        assert self.fm.fqpr == self.out
        assert self.fm.external_filter_directory == self.external_filter_directory
        assert self.test_filter in self.fm.filter_names
        assert self.test_filter in self.fm.filter_descriptions
        assert self.fm.filter_descriptions[self.test_filter] == 'only for testing'
        assert self.test_filter in self.fm.filter_lookup
        assert self.fm.filter_lookup[self.test_filter] in self.fm.reverse_filter_lookup
        assert self.fm.filter_file[self.test_filter] == os.path.join(self.external_filter_directory, self.test_filter) + '.py'

    def test_filter_initialize(self):
        assert self.fm.external_filter_directory == self.external_filter_directory
        self.fm.external_filter_directory = ''
        self.fm.initialize_filters()
        assert self.fm.external_filter_directory == ''
        assert self.test_filter not in self.fm.filter_names

    def test_clear_filters(self):
        self.fm.clear_filters()
        assert self.fm.filter_names == []
        assert self.fm.filter_descriptions == {}
        assert self.fm.filter_lookup == {}
        assert self.fm.reverse_filter_lookup == {}
        assert self.fm.filter_file == {}

    def test_return_filter_class(self):
        assert self.fm.return_filter_class(self.test_filter) == self.fm.filter_lookup[self.test_filter]
        assert self.fm.return_filter_class('notafilter') is None

    def test_return_optional_filter_controls(self):
        assert self.fm.return_optional_filter_controls(self.test_filter) == []
        assert self.fm.return_optional_filter_controls('filter_by_depth') == [['float', 'min_depth', 0.0, {'minimum': -100, 'maximum': 99999999, 'singleStep': 0.1}],
                                                                              ['float', 'max_depth', 500.0, {'minimum': -100, 'maximum': 99999999, 'singleStep': 0.1}]]

    def test_run_testfilter(self):
        # even with save on, since no new_status is generated, it will complete and return []
        assert self.fm.run_filter(self.test_filter, save_to_disk=True) == []
        # without save to disk, you get the same return, it just wont try and fail to save
        assert self.fm.run_filter(self.test_filter, save_to_disk=False) == []

    def _reset_filter(self):
        # we want to check the results of the filter, so we need to set all the soundings to accepted before each test
        self.fm.run_filter('reaccept_rejected')

    def test_reset_filter(self):
        self._reset_filter()
        assert np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].detectioninfo == kluster_variables.rejected_flag) == 0

    def test_filter_by_angle(self):
        self._reset_filter()
        expected_rejected_count = np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].corr_pointing_angle < -np.pi/4) + \
            np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].corr_pointing_angle > np.pi/4)

        new_status = self.fm.run_filter('filter_by_angle', save_to_disk=False, min_angle=-45, max_angle=45)
        # with save_to_disk as False, we don't affect the loaded data
        assert np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].detectioninfo == kluster_variables.rejected_flag) == 0
        # instead we get the new sounding flags returned, which should show that rejected soundings equal <-45 and >45 deg
        assert np.count_nonzero(new_status[0] == kluster_variables.rejected_flag) == expected_rejected_count
        # now if we save_to_disk, you'll see the data loaded change
        new_status = self.fm.run_filter('filter_by_angle', save_to_disk=True, min_angle=-45, max_angle=45)
        assert np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].detectioninfo == kluster_variables.rejected_flag) == expected_rejected_count

    def test_filter_by_angle_selectedindex(self):
        self._reset_filter()
        selindex = np.zeros(self.fm.fqpr.multibeam.raw_ping[0].detectioninfo.values.flatten().shape, dtype=bool)
        # lets only select the first 400 beams
        selindex[:400] = True
        expected_rejected_count = np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].corr_pointing_angle < -np.pi/4) + \
            np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].corr_pointing_angle > np.pi/4)
        expected_firstping_rejected_count = np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].isel(time=0).corr_pointing_angle < -np.pi/4) + \
            np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].isel(time=0).corr_pointing_angle > np.pi/4)

        new_status = self.fm.run_filter('filter_by_angle', selected_index=[selindex], save_to_disk=False, min_angle=-45, max_angle=45)
        # with save_to_disk as False, we don't affect the loaded data
        assert np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].detectioninfo == kluster_variables.rejected_flag) == 0
        # instead we get the new sounding flags returned, which should show that rejected soundings equal <-45 and >45 deg
        # note that selindex doesn't affect anything until we go to save
        assert np.count_nonzero(new_status[0] == kluster_variables.rejected_flag) == expected_rejected_count

        # if we save, you'll see that only the first ping that was selected is saved to disk
        new_status = self.fm.run_filter('filter_by_angle', selected_index=[selindex], save_to_disk=True, min_angle=-45, max_angle=45)
        assert np.count_nonzero(self.fm.fqpr.multibeam.raw_ping[0].detectioninfo == kluster_variables.rejected_flag) == expected_firstping_rejected_count
