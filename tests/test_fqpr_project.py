import os
import shutil
import logging

from HSTB.kluster.fqpr_convenience import process_multibeam, convert_multibeam, reload_data
from HSTB.kluster.fqpr_project import *
try:  # when running from pycharm console
    from hstb_kluster.tests.test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr, load_dataset
except ImportError:  # relative import as tests directory can vary in location depending on how kluster is installed
    from .test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr, load_dataset

from pytest import approx
from datetime import datetime
import unittest
import numpy as np
import tempfile


class TestFqprProject(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.testfile = os.path.join(os.path.dirname(__file__), 'resources', '0009_20170523_181119_FA2806.all')
        cls.expected_output = os.path.join(tempfile.tempdir, 'TestFqprProject')
        try:
            os.mkdir(cls.expected_output)
        except FileExistsError:
            shutil.rmtree(cls.expected_output)
            os.mkdir(cls.expected_output)
        cls.datapath = tempfile.mkdtemp(dir=cls.expected_output)
        cls.out = process_multibeam(convert_multibeam(cls.testfile, outfold=cls.datapath), coord_system='NAD83')

    def setUp(self) -> None:
        potential_project_file = os.path.join(self.expected_output, 'kluster_project.json')
        if os.path.exists(potential_project_file):
            os.remove(potential_project_file)
        self.project = FqprProject(project_path=self.expected_output)
        self.project.save_project()

    def tearDown(self) -> None:
        self.project.close()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.out.close()
        except:
            pass
        shutil.rmtree(cls.datapath)
        shutil.rmtree(cls.expected_output)
        resources_folder = os.path.join(os.path.dirname(__file__), 'resources')
        data_folders = [os.path.join(resources_folder, fldr) for fldr in os.listdir(resources_folder) if fldr[:9] == 'converted']
        [shutil.rmtree(fold) for fold in data_folders]

    def _access_processed_data(self):
        """
        Either reload (if data has already been processed once here) or process the test line
        """
        try:
            self.out = reload_data(self.datapath)
            if not self.out:
                self.out = process_multibeam(convert_multibeam(self.testfile, outfold=self.datapath), coord_system='NAD83')
            print('reload')
        except:
            self.out = process_multibeam(convert_multibeam(self.testfile, outfold=self.datapath), coord_system='NAD83')
            print('process')

    def test_project_setup(self):
        assert self.project.client is None
        assert self.project.path == os.path.join(self.expected_output, 'kluster_project.json')
        assert not self.project.is_gui
        assert self.project.file_format == 1.0
        assert self.project.vessel_file is None
        assert not self.project.surface_instances
        assert not self.project.fqpr_instances
        assert not self.project.fqpr_lines
        assert not self.project.fqpr_attrs
        assert not self.project.convert_path_lookup
        assert not self.project.settings
        assert not self.project.buffered_fqpr_navigation
        assert not self.project.point_cloud_for_line
        assert not self.project.node_vals_for_surf

    def test_path_relative_to_project(self):
        assert self.project.path_relative_to_project(os.path.join(self.expected_output, 'testfile')) == 'testfile'

    def test_absolute_path_from_relative(self):
        assert self.project.absolute_path_from_relative('testfile') == os.path.join(self.expected_output, 'testfile')

    def test__load_project_file(self):
        pfiledata = self.project._load_project_file(os.path.join(self.expected_output, 'kluster_project.json'))
        assert not pfiledata['vessel_file']
        assert not pfiledata['fqpr_paths']
        assert not pfiledata['surface_paths']

    def test_add_fqpr(self):
        relpath, alreadyin = self.project.add_fqpr(self.out)
        assert not alreadyin
        assert relpath
        self.project.fqpr_instances == {relpath: self.out}
        assert relpath in self.project.fqpr_attrs
        assert relpath in self.project.fqpr_lines
        self.project.remove_fqpr(relpath, relative_path=True)
        assert not self.project.fqpr_instances
        assert not self.project.fqpr_attrs
        assert not self.project.fqpr_lines

    def test_return_line_owner(self):
        relpath, alreadyin = self.project.add_fqpr(self.out)
        assert self.out == self.project.return_line_owner('0009_20170523_181119_FA2806.all')
        assert not self.project.return_line_owner('not_a_real_line')

    def test_return_fqpr(self):
        relpath, alreadyin = self.project.add_fqpr(self.out)
        assert [self.out] == self.project.return_fqpr_instances()
        assert [relpath] == self.project.return_fqpr_paths()
        assert relpath in self.project.return_project_lines()
        assert ['0009_20170523_181119_FA2806.all'] == self.project.return_sorted_line_list()
        assert self.expected_output == self.project.return_project_folder()

    def test_return_line_navigation(self):
        lat, lon = self.project.return_line_navigation('0009_20170523_181119_FA2806.all')
        assert not lat
        assert not lon
        relpath, alreadyin = self.project.add_fqpr(self.out)
        lat, lon = self.project.return_line_navigation('0009_20170523_181119_FA2806.all')
        assert lat.size == 216
        assert lon.size == 216

    def test_return_lines_in_box(self):
        relpath, alreadyin = self.project.add_fqpr(self.out)
        assert not self.project.return_lines_in_box(0, 1, 0, 1)
        assert ['0009_20170523_181119_FA2806.all'] == self.project.return_lines_in_box(47, 48, -123, -122)

    def test_return_soundings_in_polygon(self):
        relpath, alreadyin = self.project.add_fqpr(self.out)
        bad_polygon = np.array([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]])
        data = self.project.return_soundings_in_polygon(bad_polygon)
        assert not data
        good_polygon = np.array([[-122.4771132, 47.78910811], [-122.47744457, 47.78910811], [-122.47744457, 47.78936742],
                                 [-122.4771132, 47.78936742], [-122.4771132, 47.78910811]])
        data = self.project.return_soundings_in_polygon(good_polygon)
        assert relpath in data
        assert len(data[relpath]) == 9
        assert data[relpath][0].shape == (1012,)

    def test_get_fqpr_by_serial_number(self):
        relpath, alreadyin = self.project.add_fqpr(self.out)
        pth, fq = self.project.get_fqpr_by_serial_number(0, 0)
        assert not pth
        assert not fq
        pth, fq = self.project.get_fqpr_by_serial_number(self.out.multibeam.raw_ping[0].attrs['system_serial_number'][0],
                                                         self.out.multibeam.raw_ping[0].attrs['secondary_system_serial_number'][0])
        assert pth == os.path.join(self.expected_output, relpath)
        assert fq == self.out
