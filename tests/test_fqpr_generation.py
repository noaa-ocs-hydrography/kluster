import os
import shutil
import logging

from HSTB.kluster.fqpr_convenience import process_multibeam, convert_multibeam, reload_data
from HSTB.kluster.fqpr_generation import Fqpr
try:  # when running from pycharm console
    from hstb_kluster.tests.test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr, load_dataset
except ImportError:  # relative import as tests directory can vary in location depending on how kluster is installed
    from .test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr, load_dataset

from pytest import approx
from datetime import datetime
import unittest
import numpy as np
import tempfile


class TestFqprGeneration(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.testfile = os.path.join(os.path.dirname(__file__), 'resources', '0009_20170523_181119_FA2806.all')
        cls.expected_output = os.path.join(tempfile.tempdir, 'TestFqprGeneration')
        try:
            os.mkdir(cls.expected_output)
        except FileExistsError:
            shutil.rmtree(cls.expected_output)
            os.mkdir(cls.expected_output)
        cls.datapath = tempfile.mkdtemp(dir=cls.expected_output)

    def setUp(self) -> None:
        self.multicheck = os.path.join(self.datapath, 'multicheck')
        self.expected_multi = os.path.join(self.datapath, 'multicheck_40111.csv')
        self.navcheck = os.path.join(self.datapath, 'navcheck')
        self.expected_nav = os.path.join(self.datapath, 'navcheck_40111.csv')

    def tearDown(self) -> None:
        try:
            self.out.close()
        except:
            pass

    @classmethod
    def tearDownClass(cls) -> None:
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

    def test_process_testfile(self):
        """
        Run conversion and basic processing on the test file
        """

        linename = os.path.split(self.testfile)[1]
        out = convert_multibeam(self.testfile, outfold=self.datapath)
        assert not out.line_is_processed(linename)
        assert out.return_next_unprocessed_line() == linename

        out = process_multibeam(out, coord_system='NAD83')
        assert out.line_is_processed(linename)
        assert out.return_next_unprocessed_line() == ''
        out.close()

        number_of_sectors = len(out.multibeam.raw_ping)
        rp = out.multibeam.raw_ping[0].isel(time=0).isel(beam=0)
        firstbeam_angle = rp.beampointingangle.values
        firstbeam_traveltime = rp.traveltime.values
        first_counter = rp.counter.values
        first_dinfo = rp.detectioninfo.values
        first_mode = rp.mode.values
        first_modetwo = rp.modetwo.values
        first_ntx = rp.ntx.values
        firstbeam_procstatus = rp.processing_status.values
        firstbeam_qualityfactor = rp.qualityfactor.values
        first_soundspeed = rp.soundspeed.values
        first_tiltangle = rp.tiltangle.values
        first_delay = rp.delay.values
        first_frequency = rp.frequency.values
        first_yawpitch = rp.yawpitchstab.values
        firstcorr_angle = rp.corr_pointing_angle.values
        firstcorr_altitude = rp.corr_altitude.values
        firstcorr_heave = rp.corr_heave.values
        firstdepth_offset = rp.depthoffset.values
        first_status = rp.processing_status.values
        firstrel_azimuth = rp.rel_azimuth.values
        firstrx = rp.rx.values
        firstthu = rp.thu.values
        firsttvu = rp.tvu.values
        firsttx = rp.tx.values
        firstx = rp.x.values
        firsty = rp.y.values
        firstz = rp.z.values

        assert number_of_sectors == 1
        assert firstbeam_angle == approx(np.float32(74.640), 0.001)
        assert firstbeam_traveltime == approx(np.float32(0.3360895), 0.000001)
        assert first_counter == 61967
        assert first_dinfo == 2
        assert first_mode == 'FM'
        assert first_modetwo == '__FM'
        assert first_ntx == 3
        assert firstbeam_procstatus == 5
        assert firstbeam_qualityfactor == 42
        assert first_soundspeed == np.float32(1488.6)
        assert first_tiltangle == np.float32(-0.44)
        assert first_delay == approx(np.float32(0.002206038), 0.000001)
        assert first_frequency == 275000
        assert first_yawpitch == 'PY'
        assert firstcorr_angle == approx(np.float32(1.2028906), 0.000001)
        assert firstcorr_altitude == np.float32(0.0)
        assert firstcorr_heave == approx(np.float32(-0.06), 0.01)
        assert firstdepth_offset == approx(np.float32(92.162), 0.001)
        assert first_status == 5
        assert firstrel_azimuth == approx(np.float32(4.703383), 0.00001)
        assert firstrx == approx(np.array([0.7870753, 0.60869384, -0.100021675], dtype=np.float32), 0.00001)
        assert firstthu == approx(np.float32(8.10849), 0.0001)
        assert firsttvu == approx(np.float32(2.444148), 0.0001)
        assert firsttx == approx(np.array([0.6074468, -0.79435784, 0.0020107413], dtype=np.float32), 0.00001)
        assert firstx == approx(539028.450, 0.001)
        assert firsty == approx(5292783.977, 0.001)
        assert firstz == approx(np.float32(92.742), 0.001)

        assert rp.min_x == 538922.066
        assert rp.min_y == 5292774.566
        assert rp.min_z == 72.961
        assert rp.max_x == 539320.370
        assert rp.max_y == 5293236.823
        assert rp.max_z == 94.294

    def test_copy(self):
        self._access_processed_data()
        fqpr_copy = self.out.copy()
        # attributes are now distinct
        fqpr_copy.vert_ref = 'notwaterline'
        assert self.out.vert_ref == 'waterline'
        # datasets are distinct
        fqpr_copy.multibeam.raw_att.heading[0] = 0
        assert self.out.multibeam.raw_att.heading[0] == 307.17999267578125
        # dataset attributes as well
        fqpr_copy.multibeam.raw_ping[0]['vertical_reference'] = 'notwaterline'
        assert self.out.multibeam.raw_ping[0].vertical_reference == 'waterline'
        # what about the attributes that are layered dictionaries
        fqpr_copy.multibeam.raw_ping[0].xyzrph['beam_opening_angle']['1495563079'] = 999
        assert self.out.multibeam.raw_ping[0].xyzrph['beam_opening_angle']['1495563079'] == 1.3

    def test_set_variable_by_filter(self):
        self._access_processed_data()
        polygon = np.array([[-122.47798556, 47.78949665], [-122.47798556, 47.78895117], [-122.47771027, 47.78895117],
                            [-122.47771027, 47.78949665]])
        head, x, y, z, tvu, rejected, pointtime, beam = self.out.return_soundings_in_polygon(polygon)
        assert head.shape == x.shape == y.shape == z.shape == tvu.shape == rejected.shape == pointtime.shape == beam.shape
        assert x.shape == (1911,)
        assert np.count_nonzero(self.out.subset.ping_filter) == 1911  # ping filter is set on return_soundings, is a bool mask of which soundings are in the selection

        assert rejected[0] == 0  # first sounding is 0 status
        self.out.set_variable_by_filter('detectioninfo', 2, selected_index=[[0]])  # set the first selected point region to rejected=2
        head, x, y, z, tvu, rejected, pointtime, beam = self.out.return_soundings_in_polygon(polygon)
        assert rejected[0] == 2  # first sounding is now status=2
        self.out.set_variable_by_filter('detectioninfo', 2)  # set the all poitns in the return_soundings selection to status=2
        head, x, y, z, tvu, rejected, pointtime, beam = self.out.return_soundings_in_polygon(polygon)
        assert (rejected == 2).all()

    def test_get_orientation_vectors(self):
        """
        get_orientation_vectors test for the em2040 dataset
        """

        self.get_orientation_vectors(dset='real')

    def test_get_orientation_vectors_dualhead(self):
        """
        get_orientation_vectors test for the em2040 dualrx/dualtx dataset
        """

        self.get_orientation_vectors(dset='realdualhead')

    def test_build_beam_pointing_vector(self):
        """
        build_beam_pointing_vector test for the em2040 dataset
        """

        self.build_beam_pointing_vector(dset='real')

    def test_build_beam_pointing_vector_dualhead(self):
        """
        build_beam_pointing_vector test for the em2040 dualrx/dualtx dataset
        """

        self.build_beam_pointing_vector(dset='realdualhead')

    def test_sv_correct(self):
        """
        sv_correct test for the em2040 dataset
        """

        self.sv_correct(dset='real')

    def test_sv_correct_dualhead(self):
        """
        sv_correct test for the em2040 dualrx/dualtx dataset
        """

        self.sv_correct(dset='realdualhead')

    def test_georef_xyz(self):
        """
        georef_xyz test for the em2040 dataset
        """

        self.georef_xyz(dset='real')

    def test_georef_xyz_dualhead(self):
        """
        georef_xyz test for the em2040 dualrx/dualtx dataset
        """

        self.georef_xyz(dset='realdualhead')

    def test_return_total_soundings(self):
        self._access_processed_data()
        ts = self.out.return_total_soundings(min_time=1495563100, max_time=1495563130)
        assert ts == 49200
        ts = self.out.return_total_soundings()
        assert ts == 86400

    def test_return_soundings_in_polygon(self):
        self._access_processed_data()
        polygon = np.array([[-122.47798556, 47.78949665], [-122.47798556, 47.78895117], [-122.47771027, 47.78895117],
                            [-122.47771027, 47.78949665]])
        head, x, y, z, tvu, rejected, pointtime, beam = self.out.return_soundings_in_polygon(polygon)
        assert head.shape == x.shape == y.shape == z.shape == tvu.shape == rejected.shape == pointtime.shape == beam.shape
        assert x.shape == (1911,)

        # now try just pulling the corrected beam angle for the soundings
        beamangle = self.out.return_soundings_in_polygon(polygon, variable_selection=('corr_pointing_angle',))
        assert beamangle[0].shape == (1911,)

        # now use the existing filter that we set with the last return_soundings_in_polygon to get an additional variable
        getbeamangle = self.out.get_variable_by_filter('corr_pointing_angle')
        assert getbeamangle.shape == (1911,)
        assert (beamangle == getbeamangle).all()

        # try a 1d variable
        alti = self.out.get_variable_by_filter('altitude')
        assert alti.shape == (1911,)

        # try a attitude variable
        rollv = self.out.get_variable_by_filter('roll')
        assert rollv.shape == (1911,)

        # now try setting the filter separately from the return_soundings_in_polygon method.  This allows you to set the
        #    filter without loading data if you want to do that.
        self.out.set_filter_by_polygon(polygon)
        next_getbeamangle = self.out.get_variable_by_filter('corr_pointing_angle')
        assert next_getbeamangle.shape == (1911,)
        assert (beamangle == next_getbeamangle).all()

    def test_return_cast_dict(self):
        self._access_processed_data()
        cdict = self.out.return_cast_dict()
        assert cdict == {'profile_1495563079': {'location': [47.78890945494799, -122.47711319986821],
                                                'source': 'multibeam', 'time': 1495563079,
                                                'data': [[0.0, 1489.2000732421875], [0.32, 1489.2000732421875],
                                                         [0.5, 1488.7000732421875],
                                                         [0.55, 1488.300048828125], [0.61, 1487.9000244140625],
                                                         [0.65, 1488.2000732421875],
                                                         [0.67, 1488.0], [0.79, 1487.9000244140625],
                                                         [0.88, 1487.9000244140625],
                                                         [1.01, 1488.2000732421875], [1.04, 1488.0999755859375],
                                                         [1.62, 1488.0999755859375],
                                                         [2.0300000000000002, 1488.300048828125],
                                                         [2.43, 1488.9000244140625], [2.84, 1488.5],
                                                         [3.25, 1487.7000732421875], [3.67, 1487.2000732421875],
                                                         [4.45, 1486.800048828125],
                                                         [4.8500000000000005, 1486.800048828125],
                                                         [5.26, 1486.5999755859375], [6.09, 1485.7000732421875],
                                                         [6.9, 1485.0999755859375], [7.71, 1484.800048828125],
                                                         [8.51, 1484.0],
                                                         [8.91, 1483.800048828125], [10.13, 1483.7000732421875],
                                                         [11.8, 1483.0999755859375],
                                                         [12.620000000000001, 1482.9000244140625],
                                                         [16.79, 1482.9000244140625], [20.18, 1481.9000244140625],
                                                         [23.93, 1481.300048828125], [34.79, 1480.800048828125],
                                                         [51.15, 1480.800048828125],
                                                         [56.13, 1481.0], [60.67, 1481.5], [74.2, 1481.9000244140625],
                                                         [12000.0, 1675.800048828125]]}}

    def test_return_line_xyzrph(self):
        self._access_processed_data()
        xyzrph = self.out.return_line_xyzrph('0009_20170523_181119_FA2806.all')
        assert xyzrph == self.out.multibeam.xyzrph  # there is only one entry so the line and the datasets xyzrph record should be the same

    def test_multibeam_files(self):
        self._access_processed_data()
        start_time, end_time, start_lat, start_lon, end_lat, end_lon, azimuth = self.out.multibeam.raw_ping[0].multibeam_files['0009_20170523_181119_FA2806.all']
        assert start_time == 1495563079.364
        assert end_time == 1495563133.171
        assert start_lat == 47.78890945494799
        assert start_lon == -122.47711319986821
        assert end_lat == 47.78942111430487
        assert end_lon == -122.47841440033638
        assert azimuth == 307.92
        # can also use the function
        start_time, end_time, start_lat, start_lon, end_lat, end_lon, azimuth = self.out.line_attributes('0009_20170523_181119_FA2806.all')
        assert start_time == 1495563079.364
        assert end_time == 1495563133.171
        assert start_lat == 47.78890945494799
        assert start_lon == -122.47711319986821
        assert end_lat == 47.78942111430487
        assert end_lon == -122.47841440033638
        assert azimuth == 307.92

    def test_subset_by_time(self):
        self._access_processed_data()
        self.out.subset_by_time(mintime=1495563100, maxtime=1495563130)
        assert len(self.out.multibeam.raw_ping[0].time) == 123
        assert len(self.out.multibeam.raw_att.time) == 3001
        self.out.subset.restore_subset()
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_by_time_outofbounds_after(self):
        self._access_processed_data()
        err = self.out.subset_by_time(mintime=1495563134, maxtime=1495563150)
        assert err
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_by_time_outofbounds_before(self):
        self._access_processed_data()
        err = self.out.subset_by_time(mintime=1495563000, maxtime=1495563070)
        assert err
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_by_times(self):
        self._access_processed_data()
        self.out.subset_by_times([[1495563080, 1495563090], [1495563100, 1495563130]])
        assert len(self.out.multibeam.raw_ping[0].time) == 165
        assert len(self.out.multibeam.raw_att.time) == 4002
        self.out.subset.restore_subset()
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_by_line(self):
        self._access_processed_data()
        self.out.subset_by_lines('0009_20170523_181119_FA2806.all')
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5295
        self.out.subset_by_lines(['0009_20170523_181119_FA2806.all'])
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5295
        self.out.subset.restore_subset()
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_variables(self):
        self._access_processed_data()
        dset = self.out.subset_variables(['z'], ping_times=(1495563100, 1495563130))

        assert len(dset.time) == 123
        assert dset.z.shape[0] == 123

        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert self.out.multibeam.raw_ping[0].z.shape[0] == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_variables_filter(self):
        self._access_processed_data()
        dset = self.out.subset_variables(['z'], ping_times=(1495563100, 1495563130), filter_by_detection=True)

        try:
            assert len(dset.sounding) == 45059
            assert dset.z.shape[0] == 45059
        except AssertionError:  # these are true if the test suite hit test_set_variable_by_filter first
            assert len(dset.sounding) == 43178
            assert dset.z.shape[0] == 43178

        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert self.out.multibeam.raw_ping[0].z.shape[0] == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_variables_outofbounds_after(self):
        self._access_processed_data()
        dset = self.out.subset_variables(['z'], ping_times=(1495563134, 1495563150), filter_by_detection=True)
        assert dset is None
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert self.out.multibeam.raw_ping[0].z.shape[0] == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_subset_variables_by_line(self):
        self._access_processed_data()
        dset = self.out.subset_variables_by_line(['z'])

        assert list(dset.keys()) == ['0009_20170523_181119_FA2806.all']
        assert len(dset['0009_20170523_181119_FA2806.all'].time) == 216
        assert dset['0009_20170523_181119_FA2806.all'].z.shape[0] == 216

    def test_subset_variables_by_line_outofbounds(self):
        self._access_processed_data()
        # set fake start and end times to after the dataset time to test outofbounds
        self.out.multibeam.raw_ping[0].multibeam_files['0009_20170523_181119_FA2806.all'][0] = 1495563134
        self.out.multibeam.raw_ping[0].multibeam_files['0009_20170523_181119_FA2806.all'][1] = 1495563150
        dset = self.out.subset_variables_by_line(['z'])

        assert list(dset.keys()) == ['0009_20170523_181119_FA2806.all']
        assert dset['0009_20170523_181119_FA2806.all'] is None
        assert len(self.out.multibeam.raw_ping[0].time) == 216
        assert len(self.out.multibeam.raw_att.time) == 5302

    def test_intersects(self):
        self._access_processed_data()
        assert self.out.intersects(5293000, 5330000, 538950, 539300, geographic=False)
        assert not self.out.intersects(5320000, 5330000, 538950, 539300, geographic=False)
        assert self.out.intersects(47.78895, 47.790, -122.478, -122.479, geographic=True)
        assert not self.out.intersects(47.8899, 47.890, -122.478, -122.479, geographic=True)

    def test_return_unique_mode(self):
        self._access_processed_data()
        assert self.out.return_unique_mode() == ['FM']

    def test_return_rounded_frequency(self):
        self._access_processed_data()
        assert self.out.return_rounded_frequency() == [300000]

    def test_return_lines_for_times(self):
        self._access_processed_data()
        lns = self.out.return_lines_for_times(np.array([1495400000, 1495563100, 1495563132]))
        assert np.array_equal(lns, ['0009_20170523_181119_FA2806.all', '0009_20170523_181119_FA2806.all', '0009_20170523_181119_FA2806.all'])

    def test_last_operation_date(self):
        self._access_processed_data()
        assert datetime.strptime(self.out.multibeam.raw_ping[0]._total_uncertainty_complete, '%c') ==\
               self.out.last_operation_date

    def test_export_files(self):
        self._access_processed_data()
        assert len(self.out.export_pings_to_file(file_format='csv', filter_by_detection=True,
                                                 export_by_identifiers=True)) == 6
        assert len(self.out.export_pings_to_file(file_format='csv', filter_by_detection=True,
                                                 export_by_identifiers=False)) == 1
        assert len(self.out.export_pings_to_file(file_format='las', filter_by_detection=True,
                                                 export_by_identifiers=True)) == 6
        assert len(self.out.export_pings_to_file(file_format='las', filter_by_detection=True,
                                                 export_by_identifiers=False)) == 1

        self.remove_export_file('csv_export')
        self.remove_export_file('las_export')

    def test_export_lines_to_file(self):
        self._access_processed_data()
        assert len(self.out.export_lines_to_file(['0009_20170523_181119_FA2806.all'], file_format='csv',
                                                 filter_by_detection=True, export_by_identifiers=True)) == 6
        assert len(self.out.export_lines_to_file(['0009_20170523_181119_FA2806.all'], file_format='csv',
                                                 filter_by_detection=True, export_by_identifiers=False)) == 1
        assert len(self.out.export_lines_to_file(['0009_20170523_181119_FA2806.all'], file_format='las',
                                                 filter_by_detection=True, export_by_identifiers=True)) == 6
        assert len(self.out.export_lines_to_file(['0009_20170523_181119_FA2806.all'], file_format='las',
                                                 filter_by_detection=True, export_by_identifiers=False)) == 1

        self.remove_export_file('csv_export')
        self.remove_export_file('las_export')

    def test_export_variable(self):
        self._access_processed_data()
        self.out.export_variable('multibeam', 'beampointingangle', self.multicheck)
        self.check_export(self.expected_multi, 'time,beam,beampointingangle')

        self.out.export_variable('multibeam', 'beampointingangle', self.multicheck, reduce_method='mean',
                                 zero_centered=True)
        self.check_export(self.expected_multi, 'time,beampointingangle')

        self.out.export_variable('raw navigation', 'latitude', self.navcheck)
        self.check_export(self.expected_nav, 'time,latitude')

    def test_export_dataset(self):
        self._access_processed_data()
        self.out.export_dataset('multibeam', self.multicheck)
        self.check_export(self.expected_multi, 'time,mean_acrosstrack,mean_alongtrack,altitude,mean_beampointingangle,'
                                               'corr_altitude,corr_heave,mean_corr_pointing_angle,counter,'
                                               'mean_datum_uncertainty,mean_delay,mean_depthoffset,'
                                               'median_detectioninfo,median_frequency,nadir_geohash,'
                                               'latitude,longitude,mode,modetwo,ntx,median_processing_status,'
                                               'median_qualityfactor,mean_rel_azimuth,soundspeed,mean_thu,'
                                               'mean_tiltangle,mean_traveltime,mean_tvu,median_txsector_beam,mean_x,'
                                               'mean_y,yawpitchstab,mean_z')

        self.out.export_dataset('raw navigation', self.navcheck)
        self.check_export(self.expected_nav, 'time,altitude,latitude,longitude')

    def check_export(self, expected_file: str, text: str):
        assert os.path.exists(expected_file)
        with open(expected_file) as fil:
            assert fil.readline().rstrip() == text
        os.remove(expected_file)

    def remove_export_file(self, removal_file: str):
        expected_csv = os.path.join(self.datapath, removal_file)
        if os.path.exists(expected_csv):
            shutil.rmtree(expected_csv)

    def get_orientation_vectors(self, dset='realdualhead'):
        """
        Automated test of fqpr_generation get_orientation_vectors

        Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

        No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
        though, now that I have the real pings.

        Parameters
        ----------
        dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
        """

        if dset == 'real':
            synth = load_dataset(RealFqpr())
            expected_tx = [np.array([0.6136555921172974, -0.7895255928982701, 0.008726535498373935])]
            expected_rx = [np.array([0.7834063072490661, 0.6195440454987808, -0.04939365798750035])]
        elif dset == 'realdualhead':
            synth = load_dataset(RealDualheadFqpr())
            expected_tx = [np.array([-0.8173967230596009, -0.5756459946918305, -0.022232663846213512]),
                           np.array([-0.818098137098556, -0.5749317404941526, -0.013000579640495315])]
            expected_rx = [np.array([0.5707251056249292, -0.8178104883650188, 0.07388380848347877]),
                           np.array([0.5752302545527056, -0.8157217016726686, -0.060896177270015645])]
        else:
            raise NotImplementedError('mode not recognized')

        fq = Fqpr(synth)
        fq.logger = logging.getLogger()
        fq.logger.setLevel(logging.INFO)
        fq.read_from_source()
        # dump_data/delete_futs set the workflow to either keeping everything in memory after completion (False) or writing
        #     data to disk (both are True).  Could probably condense these arguments to one argument in the future.
        fq.get_orientation_vectors(dump_data=False, initial_interp=False)

        # arrays of computed vectors
        sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
        tstmp = list(fq.intermediate_dat[sysid[0]]['orientation'].keys())[0]
        # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
        loaded_data = [fq.intermediate_dat[s]['orientation'][tstmp][0][0] for s in sysid]

        # we examine the tx vector for each sector (not beam based) and the rx vector for each sector's first beam (rx
        #     vectors change per beam, as attitude changes based on beam traveltime)
        txvecdata = [ld[0].values[0][0] for ld in loaded_data]
        rxvecdata = [ld[1].values[0][0] for ld in loaded_data]

        print('ORIENTATION {}'.format(dset))
        print([x for y in txvecdata for x in y.flatten()])
        print([x for y in rxvecdata for x in y.flatten()])

        # check for the expected tx orientation vectors
        for i in range(len(expected_tx)):
            assert expected_tx[i] == approx(txvecdata[i], 0.000001)

        # check for the expected rx orientation vectors
        for i in range(len(expected_rx)):
            assert expected_rx[i] == approx(rxvecdata[i], 0.000001)

        fq.close()
        print('Passed: get_orientation_vectors')

    def build_beam_pointing_vector(self, dset='realdualhead'):
        """
        Automated test of fqpr_generation build_beam_pointing_vector
    
        Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.
    
        No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
        though, now that I have the real pings.

        Parameters
        ----------
        dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
        """

        if dset == 'real':
            synth = load_dataset(RealFqpr())
            expected_ba = [np.array([4.697702878191307, 4.697679369354361, 4.697655798111743])]
            expected_bda = [np.array([1.209080677036444, 1.2074367547912856, 1.2057926824074374])]
        elif dset == 'realdualhead':
            synth = load_dataset(RealDualheadFqpr())
            expected_ba = [np.array([4.7144694193229295, 4.714486234983295, 4.714503034301336]),
                           np.array([4.72527541256665, 4.725306685935214, 4.725337688174256])]
            expected_bda = [np.array([1.2049043892451596, 1.20385629874863, 1.2028083855561609]),
                            np.array([0.5239366688735714, 0.5181768253459791, 0.5124169874635531])]
        else:
            raise NotImplementedError('mode not recognized')

        fq = Fqpr(synth)
        fq.logger = logging.getLogger()
        fq.logger.setLevel(logging.INFO)
        fq.read_from_source()
        fq.get_orientation_vectors(dump_data=False, initial_interp=False)
        fq.get_beam_pointing_vectors(dump_data=False)

        # arrays of computed vectors
        sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
        tstmp = list(fq.intermediate_dat[sysid[0]]['bpv'].keys())[0]
        # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
        loaded_data = [fq.intermediate_dat[s]['bpv'][tstmp][0][0] for s in sysid]

        ba_data = [ld[0].isel(time=0).values[0:3] for ld in loaded_data]
        bda_data = [ld[1].isel(time=0).values[0:3] for ld in loaded_data]

        print('BEAMPOINTING {}'.format(dset))
        print([x for y in ba_data for x in y.flatten()])
        print([x for y in bda_data for x in y.flatten()])

        # beam azimuth check
        for i in range(len(ba_data)):
            assert ba_data[i] == approx(expected_ba[i], 0.0000001)

        # beam depression angle check
        for i in range(len(bda_data)):
            assert bda_data[i] == approx(expected_bda[i], 0.0000001)

        fq.close()
        print('Passed: build_beam_pointing_vector')

    def sv_correct(self, dset='realdualhead'):
        """
        Automated test of fqpr_generation sv_correct
    
        Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.
    
        No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
        though, now that I have the real pings.
    
        Parameters
        ----------
        dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
        """

        if dset == 'real':
            synth = load_dataset(RealFqpr())
            expected_x = [np.array([-3.419, -3.406, -3.392])]
            expected_y = [np.array([-232.877, -231.562, -230.249])]
            expected_z = [np.array([91.139, 91.049, 90.955])]
        elif dset == 'realdualhead':
            synth = load_dataset(RealDualheadFqpr())
            expected_x = [np.array([0.692, 0.693, 0.693]),
                          np.array([0.567, 0.565, 0.564])]
            expected_y = [np.array([-59.992, -59.945, -59.848]),
                          np.array([-9.351, -9.215, -9.078])]
            expected_z = [np.array([18.305, 18.342, 18.359]),
                          np.array([18.861, 18.873, 18.883])]
        else:
            raise NotImplementedError('mode not recognized')

        fq = Fqpr(synth)
        fq.logger = logging.getLogger()
        fq.logger.setLevel(logging.INFO)
        fq.read_from_source()
        fq.get_orientation_vectors(dump_data=False, initial_interp=False)
        fq.get_beam_pointing_vectors(dump_data=False)
        fq.sv_correct(dump_data=False)

        # arrays of computed vectors
        sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
        tstmp = list(fq.intermediate_dat[sysid[0]]['sv_corr'].keys())[0]
        # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
        loaded_data = [fq.intermediate_dat[s]['sv_corr'][tstmp][0][0] for s in sysid]

        x_data = [ld[0].isel(time=0).values[0:3] for ld in loaded_data]
        y_data = [ld[1].isel(time=0).values[0:3] for ld in loaded_data]
        z_data = [ld[2].isel(time=0).values[0:3] for ld in loaded_data]

        print('SVCORR {}'.format(dset))
        print([x for y in x_data for x in y.flatten()])
        print([x for y in y_data for x in y.flatten()])
        print([x for y in z_data for x in y.flatten()])

        # forward offset check
        for i in range(len(x_data)):
            assert x_data[i] == approx(expected_x[i], 0.001)

        # acrosstrack offset check
        for i in range(len(y_data)):
            assert y_data[i] == approx(expected_y[i], 0.001)

        # depth offset check
        for i in range(len(z_data)):
            assert z_data[i] == approx(expected_z[i], 0.001)

        fq.close()
        print('Passed: sv_correct')

    def georef_xyz(self, dset='realdualhead'):
        """
        Automated test of fqpr_generation sv_correct
    
        Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.
    
        No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
        though, now that I have the real pings.
    
        Parameters
        ----------
        dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'
        """

        vert_ref = 'waterline'
        datum = 'NAD83'

        if dset == 'real':
            synth = load_dataset(RealFqpr())
            expected_x = [np.array([539017.745, 539018.535, 539019.322], dtype=np.float64)]
            expected_y = [np.array([5292788.295, 5292789.346, 5292790.396], dtype=np.float64)]
            expected_z = [np.array([91.789, 91.699, 91.605], dtype=np.float32)]
        elif dset == 'realdualhead':
            synth = load_dataset(RealDualheadFqpr())
            expected_x = [np.array([492984.906, 492984.867, 492984.787], dtype=np.float64),
                          np.array([492943.083, 492942.971, 492942.859], dtype=np.float64)]
            expected_y = [np.array([3365068.225, 3365068.25, 3365068.305], dtype=np.float64),
                          np.array([3365096.742, 3365096.82, 3365096.898], dtype=np.float64)]
            expected_z = [np.array([22.087, 22.124, 22.141], dtype=np.float32),
                          np.array([22.692, 22.704, 22.714], dtype=np.float32)]
        else:
            raise NotImplementedError('mode not recognized')

        fq = Fqpr(synth)
        fq.logger = logging.getLogger()
        fq.logger.setLevel(logging.INFO)
        fq.read_from_source()
        fq.get_orientation_vectors(dump_data=False, initial_interp=False)
        fq.get_beam_pointing_vectors(dump_data=False)
        fq.sv_correct(dump_data=False)
        fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
        fq.georef_xyz(dump_data=False)

        # arrays of computed vectors
        sysid = [rp.system_identifier for rp in fq.multibeam.raw_ping]
        tstmp = list(fq.intermediate_dat[sysid[0]]['georef'].keys())[0]
        # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
        loaded_data = [fq.intermediate_dat[s]['georef'][tstmp][0][0] for s in sysid]

        x_data = [ld[0].isel(time=0).values[0:3] for ld in loaded_data]
        y_data = [ld[1].isel(time=0).values[0:3] for ld in loaded_data]
        z_data = [ld[2].isel(time=0).values[0:3] for ld in loaded_data]

        print('GEOREF {}'.format(dset))
        print([x for y in x_data for x in y.flatten()])
        print([x for y in y_data for x in y.flatten()])
        print([x for y in z_data for x in y.flatten()])

        # easting
        for i in range(len(x_data)):
            assert x_data[i] == approx(expected_x[i], 0.001)

        # northing
        for i in range(len(y_data)):
            assert y_data[i] == approx(expected_y[i], 0.001)

        # depth
        for i in range(len(z_data)):
            assert z_data[i] == approx(expected_z[i], 0.001)

        fq.close()
        print('Passed: georef_xyz')

    # def build_georef_correct_comparison(self, dset='realdual', vert_ref='waterline', datum='NAD83'):
    #     """
    #    Generate mine/kongsberg xyz88 data set from the test dataset.
    #
    #    Will run using the 'realdualhead' dataset included in this file or a small synthetic test dataset with meaningless
    #    numbers that I've just come up with.
    #
    #    Parameters
    #    ----------
    #    dset: str, specify which dataset you want to use
    #    vert_ref: str, vertical reference, one of ['waterline', 'vessel', 'ellipse']
    #    datum: str, datum identifier, anything recognized by pyproj CRS
    #
    #    """
    #
    #     if dset == 'real':
    #         synth_dat = RealFqpr()
    #         synth = self.load_dataset(synth_dat)
    #     elif dset == 'realdual':
    #         synth_dat = RealDualheadFqpr()
    #         synth = self.load_dataset(synth_dat)
    #     else:
    #         raise NotImplementedError('mode not recognized')
    #
    #     fq = fqpr_generation.Fqpr(synth)
    #     fq.logger = logging.getLogger()
    #     fq.logger.setLevel(logging.INFO)
    #     fq.read_from_source()
    #     fq.get_orientation_vectors(dump_data=False, initial_interp=False)
    #     fq.get_beam_pointing_vectors(dump_data=False)
    #     fq.sv_correct(dump_data=False)
    #     fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
    #     fq.georef_xyz(dump_data=False)
    #
    #     secs = fq.return_sector_ids()
    #     tstmp = list(fq.intermediate_dat[secs[0]]['xyz'].keys())[0]
    #
    #     loaded_xyz_data = [fq.intermediate_dat[s]['xyz'][tstmp][0][0].result() for s in fq.return_sector_ids()]
    #     loaded_sv_data = [fq.intermediate_dat[s]['sv_corr'][tstmp][0][0].result() for s in fq.return_sector_ids()]
    #     loaded_ang_data = [np.rad2deg(fq.intermediate_dat[s]['bpv'][tstmp][0][0].result()[1]) for s in
    #                        fq.return_sector_ids()]
    #
    #     fq.intermediate_dat = {}
    #
    #     if dset == 'realdual':
    #         loaded_data = [
    #             [loaded_sv_data[i][0].values[0], loaded_sv_data[i][1].values[0], loaded_xyz_data[i][2].values[0],
    #              loaded_ang_data[i].values[0]] for i in range(int(len(loaded_xyz_data)))]
    #
    #         # apply waterline, z lever arm and z phase center offsets to get at the actual waterline rel value
    #         depth_wline_addtl = [-float(fq.multibeam.xyzrph['waterline'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_port_z'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_port_z_1'][tstmp]),
    #                              -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_port_z'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_port_z_1'][tstmp]),
    #                              -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_stbd_z'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_stbd_z_1'][tstmp]),
    #                              -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_stbd_z'][tstmp]) +
    #                              float(fq.multibeam.xyzrph['tx_stbd_z_1'][tstmp])]
    #
    #         # kongsberg angles are rel horiz, here is what I came up with to get vert rel angles (to match kluster)
    #         xyz_88_corrangle = [90 - np.array(synth_dat.xyz88_corrangle[0]),
    #                             90 - np.array(synth_dat.xyz88_corrangle[1]),
    #                             np.array(synth_dat.xyz88_corrangle[2]) - 90,
    #                             np.array(synth_dat.xyz88_corrangle[3]) - 90]
    #         xyz88_data = [[np.array(synth_dat.xyz88_alongtrack[i]), np.array(synth_dat.xyz88_acrosstrack[i]),
    #                        np.array(synth_dat.xyz88_depth[i]) + depth_wline_addtl[i],
    #                        xyz_88_corrangle[i]] for i in range(int(len(synth_dat.xyz88_depth)))]
    #
    #     elif dset == 'real':
    #         loaded_data = []
    #         for tme in [0, 1]:
    #             for secs in [[0, 2, 4], [1, 3, 5]]:
    #                 dpth = np.concatenate(
    #                     [loaded_xyz_data[secs[0]][2].values[tme][~np.isnan(loaded_xyz_data[secs[0]][2].values[tme])],
    #                      loaded_xyz_data[secs[1]][2].values[tme][~np.isnan(loaded_xyz_data[secs[1]][2].values[tme])],
    #                      loaded_xyz_data[secs[2]][2].values[tme][~np.isnan(loaded_xyz_data[secs[2]][2].values[tme])]])
    #                 along = np.concatenate(
    #                     [loaded_sv_data[secs[0]][0].values[tme][~np.isnan(loaded_sv_data[secs[0]][0].values[tme])],
    #                      loaded_sv_data[secs[1]][0].values[tme][~np.isnan(loaded_sv_data[secs[1]][0].values[tme])],
    #                      loaded_sv_data[secs[2]][0].values[tme][~np.isnan(loaded_sv_data[secs[2]][0].values[tme])]])
    #                 across = np.concatenate(
    #                     [loaded_sv_data[secs[0]][1].values[tme][~np.isnan(loaded_sv_data[secs[0]][1].values[tme])],
    #                      loaded_sv_data[secs[1]][1].values[tme][~np.isnan(loaded_sv_data[secs[1]][1].values[tme])],
    #                      loaded_sv_data[secs[2]][1].values[tme][~np.isnan(loaded_sv_data[secs[2]][1].values[tme])]])
    #                 angle = np.concatenate(
    #                     [loaded_ang_data[secs[0]].values[tme][~np.isnan(loaded_ang_data[secs[0]].values[tme])],
    #                      loaded_ang_data[secs[1]].values[tme][~np.isnan(loaded_ang_data[secs[1]].values[tme])],
    #                      loaded_ang_data[secs[2]].values[tme][~np.isnan(loaded_ang_data[secs[2]].values[tme])]])
    #                 loaded_data.append([along, across, dpth, angle])
    #
    #         # in the future, include sec index to get the additional phase center offsets included here
    #         depth_wline_addtl = -float(fq.multibeam.xyzrph['waterline'][tstmp]) + float(
    #             fq.multibeam.xyzrph['tx_z'][tstmp])
    #
    #         # kongsberg angles are rel horiz, here is what I came up with to get vert rel angles (to match kluster)
    #         xyz_88_corrangle = []
    #         for ang in synth_dat.xyz88_corrangle:
    #             ang = 90 - np.array(ang)
    #             ang[np.argmin(ang):] = ang[np.argmin(ang):] * -1
    #             xyz_88_corrangle.append(ang)
    #
    #         xyz88_data = [[np.array(synth_dat.xyz88_alongtrack[i]), np.array(synth_dat.xyz88_acrosstrack[i]),
    #                        np.array(synth_dat.xyz88_depth[i]) + depth_wline_addtl, xyz_88_corrangle[i]] for i in
    #                       range(int(len(synth_dat.xyz88_depth)))]
    #
    #     else:
    #         raise NotImplementedError('only real and realdual are currently implemented')
    #
    #     fq.close()
    #     return loaded_data, xyz88_data

    # def build_kongs_comparison_plots(self, dset='realdual', vert_ref='waterline', datum='NAD83'):
    #     """
    #     Use the build_georef_correct_comparison function to get kongsberg and my created values from the test_dataset
    #     and build some comparison plots.
    #
    #     Parameters
    #     ----------
    #     dset: string identifier, identifies which of the test_datasets to use
    #     vert_ref: str, vertical reference, one of ['waterline', 'vessel', 'ellipse']
    #     datum: str, datum identifier, anything recognized by pyproj CRS
    #
    #     Returns
    #     -------
    #     plots: list, each element of the list is a tuple of the figure and all the subplots associated with that ping
    #
    #     """
    #     mine, kongsberg = self.build_georef_correct_comparison(dset=dset, vert_ref=vert_ref, datum=datum)
    #
    #     plots = []
    #
    #     if dset == 'realdual':
    #         for cnt, idxs in enumerate([[0, 2], [1, 3]]):
    #             print('Generating Ping {} plot'.format(cnt + 1))
    #
    #             fig, (z_plt, x_plt, y_plt, ang_plt) = plt.subplots(4)
    #
    #             fig.suptitle('Ping {}'.format(cnt + 1))
    #             z_plt.set_title('depth compare')
    #             x_plt.set_title('along compare')
    #             y_plt.set_title('across compare')
    #             ang_plt.set_title('angle compare')
    #
    #             z_plt.plot(np.concatenate([mine[idxs[0]][2], mine[idxs[1]][2]]), c='b')
    #             z_plt.plot(np.concatenate([kongsberg[idxs[0]][2], kongsberg[idxs[1]][2]]), c='r')
    #             x_plt.plot(np.concatenate([mine[idxs[0]][0], mine[idxs[1]][0]]), c='b')
    #             x_plt.plot(np.concatenate([kongsberg[idxs[0]][0], kongsberg[idxs[1]][0]]), c='r')
    #             y_plt.plot(np.concatenate([mine[idxs[0]][1], mine[idxs[1]][1]]), c='b')
    #             y_plt.plot(np.concatenate([kongsberg[idxs[0]][1], kongsberg[idxs[1]][1]]), c='r')
    #             ang_plt.plot(np.concatenate([mine[idxs[0]][3], mine[idxs[1]][3]]), c='b')
    #             ang_plt.plot(np.concatenate([kongsberg[idxs[0]][3], kongsberg[idxs[1]][3]]), c='r')
    #             plots.append([fig, z_plt, x_plt, y_plt, ang_plt])
    #     else:
    #         for i in range(len(mine)):
    #             print('Generating Ping {} plot'.format(i + 1))
    #
    #             fig, (z_plt, x_plt, y_plt, ang_plt) = plt.subplots(4)
    #
    #             fig.suptitle('Ping {}'.format(i + 1))
    #             z_plt.set_title('depth compare')
    #             x_plt.set_title('along compare')
    #             y_plt.set_title('across compare')
    #             ang_plt.set_title('angle compare')
    #
    #             z_plt.plot(mine[i][2], c='b')
    #             z_plt.plot(kongsberg[i][2], c='r')
    #             x_plt.plot(mine[i][0], c='b')
    #             x_plt.plot(kongsberg[i][0], c='r')
    #             y_plt.plot(mine[i][1], c='b')
    #             y_plt.plot(kongsberg[i][1], c='r')
    #             ang_plt.plot(mine[i][3], c='b')
    #             ang_plt.plot(kongsberg[i][3], c='r')
    #             plots.append([fig, z_plt, x_plt, y_plt, ang_plt])
    #
    #     return plots
