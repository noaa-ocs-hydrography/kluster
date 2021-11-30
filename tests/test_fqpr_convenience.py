import os
import shutil
import unittest
import numpy as np
from HSTB.drivers import par3
from pytest import approx

from HSTB.kluster.fqpr_convenience import convert_multibeam, reload_data, process_multibeam


class TestFqprConvenience(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.testfile = os.path.join(os.path.dirname(__file__), 'resources', '0009_20170523_181119_FA2806.all')
        cls.expected_output = os.path.join(os.path.dirname(__file__), 'resources', 'converted')
        assert os.path.exists(cls.testfile)

        cls.out = convert_multibeam(cls.testfile)
        assert os.path.exists(cls.expected_output)
        cls.datapath = cls.out.multibeam.converted_pth

    @classmethod
    def tearDownClass(cls) -> None:
        cls.out.close()
        if os.path.exists(cls.expected_output):
            shutil.rmtree(cls.expected_output)
        proj_file = os.path.join(os.path.dirname(cls.expected_output), 'kluster_project.json')
        if os.path.exists(proj_file):
            os.remove(proj_file)

    def test_converted_data_content(self):
        out = reload_data(self.datapath)
        ad = par3.AllRead(self.testfile)
        ad.mapfile()

        # assert that they have the same number of pings
        assert out.multibeam.raw_ping[0].time.shape[0] == ad.map.getnum(78)

        # assert that there are the same number of attitude/navigation packets
        totatt = 0
        for i in range(ad.map.getnum(65)):
            rec = ad.getrecord(65, i)
            totatt += rec.data['Time'].shape[0]
        assert out.multibeam.raw_att.time.shape[0] == totatt

        ad.close()
        out.close()

    def test_return_all_profiles(self):
        pkeys, pcasts, pcasttimes, pcastlocs = self.out.multibeam.return_all_profiles()
        assert pkeys == ['profile_1495563079']
        assert pcasts == [[[0.0, 0.32, 0.5, 0.55, 0.61, 0.65, 0.67, 0.79, 0.88, 1.01, 1.04, 1.62, 2.0300000000000002, 2.43, 2.84, 3.25, 3.67,
                            4.45, 4.8500000000000005, 5.26, 6.09, 6.9, 7.71, 8.51, 8.91, 10.13, 11.8, 12.620000000000001, 16.79, 20.18, 23.93,
                            34.79, 51.15, 56.13, 60.67, 74.2, 12000.0],
                           [1489.2000732421875, 1489.2000732421875, 1488.7000732421875, 1488.300048828125, 1487.9000244140625, 1488.2000732421875,
                            1488.0, 1487.9000244140625, 1487.9000244140625, 1488.2000732421875, 1488.0999755859375, 1488.0999755859375, 1488.300048828125,
                            1488.9000244140625, 1488.5, 1487.7000732421875, 1487.2000732421875, 1486.800048828125, 1486.800048828125, 1486.5999755859375,
                            1485.7000732421875, 1485.0999755859375, 1484.800048828125, 1484.0, 1483.800048828125, 1483.7000732421875, 1483.0999755859375,
                            1482.9000244140625, 1482.9000244140625, 1481.9000244140625, 1481.300048828125, 1480.800048828125, 1480.800048828125, 1481.0,
                            1481.5, 1481.9000244140625, 1675.800048828125]]]
        assert pcasttimes == [1495563079]
        assert pcastlocs == [[47.78890945494799, -122.47711319986821]]

    def test_is_dual_head(self):
        assert not self.out.multibeam.is_dual_head()

    def test_return_tpu_parameters(self):
        params = self.out.multibeam.return_tpu_parameters('1495563079')
        assert params == {'tx_to_antenna_x': 0.0, 'tx_to_antenna_y': 0.0, 'tx_to_antenna_z': 0.0, 'heave_error': 0.05,
                          'roll_sensor_error': 0.001, 'pitch_sensor_error': 0.001, 'heading_sensor_error': 0.02, 'x_offset_error': 0.2,
                          'y_offset_error': 0.2, 'z_offset_error': 0.2, 'surface_sv_error': 0.5, 'roll_patch_error': 0.1, 'pitch_patch_error': 0.1,
                          'heading_patch_error': 0.5, 'latency_patch_error': 0.0, 'timing_latency_error': 0.001, 'separation_model_error': 0.0,
                          'waterline_error': 0.02, 'vessel_speed_error': 0.1, 'horizontal_positioning_error': 1.5, 'vertical_positioning_error': 1.0,
                          'beam_opening_angle': 1.3}

    def test_return_system_time_indexed_array(self):
        sysidx = self.out.multibeam.return_system_time_indexed_array()
        assert len(sysidx) == 1  # there is only one head for this sonar.
        tstmp_list = sysidx[0]
        assert len(tstmp_list) == 1  # there is only one installation parameter entry for this dataset
        parameters_list = tstmp_list[0]
        sonar_idxs = parameters_list[0]
        assert sonar_idxs.shape == self.out.multibeam.raw_ping[0].time.shape  # first element are the indices for the applicable data
        assert sonar_idxs.all()
        sonar_tstmp = parameters_list[1]
        assert sonar_tstmp == '1495563079'  # second element is the utc timestamp for the applicable installation parameters entry
        sonar_txrx = parameters_list[2]
        assert sonar_txrx == ['tx', 'rx']  # third element are the prefixes used to look up the installation parameters

        subset_start = float(self.out.multibeam.raw_ping[0].time[50])
        subset_end = float(self.out.multibeam.raw_ping[0].time[100])
        sysidx = self.out.multibeam.return_system_time_indexed_array(subset_time=[subset_start, subset_end])
        tstmp_list = sysidx[0]
        parameters_list = tstmp_list[0]
        sonar_idxs = parameters_list[0]
        assert np.count_nonzero(sonar_idxs) == 51

    def test_return_utm_zone_number(self):
        assert self.out.multibeam.return_utm_zone_number() == '10N'

    #TODO: Move back to generation
    def test_process_testfile(self):
        """
        Run conversion and basic processing on the test file
        """
        linename = os.path.split(self.testfile)[1]
        assert not self.out.line_is_processed(linename)
        assert self.out.return_next_unprocessed_line() == linename

        out = process_multibeam(self.out, coord_system='NAD83')
        assert out.line_is_processed(linename)
        assert out.return_next_unprocessed_line() == ''

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
        assert firstthu == approx(np.float32(8.680531), 0.0001)
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

    def test_return_total_pings(self):
        pc = self.out.return_total_pings(min_time=1495563100, max_time=1495563130)
        assert pc == 123
        pc = self.out.return_total_pings()
        assert pc == 216