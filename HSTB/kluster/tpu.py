import os
import numpy as np
import xarray as xr
from typing import Union
import matplotlib.pyplot as plt


def distrib_run_calculate_tpu(dat: list):
    """
    Convenience function for mapping calculate_tpu across cluster.  Assumes that you are mapping this function with a
    list of data.

    Parameters
    ----------
    dat
        [roll, raw_beam_angles, beam_angles, acrosstrack_offset, depth_offset, soundspeed, tpu_dict, quality_factor, north_position_error,
        east_position_error, down_position_error, qf_type, vert_ref, tpu_image]

    Returns
    -------
    xr.DataArray
        total vertical uncertainty (time, beam)
    xr.DataArray
        total horizontal uncertainty (time, beam)
    """

    tvu, thu = calculate_tpu(dat[0], dat[1], dat[2], dat[3], dat[4], dat[5], dat[6], dat[7], dat[8], dat[9], dat[10],
                             dat[11], dat[12], dat[13], roll_in_degrees=True, raw_beam_angles_in_degrees=True,
                             beam_angles_in_degrees=False, qf_type=dat[14], vert_ref=dat[15], tpu_image=dat[16])
    return tvu, thu


def calculate_tpu(roll: Union[xr.DataArray, np.array], raw_beam_angles: Union[xr.DataArray, np.array],
                  beam_angles: Union[xr.DataArray, np.array], acrosstrack_offset: Union[xr.DataArray, np.array],
                  depth_offset: Union[xr.DataArray, np.array], surf_sound_speed: Union[xr.DataArray, np.array],
                  tpu_dict: dict = None, quality_factor: Union[xr.DataArray, np.array] = None,
                  north_position_error: Union[xr.DataArray, np.array] = None,
                  east_position_error: Union[xr.DataArray, np.array] = None,
                  down_position_error: Union[xr.DataArray, np.array] = None,
                  roll_error: Union[xr.DataArray, np.array] = None, pitch_error: Union[xr.DataArray, np.array] = None,
                  heading_error: Union[xr.DataArray, np.array] = None, roll_in_degrees: bool = True,
                  raw_beam_angles_in_degrees: bool = True, beam_angles_in_degrees: bool = False,
                  qf_type: str = 'ifremer', vert_ref: str = 'ellipse', tpu_image: Union[str, bool] = False):
    """
    Use the Tpu class to calculate total propagated uncertainty (horizontal and vertical) for the provided sounder
    data.  Designed to be used with Kluster.

    Parameters
    ----------
    roll
        roll value at ping time
    raw_beam_angles
        raw beam angles
    beam_angles
        corrected beam angles, for attitude and mounting angles
    acrosstrack_offset
        offset in meters to each beam in across track direction (+ STBD)
    depth_offset
        offset in meters down to each sounding (+ DOWN)
    surf_sound_speed
        surface sound speed sensor
    tpu_dict
        dictionary of options that you want to use to override the defaults in the Tpu attributes
    quality_factor
        sonar uncertainty provided as a quality factor, can either be Kongsberg std dev or Kongsberg reported Ifremer
        quality factor
    north_position_error
        pospac reported rms north position error
    east_position_error
        pospac reported rms east position error
    down_position_error
        pospac reported rms down position error
    roll_error
        pospac reported rms roll error in degrees
    pitch_error
        pospac reported rms pitch error in degrees
    heading_error
        pospac reported rms heading error in degrees
    roll_in_degrees
        whether or not the provided roll is in degrees (True) or radians (False)
    raw_beam_angles_in_degrees
        whether or not the provided raw_beam_angles are in degrees (True) or radians (False)
    beam_angles_in_degrees
        whether or not the provided beam_angles are in degrees (True) or radians (False)
    qf_type
        whether or not the provided quality factor is Ifremer ('ifremer') or Kongsberg std dev ('kongsberg')
    vert_ref
        vertical reference of the survey, one of 'ellipse' or 'tidal' or 'waterline'
    tpu_image
        either False to generate no image, or True to generate and show an image, or a string path if the image is to
        be saved directly to file

    Returns
    -------
    Union[xr.DataArray, np.array]
        total vertical uncertainty in meters for each sounding (time, beam)
    Union[xr.DataArray, np.array]
        total horizontal uncertainty in meters for each sounding (time, beam)
    """

    tp = Tpu(plot_tpu=tpu_image)
    if tpu_dict is not None:
        tp.populate_from_dict(tpu_dict)
    tp.load_from_data(roll, raw_beam_angles, beam_angles, acrosstrack_offset, depth_offset, surf_sound_speed,
                      quality_factor=quality_factor, north_position_error=north_position_error, east_position_error=east_position_error,
                      down_position_error=down_position_error, roll_error=roll_error, pitch_error=pitch_error,
                      heading_error=heading_error, roll_in_degrees=roll_in_degrees, raw_beam_angles_in_degrees=raw_beam_angles_in_degrees,
                      beam_angles_in_degrees=beam_angles_in_degrees, qf_type=qf_type)
    tvu, thu = tp.generate_total_uncertainties(vert_ref=vert_ref)
    return tvu, thu


class Tpu:
    """
    | Total propogated uncertainty - following the Rob Hare model
    | See "Depth and Position Error Budgets for Multibeam Echosounding" by Rob Hare

    Here we provide the framework of the Rob Hare model, replacing as much of the modeled uncertainty elements with
    manufacturer provided records.  Currently this class will use the following:
    - TVU (tidal) => sounder vert uncert, separation_model, dynamic_draft, waterline
    - TVU (ellipse) => sounder vert uncert, separation_model, pospac vert_position error
    - THU => sounder horizontal uncert, pospac north_position error, pospac east_position_error

    I suspect this is not a 100% complete interpretation of the Hare model.  The sounder uncertainty elements that
    rely on attitude/sound velocity (numbers 3-6 on page 67) and the horizontal elements that are not based on the
    positioning system (lines 2-4 in number 10) probably need to be included.

    All the attributes in this class are in degrees or meters.  The only inputs allowed to be in radians have
    switches in the load_from_data method.
    """

    def __init__(self, debug=False, plot_tpu=False):
        self.debug = debug
        self.plot_tpu = plot_tpu

        self.tx_to_antenna_x = 0  # offset (+ FWD) to antenna from transducer
        self.tx_to_antenna_y = 0  # offset (+ STBD) to antenna from transducer
        self.tx_to_antenna_z = 0  # offset (+ DOWN) to antenna from transducer
        self.heave = 0.05  # 1 sigma standard deviation for the heave data (meters)
        self.roll_sensor_error = 0.0005  # 1 sigma standard deviation in the roll sensor (degrees)
        self.pitch_sensor_error = 0.0005  # 1 sigma standard deviation in the pitch sensor (degrees)
        self.heading_sensor_error = 0.02  # 1 sigma standard deviation in the pitch sensor (degrees)
        self.x_offset = 0.2  # 1 sigma standard deviation in your measurement of x lever arm (meters)
        self.y_offset = 0.2  # 1 sigma standard deviation in your measurement of y lever arm (meters)
        self.z_offset = 0.2  # 1 sigma standard deviation in your measurement of z lever arm (meters)
        self.surface_sv = 0.5  # 1 sigma standard deviation in surface sv sensor (meters/second)
        self.roll_patch = 0.1  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        self.pitch_patch = 0.1  # 1 sigma standard deviation in your pitch angle patch test procedure (degrees)
        self.heading_patch = 0.5  # 1 sigma standard deviation in your heading angle patch test procedure (degrees)
        self.latency_patch = 0.0  # 1 sigma standard deviation in your latency calculation (seconds)
        self.timing_latency = 0.001  # 1 sigma standard deviation of the timing accuracy of the system (seconds)
        self.dynamic_draft = 0.1  # 1 sigma standard deviation of the dynamic draft measurement (meters)
        self.separation_model = 0.0  # 1 sigma standard deivation in the sep model (tidal, ellipsoidal, etc) (meters)
        self.waterline = 0.02  # 1 sigma standard deviation of the waterline (meters)
        self.vessel_speed = 0.1  # 1 sigma standard deviation of the vessel speed (meters/second)
        self.horizontal_positioning = 1.5  # 1 sigma standard deviation of the horizontal positioning (meters)
        self.vertical_positioning = 1.0  # 1 sigma standard deviation of the horizontal positioning (meters)

        # vectors from sensors necessary for computation
        self.kongsberg_quality_factor = None
        self.ifremer_quality_factor = None
        self.roll = None
        self.raw_beam_angles = None
        self.beam_angles = None
        self.acrosstrack_offset = None
        self.depth_offset = None
        self.surf_sound_speed = None
        self.quality_factor = None
        self.qf_type = None

        # inputs from pospac processing.  All errors here are RMS, i.e. 1 sigma
        self.north_position_error = None
        self.east_position_error = None
        self.down_position_error = None
        self.sbet_roll_error = None
        self.sbet_pitch_error = None
        self.sbet_heading_error = None

        # if plotting is enabled, will save the components to this dict
        self.plot_components = {}

    def _print_if_debug(self, msg):
        """
        Handle debug prints, too lazy to worry about logging just yet
        """
        if self.debug:
            print(msg)

    def populate_from_dict(self, tpu_dict: dict):
        """
        Populate the attribution of this class by matching with the provided dictionary of keys/values

        Parameters
        ----------
        tpu_dict
            dictionary of keys that match the attributes of this class

        """

        for ky, val in tpu_dict.items():
            self.__setattr__(ky, val)

    def load_from_data(self, roll: Union[xr.DataArray, np.array], raw_beam_angles: Union[xr.DataArray, np.array],
                       beam_angles: Union[xr.DataArray, np.array], acrosstrack_offset: Union[xr.DataArray, np.array],
                       depth_offset: Union[xr.DataArray, np.array], surf_sound_speed: Union[xr.DataArray, np.array],
                       quality_factor: Union[xr.DataArray, np.array] = None,
                       north_position_error: Union[xr.DataArray, np.array] = None,
                       east_position_error: Union[xr.DataArray, np.array] = None,
                       down_position_error: Union[xr.DataArray, np.array] = None,
                       roll_error: Union[xr.DataArray, np.array] = None,
                       pitch_error: Union[xr.DataArray, np.array] = None,
                       heading_error: Union[xr.DataArray, np.array] = None,
                       roll_in_degrees: bool = True, raw_beam_angles_in_degrees: bool = True,
                       beam_angles_in_degrees: bool = True, qf_type: str = 'ifremer'):
        """
        Load the provided data here into the class, doing any necessary conversion of units and validation

        Parameters
        ----------
        roll
            roll value at ping time
        raw_beam_angles
            raw beam angles
        beam_angles
            corrected beam angles, for attitude and mounting angles
        acrosstrack_offset
            offset in meters to each beam in across track direction (+ STBD)
        depth_offset
            offset in meters down to each sounding (+ DOWN)
        surf_sound_speed
            surface sound speed sensor (meters/second)
        quality_factor
            sonar uncertainty provided as a quality factor, can either be Kongsberg std dev or Kongsberg reported Ifremer
            quality factor
        north_position_error
            pospac reported rms north position error
        east_position_error
            pospac reported rms east position error
        down_position_error
            pospac reported rms down position error
        roll_error
            pospac reported rms roll error in degrees
        pitch_error
            pospac reported rms pitch error in degrees
        heading_error
            pospac reported rms heading error in degrees
        roll_in_degrees
            whether or not the provided roll is in degrees (True) or radians (False)
        raw_beam_angles_in_degrees
            whether or not the provided raw_beam_angles are in degrees (True) or radians (False)
        beam_angles_in_degrees
            whether or not the provided beam_angles are in degrees (True) or radians (False)
        qf_type
            whether or not the provided quality factor is Ifremer ('ifremer') or Kongsberg std dev ('kongsberg')
        """

        nms = ['roll', 'beam_angles', 'acrosstrack_offset', 'depth_offset']
        lens = []
        for cnt, dataset in enumerate([roll, beam_angles, acrosstrack_offset, depth_offset]):
            if not isinstance(dataset, (np.ndarray, np.generic, xr.DataArray)):
                raise ValueError('tpu: {} must be either a numpy array or an xarray DataArray'.format(nms[cnt]))
            lens.append(len(dataset))

        if roll_in_degrees:
            roll = np.deg2rad(roll)
        else:
            roll = roll
        if beam_angles_in_degrees:
            beam_angles = np.deg2rad(beam_angles)
        else:
            beam_angles = beam_angles
        if raw_beam_angles_in_degrees:
            raw_beam_angles = np.deg2rad(raw_beam_angles)
        else:
            raw_beam_angles = raw_beam_angles

        self.roll = roll
        self.raw_beam_angles = raw_beam_angles
        self.beam_angles = beam_angles
        self.acrosstrack_offset = acrosstrack_offset
        self.depth_offset = depth_offset
        self.surf_sound_speed = surf_sound_speed
        self.quality_factor = quality_factor
        self.qf_type = qf_type
        self.north_position_error = north_position_error
        self.east_position_error = east_position_error
        self.down_position_error = down_position_error
        if self.sbet_roll_error is not None:
            self.sbet_roll_error = np.deg2rad(roll_error)
        if self.sbet_pitch_error is not None:
            self.sbet_pitch_error = np.deg2rad(pitch_error)
        if self.sbet_heading_error is not None:
            self.sbet_heading_error = np.deg2rad(heading_error)

    def generate_total_uncertainties(self, vert_ref: str = 'ellipse', sigma: int = 2):
        """
        Build the total vertical/horizontal uncertainties from the provided data.  The vertical uncertainty calculation
        depends on the vertical reference of the survey.  Ellipse will involve the sbet vert uncertainty, Tidal will
        use the waterlevel related static uncertainty values.

        Parameters
        ----------
        vert_ref
            vertical reference of the survey, one of 'ellipse' or 'tidal'
        sigma
            specify the number of stddev you want the error to represent, sigma=2 would generate 2sigma uncertainty.

        Returns
        -------
        Union[xr.DataArray, np.array]
            total vertical uncertainty in meters for each sounding (time, beam)
        Union[xr.DataArray, np.array]
            total horizontal uncertainty in meters for each sounding (time, beam)
        """

        v_unc, h_unc = self._calculate_sonar_uncertainty()
        if self.plot_tpu:
            self.plot_components['sounder_vertical'] = np.nanmedian(v_unc, axis=1)
            self.plot_components['sounder_horizontal'] = np.nanmedian(h_unc, axis=1)
        dpth_unc = self._calculate_total_depth_uncertainty(vert_ref, v_unc)
        pos_unc = self._calculate_total_horizontal_uncertainty(h_unc)
        if self.plot_tpu:
            self.plot_components['total_vertical_uncertainty'] = np.nanmedian(dpth_unc, axis=1)
            self.plot_components['total_horizontal_uncertainty'] = np.nanmedian(pos_unc, axis=1)
            self._plot_tpu_components()
        return dpth_unc * sigma, pos_unc * sigma

    def _plot_tpu_components(self):
        """
        If the class plot_tpu is enabled, generate these plots along with the calculated values
        """

        horiz_components = ['distance_rms', 'sounder_horizontal', 'total_horizontal_uncertainty']
        vert_components = ['sounder_vertical', 'roll', 'refraction', 'down_position', 'separation_model', 'heave',
                           'dynamic_draft', 'waterline', 'total_vertical_uncertainty']
        drive_plots_to_file = isinstance(self.plot_tpu, str)

        if drive_plots_to_file:
            plt.ioff()  # turn off interactive plotting
            if os.path.isdir(self.plot_tpu):
                horiz_fname = os.path.join(self.plot_tpu, 'horizontal_tpu_sample.png')
                vert_fname = os.path.join(self.plot_tpu, 'vertical_tpu_sample.png')
            elif os.path.isfile(self.plot_tpu):
                horiz_fname = os.path.join(os.path.splitext(self.plot_tpu)[0] + '_horizontal.png')
                vert_fname = os.path.join(os.path.splitext(self.plot_tpu)[0] + '_vertical.png')

        horiz_figure = plt.figure(figsize=(12, 9))
        plt.title('horizontal_uncertainty (1sigma)')
        plt.ylabel('meters')
        plt.xlabel('ping')
        for horz in horiz_components:
            if horz in self.plot_components:
                plt.plot(self.plot_components[horz], label=horz)
        plt.legend()
        if drive_plots_to_file:
            plt.savefig(horiz_fname)

        vert_figure = plt.figure(figsize=(12, 9))
        plt.ylabel('meters')
        plt.xlabel('ping')
        plt.title('vertical_uncertainty (1sigma)')
        for vert in vert_components:
            if vert in self.plot_components:
                plt.plot(self.plot_components[vert], label=vert)
        plt.legend()
        if drive_plots_to_file:
            plt.savefig(vert_fname)

    def _calculate_roll_variance(self):
        """
        Use sbet roll error if available, otherwise rely on the scalar roll error modeled value.  Will also include
        patch test error.
        """
        rpatch = np.deg2rad(self.roll_patch)
        rsensor = np.deg2rad(self.roll_sensor_error)
        if self.sbet_roll_error is not None:
            roll_variance = (self.acrosstrack_offset ** 2) * (self.sbet_roll_error ** 2) * (rpatch ** 2)
        else:
            print('Roll_variance: falling back to static value')
            roll_variance = (self.acrosstrack_offset ** 2) * (rsensor ** 2) * (rpatch ** 2)
        return roll_variance

    def _calculate_heave_variance(self):
        """
        A lazy interpretation of the heave error equation in Hare's paper.  With our surveys being ERS mainly, this
        probably won't see much use.
        """
        hve = np.full((self.acrosstrack_offset.shape[0], 1), self.heave, dtype=np.float32)
        return hve ** 2

    def _calculate_refraction_variance(self):
        """
        Not implemented yet.  Would be something like:
        ref_var = ( (depthoffset / surfssv)**2 + acrosstrack*((tan(corrbpa)/2*surfssv)**2 + (tan(rawbpa-corrbpa)/surfssv)**2) ) * self.surface_sv**2
        """
        first_component = (self.depth_offset / self.surf_sound_speed)**2
        second_component = (np.tan(self.beam_angles) / (2 * self.surf_sound_speed)) ** 2
        third_component = ((np.tan(self.beam_angles - self.raw_beam_angles) / self.surf_sound_speed) ** 2)
        ref_var = (first_component + ((self.acrosstrack_offset ** 2) * (second_component + third_component))) * (self.surface_sv ** 2)
        return ref_var

    def _calculate_distance_variance(self):
        """
        Calculate the distance variance, the radial positioning error related to positioning system
        """
        if self.north_position_error is not None and self.east_position_error is not None:
            xy = (self.north_position_error ** 2) + (self.east_position_error ** 2)
        else:
            xy = (self.horizontal_positioning ** 2) + (self.horizontal_positioning ** 2)
        return xy

    def _calculate_antenna_to_transducer_variance(self):
        """
        Determine the horizontal error related to the antenna transducer lever arm
        """

        if self.north_position_error is not None and self.east_position_error is not None:
            xy = (self.north_position_error ** 2) + (self.east_position_error ** 2)
        else:
            xy = (self.horizontal_positioning ** 2) + (self.horizontal_positioning ** 2)
        if self.sbet_heading_error is not None:
            heading = (self.tx_to_antenna_x ** 2 + self.tx_to_antenna_y ** 2) * self.sbet_heading_error
        else:
            heading = (self.tx_to_antenna_x ** 2 + self.tx_to_antenna_y ** 2) * self.heading_sensor_error
        if self.sbet_pitch_error is not None:
            rollpitch = (self.roll_sensor_error ** 2 + self.sbet_pitch_error ** 2) * self.tx_to_antenna_z
        else:
            rollpitch = (self.roll_sensor_error ** 2 + self.pitch_sensor_error ** 2) * self.tx_to_antenna_z

        return xy + heading + rollpitch

    def _calculate_total_depth_uncertainty(self, vert_ref, v_unc):
        """
        Pick the appropriate depth uncertainty calculation based on the provided vertical reference
        """

        if vert_ref == 'ellipse':
            dpth_unc = self._total_depth_unc_ref_ellipse(v_unc)
        elif vert_ref in ['tidal', 'waterline']:
            dpth_unc = self._total_depth_unc_ref_waterlevels(v_unc)
        else:
            raise NotImplementedError('tpu: vert_ref must be one of "ellipse", "tidal", "waterline".  found: {}'.format(vert_ref))
        return dpth_unc

    def _calculate_total_horizontal_uncertainty(self, h_unc):
        """
        Calculate the total horizontal uncertainty
        """

        d_var = self._calculate_distance_variance()
        # leverarm_var = self._calculate_antenna_to_transducer_variance()
        if isinstance(d_var, float):
            d_var = np.full((h_unc.shape[0], 1), d_var, dtype=np.float32)

        if self.plot_tpu:
            self.plot_components['distance_rms'] = d_var ** 0.5
            # self.plot_components['antenna_lever_arm'] = leverarm_var ** 0.5
        # return (h_unc ** 2 + d_var + leverarm_var) ** 0.5
        return (h_unc ** 2 + d_var) ** 0.5

    def _calculate_sonar_uncertainty(self):
        """
        Calculate sounder uncertainty by selecting the appropriate method based on the quality factor type.

        All the kongsberg .all files processed with kluster will have the 'kongsberg' method, as kluster reads the
        qualityfactor from the rangeangle datagram.  There is an Ifremer datagram in the .all file, but it seems kind
        of an afterthought, so I was nervous about relying on it.

        All the kongsberg .kmall files processed have the Ifremer datagram saved in the MRZ datagram, which kluster
        reads

        Returns the total horizontal uncertainty, total vertical uncertainty
        """

        if self.quality_factor is None:
            raise NotImplementedError('tpu: You must provide sonar uncertainty, manual calculation is not supported yet')
        if self.qf_type == 'ifremer':
            v_unc, h_unc = calculate_uncertainty_ifremer(self.depth_offset, self.acrosstrack_offset, self.quality_factor)
        elif self.qf_type == 'kongsberg':
            v_unc, h_unc = calculate_uncertainty_kongsberg(self.depth_offset, self.acrosstrack_offset, self.quality_factor)
        else:
            raise NotImplementedError('tpu: Only "ifremer" and "kongsberg" quality factor types accepted currently')
        return v_unc, h_unc

    def _total_depth_measurement_error(self, v_unc, vert_ref):
        """
        Convenience function to wrap up all the depth measured error relative to the provided vertical reference
        """
        r_var = self._calculate_roll_variance()
        refract_var = self._calculate_refraction_variance()
        if self.plot_tpu:
            self.plot_components['roll'] = np.nanmedian(r_var, axis=1) ** 0.5
            self.plot_components['refraction'] = np.nanmedian(refract_var, axis=1) ** 0.5
        if vert_ref in ['tidal', 'waterline']:
            hve_var = self._calculate_heave_variance()
            self.plot_components['heave'] = hve_var ** 0.5
            downpos = 0
        elif vert_ref == 'ellipse':
            if self.down_position_error is not None:
                downpos = self.down_position_error ** 2
            else:
                downpos = np.full((v_unc.shape[0], 1), self.vertical_positioning ** 2, dtype=np.float32)
            self.plot_components['down_position'] = downpos ** 0.5
            hve_var = 0
        else:
            raise NotImplementedError('tpu: vert_ref must be one of "ellipse", "tidal", "waterline".  found: {}'.format(vert_ref))
        return (v_unc ** 2 + r_var + hve_var + downpos + refract_var) ** 0.5

    def _total_depth_unc_ref_waterlevels(self, v_unc):
        """
        total vertical uncertainty with waterlevels as the vertical reference will include the sounder uncertainty
        and all the scalar modeled values for water level related uncertainty
        """

        d_measured = self._total_depth_measurement_error(v_unc, 'tidal')
        self.separation_model = np.full((v_unc.shape[0], 1), self.separation_model, dtype=np.float32)
        self.dynamic_draft = np.full((v_unc.shape[0], 1), self.dynamic_draft, dtype=np.float32)
        self.waterline = np.full((v_unc.shape[0], 1), self.waterline, dtype=np.float32)

        if self.plot_tpu:
            self.plot_components['separation_model'] = self.separation_model
            self.plot_components['dynamic_draft'] = self.dynamic_draft
            self.plot_components['waterline'] = self.waterline
        return (d_measured + self.separation_model ** 2 + self.dynamic_draft ** 2 + self.waterline ** 2) ** 0.5

    def _total_depth_unc_ref_ellipse(self, v_unc):
        """
        Total vertical uncertainty with ellipsoid as the vertical reference just use the sounder and pospac sbet
        related uncertainty values
        """

        d_measured = self._total_depth_measurement_error(v_unc, 'ellipse')
        self.separation_model = np.full((v_unc.shape[0], 1), self.separation_model, dtype=np.float32)

        if self.plot_tpu:
            self.plot_components['separation_model'] = self.separation_model
        return (d_measured ** 2 + self.separation_model ** 2) ** 0.5


def calculate_uncertainty_ifremer(depth_offset: Union[xr.DataArray, np.array],
                                  acrosstrack_offset: Union[xr.DataArray, np.array],
                                  quality_factor: Union[xr.DataArray, np.array]):
    """
    Use the kongsberg reported Ifremer quality factor to calculate horizontal and vertical sonar uncertainty.  This is
    the quality factor type reported in the MRZ datagram in .kmall files, and in the quality factor datagram in .all files.

    Currently, soundings that fail the Kongsberg qf calculation are given a qf of 0.0.  Still need a good way to deal with that

    Parameters
    ----------
    depth_offset
        2d array (time, beam) of depth offsets relative to the transducer
    acrosstrack_offset
        2d array (time, beam) of across track offsets relative to the transducer
    quality_factor
        2d array (time, beam) of Ifremer quality factor.  Assumes it is given in the kongsberg way (QF = Est(dz)/z = 100*10^-IQF)

    Returns
    -------
    Union[xr.DataArray, np.array]
        vertical uncertainty in meters for each sounding
    Union[xr.DataArray, np.array]
        horizontal uncertainty in meters for each sounding

    """
    vert_uncertainty = depth_offset * (quality_factor / 100.0)
    horiz_uncertainty = np.abs(acrosstrack_offset) * (quality_factor / 100.0)
    return vert_uncertainty, horiz_uncertainty


def calculate_uncertainty_kongsberg(depth_offset: Union[xr.DataArray, np.array],
                                    acrosstrack_offset: Union[xr.DataArray, np.array],
                                    quality_factor: Union[xr.DataArray, np.array]):
    """
    Use the kongsberg reported quality factor to calculate horizontal and vertical sonar uncertainty.  This is
    the quality factor type reported in the .all range and angle datagram.  Quality factor is describted as the Scaled
    standard deviation (sd) of the range detection divided by the detected range (dr)

    Parameters
    ----------
    depth_offset
        2d array (time, beam) of depth offsets relative to the transducer
    acrosstrack_offset
        2d array (time, beam) of across track offsets relative to the transducer
    quality_factor
        2d array (time, beam) of Kongsberg quality factor.  Assumes it is given in the kongsberg way (Quality factor = 250*sd/dr)

    Returns
    -------
    Union[xr.DataArray, np.array]
        vertical uncertainty in meters for each sounding
    Union[xr.DataArray, np.array]
        horizontal uncertainty in meters for each sounding

    """
    vert_uncertainty = depth_offset * (quality_factor / 2500)
    horiz_uncertainty = np.abs(acrosstrack_offset) * (quality_factor / 2500)
    return vert_uncertainty, horiz_uncertainty
