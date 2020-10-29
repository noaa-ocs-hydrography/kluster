import numpy as np
import xarray as xr
from typing import Union


def distrib_run_calculate_tpu(dat: list):
    """
    Convenience function for mapping calculate_tpu across cluster.  Assumes that you are mapping this function with a
    list of data.

    Parameters
    ----------
    dat
        [roll, beam_angles, acrosstrack_offset, depth_offset, tpu_dict, quality_factor, north_position_error,
        east_position_error, down_position_error, qf_type, vert_ref]

    Returns
    -------
    xr.DataArray
        total vertical uncertainty (time, beam)
    xr.DataArray
        total horizontal uncertainty (time, beam)
    """

    tvu, thu = calculate_tpu(dat[0], dat[1], dat[2], dat[3], dat[4], dat[5], dat[6], dat[7], dat[8], roll_in_degrees=True,
                             beam_angles_in_degrees=False, qf_type=dat[9], vert_ref=dat[10])
    return tvu, thu


def calculate_tpu(roll: Union[xr.DataArray, np.array], beam_angles: Union[xr.DataArray, np.array],
                  acrosstrack_offset: Union[xr.DataArray, np.array], depth_offset: Union[xr.DataArray, np.array],
                  tpu_dict: dict = None, quality_factor: Union[xr.DataArray, np.array] = None,
                  north_position_error: Union[xr.DataArray, np.array] = None,
                  east_position_error: Union[xr.DataArray, np.array] = None,
                  down_position_error: Union[xr.DataArray, np.array] = None, roll_in_degrees: bool = True,
                  beam_angles_in_degrees: bool = False, qf_type: str = 'ifremer', vert_ref: str = 'ellipse'):
    """
    Use the Tpu class to calculate total propagated uncertainty (horizontal and vertical) for the provided sounder
    data.  Designed to be used with Kluster.

    Parameters
    ----------
    roll
        roll value at ping time
    beam_angles
        corrected beam angles
    acrosstrack_offset
        offset in meters to each beam in across track direction (+ STBD)
    depth_offset
        offset in meters down to each sounding (+ DOWN)
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
    roll_in_degrees
        whether or not the provided roll is in degrees (True) or radians (False)
    beam_angles_in_degrees
        whether or not the provided beam_angles are in degrees (True) or radians (False)
    qf_type
        whether or not the provided quality factor is Ifremer ('ifremer') or Kongsberg std dev ('kongsberg')
    vert_ref
        vertical reference of the survey, one of 'ellipse' or 'tidal'

    Returns
    -------
    Union[xr.DataArray, np.array]
        total vertical uncertainty in meters for each sounding (time, beam)
    Union[xr.DataArray, np.array]
        total horizontal uncertainty in meters for each sounding (time, beam)
    """

    tp = Tpu()
    if tpu_dict is not None:
        tp.populate_from_dict(tpu_dict)
    tp.load_from_data(roll, beam_angles, acrosstrack_offset, depth_offset, quality_factor=quality_factor,
                      north_position_error=north_position_error, east_position_error=east_position_error,
                      down_position_error=down_position_error, roll_in_degrees=roll_in_degrees,
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

    """

    def __init__(self):
        self.tx_to_antenna_x = 0  # offset (+ FWD) to antenna from transducer
        self.tx_to_antenna_y = 0  # offset (+ STBD) to antenna from transducer
        self.tx_to_antenna_z = 0  # offset (+ DOWN) to antenna from transducer
        self.heave = 0.05  # 1 sigma standard deviation for the heave data (meters)
        self.heave_percent = 0.05  # percentage of the instantaneous heave (percent)
        self.x_offset = 0.2  # 1 sigma standard deviation in your measurement of x lever arm (meters)
        self.y_offset = 0.2  # 1 sigma standard deviation in your measurement of y lever arm (meters)
        self.z_offset = 0.2  # 1 sigma standard deviation in your measurement of z lever arm (meters)
        self.svp = 2.0  # 1 sigma standard deviation in sv profile sensor (meters/second)
        self.surface_sv = 0.5  # 1 sigma standard deviation in surface sv sensor (meters/second)
        self.roll_patch = 0.1  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        self.pitch_patch = 0.1  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        self.heading_patch = 0.5  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        self.latency_patch = 0.0  # 1 sigma standard deviation in your latency calculation (seconds)
        self.timing_latency = 0.001  # 1 sigma standard deviation of the timing accuracy of the system (seconds)
        self.dynamic_draft = 0.1  # 1 sigma standard deviation of the dynamic draft measurement (meters)
        self.separation_model = 0.1  # 1 sigma standard deivation in the sep model (tidal, ellipsoidal, etc) (meters)
        self.waterline = 0.02  # 1 sigma standard deviation of the waterline (meters)
        self.vessel_speed = 0.1  # 1 sigma standard deviation of the vessel speed (meters/second)
        self.horizontal_positioning = 5  # 1 sigma standard deviation of the horizontal positioning (meters)

        # vectors from sensors necessary for computation
        self.kongsberg_quality_factor = None
        self.ifremer_quality_factor = None
        self.roll = None
        self.beam_angles = None
        self.acrosstrack_offset = None
        self.depth_offset = None
        self.quality_factor = None
        self.qf_type = None
        self.north_position_error = None
        self.east_position_error = None
        self.down_position_error = None

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

    def load_from_data(self, roll: Union[xr.DataArray, np.array], beam_angles: Union[xr.DataArray, np.array],
                       acrosstrack_offset: Union[xr.DataArray, np.array], depth_offset: Union[xr.DataArray, np.array],
                       quality_factor: Union[xr.DataArray, np.array] = None,
                       north_position_error: Union[xr.DataArray, np.array] = None,
                       east_position_error: Union[xr.DataArray, np.array] = None,
                       down_position_error: Union[xr.DataArray, np.array] = None, roll_in_degrees: bool = True,
                       beam_angles_in_degrees: bool = True, qf_type: str = 'ifremer'):
        """
        Load the provided data here into the class, doing any necessary conversion of units and validation

        Parameters
        ----------
        roll
            roll value at ping time
        beam_angles
            corrected beam angles
        acrosstrack_offset
            offset in meters to each beam in across track direction (+ STBD)
        depth_offset
            offset in meters down to each sounding (+ DOWN)
        quality_factor
            sonar uncertainty provided as a quality factor, can either be Kongsberg std dev or Kongsberg reported Ifremer
            quality factor
        north_position_error
            pospac reported rms north position error
        east_position_error
            pospac reported rms east position error
        down_position_error
            pospac reported rms down position error
        roll_in_degrees
            whether or not the provided roll is in degrees (True) or radians (False)
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

        self.roll = roll
        self.beam_angles = beam_angles
        self.acrosstrack_offset = acrosstrack_offset
        self.depth_offset = depth_offset
        self.quality_factor = quality_factor
        self.qf_type = qf_type
        self.north_position_error = north_position_error
        self.east_position_error = east_position_error
        self.down_position_error = down_position_error

    def generate_total_uncertainties(self, vert_ref: str = 'ellipse'):
        """
        Build the total vertical/horizontal uncertainties from the provided data.  The vertical uncertainty calculation
        depends on the vertical reference of the survey.  Ellipse will involve the sbet vert uncertainty, Tidal will
        use the waterlevel related static uncertainty values.

        Parameters
        ----------
        vert_ref
            vertical reference of the survey, one of 'ellipse' or 'tidal'

        Returns
        -------
        Union[xr.DataArray, np.array]
            total vertical uncertainty in meters for each sounding (time, beam)
        Union[xr.DataArray, np.array]
            total horizontal uncertainty in meters for each sounding (time, beam)
        """

        v_unc, h_unc = self._calculate_sonar_uncertainty()
        dpth_unc = self._calculate_total_depth_uncertainty(vert_ref, v_unc)
        pos_unc = self._calculate_total_horizontal_uncertainty(h_unc)
        return dpth_unc, pos_unc

    def _calculate_total_depth_uncertainty(self, vert_ref, v_unc):
        """
        Pick the appropriate depth uncertainty calculation based on the provided vertical reference
        """

        if vert_ref == 'ellipse':
            dpth_unc = self._total_depth_unc_ref_ellipse(v_unc)
        elif vert_ref == 'tidal':
            dpth_unc = self._total_depth_unc_ref_waterlevels(v_unc)
        else:
            raise NotImplementedError('tpu: vert_ref must be one of "ellipse", "tidal".  found: {}'.format(vert_ref))
        return dpth_unc

    def _calculate_total_horizontal_uncertainty(self, h_unc):
        """
        Calculate the total horizontal uncertainty
        """

        if self.north_position_error is None or self.east_position_error is None:
            raise ValueError('tpu: you must provide horizontal positioning error to calculate ellipsoidally referenced depth error')
        return (h_unc ** 2 + self.north_position_error ** 2 + self.east_position_error ** 2) ** 0.5

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

    def _total_depth_unc_ref_waterlevels(self, v_unc):
        """
        total vertical uncertainty with waterlevels as the vertical reference will include the sounder uncertainty
        and all the scalar modeled values for water level related uncertainty
        """

        return (v_unc ** 2 + self.separation_model ** 2 + self.dynamic_draft ** 2 + self.waterline ** 2) ** 0.5

    def _total_depth_unc_ref_ellipse(self, v_unc):
        """
        Total vertical uncertainty with ellipsoid as the vertical reference just use the sounder and pospac sbet
        related uncertainty values
        """

        if self.down_position_error is None:
            raise ValueError('tpu: you must provide vertical positioning error to calculate ellipsoidally referenced depth error')
        return (v_unc ** 2 + self.separation_model ** 2 + self.down_position_error ** 2) ** 0.5


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
