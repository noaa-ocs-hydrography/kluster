import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt


def generate_avg_corrector(corrected_bscatter: xr.DataArray, beam_angles_degrees: xr.DataArray, bin_size_degree: float = 1.0,
                           reference_angle: float = 45):
    """
    Compute a new angle varying gain corrector for the provided processed backscatter dataset.  This corrector is then used during
    backscatter surface generation to remove the angle dependance in the processed backscatter.

    Parameters
    ----------
    corrected_bscatter
        processed backscatter returned from one of the Bscatter classes below, 2d array
    beam_angles_degrees
        corrected beam angles for the beams in degrees, 2d array
    bin_size_degree
        size of the bin used to generate the corrector, in degrees
    reference_angle
        angle used to determine the reference backscatter level

    Returns
    -------
    dict
        dictionary of {angles (degrees): avg correctors (dB)}
    """

    bins = np.arange(-90, 90 + bin_size_degree, bin_size_degree)
    # get the mean bscatter value in each angle bin
    meanvals = corrected_bscatter.groupby_bins(beam_angles_degrees, bins, right=True).mean().values
    msk = np.isnan(meanvals)
    # figure out which angle bin is closest to the desired reference angle to use as the reference value
    refval_idx = np.max([np.argmin(np.abs(bins - reference_angle)), 1])
    refval = meanvals[refval_idx - 1]
    # final avg correction is the difference between the angle-bin-mean value and the reference value.  Angles that are
    #  unused in the provided dataset are left as zero

    # cast to string here to allow dumping to json without serialization issues
    lookup = {str(bins[idx]): str(meanvals[idx] - refval) if not msk[idx] else 0 for idx in range(len(meanvals))}
    return lookup


def avg_correct(beam_angles_degrees: xr.DataArray, avg_corrector: dict):
    """
    Return the backscatter corrector for the provided beamangle and avg_corrector dataset

    Use by subtracting from the processed backscatter to correctly avg correct.

    Parameters
    ----------
    beam_angles_degrees
        corrected beam angles for the beams in degrees, 2d array
    avg_corrector
        dictionary of {angles (degrees): avg correctors (dB)}

    Returns
    -------
    np.ndarray
        avg backscatter corrector in dB
    """

    flat_angles = beam_angles_degrees.values.flatten()
    sort_idx = flat_angles.argsort()
    avg_angle, avg_value = list(avg_corrector.keys()), list(avg_corrector.values())
    bins = avg_angle + [avg_angle[-1] + avg_angle[-1] - avg_angle[-2]]
    bin_idx = np.digitize(flat_angles[sort_idx], bins, right=True)
    corrector = np.array(avg_value)[bin_idx]
    revsort_idx = np.argsort(sort_idx)
    corrector = corrector[revsort_idx].reshape(beam_angles_degrees.shape)
    return corrector


class BScatter:
    """
    Backscatter processing module for all sonars.  Inherit from this for your manufacturer specific model.
    """
    absorption_description = ''
    tx_beam_width_description = ''
    rx_beam_width_description = ''
    pulse_length_description = ''
    raw_intensity_description = ''

    def __init__(self, raw_intensity: xr.DataArray, slant_range: xr.DataArray, surface_sound_speed: xr.DataArray,
                 beam_angle: xr.DataArray, plot_backscatter: str = None):
        """
        Base class for all manufacturer specific backscatter classes.  The methods contained within are sonar agnostic.

        Parameters
        ----------
        raw_intensity
            see other Bscatter classes to get the origin of this
        slant_range
            kluster will calculate this using the svcorrected alongtrack/acrosstrack/depth values
        surface_sound_speed
            from converted multibeam data
        beam_angle
            the attitude/mounting angle corrected beam angles, in radians
        plot_backscatter
            if plot_backscatter is the path to a folder where you want the plots, this will save plots for you.  If
            plot_backscatter=None, will skip plots.
        """
        self.raw_intensity = raw_intensity  #
        self.slant_range = slant_range  #
        self.surface_sound_speed = surface_sound_speed  #
        self.beam_angle = beam_angle  #

        # these are required by any inheriting class
        self.absorption_db_m = None
        self.tx_beam_width = None
        self.rx_beam_width = None
        self.pulse_length = None

        self.plot_backscatter = plot_backscatter
        self.plot_components = {}

    @property
    def spherical_spreading(self):
        return 20 * np.log10(self.slant_range)

    @property
    def transmission_loss(self):
        return 2 * (self.spherical_spreading + self.attenuation)

    @property
    def fixed_gain(self):
        raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')

    @property
    def attenuation(self):
        if self.absorption_db_m is None:
            raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')
        return self.absorption_db_m * self.slant_range

    @property
    def tvg(self):
        raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')

    @property
    def area_correction(self):
        if self.tx_beam_width is None:
            raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')
        area_beam_limited = self.tx_beam_width * self.rx_beam_width * ((self.slant_range * np.pi/180) ** 2)
        area_pulse_limited = (self.surface_sound_speed * self.pulse_length * self.tx_beam_width * self.slant_range * (np.pi / 180)) / (2 * np.sin(np.abs(self.beam_angle)))
        return 10 * np.log10(np.minimum(area_beam_limited, area_pulse_limited))

    @classmethod
    def return_settings(cls, fixed_gain_corrected: bool = True, tvg_corrected: bool = True,
                        transmission_loss_corrected: bool = True, area_corrected: bool = True):
        if not cls.absorption_description:
            raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')
        setts = {'raw_intensity': cls.raw_intensity_description}
        if transmission_loss_corrected:
            setts['transmission_loss_corrector'] = f'40 * log10(svcorr slant range) + (2 * {cls.absorption_description} * svcorr slant range)'
        else:
            setts['transmission_loss_corrector'] = 'disabled by user'
        if area_corrected:
            setts['area_beam_limited'] = f'{cls.tx_beam_width_description} * {cls.rx_beam_width_description} * (svcorr slant range * PI/180)^2'
            setts['area_pulse_limited'] = f'(surface sound speed * {cls.pulse_length_description} * {cls.tx_beam_width_description} * svcorr slant range * PI/180) / (2 * sin(abs(beam angle)))'
            setts['area_correction_added'] ='10 * log10(minimum of (area_beam_limited, area_pulse_limited))'
        else:
            setts['area_correction_added'] = 'disabled by user'
        return setts

    def _add_plot_component(self, pc_tag: str, data):
        """
        Record either all beams for the first ping of the given data, or the data itself if it is a float/int
        """
        if self.plot_backscatter:
            try:
                self.plot_components[pc_tag] = data.isel(time=0).values
            except AttributeError:
                self.plot_components[pc_tag] = data

    def plot_backscatter_components(self):
        if self.plot_backscatter:
            plt.ioff()  # turn off interactive plotting
            if os.path.isdir(self.plot_backscatter):
                bscat_fname = os.path.join(self.plot_backscatter, 'backscatter_firstping_sample.png')
            elif os.path.isfile(self.plot_backscatter):
                bscat_fname = os.path.join(os.path.splitext(self.plot_backscatter)[0] + '_sample.png')
            else:
                raise ValueError(f'plot_backscatter_components: Expected either a directory path, file path, or None, got plot_backscatter={self.plot_backscatter}')
            bscat_figure = plt.figure(figsize=(12, 9))
            plt.title('backscatter components of first ping')
            plt.ylabel('dB')
            plt.xlabel('beam')
            for comp in self.plot_components.keys():
                if isinstance(self.plot_components[comp], (float, int)):
                    plt.axhline(y=self.plot_components[comp], linestyle='dashed', label=comp)
                else:
                    plt.plot(self.plot_components[comp], label=comp)
            plt.legend()
            plt.savefig(bscat_fname)
            plt.close(bscat_figure)

    def process(self, fixed_gain_corrected: bool = True, tvg_corrected: bool = True,
                transmission_loss_corrected: bool = True, area_corrected: bool = True):
        """
        Process the raw intensity for each of the given correctors.  Uses the following basic equation:

        final intensity = raw intensity - fixed gain - manufacturer tvg + calculated transmission loss - calculated area correction

        Will optionally save plots if you provided plot_backscatter.

        Parameters
        ----------
        fixed_gain_corrected
            if True, will remove fixed_gain from raw reflectivity
        tvg_corrected
            if True, will remove tvg from raw reflectivity
        transmission_loss_corrected
            if True, will add on the transmission loss corrector
        area_corrected
            if True, will add on the area correction
        """
        out_intensity = self.raw_intensity
        self._add_plot_component('raw_intensity', out_intensity)
        if fixed_gain_corrected:
            corrector = self.fixed_gain
            out_intensity -= corrector
            self._add_plot_component('fixed_gain_removed', corrector)
        if tvg_corrected:
            corrector = self.tvg
            out_intensity -= corrector
            self._add_plot_component('tvg_removed', corrector)
        if transmission_loss_corrected:
            corrector = self.transmission_loss
            out_intensity += corrector
            self._add_plot_component('transmission_loss_corrector', corrector)
        if area_corrected:
            corrector = self.area_correction
            out_intensity -= corrector
            self._add_plot_component('area_correction_added', corrector)
        self._add_plot_component('final_intensity', out_intensity)
        if self.plot_backscatter:
            self.plot_backscatter_components()
        return out_intensity


class S7kscatter(BScatter):
    """
    Backscatter processing module for the Reson .s7k format.  See the description class variables to get a sense
    of where the raw data comes from.
    """
    # see s7k datagram definition document for more information about these entries.
    absorption_description = 'runtime parameters absorption_db_km'
    spreading_description = 'runtime parameters spreading_loss_db'
    power_selection_description = 'runtime parameters power_selection_db_re_1micropascal'
    tx_beam_width_description = 'runtime parameters ProjectorBeamWidthVertical'
    rx_beam_width_description = 'runtime parameters ReceiveBeamWidth'
    pulse_length_description = 'runtime parameters tx_pulse_width_seconds'
    gain_selection_description = 'runtime parameters gain_selection_db'
    raw_intensity_description = 'RawDetection7027 Intensity'

    def __init__(self, runtime_parameters: dict, raw_intensity: xr.DataArray, slant_range: xr.DataArray, surface_sound_speed: xr.DataArray,
                 beam_angle: xr.DataArray, tx_beam_width: float, rx_beam_width: float, plot_backscatter: str = None):
        super().__init__(raw_intensity, slant_range, surface_sound_speed, beam_angle, plot_backscatter)
        # Kluster will process in 1000 ping chunks.  This runtime parameters is the entry that is nearest in time to this
        #    chunk.
        self.runtime_parameters = runtime_parameters

        # we rely on the runtime parameters for all of these variables.  Only the raw intensity is (time, beam) dim for s7k.
        self.absorption_db_m = float(self.runtime_parameters['absorption_db_km']) / 1000
        self.spreading_loss_db = float(self.runtime_parameters['spreading_loss_db'])
        self.power_selection_db_re_1micropascal = float(self.runtime_parameters['power_selection_db_re_1micropascal'])
        self.pulse_length = float(self.runtime_parameters['tx_pulse_width_seconds'])
        self.gain_selection_db = float(self.runtime_parameters['gain_selection_db'])

        self.tx_beam_width = tx_beam_width
        self.rx_beam_width = rx_beam_width

    @property
    def fixed_gain(self):
        """
        For s7k, the fixed_gain that we can remove from the raw intensity is the user set gain + power.
        """
        return self.gain_selection_db + self.power_selection_db_re_1micropascal

    @property
    def tvg(self):
        """
        For s7k, the tvg that we can remove from the raw intensity is calculated based on these other runtime parameters
        and the calculated slant range.
        """
        return 2 * ((self.spreading_loss_db * np.log10(self.slant_range)) + self.attenuation)

    @classmethod
    def return_settings(cls, fixed_gain_corrected: bool = True, tvg_corrected: bool = True,
                        transmission_loss_corrected: bool = True, area_corrected: bool = True):
        """
        Return a dict of descriptions for each corrector

        Parameters
        ----------
        fixed_gain_corrected
            if True, will return descriptor for removing fixed_gain from raw reflectivity
        tvg_corrected
            if True, will return descriptor for removing tvg from raw reflectivity
        transmission_loss_corrected
            if True, will return descriptor for adding on the transmission loss corrector
        area_corrected
            if True, will return descriptor for adding on the area correction
        """

        setts = super().return_settings(fixed_gain_corrected, tvg_corrected, transmission_loss_corrected, area_corrected)
        if fixed_gain_corrected:
            setts['fixed_gain_removed'] = f'{cls.gain_selection_description} + {cls.power_selection_description}'
        else:
            setts['fixed_gain_removed'] = 'disabled by user'
        if tvg_corrected:
            setts['tvg_removed'] = f'{cls.spreading_description} * log10(svcorr slant range) + (2 * {cls.absorption_description} * svcorr slant range)'
        else:
            setts['tvg_removed'] = 'disabled by user'
        return setts


class Allscatter(BScatter):
    """
    Backscatter processing module for the Kongsberg .all format.  See the description class variables to get a sense
    of where the raw data comes from.
    """
    # see .all datagram definition document for more information about these entries.
    absorption_description = 'runtime parameters AbsorptionCoefficent'
    tx_beam_width_description = 'runtime parameters TransmitBeamWidth'
    rx_beam_width_description = 'runtime parameters ReceiveBeamWidth'
    near_normal_description = '(BSnormal_dB - BSoblique_dB) * (1 - sqrt((range - minrange) / ((minrange / crossoverangle) - minrange)))'
    pulse_length_description = 'RangeandAngleDatagram SignalLength'
    raw_intensity_description = 'SeaBedImage89 Reflectivity'

    def __init__(self, runtime_parameters: dict, raw_intensity: xr.DataArray, slant_range: xr.DataArray, surface_sound_speed: xr.DataArray,
                 beam_angle: xr.DataArray, tx_beam_width: float, rx_beam_width: float, near_normal_corrector: xr.DataArray,
                 pulse_length: xr.DataArray, plot_backscatter: str = None):
        super().__init__(raw_intensity, slant_range, surface_sound_speed, beam_angle, plot_backscatter)
        # Kluster will process in 1000 ping chunks.  This runtime parameters is the entry that is nearest in time to this
        #    chunk.
        self.runtime_parameters = runtime_parameters
        self.absorption_db_m = float(self.runtime_parameters['AbsorptionCoefficent']) / 1000
        self.tx_beam_width = tx_beam_width  # see tx_beam_width_description
        self.rx_beam_width = rx_beam_width  # see rx_beam_width_description

        # note that the near_normal_corrector relies on a number of different variables from the .all file.  Instead
        #   of carrying all of that around in Kluster, Kluster will generate a 'nearnormal' variable on conversion.  See the
        #   par3 module nearnormal_correction function.
        self.near_normal_corrector = near_normal_corrector  # see near_normal_description
        self.pulse_length = pulse_length  # see pulse_length_description

    @property
    def fixed_gain(self):
        """
        There is a ReceiverFixedGain runtime parameter but it contains Mode2 for most sonar.  It appears to mostly be a legacy thing.
        from the docs "Receiver fixed gain setting in dB (only valid for) EM 2000, EM 1002, EM 3000, EM 3002, EM300, EM 120".

        So for this module, we leave fixed gain at 0.
        """
        return 0.0

    @property
    def tvg(self):
        """
        Kluster uses a basic model for calculating the tvg used by Kongsberg to remove from the raw reflectivity, also
        including the nearnormal corrector.
        """
        return 2 * ((20 * np.log10(self.slant_range)) + self.attenuation) - self.near_normal_corrector

    @classmethod
    def return_settings(cls, fixed_gain_corrected: bool = True, tvg_corrected: bool = True,
                        transmission_loss_corrected: bool = True, area_corrected: bool = True):
        """
        Return a dict of descriptions for each corrector

        Parameters
        ----------
        fixed_gain_corrected
            if True, will remove fixed_gain from raw reflectivity
        tvg_corrected
            if True, will remove tvg from raw reflectivity
        transmission_loss_corrected
            if True, will add on the transmission loss corrector
        area_corrected
            if True, will add on the area correction
        """

        setts = super().return_settings(fixed_gain_corrected, tvg_corrected, transmission_loss_corrected, area_corrected)
        if fixed_gain_corrected:
            setts['fixed_gain_removed'] = f'no gain for .all file'
        else:
            setts['fixed_gain_removed'] = 'disabled by user'
        if tvg_corrected:
            setts['near_normal_corrector'] = cls.near_normal_description
            setts['tvg_removed'] = f'40 * log10(svcorr slant range) + (2 * {cls.absorption_description} * svcorr slant range) - (near_normal_corrector)'
        else:
            setts['tvg_removed'] = 'disabled by user'
        return setts


class Kmallscatter(BScatter):
    """
    Backscatter processing module for the Kongsberg .kmall format.  See the description class variables to get a sense
    of where the raw data comes from.
    """
    # see kmall datagram definition document for more information about these entries.
    absorption_description = 'MRZ sounding meanAbsCoeff_dbPerkm / 1000'
    tx_beam_width_description = 'IIP sounding_size_deg (TX)'
    rx_beam_width_description = 'IIP sounding_size_deg (RX)'
    pulse_length_description = 'MRZ txSectorInfo totalSignalLength_sec'
    gain_selection_description = 'MRZ sounding sourceLevelApplied_dB + MRZ sounding receiverSensitivityApplied_dB'
    tvg_description = 'MRZ sounding TVG_dB'
    raw_intensity_description = ' MRZ sounding reflectivity2_dB'

    def __init__(self, runtime_parameters: dict, raw_intensity: xr.DataArray, slant_range: xr.DataArray, surface_sound_speed: xr.DataArray,
                 beam_angle: xr.DataArray, tx_beam_width: float, rx_beam_width: float, pulse_length: xr.DataArray, tvg: xr.DataArray,
                 fixedgain: xr.DataArray, absorption: xr.DataArray, plot_backscatter: str = None):

        super().__init__(raw_intensity, slant_range, surface_sound_speed, beam_angle, plot_backscatter)
        # Kluster will process in 1000 ping chunks.  This runtime parameters is the entry that is nearest in time to this
        #    chunk.
        self.runtime_parameters = runtime_parameters
        self.absorption_db_m = absorption / 1000
        self.fixedgain = fixedgain  # see gain_selection_description
        self.tx_beam_width = tx_beam_width  # see tx_beam_width_description
        self.rx_beam_width = rx_beam_width  # see rx_beam_width_description
        self.pulse_length = pulse_length  # see pulse_length_description
        self.tvg_arr = tvg  # see tvg_description

    @property
    def fixed_gain(self):
        """
        For kmall, the fixed_gain that we can remove from the reflectivity2 is the sourceLevel + receiverSensitivity,
        kluster will combine both of these on conversion into the 'fixedgain' variable.
        """
        return self.fixedgain

    @property
    def tvg(self):
        """
        Kluster will use the converted kmall tvg variable to represent the tvg to remove from reflectivity2
        """
        return self.tvg_arr

    @classmethod
    def return_settings(cls, fixed_gain_corrected: bool = True, tvg_corrected: bool = True,
                        transmission_loss_corrected: bool = True, area_corrected: bool = True):
        """
        Return a dict of descriptions for each corrector

        Parameters
        ----------
        fixed_gain_corrected
            if True, will remove fixed_gain from raw reflectivity
        tvg_corrected
            if True, will remove tvg from raw reflectivity
        transmission_loss_corrected
            if True, will add on the transmission loss corrector
        area_corrected
            if True, will add on the area correction
        """

        setts = super().return_settings(fixed_gain_corrected, tvg_corrected, transmission_loss_corrected,
                                        area_corrected)
        if fixed_gain_corrected:
            setts['fixed_gain_removed'] = cls.gain_selection_description
        else:
            setts['fixed_gain_removed'] = 'disabled by user'
        if tvg_corrected:
            setts['tvg_removed'] = cls.tvg_description
        else:
            setts['tvg_removed'] = 'disabled by user'
        return setts


def distrib_run_process_backscatter(worker_dat: list):
    """
    Convenience function for mapping process across cluster.  Assumes that you are mapping this function with a
    list of data.

    Parameters
    ----------
    worker_dat
        depending on sonar manufacturer, will at least include [raw_intensity, slant_range, surface_sound_speed, beam_angle]
        as the first entries.  See the init for the different backscatter classes.

    Returns
    -------
    xr.Dataarray
        xarray Dataarray of the processed backscattering strength
    """

    multibeam_extension = worker_dat[-1]
    backscatter_settings = worker_dat[-2]
    if multibeam_extension == '.all':
        bclass = Allscatter(worker_dat[0], worker_dat[1], worker_dat[2], worker_dat[3], worker_dat[4], worker_dat[5],
                            worker_dat[6], worker_dat[7], worker_dat[8], plot_backscatter=worker_dat[9])
    elif multibeam_extension == '.s7k':
        bclass = S7kscatter(worker_dat[0], worker_dat[1], worker_dat[2], worker_dat[3], worker_dat[4], worker_dat[5],
                            worker_dat[6], plot_backscatter=worker_dat[7])
    elif multibeam_extension == '.kmall':
        bclass = Kmallscatter(worker_dat[0], worker_dat[1], worker_dat[2], worker_dat[3], worker_dat[4], worker_dat[5],
                              worker_dat[6], worker_dat[7], worker_dat[8], worker_dat[9], worker_dat[10], plot_backscatter=worker_dat[11])
    else:
        raise NotImplementedError(f'distrib_run_process_backscatter: filetype {multibeam_extension} is not currently supported for backscatter processing')
    pscatter = bclass.process(**backscatter_settings)
    return pscatter


def return_backscatter_settings(multibeam_extension: str, fixed_gain_corrected: bool = True, tvg_corrected: bool = True,
                                transmission_loss_corrected: bool = True, area_corrected: bool = True):
    """
    Return the basic formula/descriptions for each corrector enabled and this given sonar file extension.

    Parameters
    ----------
    multibeam_extension
        one of [.all, .s7k, .kmall]
    fixed_gain_corrected
        if True, will return descriptor for removing fixed_gain from raw reflectivity
    tvg_corrected
        if True, will return descriptor for removing tvg from raw reflectivity
    transmission_loss_corrected
        if True, will return descriptor for adding on the transmission loss corrector
    area_corrected
        if True, will return descriptor for adding on the area correction

    Returns
    -------
    dict
        dict of processing settings for the given inputs
    """
    
    if multibeam_extension == '.s7k':
        setts = S7kscatter.return_settings(fixed_gain_corrected, tvg_corrected, transmission_loss_corrected, area_corrected)
    elif multibeam_extension == '.all':
        setts = Allscatter.return_settings(fixed_gain_corrected, tvg_corrected, transmission_loss_corrected, area_corrected)
    elif multibeam_extension == '.kmall':
        setts = Kmallscatter.return_settings(fixed_gain_corrected, tvg_corrected, transmission_loss_corrected, area_corrected)
    else:
        raise NotImplementedError(f'return_backscatter_settings: filetype {multibeam_extension} is not currently supported for backscatter processing')
    return setts
