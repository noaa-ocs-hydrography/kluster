import numpy as np
import xarray as xr


def avg_correction(corrected_bscatter: xr.DataArray, beam_angles_degrees: xr.DataArray, bin_size_degree: float = 1.0,
                   reference_angle: float = 45):
    bins = np.arange(-90, 90 + bin_size_degree, bin_size_degree)
    meanvals = corrected_bscatter.groupby_bins(beam_angles_degrees, bins, right=True).mean().values
    msk = np.isnan(meanvals)
    refval_idx = np.max([np.argmin(np.abs(bins - reference_angle)), 1])
    refval = meanvals[refval_idx - 1]
    lookup = {bins[idx]: meanvals[idx] - refval if not msk[idx] else 0 for idx in range(len(meanvals))}
    return lookup


class BScatter:
    def __init__(self, raw_intensity: xr.DataArray, slant_range: xr.DataArray, surface_sound_speed: xr.DataArray,
                 beam_angle: xr.DataArray, plot_backscatter: bool, avg_lookup: dict):
        self.raw_intensity = raw_intensity
        self.slant_range = slant_range
        self.surface_sound_speed = surface_sound_speed
        self.beam_angle = beam_angle
        self.avg_lookup = avg_lookup

        # if plotting is enabled, will save the components to this dict
        self.plot_backscatter = plot_backscatter
        self.plot_components = {}

    @property
    def spherical_spreading(self):
        return 40 * np.log10(self.slant_range)

    @property
    def transmission_loss(self):
        return self.spherical_spreading + self.attenuation

    @property
    def avg_corrector(self):
        flat_angles = self.beam_angle.values.flatten()
        sort_idx = flat_angles.argsort()
        avg_angle, avg_value = list(self.avg_lookup.keys()), list(self.avg_lookup.values())
        bins = avg_angle + [avg_angle[-1] + avg_angle[-1] - avg_angle[-2]]
        bin_idx = np.digitize(flat_angles[sort_idx], bins, right=True)
        corrector = np.array(avg_value)[bin_idx]
        revsort_idx = np.argsort(sort_idx)
        corrector = corrector[revsort_idx].reshape(self.beam_angle.shape)
        return corrector

    @property
    def fixed_gain(self):
        raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')

    @property
    def attenuation(self):
        raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')

    @property
    def tvg(self):
        raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')

    @property
    def source_level(self):
        raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')

    @property
    def area_correction(self):
        raise NotImplementedError('Expected a sonar specific backscatter class to inherit this class to process')

    def _add_plot_component(self, pc_tag: str, data):
        if self.plot_backscatter:
            try:
                self.plot_components[pc_tag] = np.nanmedian(data, axis=1)
            except np.AxisError:
                self.plot_components[pc_tag] = data

    def process(self, fixed_gain_corrected: bool = True, tvg_corrected: bool = True, source_level_corrected: bool = True,
                transmission_loss_corrected: bool = True, area_corrected: bool = True, avg_corrected: bool = False):
        out_intensity = self.raw_intensity
        self._add_plot_component('raw_intensity', out_intensity)
        if fixed_gain_corrected:
            corrector = self.fixed_gain
            out_intensity -= corrector
            self._add_plot_component('fixed_gain', corrector)
        if tvg_corrected:
            corrector = self.tvg
            out_intensity -= corrector
            self._add_plot_component('tvg', corrector)
        if source_level_corrected:
            corrector = self.source_level
            out_intensity -= corrector
            self._add_plot_component('source_level', corrector)
        if transmission_loss_corrected:
            corrector = self.transmission_loss
            out_intensity -= corrector
            self._add_plot_component('transmission_loss', corrector)
        if area_corrected:
            corrector = self.area_correction
            out_intensity -= corrector
            self._add_plot_component('area_correction', corrector)
        if avg_corrected:
            if not self.avg_lookup:
                raise ValueError('AVG Lookup table not supplied to the backscatter module!')
            corrector = self.avg_corrector
            out_intensity -= corrector
            self._add_plot_component('area_correction', corrector)


class S7kscatter(BScatter):
    def __init__(self, runtime_parameters: dict, raw_intensity: xr.DataArray, slant_range: xr.DataArray, surface_sound_speed: xr.DataArray,
                 beam_angle: xr.DataArray, plot_backscatter: bool = True, avg_lookup: dict = None):
        super().__init__(raw_intensity, slant_range, surface_sound_speed, beam_angle, plot_backscatter, avg_lookup)
        self.runtime_parameters = runtime_parameters
        self.absorption_db_m = float(self.runtime_parameters['absorption_db_km']) / 1000
        self.spreading_loss_db = float(self.runtime_parameters['spreading_loss_db'])
        self.power_selection_db_re_1micropascal = float(self.runtime_parameters['power_selection_db_re_1micropascal'])
        self.tx_beam_width = float(self.runtime_parameters['TransmitBeamWidth'])
        self.rx_beam_width = float(self.runtime_parameters['ReceiveBeamWidth'])
        self.pulse_length = float(self.runtime_parameters['tx_pulse_width_seconds'])
        self.gain_selection_db = float(self.runtime_parameters['gain_selection_db'])

    @property
    def fixed_gain(self):
        return self.gain_selection_db

    @property
    def attenuation(self):
        return 2 * self.absorption_db_m * self.slant_range

    @property
    def tvg(self):
        return (self.spreading_loss_db * np.log10(self.slant_range)) + self.attenuation

    @property
    def source_level(self):
        return self.power_selection_db_re_1micropascal

    @property
    def area_correction(self):
        area_beam_limited = self.tx_beam_width * self.rx_beam_width * (self.slant_range ** 2)
        area_pulse_limited = (self.surface_sound_speed * self.pulse_length / (2 * np.sin(np.abs(self.beam_angle)))) * (self.tx_beam_width * self.slant_range)
        return 10 * np.log10(np.minimum(area_beam_limited, area_pulse_limited))
