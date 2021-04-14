import os
import numpy as np
import xarray as xr
from dask.distributed import Client
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
from matplotlib.pyplot import Figure, Axes
from typing import Union

from HSTB.kluster.fqpr_helpers import return_directory_from_data
from HSTB.kluster.fqpr_convenience import return_data, return_surface
from HSTB.kluster.fqpr_surface import BaseSurface
from HSTB.kluster.fqpr_generation import Fqpr

pulseform_translator = {'CW': 'CW', 'FM': 'FM', 'MIX': 'MixFM_CW'}
pingmode_translator = {'VS': 'VeryShallow', 'SH': 'Shallow', 'ME': 'Medium', 'DE': 'Deep', 'VD': 'VeryDeep',
                       'ED': 'ExtraDeep',
                       'DR': 'Deeper', 'XD': 'ExtremeDeep', 'VSm': 'VeryShallowManual', 'SHm': 'ShallowManual',
                       'MEm': 'MediumManual',
                       'DEm': 'DeepManual', 'VDm': 'VeryDeepManual', 'EDm': 'ExtraDeepManual', 'DRm': 'DeeperManual',
                       'XDm': 'ExtremeDeepManual'}
pulselength_translator = {'vsCW': 'VeryShortCW', 'shCW': 'ShortCW', 'meCW': 'MediumCW', 'loCW': 'LongCW',
                          'vlCW': 'VeryLongCW',
                          'elCW': 'ExtraLongCW', 'shFM': 'ShortFM', 'loFM': 'LongFM'}


class BaseTest:
    """
    Base class for the sonar acceptance tests.  Contains some of the shared code for building the plot groups and labels
    """

    def __init__(self, fqpr, name: str):
        self.fqpr = fqpr
        self.name = name

        self.round_frequency = None
        self.frequency = None
        self.unique_freqs = None
        self.modeone = None
        self.unique_modeone = None
        self.modetwo = None
        self.unique_modetwo = None
        try:
            self.sonartype = self.fqpr.multibeam.raw_ping[0].sonartype
            self.serialnum = self.fqpr.multibeam.raw_ping[0].system_identifier
        except:
            raise ValueError('{}: Unable to read from provided fqpr instance: {}'.format(self.name, self.fqpr))

    def _build_groups(self, mode):
        """
        Build the groups that we iterate through in our SAT plots.

        Parameters
        ----------
        mode
            string identifier of the category of group we want to use

        Returns
        -------
        list
            list of categories we plot by
        np.array
            the base array we iterate off of
        str
            the plot label
        """

        if mode == 'frequency':
            groups = self.unique_freqs
            comparison = self.frequency
            lbl = 'Frequency={}Hz'
        elif mode == 'mode':
            groups = self.unique_modeone
            comparison = self.modeone
            lbl = 'mode={}'
        elif mode == 'modetwo':
            groups = self.unique_modetwo
            comparison = self.modetwo
            lbl = 'mode={}'
        else:
            raise ValueError(
                '{}: {} not supported, must be one of "frequency", "mode", "modetwo"'.format(self.name, mode))
        return groups, comparison, lbl

    def _round_frequency_ident(self):
        """
        To make the groups make sense, we allow for rounding the frequency of each sounding/sector.  This lets us plot
        '100kHz' instead of plotting '70kHz', '80kHz', etc. in different groups.  User mostly just cares about the
        broad category of frequency.

        Returns
        -------
        np.array
            rounded frequency

        """
        freq_arr = self.frequency
        if self.round_frequency:
            if self.frequency.max() > 200000:
                freq_arr = np.round(self.frequency / 100000) * 100000
            elif self.frequency.max() > 20000:
                freq_arr = np.round(self.frequency / 10000) * 10000
            else:
                freq_arr = np.round(self.frequency / 1000) * 1000
        return freq_arr.astype(np.int)

    def _translate_label(self, mode, grp, lbl):
        """
        Here we fix the plot legend categories to make them more intelligible.  Add translation for the modes, or
        frequency units.

        Parameters
        ----------
        mode
            string identifier for the mode
        grp
            the group identifier, the mode or frequency for the group
        lbl
            the eventual plot label for this plot

        Returns
        -------
        grp
            the group identifier translated
        lbl
            the eventual plot label for this plot modified
        """

        if mode == 'frequency' and self.round_frequency:
            grp = int(grp / 1000)
            lbl = 'Frequency={}kHz'
        elif grp in pingmode_translator:
            lbl = 'PingMode={}'
            grp = pingmode_translator[grp]
        elif grp in pulseform_translator:
            lbl = 'PulseForm={}'
            grp = pulseform_translator[grp]
        elif grp in pulselength_translator:
            lbl = 'PulseLength={}'
            grp = pulselength_translator[grp]
        return lbl, grp

    def _plot_depth_guidelines(self, totalmindepth: float, totalmaxdepth: float):
        """
        Plot the '2x/3x/4x/5x/6x water depth' guidelines in the extinction plot

        Parameters
        ----------
        totalmindepth
            minimum depth of all points
        totalmaxdepth
            maximum depth of all points
        """

        plt.plot([totalmindepth, totalmaxdepth], [totalmindepth, totalmaxdepth], '--', c='black', label='2X Water Depth')
        plt.plot([-totalmindepth, -totalmaxdepth], [totalmindepth, totalmaxdepth], '--', c='black')
        plt.plot([totalmindepth * 1.5, totalmaxdepth * 1.5], [totalmindepth, totalmaxdepth], '--', c='dimgray', label='3X Water Depth')
        plt.plot([-totalmindepth * 1.5, -totalmaxdepth * 1.5], [totalmindepth, totalmaxdepth], '--', c='dimgray')
        plt.plot([totalmindepth * 2, totalmaxdepth * 2], [totalmindepth, totalmaxdepth], '--', c='gray', label='4X Water Depth')
        plt.plot([-totalmindepth * 2, -totalmaxdepth * 2], [totalmindepth, totalmaxdepth], '--', c='gray')
        plt.plot([totalmindepth * 2.5, totalmaxdepth * 2.5], [totalmindepth, totalmaxdepth], '--', c='darkgray', label='5X Water Depth')
        plt.plot([-totalmindepth * 2.5, -totalmaxdepth * 2.5], [totalmindepth, totalmaxdepth], '--', c='darkgray')
        plt.plot([totalmindepth * 3, totalmaxdepth * 3], [totalmindepth, totalmaxdepth], '--', c='lightgray', label='6X Water Depth')
        plt.plot([-totalmindepth * 3, -totalmaxdepth * 3], [totalmindepth, totalmaxdepth], '--', c='lightgray')


class ExtinctionTest(BaseTest):
    """
    Plot the outermost sound velocity corrected alongtrack/depth offsets to give a sense of the maximum swath coverage
    versus depth.  Useful for operational planning where you can think to yourself, 'At 50 meters depth, I can expect
    about 4 x 50 meters coverage (4x water depth)'

    Requires processed fqpr instance, see fqpr_generation.Fqpr.
    """

    def __init__(self, fqpr, round_frequency: bool = True):
        super().__init__(fqpr, 'ExtinctionTest')
        self.round_frequency = round_frequency

        self.alongtrack = None
        self.depth = None
        self.frequency = None
        self.unique_freqs = None
        self.modeone = None
        self.unique_modeone = None
        self.modetwo = None
        self.unique_modetwo = None

        self._load_data()

    def _load_data(self):
        """
        Load and preprocess the data from the fqpr instance
        """

        print('Loading data for extinction test')
        try:
            dset = self.fqpr.subset_variables(['acrosstrack', 'depthoffset', 'frequency', 'mode', 'modetwo'],
                                              skip_subset_by_time=True)
        except KeyError:
            print(
                "Unable to find 'acrosstrack' and 'depthoffset' in given fqpr instance.  Are you sure you've run svcorrect?")
            return

        maxbeam = dset.beam.shape[0]

        self.alongtrack = np.ravel(dset.acrosstrack)
        self.depth = np.ravel(dset.depthoffset)
        self.frequency = np.ravel(dset.frequency)

        self.modeone = np.repeat(dset.mode.values[:, np.newaxis], maxbeam, axis=1)
        self.modeone = np.ravel(self.modeone)
        self.modetwo = np.repeat(dset.modetwo.values[:, np.newaxis], maxbeam, axis=1)
        self.modetwo = np.ravel(self.modetwo)

        self.frequency = self._round_frequency_ident()

        # filter out zero depth values, get inserted sometimes when empty beams are found during conversion
        idx = self.depth != 0
        self.alongtrack = self.alongtrack[idx]
        self.depth = self.depth[idx]
        self.frequency = self.frequency[idx]
        self.modeone = self.modeone[idx]
        self.modetwo = self.modetwo[idx]

        # some systems have NaN beams where the max beams vary in time, filter out the empty beams here
        self.unique_freqs = [x for x in np.unique(self.frequency) if x]
        self.unique_modeone = [x for x in np.unique(self.modeone) if x]
        self.unique_modetwo = [x for x in np.unique(self.modetwo) if x]

    def plot(self, mode: str = 'frequency', depth_bin_size: float = 1.0, filter_incomplete_swaths: bool = True):
        """
        Plot all outermost points binned in the depth dimension according to the provided size.  Each plot is organized
        by the given mode, if mode is frequency will plot once per frequency, such that the colors let you know the extinction
        at each frequency.

        Parameters
        ----------
        mode
            allowable plot mode, must be one of "frequency", "mode", "modetwo"
        depth_bin_size
            bin size in meters for the depth, size of 1 will produce one point for each meter of depth
        filter_incomplete_swaths
            If True, will only plot outermost points if the outermost port alongtrack value is negative, outermost starboard alongtrack value is positive
        """

        if self.alongtrack is None or self.depth is None:
            print('Data was not successfully loaded, ExtinctionTest must be recreated')
            return

        fig = plt.figure()
        totalmindepth = 9999
        totalmaxdepth = 0
        totalminacross = 0
        totalmaxacross = 0

        groups, comparison, lbl = self._build_groups(mode)

        colors = iter(cm.rainbow(np.linspace(0, 1, len(groups))))
        for grp in groups:
            print('Building plot for {}={}'.format(mode, grp))
            idx = comparison == grp
            atrack_by_idx = self.alongtrack[idx]
            dpth_by_idx = self.depth[idx]

            mindepth = np.int(np.min(dpth_by_idx))
            maxdepth = np.ceil(np.max(dpth_by_idx))
            minacross = np.int(np.min(atrack_by_idx))
            maxacross = np.ceil(np.max(atrack_by_idx))

            totalmindepth = min(mindepth, totalmindepth)
            totalmaxdepth = max(maxdepth, totalmaxdepth)
            totalminacross = min(minacross, totalminacross)
            totalmaxacross = max(maxacross, totalmaxacross)

            # maintain at least 5 bins just to make a halfway decent plot if they pick a bad bin size
            bins = np.linspace(mindepth, maxdepth, max(int((maxdepth - mindepth) / depth_bin_size), 5))
            dpth_indices = np.digitize(dpth_by_idx, bins) - 1

            min_across = np.array(
                [atrack_by_idx[dpth_indices == i].min() for i in range(len(bins) - 1) if i in dpth_indices])
            max_across = np.array(
                [atrack_by_idx[dpth_indices == i].max() for i in range(len(bins) - 1) if i in dpth_indices])
            dpth_vals = np.array([bins[i] for i in range(len(bins) - 1) if i in dpth_indices])

            # filter by those areas where the freq is not found on port and starboard sides
            if filter_incomplete_swaths:
                swath_filter = np.logical_and(min_across < 0, max_across > 0)
                min_across = min_across[swath_filter]
                max_across = max_across[swath_filter]
                dpth_vals = dpth_vals[swath_filter]

            c = next(colors)
            lbl, grp = self._translate_label(mode, grp, lbl)
            plt.scatter(min_across, dpth_vals, c=np.array([c]), label=lbl.format(grp))
            plt.scatter(max_across, dpth_vals, c=np.array([c]))

        self._plot_depth_guidelines(totalmindepth, totalmaxdepth)
        plt.xlim(-totalmaxacross * 1.3, totalmaxacross * 1.3)
        plt.gca().invert_yaxis()
        plt.title('{} (SN{}): {} by {}'.format(self.sonartype, self.serialnum, self.name, mode))
        plt.xlabel('AcrossTrack Distance (meters)')
        plt.ylabel('Depth (meters, +down)')
        plt.legend()
        plt.show()


class PingPeriodTest(BaseTest):
    """
    Plot the period of the pings binned by depth.  Illustrates the increase in ping period as depth increases.  Gets
    some odd results with dual swath/dual head sonar.  We try to plot the rolling mean of the ping period in these
    cases.
    """

    def __init__(self, fqpr, round_frequency: bool = True):
        self.fqpr = fqpr
        super().__init__(fqpr, 'PingPeriodTest')
        self.round_frequency = round_frequency

        self.time = None
        self.time_dif = None
        self.depth = None
        self.frequency = None
        self.unique_freqs = None
        self.modeone = None
        self.unique_modeone = None
        self.modetwo = None
        self.unique_modetwo = None

        self._load_data()

    def _load_data(self):
        """
        Load and preprocess the data from the fqpr instance
        """

        print('Loading data for ping period test')
        try:
            self.depth = self.fqpr.multibeam.raw_ping[0].depthoffset.mean(dim='beam')
        except KeyError:
            print("Unable to find 'depthoffset' in given fqpr instance.  Are you sure you've run svcorrect?")
            return

        maxbeam = self.fqpr.multibeam.raw_ping[0].beam.shape[0]

        self.time = self.fqpr.multibeam.raw_ping[0].time
        self.frequency = self.fqpr.multibeam.raw_ping[0].frequency.isel(beam=int(maxbeam / 2))
        self.modeone = self.fqpr.multibeam.raw_ping[0].mode
        self.modetwo = self.fqpr.multibeam.raw_ping[0].modetwo

        self.time_dif = np.append([0], np.diff(self.time))

        # filter out the dual ping and time differences between lines
        no_zeros = self.time_dif > 0
        no_line_gaps = self.time_dif < 3
        idx = np.logical_and(no_zeros, no_line_gaps)

        self.depth = self.depth[idx]
        self.time = self.time[idx]
        self.frequency = self.frequency[idx]
        self.modeone = self.modeone[idx]
        self.modetwo = self.modetwo[idx]
        self.time_dif = self.time_dif[idx]

        # check for alternating times, matching what we would expect with dual ping sonar
        rolling_average = False
        samplemean = np.mean(self.time_dif[1:11])
        checks = [self.time_dif[1] * 2 < samplemean, self.time_dif[2] * 2 < samplemean,
                  self.time_dif[3] * 2 < samplemean,
                  self.time_dif[4] * 2 < samplemean]
        if checks == [False, True, False, True] or checks == [True, False, True, False]:
            print('Averaging over dual swath periods...')
            rolling_average = True

        if rolling_average:
            self.time_dif = np.convolve(self.time_dif, np.ones(2) / 2, mode='valid')
            self.time_dif = np.append(self.time_dif, self.time_dif[-1])

        self.frequency = self._round_frequency_ident()

        # some systems have NaN beams where the max beams vary in time, filter out the empty beams here
        self.unique_freqs = [x for x in np.unique(self.frequency) if x]
        self.unique_modeone = [x for x in np.unique(self.modeone) if x]
        self.unique_modetwo = [x for x in np.unique(self.modetwo) if x]

    def plot(self, mode: str = 'frequency', depth_bin_size: float = 5.0):
        """
        Plot all outermost points binned in the depth dimension according to the provided size.  Each plot is organized
        by the given mode, if mode is frequency will plot once per frequency, such that the colors let you know the extinction
        at each frequency.

        Parameters
        ----------
        mode
            allowable plot mode, must be one of "frequency", "mode", "modetwo"
        depth_bin_size
            bin size in meters for the depth, size of 1 will produce one point for each meter of depth
        """

        if self.depth is None:
            print('Data was not successfully loaded, PingPeriodTest must be recreated')
            return

        fig = plt.figure()
        totalmindepth = 9999
        totalmaxdepth = 0
        totalmaxperiod = 0
        groups, comparison, lbl = self._build_groups(mode)

        colors = iter(cm.rainbow(np.linspace(0, 1, len(groups))))
        for grp in groups:
            print('Building plot for {}={}'.format(mode, grp))
            idx = comparison == grp
            dpth_by_idx = self.depth[idx]
            diff_by_idx = self.time_dif[idx]

            mindepth = np.int(np.min(dpth_by_idx))
            maxdepth = np.ceil(np.max(dpth_by_idx))

            totalmindepth = min(mindepth, totalmindepth)
            totalmaxdepth = max(maxdepth, totalmaxdepth)

            bins = np.linspace(mindepth, maxdepth, max(int((maxdepth - mindepth) / depth_bin_size), 5))
            dpth_indices = np.digitize(dpth_by_idx, bins) - 1

            diff_vals = np.array(
                [diff_by_idx[dpth_indices == i].mean() for i in range(len(bins) - 1) if i in dpth_indices])
            dpth_vals = np.array([bins[i] for i in range(len(bins) - 1) if i in dpth_indices])

            totalmaxperiod = max(totalmaxperiod, np.max(diff_vals))

            c = next(colors)
            lbl, grp = self._translate_label(mode, grp, lbl)
            plt.plot(dpth_vals, diff_vals, c=c, label=lbl.format(grp))

        # self._plot_depth_guidelines(totalmindepth, totalmaxdepth)
        plt.title('{} (SN{}): {} by {}'.format(self.sonartype, self.serialnum, self.name, mode))
        plt.xlabel('Depth (meters, +down)')
        plt.ylabel('Period of Ping (seconds)')
        plt.ylim(0, totalmaxperiod * 2)
        plt.xlim(0, totalmaxdepth * 2)
        plt.legend()
        plt.show()


def calc_order(depth: np.array):
    """
    This function takes an array of depths and returns Order 1 and Special Order values based on (b * depth).  The "a"
    factor used in the IHO standards is omitted.  This is because the depth invarient part of the uncertainty should be
    zero because the system is being evaluated against itself.

    Parameters
    ----------
    depth
        numpy array, array of depth values

    Returns
    -------
    order1_min: min value according to IHO order 1
    order1_max: max value according to IHO order 1
    specialorder_min: min value according to IHO special order
    specialorder_max: max value according to IHO special order
    """

    order1_max = 0.013 * depth.max()
    order1_min = 0.013 * depth.min()
    specialorder_max = 0.0075 * depth.max()
    specialorder_min = 0.0075 * depth.min()
    return order1_min, order1_max, specialorder_min, specialorder_max


def difference_grid_and_soundings(bs: BaseSurface, linefq: Fqpr, ang: xr.DataArray):
    """
    Given base surface instance (bs) and Fqpr instance (linefq) determine the depth difference between the nearest
    soundings to the surface node and the nodal depth.

    Parameters
    ----------
    bs
        fqpr_surface BaseSurface instance, represents the reference surface data
    linefq
        fqpr_generation Fqpr instance, represents the accuracy lines
    ang
        xarray DataArray, beam angle for soundings, added in here separately as this is not stored in the xyz_dat
        dataset so has to be reformed previously.

    Returns
    -------
    np.array
        depth difference between grid node and sounding for each sounding
    np.array
        grid depth found for each sounding
    np.array
        beam numbers for each returned sounding
    np.array
        angle values for each returned sounding
    """

    grid_loc_per_sounding_x, grid_loc_per_sounding_y = bs.calculate_grid_indices(linefq.soundings.x, linefq.soundings.y,
                                                                                 only_nearest=True)
    sounding_idx = grid_loc_per_sounding_x != -1  # soundings that are inside the grid
    grid_depth_at_loc = bs.surf[grid_loc_per_sounding_x[sounding_idx], grid_loc_per_sounding_y[sounding_idx]]
    empty_grid_idx = np.isnan(grid_depth_at_loc)

    grid_depth_at_loc = grid_depth_at_loc[~empty_grid_idx]
    soundings_depth_at_loc = linefq.soundings.z[sounding_idx][~empty_grid_idx]
    soundings_beam_at_loc = linefq.soundings.beam_idx[sounding_idx][~empty_grid_idx]
    soundings_angle_at_loc = np.rad2deg(ang[sounding_idx][~empty_grid_idx])
    depth_diff = soundings_depth_at_loc - grid_depth_at_loc

    return depth_diff.values, grid_depth_at_loc, soundings_beam_at_loc.values, soundings_angle_at_loc


def _acctest_generate_stats(soundings_xdim: np.array, depth_diff: np.array, surf_depth: np.array,
                            client: Client = None):
    """
    Build the accuracy test statistics for given beam values/angle values and the depths determined previously.

    Parameters
    ----------
    soundings_xdim
        numpy array, beam or angle values per sounding
    depth_diff
        numpy array, depth difference between grid node and sounding for each sounding
    surf_depth
        numpy array, grid depth found for each sounding
    client
        optional, dask client instance if you want to do the operation in parallel (CURRENTLY NOT SUPPORTED)

    Returns
    -------
    np.array
        mean depth difference at each beam/angle value
    np.array
        standard deviation of the soundings at each beam/angle value
    np.array
        mean depth difference at each beam/angle value as a percent of grid depth
    np.array
        standard deviation of the soundings at each beam/angle value as a percent of grid depth
    np.array
        mean surface depth at each beam/angle value
    np.array
        binned range of values for beam/angle
    """

    if client is not None:
        raise NotImplementedError('Not yet ready for dask')

    bins = np.arange(int(soundings_xdim.min()), np.ceil(soundings_xdim.max()) + 1, 1)
    bins_dig = np.sort(np.digitize(soundings_xdim, bins))
    unique_bins, u_idx = np.unique(bins_dig, return_index=True)
    percentdpth_rel_xdim_avg = []
    percentdpth_rel_xdim_stddev = []
    dpthdiff_rel_xdim_avg = []
    dpthdiff_rel_xdim_stddev = []
    mean_surf_depth = []

    if len(bins) != len(unique_bins):
        bins = bins[unique_bins]  # eliminate empty bins where there are no beams/angles
    split_diff = np.split(depth_diff, u_idx[1:])
    split_dpth = np.split(surf_depth, u_idx[1:])
    if client is None:
        for i in range(len(split_diff)):
            split_diff_chnk = split_diff[i]
            split_dpth_chunk = split_dpth[i]
            dpthdiff_rel_xdim_avg.append(split_diff_chnk.mean())
            dpthdiff_rel_xdim_stddev.append(split_diff_chnk.std())
            percentdpth_rel_xdim_avg.append(100 * (split_diff_chnk / split_dpth_chunk).mean())
            percentdpth_rel_xdim_stddev.append(100 * (split_diff_chnk / split_dpth_chunk).std())
            mean_surf_depth.append(split_dpth_chunk.mean())
    else:
        raise NotImplementedError('Not yet ready for dask')

    dpth_avg = np.array(dpthdiff_rel_xdim_avg)
    dpth_stddev = np.array(np.array(dpthdiff_rel_xdim_stddev))
    per_dpth_avg = np.array(percentdpth_rel_xdim_avg)
    per_dpth_stddev = np.array(percentdpth_rel_xdim_stddev)
    mean_surf_depth = np.array(mean_surf_depth)

    return dpth_avg, dpth_stddev, per_dpth_avg, per_dpth_stddev, mean_surf_depth, bins


def _acctest_percent_plots(arr_mean: np.array, arr_std: np.array, xdim: np.array, xdim_bins: np.array,
                           depth_diff: np.array, surf_depth: np.array, mode: str, output_pth: str,
                           depth_offset: float = None, show: bool = False):
    """
    Accuracy plots for percentage based values.  Different enough in comparison with _acctest_plots
    to need a separate function. (unified function was too messy)

    Plot the given mean/std for beam or angle based binned values as a blue line, the sounding depth difference
    as a percent of surface depth as a point cloud and the 2 standard deviation fill values.  Include horizontal
    lines for the IHO spec values.

    Parameters
    ----------
    arr_mean
        numpy array, mean depth difference at each beam/angle value as a percent of grid depth
    arr_std
        numpy array, standard deviation of the soundings at each beam/angle value as a percent of grid depth
    xdim
        numpy array, beam/angle values for each sounding
    xdim_bins
        numpy array, bins for the beam/angle values
    depth_diff
        numpy array, depth difference between grid node and sounding for each sounding
    surf_depth
        numpy array, grid depth found for each sounding
    mode
        str, one of 'beam' or 'angle', determines the plot labels
    output_pth
        str, path to where you want to save the plot
    depth_offset
        float, optional depth offset used to account for vert difference between surf and accuracy lines.  If
        None, will use the mean diff between them
    show
        bool, if false, does not show the plot (useful when you batch run this many times)

    Returns
    -------
    Figure
        Figure for plot
    Axes
        Axes for plot
    """

    if mode == 'beam':
        xlabel = 'Beam Number'
        ylabel = 'Depth Bias (% Water Depth)'
    elif mode == 'angle':
        xlabel = 'Angle (Degrees)'
        ylabel = 'Depth Bias (% Water Depth)'
    else:
        raise ValueError('Mode must be one of beam or angle')

    if depth_offset is None:
        # get average depthdiff as vert offset between surf and accuracy lines
        depth_offset = np.mean(arr_mean)

    order1 = 100 * 0.013
    special = 100 * 0.0075
    f, a = plt.subplots(1, 1)
    plus = arr_mean + 1.96 * arr_std - depth_offset
    minus = arr_mean - 1.96 * arr_std - depth_offset
    # plot the soundings
    sval = 100 * depth_diff / surf_depth
    a.scatter(xdim, sval - depth_offset, c='0.5', alpha=0.1, edgecolors='none', label='Soundings')
    # plot 2 std
    a.fill_between(xdim_bins, minus, plus, facecolor='red', interpolate=True, alpha=0.1)
    # plot mean line
    a.plot(xdim_bins, arr_mean - depth_offset, 'b', linewidth=3, label='Mean Depth')
    # set axes
    a.grid()
    a.set_xlim(xdim_bins.min(), xdim_bins.max())
    a.set_ylim(-5, 5)
    a.set_xlabel(xlabel)
    a.set_ylabel(ylabel)
    a.set_title('accuracy test: percent depth bias vs {}'.format(mode, np.round(depth_offset, 1)))
    # Order 1 line
    a.hlines(order1, xdim_bins.min(), xdim_bins.max(), colors='k', linestyles='dashed', linewidth=3, alpha=0.5,
             label='Order 1')
    a.hlines(-order1, xdim_bins.min(), xdim_bins.max(), colors='k', linestyles='dashed', linewidth=3, alpha=0.5)
    # Special Order Line
    a.hlines(special, xdim_bins.min(), xdim_bins.max(), colors='g', linestyles='dashed', linewidth=3, alpha=0.5,
             label='Special Order')
    a.hlines(-special, xdim_bins.min(), xdim_bins.max(), colors='g', linestyles='dashed', linewidth=3, alpha=0.5)
    a.legend(loc='upper left')

    f.savefig(output_pth)
    if not show:
        plt.close(f)
    return f, a


def _acctest_plots(arr_mean: np.array, arr_std: np.array, xdim: np.array, xdim_bins: np.array, depth_diff: np.array,
                   surf_depth: np.array, mode: str, output_pth: str, depth_offset: float = None, show: bool = False):
    """
    Accuracy plots for sounding/surface difference values.  Different enough in comparison with _acctest_percent_plots
    to need a separate function. (unified function was too messy)

    Plot the given mean/std for beam or angle based binned values as a blue line, the sounding depth difference
    as a percent of surface depth as a point cloud and the 2 standard deviation fill values.  Include fill for the IHO
    spec values.

    Parameters
    ----------
    arr_mean
        numpy array, mean depth difference at each beam/angle value
    arr_std
        numpy array, standard deviation of the soundings at each beam/angle value
    xdim
        numpy array, beam/angle values for each sounding
    xdim_bins
        numpy array, bins for the beam/angle values
    depth_diff
        numpy array, depth difference between grid node and sounding for each sounding
    surf_depth
        numpy array, grid depth found for each sounding
    mode
        str, one of 'beam' or 'angle', determines the plot labels
    output_pth
        str, path to where you want to save the plot
    depth_offset
        float, optional depth offset used to account for vert difference between surf and accuracy lines.  If None,
        will use the mean diff between them
    show
        bool, if false, does not show the plot (useful when you batch run this many times)

    Returns
    -------
    Figure
        Figure for plot
    Axes
        Axes for plot
    """

    if mode == 'beam':
        xlabel = 'Beam Number'
        ylabel = 'Depth Bias (meters)'
    elif mode == 'angle':
        xlabel = 'Angle (Degrees)'
        ylabel = 'Depth Bias (meters)'
    else:
        raise ValueError('Mode must be one of beam or angle')

    if depth_offset is None:
        # get average depthdiff as vert offset between surf and accuracy lines
        depth_offset = np.mean(arr_mean)

    o1_min, o1_max, so_min, so_max = calc_order(surf_depth)

    f, a = plt.subplots(1, 1)
    plus = arr_mean + 1.96 * arr_std - depth_offset
    minus = arr_mean - 1.96 * arr_std - depth_offset
    # plot the soundings
    a.scatter(xdim, depth_diff - depth_offset, c='0.5', alpha=0.1, edgecolors='none', label='Soundings')
    # plot 2 std
    a.fill_between(xdim_bins, minus, plus, facecolor='red', interpolate=True, alpha=0.1)
    # plot mean line
    a.plot(xdim_bins, arr_mean - depth_offset, 'b', linewidth=3, label='Mean Depth')
    # set axes
    a.grid()
    a.set_xlim(xdim_bins.min(), xdim_bins.max())
    a.set_ylim(-2, 2)
    a.set_xlabel(xlabel)
    a.set_ylabel(ylabel)
    a.set_title('accuracy test: depth bias vs {}'.format(mode, np.round(depth_offset, 1)))
    # Order 1 line
    a.fill_between(xdim_bins, o1_min, o1_max, facecolor='black', alpha=0.5, label='Order 1')
    a.fill_between(xdim_bins, -o1_min, -o1_max, facecolor='black', alpha=0.5)
    # Special Order Line
    a.fill_between(xdim_bins, so_min, so_max, facecolor='green', alpha=0.1, label='Special Order')
    a.fill_between(-xdim_bins, -so_min, -so_max, facecolor='green', alpha=0.1)
    a.legend(loc='upper left')

    f.savefig(output_pth)
    if not show:
        plt.close(f)
    return f, a


def accuracy_test(ref_surf_pth: Union[list, str], line_pairs: list, resolution: int = 1,
                  vert_ref: str = 'ellipse', output_directory: str = None):
    """
    Accuracy test: takes a reference surface and accuracy test lines and creates plots of depth difference between
    surface and lines for the soundings nearest the grid nodes.  Plots are by beam/by angle averages.

    Parameters
    ----------
    ref_surf_pth
        a path to a zarr store, a path to a directory of multibeam files, a list of paths to multibeam files, a path to
        a single multibeam file or a path to an existing surface.
    line_pairs
        list of either a path to a zarr store, a path to a directory of multibeam files, a list of paths to multibeam
        files or a path to a single multibeam file
    resolution
        int, resolution of the surface (if this is to make a surface)
    vert_ref
        str, vertical reference identifier ex: waterline
    output_directory
        str, optional, if None, puts the output files where the input line_pairs are.
    """

    bs = return_surface(ref_surf_pth, vert_ref, resolution)
    for lines in line_pairs:
        if output_directory is None:
            output_directory = return_directory_from_data(lines)
        print('Operating on {}'.format(lines))
        linefq = return_data(lines, vert_ref)
        pingmode = linefq.return_unique_mode()
        freq = linefq.return_rounded_freq()
        print('mode: {}, freq: {}'.format(pingmode, freq))

        u_tms = linefq.return_unique_times_across_sectors()
        # Im returning detectioninfo here as well because I keep getting some instances where detection info is all
        #   nan for a ping.  Checking against detection info will match the indexing used with xyz_dat
        dat, ids, tms = linefq.reform_2d_vars_across_sectors_at_time(['corr_pointing_angle', 'detectioninfo'], u_tms)
        line_fq_ang = dat[0].ravel()
        depth_diff, surf_depth, soundings_beam, soundings_angle = difference_grid_and_soundings(bs, linefq,
                                                                                                line_fq_ang.ravel())
        d_rel_a_avg, d_rel_a_stddev, pd_rel_a_avg, pd_rel_a_stddev, ang_mean_surf, angbins = _acctest_generate_stats(
            soundings_angle, depth_diff, surf_depth)
        d_rel_b_avg, d_rel_b_stddev, pd_rel_b_avg, pd_rel_b_stddev, beam_mean_surf, beambins = _acctest_generate_stats(
            soundings_beam, depth_diff, surf_depth)

        # for plots, we limit to max 10000 soundings, the plot chokes with more than that
        soundings_filter = int(np.ceil(len(soundings_beam) / 30000))
        filter_beam = soundings_beam[::soundings_filter]
        filter_angle = soundings_angle[::soundings_filter]
        filter_diff = depth_diff[::soundings_filter]
        filter_surf = surf_depth[::soundings_filter]

        first_fname = os.path.splitext(list(linefq.soundings.multibeam_files.keys())[0])[0]
        _acctest_percent_plots(pd_rel_b_avg, pd_rel_b_stddev, filter_beam, beambins, filter_diff, filter_surf,
                               mode='beam',
                               output_pth=os.path.join(output_directory, first_fname + '_acc_beampercent.png'))
        _acctest_percent_plots(pd_rel_a_avg, pd_rel_a_stddev, filter_angle, angbins, filter_diff, filter_surf,
                               mode='angle',
                               output_pth=os.path.join(output_directory, first_fname + '_acc_anglepercent.png'))
        _acctest_plots(d_rel_b_avg, d_rel_b_stddev, filter_beam, beambins, filter_diff, filter_surf, mode='beam',
                       output_pth=os.path.join(output_directory, first_fname + '_acc_beam.png'))
        _acctest_plots(d_rel_a_avg, d_rel_a_stddev, filter_angle, angbins, filter_diff, filter_surf, mode='angle',
                       output_pth=os.path.join(output_directory, first_fname + '_acc_angle.png'))
