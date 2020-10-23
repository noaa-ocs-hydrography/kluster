import os
import numpy as np
import xarray as xr
from dask.distributed import Client
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib.pyplot import Figure, Axes
from scipy.signal import firwin, lfilter, freqz
from typing import Union

from HSTB.kluster.fqpr_helpers import return_directory_from_data, return_data, return_surface
from HSTB.kluster.xarray_helpers import interp_across_chunks
from HSTB.kluster.fqpr_surface import BaseSurface
from HSTB.kluster.fqpr_generation import Fqpr


class WobbleTest:
    """
    Implementation of 'Dynamic Motion Residuals in Swath Sonar Data: Ironing out the Creases' using Kluster processed
    multibeam data.

    http://www.omg.unb.ca/omg/papers/Lect_26_paper_ihr03.pdf

    WobbleTest will generate the high pass filtered mean depth and ping-wise slope, and build the correlation plots
    as described in the paper.

    | test = r"C:\data_dir\kluster_converted"
    | fq = fqpr_convenience.reload_data(test)
    | wb = WobbleTest(fq)
    | wb.generate_starting_data()
    | wb.plot_correlation_table()
    """

    def __init__(self, fqpr):
        self.fqpr = fqpr
        self.records = None
        self.sectors = None
        self.times = None
        self.depth = None
        self.beampointingangle = None
        self.heave = None
        self.altitude = None
        self.max_period = None
        self.vert_ref = None

        self.roll_at_ping_time = None
        self.rollrate_at_ping_time = None
        self.pitch_at_ping_time = None
        self.vert_motion_at_ping_time = None

        self.hpf_slope = None
        self.hpf_inner_slope = None
        self.hpf_port_slope = None
        self.hpf_stbd_slope = None
        self.hpf_depth = None
        self.slope_percent_deviation = None

        self.correlation_table = None

    def generate_starting_data(self, use_altitude: bool = True):
        """
        Use the depthoffset (an output from kluster svcorrect) and corr_pointing_angle (an output from kluster
        get_beam_pointing_vectors to build the highpass filtered slope and depth.

        High pass filter window is based on the maximum period across all attitude signals (self.max_period).

        Parameters
        ----------
        use_altitude
            If true, will use altitude instead of heave for the plots
        """

        print('Generating wobble data for pings')
        utms = self.fqpr.return_unique_times_across_sectors()
        varnames = ['depthoffset', 'corr_pointing_angle']
        try:
            # fqpr_generation stores ping records in separate datasets for frequency/sector/serial number.  Use
            #   reform_vars to rebuild the ping/beam arrays
            self.records, self.sectors, self.times = self.fqpr.reform_2d_vars_across_sectors_at_time(varnames, utms)
        except KeyError:
            print("Unable to find 'corr_pointing_angle' and 'depthoffset' in given fqpr instance.  Are you sure you've run svcorrect?")
            return
        varnames = ['corr_heave', 'corr_altitude']
        try:
            onedrecords, _ = self.fqpr.reform_1d_vars_across_sectors_at_time(varnames, utms)
        except KeyError:
            print("Unable to find 'corr_heave' and 'corr_altitude' in given fqpr instance.  Are you sure you've run georef?")
            return

        self.depth, self.beampointingangle = self.records[0, :, :], self.records[1, :, :]
        self.heave, self.altitude = onedrecords[0, :], onedrecords[1, :]

        self.beampointingangle = np.rad2deg(self.beampointingangle)

        # max period of all the attitude signals, drives the filter coefficients
        self.max_period = np.max([return_period_of_signal(self.fqpr.source_dat.raw_att['roll']),
                                  return_period_of_signal(self.fqpr.source_dat.raw_att['pitch']),
                                  return_period_of_signal(self.fqpr.source_dat.raw_att['heave'])])

        roll_rate = np.abs(np.diff(self.fqpr.source_dat.raw_att['roll']))/np.diff(self.fqpr.source_dat.raw_att['roll'].time)
        roll_rate = np.append(roll_rate, roll_rate[-1])  # extend to retain original shape
        roll_rate = xr.DataArray(roll_rate, coords=[self.fqpr.source_dat.raw_att['roll'].time], dims=['time'])

        # we want the roll/pitch/heave at the same times as depth/pointingangle
        self.roll_at_ping_time = interp_across_chunks(self.fqpr.source_dat.raw_att['roll'], self.times).values
        self.rollrate_at_ping_time = interp_across_chunks(roll_rate, self.times).values
        self.pitch_at_ping_time = interp_across_chunks(self.fqpr.source_dat.raw_att['pitch'], self.times).values

        self.vert_ref = self.fqpr.source_dat.raw_ping[0].vertical_reference
        if use_altitude:
            if self.vert_ref == 'ellipse':
                self.vert_motion_at_ping_time = self.altitude - self.altitude.mean()
            else:
                print("use_altitude option selected, but data was processed with vert ref: {}".format(self.vert_ref))
                return
        else:
            if self.vert_ref != 'ellipse':
                self.vert_motion_at_ping_time = self.heave
            else:
                print("use_altitude option not selected, but data was processed with vert ref: {}".format(self.vert_ref))
                return

        numtaps = 101  # filter length
        self.hpf_depth = return_high_pass_filtered_depth(self.depth, self.max_period, numtaps=numtaps)
        self.hpf_port_slope, _ = return_high_pass_filtered_slope(self.beampointingangle[:, 0:200], self.depth[:, 0:200],
                                                                 self.max_period * 6, numtaps=numtaps)
        self.hpf_stbd_slope, _ = return_high_pass_filtered_slope(self.beampointingangle[:, 200:400], self.depth[:, 200:400],
                                                                 self.max_period * 6, numtaps=numtaps)

        # want to only include 45 to -45 deg for inner slope
        try:
            strt = np.where(self.beampointingangle[0] > 45)[0][-1]
            end = np.where(self.beampointingangle[0] < -45)[0][0]
        except IndexError:  # for some reason, there isn't anything before or after 45 deg, include whole swath i guess
            print('no 45 deg points found, inner swath calculations will include the full swath')
            strt = 0
            end = self.beampointingangle.shape[-1]

        self.hpf_inner_slope, _ = return_high_pass_filtered_slope(self.beampointingangle[:, strt:end],
                                                                  self.depth[:, strt:end],
                                                                  self.max_period * 6, numtaps=numtaps)

        self.hpf_slope, self.slope_percent_deviation = return_high_pass_filtered_slope(self.beampointingangle,
                                                                                       self.depth, self.max_period * 6,
                                                                                       numtaps=numtaps)

        # filtering process, we remove the bad samples at the beginning, now remove samples from the end of the original
        #   data to match the length of the filtered data
        self.beampointingangle = self.beampointingangle[:-int(numtaps / 2)]
        self.depth = self.depth[:-int(numtaps / 2)]
        self.roll_at_ping_time = self.roll_at_ping_time[:-int(numtaps / 2)]
        self.rollrate_at_ping_time = self.rollrate_at_ping_time[:-int(numtaps / 2)]
        self.pitch_at_ping_time = self.pitch_at_ping_time[:-int(numtaps / 2)]
        self.vert_motion_at_ping_time = self.vert_motion_at_ping_time[:-int(numtaps / 2)]
        print('Initial data generation complete.')

    def _add_regression_line(self, ax: plt.subplot, x: np.array, y: np.array):
        """
        Build linear regression of x y data and plot on included ax

        Parameters
        ----------
        ax
            pyplot AxesSubplot instance
        x
            numpy array, 1d
        y
            numpy array, 1d
        """

        slopes, intercepts, stderrs, percent_deviation = linear_regression(x, y)
        ax.plot(x, slopes * x + intercepts,
                label='y = {}x + {}'.format(np.round(slopes, 6), np.round(intercepts, 6)), color='red')
        ax.legend()

    def plot_allowable_percent_deviation(self, subplot: plt.subplot = None):
        """
        Plot the correlation plot between ping time and percent deviation in the ping slope linear regression.  Percent
        deviation here is related to the standard error of the y in the regression.  Include bounds for invalid data
        in the plot as a filled in red area.  According to source paper, greater than 5% should be rejected.

        Need to include segment identification in final version for exluding greater than 5%

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.plot(self.times, self.slope_percent_deviation)
        subplot.set_ylim(self.slope_percent_deviation.min(), np.max([6, self.slope_percent_deviation.max() * 1.5]))
        subplot.axhline(5, c='red', linestyle='--')
        subplot.fill_between(self.times, 5, np.max([6, self.slope_percent_deviation.max() * 1.5]), color='red', alpha=0.2)
        subplot.set_title('Allowable Percent Deviation (red = data unusable)')
        subplot.set_xlabel('Time (s)')
        subplot.set_ylabel('% deviation')

    def plot_attitude_scaling_one(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        | This plot (as well as the trimmed plot scaling_two) deal with identifying:
        |  1. sensor scaling issues (should not be present in modern systems I think)
        |  2. rolling with imperfect sound speed (probably more likely)

        Focusing on the second one:

        When the soundspeed at the face is incorrect, roll angles will introduce steering angle error, so your
        beampointingangle will be off.  As the roll changes, the error will change, making this a dynamic error that
        is correlated with roll.

        We aren't going to make up some kind of time series bpa corrector for this error, so if you have this, i
        believe you are just screwed.

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.roll_at_ping_time, self.hpf_slope)
        subplot.set_ylim(-self.hpf_slope.max() * 4, self.hpf_slope.max() * 4)
        subplot.set_title('Attitude Scaling/Surface Sound Speed 1')
        subplot.set_xlabel('Roll (deg)')
        subplot.set_ylabel('Ping Slope (deg)')
        if add_regression:
            self._add_regression_line(subplot, self.roll_at_ping_time, self.hpf_slope)

    def plot_attitude_scaling_two(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        See attitude_scaling_one, same concept.  We have two plots for a good reason.  If you are trying to
        differentiate between 1. and 2., do the following:
        | - if scaling_one and scaling_two have your artifact, its probably a scaling issue
        | - otherwise, if the plots are different, it most likely is the sound speed one.  Inner swath and outer swath
        |   will differ as the swath is curved

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.roll_at_ping_time, self.hpf_inner_slope)
        subplot.set_ylim(-self.hpf_inner_slope.max() * 4, self.hpf_inner_slope.max() * 4)
        subplot.set_title('Attitude Scaling/Surface Sound Speed 2')
        subplot.set_xlabel('Roll (deg)')
        subplot.set_ylabel('Trimmed Slope (deg)')
        if add_regression:
            self._add_regression_line(subplot, self.roll_at_ping_time, self.hpf_inner_slope)

    def plot_attitude_latency(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        Plot to determine the attitude latency either in the POSMV initial processing or the transmission to the sonar.
        We use roll just because it is the most sensitive, most easy to notice.  It's a linear tilt we are looking for,
        so the timing latency would be equal to the slope of the regression of roll rate vs ping slope.

        If you add_regression, you can get the slope that equates to the latency adjustment

        slope of regression = ping slope (deg) / roll rate (deg/s) = latency (s)

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.rollrate_at_ping_time, self.hpf_slope)
        subplot.set_ylim(-self.hpf_slope.max() * 4, self.hpf_slope.max() * 4)
        subplot.set_title('Attitude Time Latency')
        subplot.set_xlabel('Roll Rate (deg/s)')
        subplot.set_ylabel('Ping Slope (deg)')
        if add_regression:
            self._add_regression_line(subplot, self.rollrate_at_ping_time, self.hpf_slope)

    def plot_yaw_alignment(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        Plot to determine the misalignment between roll/pitch and heading.  For us, the POSMV is a tightly
        coupled system that provides these three data streams, so there really shouldn't be any yaw misalignment with
        roll/pitch.

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.pitch_at_ping_time, self.hpf_slope)
        subplot.set_ylim(-self.hpf_slope.max() * 4, self.hpf_slope.max() * 4)
        subplot.set_title('Yaw Alignment with Reference')
        subplot.set_xlabel('Pitch (deg)')
        subplot.set_ylabel('Ping Slope (deg)')
        if add_regression:
            self._add_regression_line(subplot, self.pitch_at_ping_time, self.hpf_slope)

    def plot_x_lever_arm_error(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        Plot to find the x lever arm error, which is determined by looking at the correlation between filtered depth
        and pitch.  X lever arm error affects the induced heave by the following equation:

        Induced Heave Error = -(x_error) * sin(pitch) + (y_error) * sin(roll) * cos(pitch) + (z_error) * (1 - cos(roll) * cos(pitch))

        Or in isolating the x

        Induced Heave Error = -x_error * sin(pitch)

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.pitch_at_ping_time, self.hpf_depth)
        subplot.set_ylim(-self.hpf_depth.max() * 4, self.hpf_depth.max() * 4)
        subplot.set_title('X Sonar Offset Error')
        subplot.set_xlabel('Pitch (deg)')
        subplot.set_ylabel('Mean Depth (m)')
        if add_regression:
            self._add_regression_line(subplot, self.pitch_at_ping_time, self.hpf_depth)

    def plot_y_lever_arm_error(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        Plot to find the y lever arm error, which is determined by looking at the correlation between filtered depth
        and roll.  Y lever arm error affects the induced heave by the following equation:

        Induced Heave Error = -(x_error) * sin(pitch) + (y_error) * sin(roll) * cos(pitch) +
                              (z_error) * (1 - cos(roll) * cos(pitch))

        or in isloating the y

        Induced Heave Error (y) = y_error * sin(roll) * cos(pitch)

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.roll_at_ping_time, self.hpf_depth)
        subplot.set_ylim(-self.hpf_depth.max() * 4, self.hpf_depth.max() * 4)
        subplot.set_title('Y Sonar Offset Error')
        subplot.set_xlabel('Roll (deg)')
        subplot.set_ylabel('Mean Depth (m)')
        if add_regression:
            self._add_regression_line(subplot, self.roll_at_ping_time, self.hpf_depth)

    def plot_heave_sound_speed_one(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        Plot to find error associated with heaving through sound speed layers.  For flat face sonar that are mostly
        level while receiving, this affect should be minimal.  If I'm understanding this correctly, it's because the
        system is actively steering the beams using the surface sv sensor.  For barrel arrays, there is no active
        beam steering so there will be an error in the beam angles.

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.vert_motion_at_ping_time, self.hpf_port_slope)
        subplot.set_ylim(-self.hpf_port_slope.max() * 4, self.hpf_port_slope.max() * 4)
        subplot.set_title('Heave Through Sound Speed Layers 1')
        subplot.set_xlabel('Heave (m)')
        subplot.set_ylabel('Port Slope (deg)')
        if add_regression:
            self._add_regression_line(subplot, self.vert_motion_at_ping_time, self.hpf_port_slope)

    def plot_heave_sound_speed_two(self, subplot: plt.subplot = None, add_regression: bool = True):
        """
        See plot_heave_sound_speed_one.  There are two plots for the port/starboard swaths.  You need two as the
        swath artifact is a smile/frown, so the two plots should be mirror images if the artifact exists.  A full
        swath analysis would not show this.

        Parameters
        ----------
        subplot
            pyplot AxesSubplot instance to add to, if None will generate new instance
        add_regression
            bool, if True, will include a regression line
        """

        if subplot is None:
            subplot = plt.subplots(1)[1]
        subplot.scatter(self.vert_motion_at_ping_time, self.hpf_stbd_slope)
        subplot.set_ylim(-self.hpf_stbd_slope.max() * 4, self.hpf_stbd_slope.max() * 4)
        subplot.set_title('Heave Through Sound Speed Layers 2')
        subplot.set_xlabel('Heave (m)')
        subplot.set_ylabel('Stbd Slope (deg)')
        if add_regression:
            self._add_regression_line(subplot, self.vert_motion_at_ping_time, self.hpf_stbd_slope)

    def plot_correlation_table(self):
        """
        Use the class methods for generating each plot and build a grid of plots.  The table allows the user to
        view multiple results at once, to determine the appropriate course of action.
        """

        self.correlation_table = plt.figure(constrained_layout=True)
        gs = GridSpec(3, 3, figure=self.correlation_table)

        percent_dev = self.correlation_table.add_subplot(gs[0, 0])
        self.plot_allowable_percent_deviation(subplot=percent_dev)
        roll_v_full_slope = self.correlation_table.add_subplot(gs[0, 1])
        self.plot_attitude_scaling_one(subplot=roll_v_full_slope)
        roll_v_inner_slope = self.correlation_table.add_subplot(gs[0, 2])
        self.plot_attitude_scaling_two(subplot=roll_v_inner_slope)
        rollrate_v_full_slope = self.correlation_table.add_subplot(gs[1, 0])
        self.plot_attitude_latency(subplot=rollrate_v_full_slope)
        pitch_v_full_slope = self.correlation_table.add_subplot(gs[1, 1])
        self.plot_yaw_alignment(subplot=pitch_v_full_slope)
        pitch_v_depth = self.correlation_table.add_subplot(gs[1, 2])
        self.plot_x_lever_arm_error(subplot=pitch_v_depth)
        roll_v_depth = self.correlation_table.add_subplot(gs[2, 0])
        self.plot_y_lever_arm_error(subplot=roll_v_depth)
        heave_v_port_slope = self.correlation_table.add_subplot(gs[2, 1])
        self.plot_heave_sound_speed_one(subplot=heave_v_port_slope)
        heave_v_stbd_slope = self.correlation_table.add_subplot(gs[2, 2])
        self.plot_heave_sound_speed_two(subplot=heave_v_stbd_slope)


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


def _plot_firwin_freq_response(b: np.array, a: int = 1):
    """
    Use scipy.signal.freqz to plot the frequency response of the filter

    Parameters
    ----------
    b
        numerator of a linear filter
    a
        denominator of a linear filter
    """

    w, h = freqz(b, a)
    h_dB = 20 * np.log10(abs(h))
    plt.subplot(211)
    plt.plot(w/max(w),h_dB)
    plt.ylim(-150, 5)
    plt.ylabel('Magnitude (db)')
    plt.xlabel(r'Normalized Frequency (x$\pi$rad/sample)')
    plt.title(r'Frequency response')
    plt.subplot(212)
    h_Phase = np.unwrap(np.arctan2(np.imag(h), np.real(h)))
    plt.plot(w/max(w),h_Phase)
    plt.ylabel('Phase (radians)')
    plt.xlabel(r'Normalized Frequency (x$\pi$rad/sample)')
    plt.title(r'Phase response')
    plt.subplots_adjust(hspace=0.5)


def _plot_firwin_impulse_response(b: np.array, a: int = 1):
    """
    Use scipy.signal.lfilter to plot the impulse response of the filter

    Parameters
    ----------
    b
        numerator of a linear filter
    a
        denominator of a linear filter
    """

    l = len(b)
    impulse = np.repeat(0., l)
    impulse[0] = 1.0
    x = np.arange(0, l)
    response = lfilter(b, a, impulse)
    plt.subplot(211)
    plt.stem(x, response)
    plt.ylabel('Amplitude')
    plt.xlabel(r'n (samples)')
    plt.title(r'Impulse response')
    plt.subplot(212)
    step = np.cumsum(response)
    plt.stem(x, step)
    plt.ylabel('Amplitude')
    plt.xlabel(r'n (samples)')
    plt.title(r'Step response')
    plt.subplots_adjust(hspace=0.5)


def build_highpass_filter_coeff(cutoff_freq: float, numtaps: int = 101, show_freq_response: bool = False,
                                show_impulse_response: bool = False):
    """
    Construct a highpass filter using Scipy

    http://mpastell.com/2010/01/18/fir-with-scipy/

    Parameters
    ----------
    cutoff_freq
        cutoff frequency of the filter
    numtaps
        filter length, must be odd
    show_freq_response
        if True, will generate plot of frequency response
    show_impulse_response
        if True, will generate plot of impulse response

    Returns
    -------
    np.array
        filter coefficients
    """

    coef = firwin(numtaps, cutoff=cutoff_freq, window="hanning")
    # Spectral inversion to get highpass from lowpass
    coef = -coef
    coef[int(numtaps / 2)] = coef[int(numtaps / 2)] + 1

    if show_freq_response:
        _plot_firwin_freq_response(coef)
    if show_impulse_response:
        _plot_firwin_impulse_response(coef)
    return coef


def return_period_of_signal(sig: xr.DataArray):
    """
    Use autocorrelation to find the frequency/period of the signal.  Autocorrelation will generate a copy of the input
    signal, time-lagged, and slide it over the original until it matches.

    Parameters
    ----------
    sig
        signal values with coordinates equal to time

    Returns
    -------
    float
        max period of the input signals
    """

    # generate indices where the signal matches, return is symmetrical so only look at last half
    acf = np.correlate(sig, sig, 'full')[-len(sig):]
    # first index is always zero of course, since the two signals with zero delay will match
    inflection = np.diff(np.sign(np.diff(acf)))  # Find the second-order differences
    peaks = (inflection < 0).nonzero()[0] + 1  # Find where they are negative
    delay = peaks[acf[peaks].argmax()]  # Of those, find the index with the maximum value

    period = sig.time.values[delay] - sig.time.values[0]
    return period


def return_high_pass_filtered_depth(z: np.array, max_period: float, numtaps: int = 101):
    """
    Take in two dim array of depth (pings, beams) and return a high pass filtered mean depth for each ping.  Following
    the JHC 'Dynamic Motion Residuals...' paper which suggests using a 4 * max period cutoff.  I've found a 6 * max
    period seems to retain more of the signal that we want, but I don't really know what I'm doing here yet.

    Parameters
    ----------
    z
        numpy array (ping, beam) for depth
    max_period
        float, max period of the attitude arrays (roll, pitch, heave)
    numtaps
        filter length, must be odd

    Returns
    -------
    np.array
        HPF ping-wise mean depth
    """

    meandepth = z.mean(axis=1)
    zerocentered_meandepth = meandepth - meandepth.mean()

    # butterworth filter I never quite got to work in a way i understood
    # sos = butter(numtaps, 1 / max_period, btype='highpass', output='sos')
    # filt = sosfilt(sos, meandepth)

    coef = build_highpass_filter_coeff(1 / (max_period * 4), order=numtaps)
    filt_depth = lfilter(coef, 1.0, zerocentered_meandepth)
    # trim the bad sections from the start of the filtered depth
    trimfilt_depth = filt_depth[int(numtaps / 2):]
    return trimfilt_depth


def linear_regression(x: np.array, y: np.array):
    """
    Wrap numpy's polyfit (degree one) to also generate percent deviation from the standard error of y values

    x and y inputs can be 1d or 2d

    Parameters
    ----------
    x
        numpy array (2d ping/beam or 1d vals) for x vals
    y
        numpy array (2d ping/beam or 1d vals) for y val

    Returns
    -------
    np.array
        numpy array (ping) slope for each ping
    np.array
        numpy array (ping) y intercept from regression
    np.array
        numpy array (ping) standard deviation of the noise in z (standard error of the model)
    np.array
        numpy array (ping) percent deviation of the model
    """

    slopes = []
    intercepts = []
    stderrs = []
    percent_deviation = []

    if x.ndim == 1:
        x = np.expand_dims(x, axis=0)
        y = np.expand_dims(y, axis=0)

    for i in np.arange(x.shape[0]):
        x_val = x[i, :]
        y_val = y[i, :]
        fit = np.polyfit(x_val, y_val, deg=1)
        n = len(x_val)
        m = fit[0]
        b = fit[1]
        y_pred = m * x_val + b
        std_error = (((y_val - y_pred) ** 2).sum() / (n - 1)) ** 0.5
        slopes.append(m)
        intercepts.append(b)
        stderrs.append(std_error)
        percent_deviation.append((std_error / y_val.mean()) * 100)

    if len(slopes) == 1:
        slopes = slopes[0]
        intercepts = intercepts[0]
        stderrs = stderrs[0]
        percent_deviation = percent_deviation[0]
    else:
        slopes = np.array(slopes)
        intercepts = np.array(intercepts)
        stderrs = np.array(stderrs)
        percent_deviation = np.array(percent_deviation)

    return slopes, intercepts, stderrs, percent_deviation


def return_high_pass_filtered_slope(y: np.array, z: np.array, max_period: float, numtaps: int = 101):
    """
    Perform linear regression on acrosstrack/depth offsets to get ping slope.  High pass filter the slope to retain only
    the signal related to attitude.

    Parameters
    ----------
    y
        numpy array (ping, beam) for alongtrack offset rel RP
    z
        numpy array (ping, beam) for depth rel wline
    max_period
        float, max period of the attitude arrays (roll, pitch, heave)
    numtaps
        filter length, must be odd

    Returns
    -------
    np.array
        numpy array (ping) high pass filtered slope for each ping
    np.array
        numpy array (ping) percent deviation of the model
    """

    slopes, intercepts, stderrs, percent_deviation = linear_regression(y, z)
    zero_centered_slopes = slopes - slopes.mean()

    # never got the butterworth filter to work
    # sos = butter(numtaps, 1 / max_period, btype='highpass', output='sos')
    # filt_slopes = sosfilt(sos, zero_centered_slopes)

    coef = build_highpass_filter_coeff(1 / (max_period * 4), numtaps=numtaps)
    filt_slope = lfilter(coef, 1.0, zero_centered_slopes)
    # trim the bad sections from the start of the filtered depth
    trimfilt_slope = filt_slope[int(numtaps / 2):]

    return trimfilt_slope, percent_deviation


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
    a.hlines(order1, xdim_bins.min(), xdim_bins.max(), colors='k', linestyles='dashed', linewidth=3, alpha=0.5, label='Order 1')
    a.hlines(-order1, xdim_bins.min(), xdim_bins.max(), colors='k', linestyles='dashed', linewidth=3, alpha=0.5)
    # Special Order Line
    a.hlines(special, xdim_bins.min(), xdim_bins.max(), colors='g', linestyles='dashed', linewidth=3, alpha=0.5, label='Special Order')
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
        str, one of ['waterline', 'ellipse', 'vessel']
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
        depth_diff, surf_depth, soundings_beam, soundings_angle = difference_grid_and_soundings(bs, linefq, line_fq_ang.ravel())
        d_rel_a_avg, d_rel_a_stddev, pd_rel_a_avg, pd_rel_a_stddev, ang_mean_surf, angbins = _acctest_generate_stats(soundings_angle, depth_diff, surf_depth)
        d_rel_b_avg, d_rel_b_stddev, pd_rel_b_avg, pd_rel_b_stddev, beam_mean_surf, beambins = _acctest_generate_stats(soundings_beam, depth_diff, surf_depth)

        # for plots, we limit to max 10000 soundings, the plot chokes with more than that
        soundings_filter = int(np.ceil(len(soundings_beam) / 30000))
        filter_beam = soundings_beam[::soundings_filter]
        filter_angle = soundings_angle[::soundings_filter]
        filter_diff = depth_diff[::soundings_filter]
        filter_surf = surf_depth[::soundings_filter]

        first_fname = os.path.splitext(list(linefq.soundings.multibeam_files.keys())[0])[0]
        _acctest_percent_plots(pd_rel_b_avg, pd_rel_b_stddev, filter_beam, beambins, filter_diff, filter_surf,
                               mode='beam', output_pth=os.path.join(output_directory, first_fname + '_acc_beampercent.png'))
        _acctest_percent_plots(pd_rel_a_avg, pd_rel_a_stddev, filter_angle, angbins, filter_diff, filter_surf,
                               mode='angle', output_pth=os.path.join(output_directory, first_fname + '_acc_anglepercent.png'))
        _acctest_plots(d_rel_b_avg, d_rel_b_stddev, filter_beam, beambins, filter_diff, filter_surf, mode='beam',
                       output_pth=os.path.join(output_directory, first_fname + '_acc_beam.png'))
        _acctest_plots(d_rel_a_avg, d_rel_a_stddev, filter_angle, angbins, filter_diff, filter_surf, mode='angle',
                       output_pth=os.path.join(output_directory, first_fname + '_acc_angle.png'))
