import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from scipy.signal import firwin, lfilter, freqz
from scipy.optimize import curve_fit

from HSTB.kluster.xarray_helpers import interp_across_chunks


class WobbleTest:
    """
    Implementation of 'Dynamic Motion Residuals in Swath Sonar Data: Ironing out the Creases' using Kluster processed
    multibeam data.

    http://www.omg.unb.ca/omg/papers/Lect_26_paper_ihr03.pdf

    WobbleTest will generate the high pass filtered mean depth and ping-wise slope, and build the correlation plots
    as described in the paper.

    | test = r"C:\data_dir\kluster_converted"
    | fq = fqpr_convenience.reload_data(test)
    | fq.subset_by_time(mintime, maxtime)  # subset by the time of interest, will make this a bit faster
    | wb = WobbleTest(fq)
    | wb.generate_starting_data()
    | wb.plot_correlation_table()
    """

    def __init__(self, fqpr):
        self.fqpr = fqpr
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

        try:
            self.sonartype = self.fqpr.multibeam.raw_ping[0].sonartype
            self.serialnum = self.fqpr.multibeam.raw_ping[0].system_identifier
        except:
            raise ValueError('WobbleTest: Unable to read from provided fqpr instance: {}'.format(self.fqpr))

    def generate_starting_data(self, filter_rugged: bool = False):
        """
        Use the depthoffset (an output from kluster svcorrect) and corr_pointing_angle (an output from kluster
        get_beam_pointing_vectors to build the highpass filtered slope and depth.

        High pass filter window is based on the maximum period across all attitude signals (self.max_period).

        Parameters
        ----------
        filter_rugged
            if True, will filter out data that has percent deviation greater than 5
        """

        print('Generating wobble data for pings')
        varnames = ['depthoffset', 'corr_pointing_angle', 'corr_heave', 'corr_altitude']
        try:
            # fqpr_generation stores ping records in separate datasets for frequency/sector/serial number.  Use
            #   reform_vars to rebuild the ping/beam arrays
            dset = self.fqpr.subset_variables(varnames, skip_subset_by_time=True)
        except KeyError:
            print("Unable to find 'corr_pointing_angle' and 'depthoffset' in given fqpr instance.  Are you sure you've run svcorrect?")
            return

        self.times = dset.time
        self.depth = dset.depthoffset
        self.beampointingangle = dset.corr_pointing_angle
        if self.fqpr.multibeam.raw_ping[0].units['corr_pointing_angle'] == 'radians':
            self.beampointingangle = np.rad2deg(self.beampointingangle)
        self.heave = dset.corr_heave
        self.altitude = dset.corr_altitude

        # not sure about the logic here, but we'll say the port slope is the port transducer for dual head, etc.
        maxbeam = self.depth.shape[1]
        nadir_beam = int(maxbeam / 2)

        # max period of all the attitude signals, drives the filter coefficients, just use a slice of attitude to resolve
        att_slice = np.min([20000, self.fqpr.multibeam.raw_att['roll'].size])
        self.max_period = np.max([return_period_of_signal(self.fqpr.multibeam.raw_att['roll'][:att_slice]),
                                  return_period_of_signal(self.fqpr.multibeam.raw_att['pitch'][:att_slice]),
                                  return_period_of_signal(self.fqpr.multibeam.raw_att['heave'][:att_slice])])

        print('Building attitude at ping time...')
        # we want the roll/pitch/heave at the same times as depth/pointingangle
        self.roll_at_ping_time = interp_across_chunks(self.fqpr.multibeam.raw_att['roll'], self.times).values
        self.rollrate_at_ping_time = abs(smooth_signal(np.diff(self.roll_at_ping_time)/np.diff(self.times), window_len=30, maintain_input_shape=True))
        self.rollrate_at_ping_time = np.append(self.rollrate_at_ping_time, self.rollrate_at_ping_time[-1])  # extend to match rollatpingtime

        self.pitch_at_ping_time = interp_across_chunks(self.fqpr.multibeam.raw_att['pitch'], self.times).values

        self.vert_ref = self.fqpr.multibeam.raw_ping[0].vertical_reference
        if self.vert_ref == 'ellipse':
            self.vert_motion_at_ping_time = self.altitude - self.altitude.mean()
            print('Using corrected altitude to represent vertical motion')
        else:
            self.vert_motion_at_ping_time = self.heave - self.heave.mean()
            print('Using corrected heave to represent vertical motion')

        print('Building high pass filtered depth and slope...')
        numtaps = 101  # filter length
        self.hpf_depth = return_high_pass_filtered_depth(self.depth, self.max_period, numtaps=numtaps)
        self.hpf_port_slope, _ = return_high_pass_filtered_slope(self.beampointingangle[:, 0:nadir_beam], self.depth[:, 0:nadir_beam],
                                                                 self.max_period * 6, numtaps=numtaps)
        self.hpf_stbd_slope, _ = return_high_pass_filtered_slope(self.beampointingangle[:, nadir_beam:maxbeam], self.depth[:, nadir_beam:maxbeam],
                                                                 self.max_period * 6, numtaps=numtaps)

        # want to only include 45 to -45 deg for inner slope
        try:
            strt = np.where(self.beampointingangle[0] > 45)[0][-1]
            end = np.where(self.beampointingangle[0] < -45)[0][0]
        except IndexError:  # for some reason, there isn't anything before or after 45 deg, include whole swath i guess
            print('no 45 deg points found, inner swath calculations will include the full swath')
            strt = 0
            end = self.beampointingangle.shape[-1]

        print('Building inner/full slopes....')
        self.hpf_inner_slope, _ = return_high_pass_filtered_slope(self.beampointingangle[:, strt:end],
                                                                  self.depth[:, strt:end],
                                                                  self.max_period * 6, numtaps=numtaps)

        self.hpf_slope, self.slope_percent_deviation = return_high_pass_filtered_slope(self.beampointingangle,
                                                                                       self.depth, self.max_period * 6,
                                                                                       numtaps=numtaps)

        # filtering process, we remove the bad samples at the beginning, now remove samples from the end of the original
        #   data to match the length of the filtered data
        self.times = self.times[:-int(numtaps / 2)]
        self.slope_percent_deviation = self.slope_percent_deviation[:-int(numtaps / 2)]
        self.beampointingangle = self.beampointingangle[:-int(numtaps / 2)]
        self.depth = self.depth[:-int(numtaps / 2)]
        self.roll_at_ping_time = self.roll_at_ping_time[:-int(numtaps / 2)]
        self.rollrate_at_ping_time = self.rollrate_at_ping_time[:-int(numtaps / 2)]
        self.pitch_at_ping_time = self.pitch_at_ping_time[:-int(numtaps / 2)]
        self.vert_motion_at_ping_time = self.vert_motion_at_ping_time[:-int(numtaps / 2)]

        # filter out data that fails the percent deviation test
        if filter_rugged:
            filt = self.slope_percent_deviation < 5
            self.times = self.times[filt]
            self.hpf_slope = self.hpf_slope[filt]
            self.hpf_stbd_slope = self.hpf_stbd_slope[filt]
            self.hpf_port_slope = self.hpf_port_slope[filt]
            self.hpf_depth = self.hpf_depth[filt]
            self.hpf_inner_slope = self.hpf_inner_slope[filt]

            self.beampointingangle = self.beampointingangle[filt]
            self.depth = self.depth[filt]
            self.roll_at_ping_time = self.roll_at_ping_time[filt]
            self.rollrate_at_ping_time = self.rollrate_at_ping_time[filt]
            self.pitch_at_ping_time = self.pitch_at_ping_time[filt]
            self.vert_motion_at_ping_time = self.vert_motion_at_ping_time[filt]
            self.slope_percent_deviation = self.slope_percent_deviation[filt]

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

        Returns
        -------
        str
            regression line slope rounded to 6 places as string
        """
        # nan not allowed for regression line
        filter_nan = np.logical_or(np.isnan(x), np.isnan(y))
        x = x[~filter_nan]
        y = y[~filter_nan]

        slopes, intercepts, stderrs, percent_deviation = linear_regression(x, y)
        slope_label = np.round(slopes, 6)
        ax.plot(x, slopes * x + intercepts,
                label='y = {}x + {}'.format(slope_label, np.round(intercepts, 6)), color='red')
        ax.legend()
        return slope_label

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
        maxslope = self.hpf_slope.max()
        subplot.set_ylim(-maxslope * 4, maxslope * 4)
        subplot.set_title('Attitude Time Latency')
        subplot.set_xlabel('Roll Rate (deg/s)')
        subplot.set_ylabel('Ping Slope (deg)')
        if add_regression:
            slope = self._add_regression_line(subplot, self.rollrate_at_ping_time, self.hpf_slope)
            subplot.text(0, maxslope * 3.5, 'calculated latency = {} seconds'.format(slope))

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
        subplot.set_ylabel('Highpassfilter Depth (m)')
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
        subplot.set_ylabel('Highpassfilter Depth (m)')
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

        self.correlation_table.suptitle('{} (SN{}): WobbleTest Dashboard'.format(self.sonartype, self.serialnum))


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

    # center the signal at zero to get this logic to work, as we use positive/negative peaks
    sig = sig - np.mean(sig)
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

    coef = build_highpass_filter_coeff(1 / (max_period * 4), numtaps=numtaps)
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


def _sinfunc(t, A, w, p, c):
    """
    Build sin func from parameters for fit_sin
    """
    return A * np.sin(w * t + p) + c


def fit_sin(x, y):
    """
    Fit sin to the input time sequence, and return fitting parameters "amp", "omega", "phase", "offset", "freq", "period" and "fitfunc"

    res = fit_sin(x, y)
    plt.plot(res['fitfunc'](x, res['amp'], res['omega'], res['phase'], res['offset']))

    Parameters
    ----------
    x
    y

    Returns
    -------

    """

    ff = np.fft.fftfreq(len(x), (x[1]-x[0]))   # assume uniform spacing
    Fyy = abs(np.fft.fft(y))
    guess_freq = abs(ff[np.argmax(Fyy[1:])+1])   # excluding the zero frequency "peak", which is related to offset
    guess_amp = np.std(y) * 2.**0.5
    guess_offset = np.mean(y)
    guess = np.array([guess_amp, 2. * np.pi * guess_freq, 0., guess_offset])
    popt, pcov = curve_fit(_sinfunc, x, y, p0=guess)
    A, w, p, c = popt
    f = w/(2. * np.pi)

    return {"amp": A, "omega": w, "phase": p, "offset": c, "freq": f, "period": 1. / f, "fitfunc": _sinfunc, "maxcov": np.max(pcov), "rawres": (guess, popt, pcov)}


def smooth_signal(x: np.array, window_len: int = 20, window: str = 'hanning', maintain_input_shape: bool = True):
    """
    smooth the data using a window with requested size. This method is based on the convolution of a scaled window with
    the signal. The signal is prepared by introducing reflected copies of the signal (with the window size) in both
    ends so that transient parts are minimized in the begining and end part of the output signal.
    | 
    | ex:
    | t=linspace(-2,2,0.1)
    | x=sin(t)+randn(len(t))*0.1
    | y=smooth_signal(x)

    Parameters
    ----------
    x
        signal to smooth
    window_len
        the dimension of the smoothing window; should be an odd integer
    window
        the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'. flat window will produce a moving average smoothing.
    maintain_input_shape
        if True, will modify return to match input shape

    Returns
    -------
    np.array
        smoothed signal

    """

    if x.ndim != 1:
        raise ValueError("smooth only accepts 1 dimension arrays.")

    if x.size < window_len:
        raise ValueError("Input vector needs to be bigger than window size.")

    if window_len < 3:
        return x

    if window not in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError("Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")

    s = np.r_[x[window_len - 1:0:-1], x, x[-2:-window_len - 1:-1]]
    # print(len(s))
    if window == 'flat':  # moving average
        w = np.ones(window_len, 'd')
    else:
        w = eval('np.' + window + '(window_len)')

    y = np.convolve(w / w.sum(), s, mode='valid')

    if maintain_input_shape:
        y = y[int(window_len/2-1):-int(np.ceil(window_len/2))]
        if x.shape[0] != y.shape[0]:
            raise ValueError('Unable to match input array size! {} != {}'.format(x.shape, y.shape))
    return y
