import os
import numpy as np
import xarray as xr
import json
from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib.pyplot import get_current_fig_manager
from matplotlib.animation import FuncAnimation, FFMpegWriter
import matplotlib.cm as cm
from matplotlib.colors import LinearSegmentedColormap
import mpl_toolkits.axes_grid1
import matplotlib.widgets

from mpl_toolkits.mplot3d import Axes3D  # need this, is used in backend for 3d plots

from HSTB.kluster.xarray_helpers import stack_nan_array


class Player(FuncAnimation):
    """
    Matplotlib FuncAnimation player that includes the ability to start/stop/speed up/slow down/skip frames.  Relies on the
    frames passed in, frames must be the values of the dimension you want to animate on.
    """
    def __init__(self, fig, func, frames=None, init_func=None, fargs=None, save_count=None, save_pth=None, pos=(0.125, 0.92), **kwargs):
        self.save_pth = save_pth
        self.i = 0
        self.min = frames[0]
        self.max = frames[-1]
        self.runs = True
        self.forwards = True
        self.fig = fig
        self.func = func
        self.frames = frames
        self.setup(pos)
        self.added_speed = 0
        FuncAnimation.__init__(self, self.fig, self.update, frames=self.play(), init_func=init_func, fargs=fargs, save_count=save_count, **kwargs )

        self._observers = []

        get_current_fig_manager().toolbar.save_figure = self.save_event
        self.fig.canvas.mpl_connect('close_event', self.close_event)

    def close_event(self, evt):
        """
        Called when the Player is closed, by closing the window.  Will call any observers that the player has been
        bound to.
        """
        self.runs = False
        for callback in self._observers:
            callback()

    def save_event(self):
        """
        Called when the user hits the save button on the Player toolbar.  Uses the FFMpegWriter to save each frame to a new
        animation.  Takes forever to run, probably need to thread this later with a progress count.
        """
        if self.save_pth is not None:
            print('\nSaving animation...')
            ffwriter = FFMpegWriter()
            base_dir, filepth = os.path.split(self.save_pth)
            if os.path.exists(self.save_pth):
                self.save_pth = os.path.join(base_dir, os.path.splitext(filepth)[0] + '_{}.mpeg'.format(datetime.now().strftime('%H%M%S')))
            if not os.path.exists(base_dir):
                os.makedirs(base_dir)
            self.save(self.save_pth, writer=ffwriter)
            print('Animation saved to {}'.format(self.save_pth))
        else:
            print('No save path provided to animation')

    def bind_to(self, callback):
        """
        Pass in a method as callback, method will be triggered on close

        Parameters
        ----------
        callback
            method that is run on Player being closed
        """

        self._observers.append(callback)

    def clear_observers(self):
        """
        Clear any functions that have been bound to the player
        """
        self._observers = []

    def play(self):
        """
        Return the next frame.  If self.forwards, that would be the frame after the current one.  Otherwise, returns the
        frame before the current one.  If you hit the beginning or end of the dataset, stops the player.
        """
        while self.runs:
            # self.i is the frame index as an integer
            self.i = self.i + self.forwards - (not self.forwards)
            try:
                # i_frame is the frame value at that index (self.frames is generally a numpy array of times)
                i_frame = self.frames[self.i]
            except:
                if self.i < 0:
                    self.i = 0
                else:
                    self.i = len(self.frames) - 1
                i_frame = self.frames[self.i]
            if self.min < i_frame < self.max:
                yield i_frame
            else:  # end of dataset
                self.stop()
                yield i_frame

    def start(self):
        """
        Start playing the animation by signaling the funcanimation event_source
        """
        self.runs = True
        self.event_source.start()

    def stop(self, event=None):
        """
        Stop playing the animation by signaling the funcanimation event_source
        """
        self.runs = False
        self.event_source.stop()

    def forward(self, event=None):
        """
        Start playing the animation in the forward direction
        """
        self.forwards = True
        self.start()

    def backward(self, event=None):
        """
        Start playing the animation in the backwards direction
        """
        self.forwards = False
        self.start()

    def oneforward(self, event=None):
        """
        Skip forward one frame, stopping the animation and freezing on that frame
        """
        self.forwards = True
        self.onestep()

    def onebackward(self, event=None):
        """
        Skip backwards one frame, stopping the animation and freezing on that frame
        """
        self.forwards = False
        self.onestep()

    def speedup(self, e):
        """
        Increases the speed of the animation by decreasing the time between frames
        """
        curr_interval = self.event_source.interval
        if curr_interval > 100:
            self.event_source.interval -= 20
        if curr_interval > 20:
            self.event_source.interval -= 10
        elif curr_interval > 1:
            self.event_source.interval = max(1, self.event_source.interval - 1)

    def slowdown(self, e):
        """
        Decreases the speed of the animation by increasing the time between frames
        """
        curr_interval = self.event_source.interval
        if curr_interval > 100:
            self.event_source.interval += 20
        if curr_interval > 20:
            self.event_source.interval += 10
        elif curr_interval >= 1:
            self.event_source.interval += 1

    def onestep(self):
        """
        Stop the animation and freeze on the next frame, direction depends on self.forwards
        """
        self.stop()
        i_frame = self.frames[self.i]
        if self.min < i_frame < self.max:
            self.i = self.i + self.forwards - (not self.forwards)
        elif i_frame == self.min and self.forwards:
            self.i += 1
        elif i_frame == self.max and not self.forwards:
            self.i -= 1
        self.func(self.frames[self.i])
        self.slider.set_val(self.frames[self.i])
        self.fig.canvas.draw_idle()

    def setup(self, pos: tuple):
        """
        Build out the widgets for the player.  player widgets are in one horizontal row, include the player controls
        (start, stop, skip frame, etc.) and the slider bar, which tracks the position in the dataset

        Parameters
        ----------
        pos
            tuple of position (left position, bottom position) to place the player bar
        """

        playerax = self.fig.add_axes([pos[0],pos[1], 0.64, 0.04])
        divider = mpl_toolkits.axes_grid1.make_axes_locatable(playerax)
        onebax = divider.append_axes("right", size="80%", pad=0.05)
        bax = divider.append_axes("right", size="80%", pad=0.05)
        sax = divider.append_axes("right", size="80%", pad=0.05)
        fax = divider.append_axes("right", size="80%", pad=0.05)
        ofax = divider.append_axes("right", size="100%", pad=0.05)
        onefwdax = divider.append_axes("right", size="80%", pad=0.05)
        sliderax = divider.append_axes("right", size="500%", pad=0.07)

        self.button_slow = matplotlib.widgets.Button(playerax, label='<<')
        self.button_oneback = matplotlib.widgets.Button(onebax, label='$\u29CF$')
        self.button_back = matplotlib.widgets.Button(bax, label='$\u25C0$')
        self.button_stop = matplotlib.widgets.Button(sax, label='$\u25A0$')
        self.button_forward = matplotlib.widgets.Button(fax, label='$\u25B6$')
        self.button_oneforward = matplotlib.widgets.Button(ofax, label='$\u29D0$')
        self.button_fast = matplotlib.widgets.Button(onefwdax, label='>>')

        self.button_slow.on_clicked(self.slowdown)
        self.button_oneback.on_clicked(self.onebackward)
        self.button_back.on_clicked(self.backward)
        self.button_stop.on_clicked(self.stop)
        self.button_forward.on_clicked(self.forward)
        self.button_oneforward.on_clicked(self.oneforward)
        self.button_fast.on_clicked(self.speedup)

        self.slider = matplotlib.widgets.Slider(sliderax, '', self.min, self.max, valinit=self.frames[self.i])
        self.slider.on_changed(self.set_pos)

    def set_pos(self, slider_time: float):
        """
        Triggered on clicking within the slider to jump to a specific time.  slider_time is not an exact coordinate (not
        within self.frames), its an interpolated value based on where you click in the slider.  Ugh!  We have to find
        the nearest real value to zoom to.

        Parameters
        ----------
        slider_time
            interpolated value of self.frames based on position of where you click
        """

        idx = (np.abs(self.frames - slider_time)).argmin()
        self.i = min(int(idx), len(self.frames) - 1)
        self.func(self.frames[self.i])

    def update(self, i_frame: float):
        """
        Triggered on each tick of the animation.  Updates the slider to the actual frame that we are on.

        Parameters
        ----------
        i_frame
            value of the dim being animated, corresponds to the current value (generally the current time of the animation)
        """
        self.slider.set_val(i_frame)


class FqprVisualizations:
    """
    Visualizations in Matplotlib built on top of FQPR class.  Includes animations of beam vectors and vessel
    orientation.

    Processed fqpr_generation.Fqpr instance is passed in as argument
    """

    def __init__(self, fqpr):
        """

        Parameters
        ----------
        fqpr
            Fqpr instance to visualize; fqpr = fully qualified ping record, the term for the datastore in kluster
        """

        self.fqpr = fqpr

        self.orientation_system = None
        self.orientation_quiver = None
        self.orientation_figure = None
        self.orientation_objects = None
        self.orientation_anim = None

        self.raw_bpv_quiver = None
        self.raw_bpv_dat = None
        self.raw_bpv_figure = None
        self.raw_bpv_objects = None
        self.raw_bpv_anim = None

        self.proc_bpv_quiver = None
        self.proc_bpv_dat = None
        self.proc_bpv_figure = None
        self.proc_bpv_objects = None
        self.proc_bpv_anim = None

    def _parse_plot_mode(self, mode: str):
        """
        Used for the soundings plot, parse the mode option and return the variable names to use in the plot, checking
        to see if they are valid for the dataset (self.fqpr)

        Parameters
        ----------
        mode
            One of 'svcorr' and 'georef', which variable you want to visualize

        Returns
        -------
        xvar: string, variable name for the x dimension
        yvar: string, variable name for the y dimension
        zvar: string, variable name for the z dimension
        """

        if mode == 'svcorr':
            xvar = 'alongtrack'
            yvar = 'acrosstrack'
            zvar = 'depthoffset'
        elif mode == 'georef':
            xvar = 'x'
            yvar = 'y'
            zvar = 'z'
        else:
            raise ValueError('Unrecognized mode, must be either "svcorr" or "georef"')

        modechks = [[v in sec] for v in [xvar, yvar, zvar] for sec in self.fqpr.multibeam.raw_ping]
        if not np.any(modechks):
            raise ValueError('{}: Unable to find one or more variables in the raw_ping records'.format(mode))
        return xvar, yvar, zvar

    def soundings_plot_3d(self, mode: str = 'svcorr', color_by: str = 'depth', start_time: float = None, end_time: float = None):
        """
        Plots a 3d representation of the alongtrack/acrosstrack/depth values generated by sv correct.
        If a time is provided, isolates that time.

        Parameters
        ----------
        mode
            str, either 'svcorr' to plot the svcorrected offsets, or 'georef' to plot the georeferenced soundings
        color_by
            str, either 'depth' or 'sector'
        start_time
            start time in utc seconds, optional if you want to subset by time
        end_time
            end time in utc seconds, optional if you want to subset by time

        Returns
        -------
        plt.Axes
            matplotlib axes object for plot
        """

        if start_time is not None or start_time is not None:
            self.fqpr.subset_by_time(start_time, end_time)

        xvar, yvar, zvar = self._parse_plot_mode(mode)

        minz = self.fqpr.calc_min_var(zvar)
        maxz = self.fqpr.calc_max_var(zvar)
        miny = self.fqpr.calc_min_var(yvar)
        maxy = self.fqpr.calc_max_var(yvar)
        if mode == 'svcorr':  # svcorrected is alongtrack/acrosstrack in meters.  Want the scales to be equal so it doesnt look weird
            minx = miny
            maxx = maxy
        else:  # georeferenced is northing/easting, scales cant be equal of course
            minx = self.fqpr.calc_min_var(xvar)
            maxx = self.fqpr.calc_max_var(xvar)

        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')

        for rp in self.fqpr.multibeam.raw_ping:
            x_idx, x_stck = stack_nan_array(rp[xvar], stack_dims=('time', 'beam'))
            y_idx, y_stck = stack_nan_array(rp[yvar], stack_dims=('time', 'beam'))
            z_idx, z_stck = stack_nan_array(rp[zvar], stack_dims=('time', 'beam'))

            if color_by == 'depth':
                ax.scatter(x_stck.values, y_stck.values, z_stck.values, marker='o', s=10, c=z_stck.values)
            elif color_by == 'sector':
                sector_vals = rp.txsector_beam.values[x_idx]
                ax.scatter(x_stck.values, y_stck.values, z_stck.values, marker='o', s=10, c=sector_vals)

        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)
        ax.set_zlim(maxz, minz)

        if start_time is not None or start_time is not None:
            self.fqpr.restore_subset()
        return ax

    def soundings_plot_2d(self, mode: str = 'svcorr', color_by: str = 'depth', start_time: float = None, end_time: float = None):
        """
        Plots a 2d representation of the acrosstrack/depth values generated by sv correct.  If sector is
        provided, isolates that sector.  If a time is provided, isolates that time.

        Parameters
        ----------
        mode
            str, either 'svcorr' to plot the svcorrected offsets, or 'georef' to plot the georeferenced soundings
        color_by
            str, either 'depth' or 'sector'
        start_time
            start time in utc seconds, optional if you want to subset by time
        end_time
            end time in utc seconds, optional if you want to subset by time

        Returns
        -------
        plt.Figure
            matplotlib.pyplot.figure instance
        """

        if start_time is not None or start_time is not None:
            self.fqpr.subset_by_time(start_time, end_time)

        xvar, yvar, zvar = self._parse_plot_mode(mode)

        minz = self.fqpr.calc_min_var(zvar)
        maxz = self.fqpr.calc_max_var(zvar)
        miny = self.fqpr.calc_min_var(yvar)
        maxy = self.fqpr.calc_max_var(yvar)
        if mode == 'svcorr':  # svcorrected is alongtrack/acrosstrack in meters.  Want the scales to be equal so it doesnt look weird
            minx = miny
            maxx = maxy
        else:  # georeferenced is northing/easting, scales cant be equal of course
            minx = self.fqpr.calc_min_var(xvar)
            maxx = self.fqpr.calc_max_var(xvar)

        fig = plt.figure()

        for rp in self.fqpr.multibeam.raw_ping:
            x_idx, x_stck = stack_nan_array(rp[xvar], stack_dims=('time', 'beam'))
            y_idx, y_stck = stack_nan_array(rp[yvar], stack_dims=('time', 'beam'))
            z_idx, z_stck = stack_nan_array(rp[zvar], stack_dims=('time', 'beam'))

            if color_by == 'depth':
                plt.scatter(y_stck, x_stck, marker='+', c=z_stck, cmap='coolwarm', s=5)
                plt.clim(minz, maxz)
            elif color_by == 'sector':
                sector_vals = rp.txsector_beam.values[x_idx]
                plt.scatter(y_stck, x_stck, marker='+', c=sector_vals, s=5)
        plt.xlim(miny, maxy)
        plt.ylim(minx, maxx)
        if color_by != 'sector':
            plt.colorbar().set_label(zvar, rotation=270, labelpad=10)
        plt.title('{}: {}/{} colored by {}'.format(mode, xvar, yvar, color_by))

        if start_time is not None or start_time is not None:
            self.fqpr.restore_subset()

        return fig

    def plot_sound_velocity_profiles(self, filter_by_time: bool = False):
        """
        Get all the sound velocity profiles attached to this fqpr instance and plot the values by depth/sv.  If the
        fqpr instance is a subset (see fqpr_generation.Fqpr.subset_by_time) then only get the casts within the dataset
        time range (with a small buffer applied)

        Parameters
        ----------
        filter_by_time
            if True, will only include casts within the time range of the dataset (use if Fqpr.subset_by_time and you only
            want to show the casts within the time range of the subset)
        """

        sv = None
        profnames, casts, cast_times, castlocations = self.fqpr.multibeam.return_all_profiles()

        if filter_by_time:
            min_search_time = float(self.fqpr.multibeam.raw_ping[0].time[0].values - 5)
            max_search_time = float(self.fqpr.multibeam.raw_ping[0].time[-1].values + 5)

        if profnames:
            fig = plt.figure()

            for profname, cast, casttime, castloc in zip(profnames, casts, cast_times, castlocations):
                # filter cast by mintime/maxtime to only get casts in the subset range, if we have subsetted this fqpr instance
                if filter_by_time and not (min_search_time < casttime < max_search_time):
                    continue
                plt.plot(cast[1], cast[0], label=profname)
                plt.legend()
        else:
            print('No sound velocity profiles found')
        plt.gca().invert_yaxis()
        plt.title('Sound Velocity Profiles')
        plt.xlabel('Sound Velocity (meters/second)')
        plt.ylabel('Depth (meters)')

    def plot_sound_velocity_map(self, filter_casts_by_time: bool = False):
        """
        Plot a latitutde/longitude overview of all multibeam lines within the current Fqpr time range and all applicable
        casts.  Plot cast positions as a scatter plot on top of the multibeam lines.

        Parameters
        ----------
        filter_casts_by_time
            if True, will only include casts within the time range of the dataset (use if Fqpr.subset_by_time and you only
            want to show the casts within the time range of the subset)
        """

        print('Building Sound Velocity Profile map...')
        nav = self.fqpr.multibeam.return_raw_navigation()
        if nav is None:
            print('no navigation found!')
            return

        minlon = 999
        maxlon = -999
        minlat = 999
        maxlat = -999
        print('Plotting lines...')
        fig = plt.figure()

        # these times based on the Fqpr subset time, which restricts the source dataset times
        min_search_time = float(nav.time[0].values - 5)
        max_search_time = float(nav.time[-1].values + 5)

        for line, times in self.fqpr.multibeam.raw_ping[0].multibeam_files.items():
            # if the line start/end is within the time range...
            if max_search_time >= times[0] >= min_search_time or max_search_time >= times[-1] >= min_search_time:
                times[0] = max(times[0], nav.time[0])
                times[1] = min(times[1], nav.time[-1])
                try:
                    nav = self.fqpr.multibeam.return_raw_navigation(times[0], times[1])
                    lats, lons = nav.latitude.values, nav.longitude.values
                    plt.plot(lons, lats, c='blue', alpha=0.5)

                    minlon = min(np.min(lons), minlon)
                    maxlon = max(np.max(lons), maxlon)
                    minlat = min(np.min(lats), minlat)
                    maxlat = max(np.max(lats), maxlat)
                except AttributeError:  # no nav at this time range
                    print('Error reading Line {}'.format(line))

        if minlon == 999:
            print('Found no lines within this time range to plot: {} to {}'.format(min_search_time, max_search_time))
            return

        lonrange = maxlon - minlon
        latrange = maxlat - minlat
        datarange = max(lonrange, latrange)
        profnames, casts, cast_times, castlocations = self.fqpr.multibeam.return_all_profiles()
        all_lats = []
        all_longs = []
        for profname, cast, casttime, castloc in zip(profnames, casts, cast_times, castlocations):
            # filter cast by mintime/maxtime to only get casts in the subset range, if we have subsetted this fqpr instance
            if filter_casts_by_time and not (min_search_time < casttime < max_search_time):
                continue
            if not castloc:  # should never get here, but this will get a fall back position of nearest nav point to the cast time
                print('building cast position for cast {}'.format(profname))
                # search times have a buffer, if the casttime is within the buffer but less than the dataset time, use the min dataset time
                if casttime <= nav.time.min():
                    castloc = [float(nav.latitude[0].values), float(nav.longitude[0].values)]
                elif casttime >= nav.time.max():
                    castloc = [float(nav.latitude[-1].values), float(nav.longitude[-1].values)]
                else:
                    interpnav = nav.interp(time=casttime, method='nearest')
                    castloc = [float(interpnav.latitude.values), float(interpnav.longitude.values)]
            print('Plotting cast at position {}'.format(castloc))
            plt.scatter(castloc[1], castloc[0], label=profname)
            all_lats.append(castloc[0])
            all_longs.append(castloc[1])

        plt.title('Sound Velocity Map')
        plt.xlabel('Longitude (degrees)')
        plt.ylabel('Latitude (degrees)')

        if all_longs and all_lats:  # found some casts, so we make sure they are within the plot range
            plt.ylim(min(minlat - 0.5 * datarange, np.min(all_lats) - 0.01), max(maxlat + 0.5 * datarange, np.max(all_lats) + 0.01))
            plt.xlim(min(minlon - 0.5 * datarange, np.min(all_longs) - 0.01), max(maxlon + 0.5 * datarange, np.max(all_longs) + 0.01))
            plt.legend()
        else:
            plt.ylim(minlat - 0.5 * datarange, maxlat + 0.5 * datarange)
            plt.xlim(minlon - 0.5 * datarange, maxlon + 0.5 * datarange)
            print('No sound velocity profiles found within the given time range')

    def _generate_orientation_vector(self, system_index: int = 0, tme: float = None):
        """
        Generate tx/rx vector data for given time value, return with values to be used with matplotlib quiver

        Parameters
        ----------
        system_index
            int, will automatically choose the first one
        tme
            float, time at this specific interval

        Returns
        -------
        tuple
            x component of starting location of vectors
        tuple
            y component of starting location of vectors
        tuple
            z component of starting location of vectors
        tuple
            x direction component of vectors
        tuple
            y direction component of vectors
        tuple
            z direction component of vectors
        """

        if tme is not None:
            tx = self.fqpr.multibeam.raw_ping[system_index].tx.sel(time=tme).values
            rx = self.fqpr.multibeam.raw_ping[system_index].rx.sel(time=tme).values
        else:
            tx = self.fqpr.multibeam.raw_ping[system_index].tx.isel(time=0).values
            rx = self.fqpr.multibeam.raw_ping[system_index].rx.isel(time=0).values

        rx = np.nanmean(rx, axis=0)
        tx = np.nanmean(tx, axis=0)
        origin = [0, 0, 0]
        x, y, z = zip(origin, origin)
        u, v, w = zip(tx, rx)
        return x, y, z, u, v, w

    def _update_orientation_vector(self, time: float):
        """
        Update method for visualize_orientation_vector, runs on each frame of the animation

        Parameters
        ----------
        time
            float, time at this specific interval
        """

        vecdata = self._generate_orientation_vector(self.orientation_system, time)
        tx_x = round(vecdata[3][0], 3)
        tx_y = round(vecdata[4][0], 3)
        tx_z = round(vecdata[5][0], 3)
        rx_x = round(vecdata[3][1], 3)
        rx_y = round(vecdata[4][1], 3)
        rx_z = round(vecdata[5][1], 3)

        self.orientation_quiver.remove()
        self.orientation_quiver = self.orientation_figure.quiver(*vecdata, color=['blue', 'red'])
        # self.orientation_objects['time'].set_text('Time: {:0.3f}'.format(time))
        self.orientation_objects['tx_vec'].set_text('TX Vector: x:{:0.3f}, y:{:0.3f}, z:{:0.3f}'.format(tx_x, tx_y, tx_z))
        self.orientation_objects['rx_vec'].set_text('RX Vector: x:{:0.3f}, y:{:0.3f}, z:{:0.3f}'.format(rx_x, rx_y, rx_z))

    def visualize_orientation_vector(self, system_index: int = 0):
        """
        Use matplotlib funcanimation to build animated representation of the transmitter/receiver across time

        Receiver orientation is based on attitude at the average time of receive (receive time differs across beams)

        Parameters
        ----------
        system_index
            int, optional will automatically choose the first (only matters with dual head, which would have two systems)
        """

        search_for_these = ['tx', 'rx']
        for rec in search_for_these:
            if rec not in self.fqpr.multibeam.raw_ping[system_index]:
                print('visualize_orientation_vector: Unable to find {} record.  Make sure you have run "All Processing - Compute Orientation" first'.format(rec))
                return None

        self.orientation_objects = {}

        self.fqpr.multibeam.raw_ping[system_index]['tx'] = self.fqpr.multibeam.raw_ping[system_index]['tx'].compute()
        self.fqpr.multibeam.raw_ping[system_index]['rx'] = self.fqpr.multibeam.raw_ping[system_index]['rx'].compute()

        fig = plt.figure('Transducer Orientation Vectors', figsize=(10, 8))
        self.orientation_figure = fig.add_subplot(111, projection='3d')
        self.orientation_figure.set_xlim(-1.2, 1.2)
        self.orientation_figure.set_ylim(-1.2, 1.2)
        self.orientation_figure.set_zlim(-1.2, 1.2)
        self.orientation_figure.set_xlabel('+ Forward')
        self.orientation_figure.set_ylabel('+ Starboard')
        self.orientation_figure.set_zlabel('+ Down')

        # self.orientation_objects['time'] = self.orientation_figure.text2D(-0.1, 0.11, '')
        self.orientation_objects['tx_vec'] = self.orientation_figure.text2D(0, 0.11, '', color='blue')
        self.orientation_objects['rx_vec'] = self.orientation_figure.text2D(0, 0.10, '', color='red')

        self.orientation_system = system_index
        self.orientation_quiver = self.orientation_figure.quiver(*self._generate_orientation_vector(system_index),
                                                                 color=['blue', 'red'])

        outfold = self.fqpr.multibeam.converted_pth  # parent folder to all the currently written data
        frames = self.fqpr.multibeam.raw_ping[system_index].time.values
        self.orientation_anim = Player(fig, self._update_orientation_vector, frames=frames,
                                       save_count=len(frames), save_pth=os.path.join(outfold, 'vessel_orientation.mpeg'),
                                       pos=(0.125, 0.02))
        self.orientation_anim.bind_to(self._orientation_cleanup)

    def _generate_bpv_arrs(self, dat: xr.Dataset):
        """
        Generate traveltime/beampointingangle vectors to be used with matplotlib quiver

        Parameters
        ----------
        dat
            dataset containing the angle/traveltimes to plot

        Returns
        -------
        tuple
            x component of starting location of vectors
        tuple
            y component of starting location of vectors
        tuple
            x direction component of vectors
        tuple
            y direction component of vectors
        """

        try:  # uncorrected version
            bpa = dat.beampointingangle.values.ravel()
        except:
            bpa = dat.corr_pointing_angle.values.ravel()
        tt = dat.traveltime.values.ravel()

        valid_bpa = ~np.isnan(bpa)
        valid_tt = ~np.isnan(tt)
        valid_idx = np.logical_and(valid_bpa, valid_tt)
        bpa = bpa[valid_idx]
        tt = tt[valid_idx]

        maxbeams = bpa.shape[0]
        u = np.sin(bpa) * tt
        v = np.cos(bpa) * tt
        u = -u / np.max(u)  # negative here for beam pointing angle so the port angles (pos) are on the left side
        v = -v / np.max(v)  # negative here for travel time so the vectors point down in the graph

        x = np.zeros(maxbeams)
        y = np.zeros(maxbeams)
        return x, y, u, v

    def _update_corr_bpv(self, time_val: float):
        """
        Update method for visualize_beam_pointing_vectors, runs on each frame of the animation

        update for the corrected beam vector viz

        Parameters
        ----------
        time_val
            ping time value
        """

        subset = self.proc_bpv_dat.sel(time=time_val)
        angles = subset.corr_pointing_angle.values
        traveltime = subset.traveltime.values
        valid_bpa = ~np.isnan(angles)
        valid_tt = ~np.isnan(traveltime)
        valid_idx = np.logical_and(valid_bpa, valid_tt)
        angles = angles[valid_idx]
        traveltime = traveltime[valid_idx]

        if self.proc_bpv_quiver is not None:
            self.proc_bpv_quiver.remove()
        if self.fqpr.multibeam.is_dual_head():
            newidx = np.where(self.proc_bpv_dat.time == time_val)[0] + 1
            try:
                subset_next = self.proc_bpv_dat.isel(time=newidx)
                nextangles = subset_next.corr_pointing_angle.values
                nexttraveltime = subset_next.traveltime.values
                nextvalid_bpa = ~np.isnan(nextangles)
                nextvalid_tt = ~np.isnan(nexttraveltime)
                nextvalid_idx = np.logical_and(nextvalid_bpa, nextvalid_tt)
                nextangles = nextangles[nextvalid_idx]
                nexttraveltime = nexttraveltime[nextvalid_idx]

                pouterang = [str(round(np.rad2deg(angles[0]), 3)), str(round(np.rad2deg(nextangles[0]), 3))]
                poutertt = [str(round(traveltime[0], 3)), str(round(nexttraveltime[0], 3))]
                pinnerang = [str(round(np.rad2deg(angles[-1]), 3)), str(round(np.rad2deg(nextangles[-1]), 3))]
                pinnertt = [str(round(traveltime[-1], 3)), str(round(nexttraveltime[-1], 3))]
                idx = [time_val, float(subset_next.time.values)]
            except IndexError:  # EOF
                pouterang = str(round(np.rad2deg(angles[0]), 3))
                poutertt = str(round(traveltime[0], 3))
                pinnerang = str(round(np.rad2deg(angles[-1]), 3))
                pinnertt = str(round(traveltime[-1], 3))
                idx = time_val
        else:
            pouterang = str(round(np.rad2deg(angles[0]), 3))
            poutertt = str(round(traveltime[0], 3))
            pinnerang = str(round(np.rad2deg(angles[-1]), 3))
            pinnertt = str(round(traveltime[-1], 3))
            idx = time_val

        self.proc_bpv_quiver = self.proc_bpv_figure.quiver(*self._generate_bpv_arrs(self.proc_bpv_dat.sel(time=idx)),
                                                           color=self._generate_bpv_colors(self.proc_bpv_dat.sel(time=idx)),
                                                           units='xy', scale=1)
        # self.proc_bpv_objects['Time'].set_text('Time: {}'.format(idx))

        self.proc_bpv_objects['Port_outer_angle'].set_text('Port outermost angle: {}°'.format(pouterang))
        self.proc_bpv_objects['Port_outer_traveltime'].set_text('Port outermost traveltime: {}s'.format(poutertt))
        self.proc_bpv_objects['Starboard_outer_angle'].set_text('Starboard outermost angle: {}°'.format(pinnerang))
        self.proc_bpv_objects['Starboard_outer_traveltime'].set_text('Starboard outermost traveltime: {}s'.format(pinnertt))

    def _update_uncorr_bpv(self, time_val: float):
        """
        Update method for visualize_beam_pointing_vectors, runs on each frame of the animation

        update function for the uncorrected beam vector viz

        Parameters
        ----------
        time_val
            ping time value
        """

        subset = self.raw_bpv_dat.sel(time=time_val)
        angles = subset.beampointingangle.values
        traveltime = subset.traveltime.values
        valid_bpa = ~np.isnan(angles)
        valid_tt = ~np.isnan(traveltime)
        valid_idx = np.logical_and(valid_bpa, valid_tt)
        angles = angles[valid_idx]
        traveltime = traveltime[valid_idx]

        if self.raw_bpv_quiver is not None:
            self.raw_bpv_quiver.remove()
        if self.fqpr.multibeam.is_dual_head():
            newidx = np.where(self.proc_bpv_dat.time == time_val)[0] + 1
            try:
                subset_next = self.raw_bpv_dat.isel(time=newidx + 1)
                nextangles = subset_next.beampointingangle.values
                nexttraveltime = subset_next.traveltime.values
                nextvalid_bpa = ~np.isnan(nextangles)
                nextvalid_tt = ~np.isnan(nexttraveltime)
                nextvalid_idx = np.logical_and(nextvalid_bpa, nextvalid_tt)
                nextangles = nextangles[nextvalid_idx]
                nexttraveltime = nexttraveltime[nextvalid_idx]

                pouterang = [str(round(np.rad2deg(angles[0]), 3)), str(round(np.rad2deg(nextangles[0]), 3))]
                poutertt = [str(round(traveltime[0], 3)), str(round(nexttraveltime[0], 3))]
                pinnerang = [str(round(np.rad2deg(angles[-1]), 3)), str(round(np.rad2deg(nextangles[-1]), 3))]
                pinnertt = [str(round(traveltime[-1], 3)), str(round(nexttraveltime[-1], 3))]
                idx = [time_val, float(subset_next.time.values)]
            except IndexError:  # EOF
                pouterang = str(round(np.rad2deg(angles[0]), 3))
                poutertt = str(round(traveltime[0], 3))
                pinnerang = str(round(np.rad2deg(angles[-1]), 3))
                pinnertt = str(round(traveltime[-1], 3))
                idx = time_val
        else:
            pouterang = str(round(np.rad2deg(angles[0]), 3))
            poutertt = str(round(traveltime[0], 3))
            pinnerang = str(round(np.rad2deg(angles[-1]), 3))
            pinnertt = str(round(traveltime[-1], 3))
            idx = time_val

        self.raw_bpv_quiver = self.raw_bpv_figure.quiver(*self._generate_bpv_arrs(self.raw_bpv_dat.sel(time=idx)),
                                                         color=self._generate_bpv_colors(self.raw_bpv_dat.sel(time=idx)),
                                                         units='xy', scale=1)
        # self.raw_bpv_objects['Time'].set_text('Time: {}'.format(idx))

        self.raw_bpv_objects['Port_outer_angle'].set_text('Port outermost angle: {}°'.format(pouterang))
        self.raw_bpv_objects['Port_outer_traveltime'].set_text('Port outermost traveltime: {}s'.format(poutertt))
        self.raw_bpv_objects['Starboard_outer_angle'].set_text('Starboard outermost angle: {}°'.format(pinnerang))
        self.raw_bpv_objects['Starboard_outer_traveltime'].set_text('Starboard outermost traveltime: {}s'.format(pinnertt))

    def _generate_bpv_colors(self, dat: xr.Dataset):
        """
        Return colormap for beams identifying unique sectors as different colors

        Parameters
        ----------
        dat
            dataset of the data for this time

        Returns
        -------
        LinearSegmentedColormap
            matplotlib colormap for that ping, colored by sector
        """

        sec_numbers = []
        max_sector = None
        try:
            num_times = int(dat.time.shape[0])
        except:  # single head, just one time here
            num_times = 1
        for i in range(num_times):  # handle dual head, where you have two pings here
            if num_times != 1:
                subsetdat = dat.isel(time=i)
            else:
                subsetdat = dat
            systemident = subsetdat.system_identifier.values
            sector_numbers = subsetdat.txsector_beam.values.ravel()
            if max_sector is None:
                max_sector = np.max(sector_numbers)
            else:
                max_sector += 1
                sector_numbers += max_sector
            sec_numbers.append(sector_numbers)

        sec_numbers = np.concatenate(sec_numbers)
        colormap = cm.rainbow
        if np.max(sec_numbers) > 0:
            # scale for the max integer count of sectors
            return colormap(sec_numbers / np.max(sec_numbers))
        else:
            return colormap(sec_numbers)

    def visualize_beam_pointing_vectors(self, corrected: bool = False):
        """
        Use matplotlib funcanimation to build animated representation of the beampointingvectors/traveltimes across
        time

        if corrected is True uses the 'corr_pointing_angle' variable that is corrected for mounting angles/attitude,
        otherwise plots the raw 'beampointingangle' variable that is uncorrected.

        Parameters
        ----------
        corrected
            if True uses the 'corr_pointing_angle', else raw beam pointing angle 'beampointingangle'
        """

        if not corrected and ('beampointingangle' not in self.fqpr.multibeam.raw_ping[0]):
            print('visualize_beam_pointing_vectors: Unable to find the raw beampointingangle record, this record comes in during conversion, you must reconvert')
            return None
        elif corrected and ('corr_pointing_angle' not in self.fqpr.multibeam.raw_ping[0]):
            print('visualize_beam_pointing_vectors: Unable to plot the corr_pointing_angle record, Make sure you have run "All Processing - Compute Beam Vectors" first')
            return None

        if corrected:
            fg = plt.figure('Corrected Beam Vectors', figsize=(10, 8))

            self.proc_bpv_objects = {}
            obj = self.proc_bpv_objects
            self.proc_bpv_figure = fg.add_subplot(1, 1, 1)

            self.proc_bpv_figure.set_xlim(-1.5, 1.5)
            self.proc_bpv_figure.set_ylim(-1.5, 0.5)
            self.proc_bpv_figure.set_xlabel('Acrosstrack (scaled)')
            self.proc_bpv_figure.set_ylabel('Travel Time (scaled)')
            self.proc_bpv_figure.set_axis_off()

            obj['Time'] = self.proc_bpv_figure.text(-1.4, 0.45, '')
            obj['Port_outer_angle'] = self.proc_bpv_figure.text(-1.4, 0.40, '')
            obj['Port_outer_traveltime'] = self.proc_bpv_figure.text(-1.4, 0.35, '')
            obj['Starboard_outer_angle'] = self.proc_bpv_figure.text(0.35, 0.40, '')
            obj['Starboard_outer_traveltime'] = self.proc_bpv_figure.text(0.35, 0.35, '')

            self.proc_bpv_dat = self.fqpr.subset_variables(['corr_pointing_angle', 'traveltime', 'txsector_beam'], skip_subset_by_time=True)
            dat = self.proc_bpv_dat
        else:
            fg = plt.figure('Raw Beam Vectors', figsize=(10, 8))

            self.raw_bpv_objects = {}
            obj = self.raw_bpv_objects
            self.raw_bpv_figure = fg.add_subplot(1, 1, 1)

            self.raw_bpv_figure.set_xlim(-1.5, 1.5)
            self.raw_bpv_figure.set_ylim(-1.5, 0.5)
            self.raw_bpv_figure.set_xlabel('Acrosstrack (scaled)')
            self.raw_bpv_figure.set_ylabel('Travel Time (scaled)')
            self.raw_bpv_figure.set_axis_off()

            obj['Time'] = self.raw_bpv_figure.text(-1.4, 0.45, '')
            obj['Port_outer_angle'] = self.raw_bpv_figure.text(-1.4, 0.40, '')
            obj['Port_outer_traveltime'] = self.raw_bpv_figure.text(-1.4, 0.35, '')
            obj['Starboard_outer_angle'] = self.raw_bpv_figure.text(0.35, 0.40, '')
            obj['Starboard_outer_traveltime'] = self.raw_bpv_figure.text(0.35, 0.35, '')

            self.raw_bpv_dat = self.fqpr.subset_variables(['beampointingangle', 'traveltime', 'txsector_beam'], skip_subset_by_time=True)
            self.raw_bpv_dat['beampointingangle'] = np.deg2rad(self.raw_bpv_dat['beampointingangle'])
            dat = self.raw_bpv_dat

        if self.fqpr.multibeam.is_dual_head():  # for dual head, we end up plotting two records each time
            frames = dat.time.values[::2]
        else:
            frames = dat.time.values

        outfold = self.fqpr.multibeam.converted_pth  # parent folder to all the currently written data
        if not corrected:
            self.raw_bpv_anim = Player(fg, self._update_uncorr_bpv, frames=frames, save_count=len(frames),
                                       save_pth=os.path.join(outfold, 'raw_beam_vectors.mpeg'), pos=(0.125, 0.02))
            self.raw_bpv_anim.bind_to(self._uncorr_cleanup)
        else:
            self.proc_bpv_anim = Player(fg, self._update_corr_bpv, frames=frames, save_count=len(frames),
                                        save_pth=os.path.join(outfold, 'corrected_beam_vectors.mpeg'), pos=(0.125, 0.02))
            self.proc_bpv_anim.bind_to(self._corr_cleanup)

    def _orientation_cleanup(self):
        """
        Delete all the data associated with the orientation animation on closing the animation
        """
        self.orientation_system = None
        self.orientation_quiver = None
        self.orientation_figure = None
        self.orientation_objects = None
        self.orientation_anim = None

    def _uncorr_cleanup(self):
        """
        Delete all the data associated with the uncorrected beam animation on closing the animation
        """
        self.raw_bpv_quiver = None
        self.raw_bpv_dat = None
        self.raw_bpv_figure = None
        self.raw_bpv_objects = None
        self.raw_bpv_anim = None

    def _corr_cleanup(self):
        """
        Delete all the data associated with the corrected beam animation on closing the animation
        """
        self.proc_bpv_quiver = None
        self.proc_bpv_dat = None
        self.proc_bpv_figure = None
        self.proc_bpv_objects = None
        self.proc_bpv_anim = None


def save_animation_mpeg(anim_instance: FuncAnimation, output_pth: str):
    """
    Save a Matplotlib FuncAnimation object to Mpeg

    Parameters
    ----------
    anim_instance
        Matplotlib FuncAnimation object
    output_pth
        str, path to where you want the mpeg to be generated
    """

    ffwriter = FFMpegWriter()
    anim_instance.save(output_pth, writer=ffwriter)
