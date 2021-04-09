# have to import PySide2 first so the matplotlib backend figures out we want PySide2
from HSTB.kluster.gui.backends._qt import QtCore, QtGui, QtWidgets, Signal

# apparently there is some problem with PySide2 + Pyinstaller, for future reference
# https://stackoverflow.com/questions/56182256/figurecanvas-not-interpreted-as-qtwidget-after-using-pyinstaller/62055972#62055972

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg, NavigationToolbar2QT
from matplotlib.figure import Figure
from matplotlib.widgets import RectangleSelector
from matplotlib.backend_bases import MouseEvent

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import sys
import numpy as np

# see block plots for surface maybe
# https://scitools.org.uk/cartopy/docs/v0.13/matplotlib/advanced_plotting.html


class MapView(FigureCanvasQTAgg):
    """
    Map view using cartopy/matplotlib to view multibeam tracklines and surfaces with a map context.
    """
    box_select = Signal(float, float, float, float)

    def __init__(self, parent=None, width: int = 5, height: int = 4, dpi: int = 100, map_proj=ccrs.PlateCarree(), settings=None):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.map_proj = map_proj
        self.axes = self.fig.add_subplot(projection=map_proj)
        # self.axes.coastlines(resolution='10m')
        self.fig.add_axes(self.axes)
        #self.fig.subplots_adjust(left=0, right=1, bottom=0, top=1)

        self.axes.gridlines(draw_labels=True, crs=self.map_proj)
        self.axes.add_feature(cfeature.LAND)
        self.axes.add_feature(cfeature.COASTLINE)

        self.line_objects = {}  # dict of {line name: [lats, lons, lineplot]}
        self.surface_objects = {}  # nested dict {surfname: {layername: [lats, lons, surfplot]}}
        self.active_layers = {}  # dict of {surfname: [layername1, layername2]}
        self.data_extents = {'min_lat': 999, 'max_lat': -999, 'min_lon': 999, 'max_lon': -999}
        self.selected_line_objects = []

        super(MapView, self).__init__(self.fig)

        self.navi_toolbar = NavigationToolbar2QT(self.fig.canvas, self)
        self.rs = RectangleSelector(self.axes, self._line_select_callback, drawtype='box', useblit=False,
                                    button=[1], minspanx=5, minspany=5, spancoords='pixels', interactive=True)
        self.set_extent(90, -90, 100, -100)

    def set_background(self, layername: str, transparency: float, surf_transparency: float):
        """
        A function for rendering different background layers in QGIS. Disabled for cartopy
        """
        pass

    def set_extent(self, max_lat: float, min_lat: float, max_lon: float, min_lon: float, buffer: bool = True):
        """
        Set the extent of the 2d window

        Parameters
        ----------
        max_lat
            set the maximum latitude of the displayed map
        min_lat
            set the minimum latitude of the displayed map
        max_lon
            set the maximum longitude of the displayed map
        min_lon
            set the minimum longitude of the displayed map
        buffer
            if True, will extend the extents by half the current width/height
        """

        self.data_extents['min_lat'] = np.min([min_lat, self.data_extents['min_lat']])
        self.data_extents['max_lat'] = np.max([max_lat, self.data_extents['max_lat']])
        self.data_extents['min_lon'] = np.min([min_lon, self.data_extents['min_lon']])
        self.data_extents['max_lon'] = np.max([max_lon, self.data_extents['max_lon']])

        if self.data_extents['min_lat'] != 999 and self.data_extents['max_lat'] != -999 and self.data_extents[
                             'min_lon'] != 999 and self.data_extents['max_lon'] != -999:
            if buffer:
                lat_buffer = np.max([(max_lat - min_lat) * 0.5, 0.5])
                lon_buffer = np.max([(max_lon - min_lon) * 0.5, 0.5])
            else:
                lat_buffer = 0
                lon_buffer = 0
            self.axes.set_extent([np.clip(min_lon - lon_buffer, -179.999999999, 179.999999999), np.clip(max_lon + lon_buffer, -179.999999999, 179.999999999),
                                  np.clip(min_lat - lat_buffer, -90, 90), np.clip(max_lat + lat_buffer, -90, 90)],
                                 crs=ccrs.Geodetic())

    def add_line(self, line_name: str, lats: np.ndarray, lons: np.ndarray, refresh: bool = False):
        """
        Draw a new multibeam trackline on the cartopy display, unless it is already there

        Parameters
        ----------
        line_name
            name of the multibeam line
        lats
            numpy array of latitude values to plot
        lons
            numpy array of longitude values to plot
        refresh
            set to True if you want to show the line after adding here, kluster will redraw the screen after adding
            lines itself
        """

        if line_name in self.line_objects:
            return
        # this is about 3x slower, use transform_points instead
        # lne = self.axes.plot(lons, lats, color='blue', linewidth=2, transform=ccrs.Geodetic())
        ret = self.axes.projection.transform_points(ccrs.Geodetic(), lons, lats)
        x = ret[..., 0]
        y = ret[..., 1]
        lne = self.axes.plot(x, y, color='blue', linewidth=2)
        self.line_objects[line_name] = [lats, lons, lne[0]]
        if refresh:
            self.refresh_screen()

    def remove_line(self, line_name, refresh=False):
        """
        Remove a multibeam line from the cartopy display

        Parameters
        ----------
        line_name
            name of the multibeam line
        refresh
            optional screen refresh, True most of the time, unless you want to remove multiple lines and then refresh
            at the end
        """

        if line_name in self.line_objects:
            lne = self.line_objects[line_name][2]
            lne.remove()
            self.line_objects.pop(line_name)
        if refresh:
            self.refresh_screen()

    def add_surface(self, surfname: str, lyrname: str, surfx: np.ndarray, surfy: np.ndarray, surfz: np.ndarray,
                    surf_crs: int):
        """
        Add a new surface/layer with the provided data

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        surfx
            1 dim numpy array for the grid x values
        surfy
            1 dim numpy array for the grid y values
        surfz
            2 dim numpy array for the grid values (depth, uncertainty, etc.)
        surf_crs
            integer epsg code
        """

        try:
            addlyr = True
            if lyrname in self.active_layers[surfname]:
                addlyr = False
        except KeyError:
            addlyr = True

        if addlyr:
            self._add_surface_layer(surfname, lyrname, surfx, surfy, surfz, surf_crs)
            self.refresh_screen()

    def hide_surface(self, surfname: str, lyrname: str):
        """
        Hide the surface layer that corresponds to the given names.

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        """

        try:
            hidelyr = True
            if lyrname not in self.active_layers[surfname]:
                hidelyr = False
        except KeyError:
            hidelyr = False

        if hidelyr:
            self._hide_surface_layer(surfname, lyrname)
            return True
        else:
            return False

    def show_surface(self, surfname: str, lyrname: str):
        """
        Cartopy backend currently just deletes/adds surface data, doesn't really hide or show.  Return False here to
        signal we did not hide
        """
        return False

    def remove_surface(self, surfname: str):
        """
        Remove the surface from memory by removing the name from the surface_objects dict

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        """

        if surfname in self.surface_objects:
            for lyr in self.surface_objects[surfname]:
                self.hide_surface(surfname, lyr)
                surf = self.surface_objects[surfname][lyr][2]
                surf.remove()
                self.surface_objects.pop(surfname)
        self.refresh_screen()

    def _add_surface_layer(self, surfname: str, lyrname: str, surfx: np.ndarray, surfy: np.ndarray, surfz: np.ndarray,
                           surf_crs: int):
        """
        Add a new surface/layer with the provided data

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        surfx
            1 dim numpy array for the grid x values
        surfy
            1 dim numpy array for the grid y values
        surfz
            2 dim numpy array for the grid values (depth, uncertainty, etc.)
        surf_crs
            integer epsg code
        """

        try:
            makelyr = True
            if lyrname in self.surface_objects[surfname]:
                makelyr = False
        except KeyError:
            makelyr = True

        if makelyr:
            desired_crs = self.map_proj
            lon2d, lat2d = np.meshgrid(surfx, surfy)
            xyz = desired_crs.transform_points(ccrs.epsg(int(surf_crs)), lon2d, lat2d)
            lons = xyz[..., 0]
            lats = xyz[..., 1]

            if lyrname != 'depth':
                vmin, vmax = np.nanmin(surfz), np.nanmax(surfz)
            else:  # need an outlier resistant min max depth range value
                twostd = np.nanstd(surfz)
                med = np.nanmedian(surfz)
                vmin, vmax = med - twostd, med + twostd
            # print(vmin, vmax)
            surfplt = self.axes.pcolormesh(lons, lats, surfz.T, vmin=vmin, vmax=vmax, zorder=10)
            setextents = False
            if not self.line_objects and not self.surface_objects:  # if this is the first thing you are loading, jump to it's extents
                setextents = True
            self._add_to_active_layers(surfname, lyrname)
            self._add_to_surface_objects(surfname, lyrname, [lats, lons, surfplt])
            if setextents:
                self.set_extents_from_surfaces()
        else:
            surfplt = self.surface_objects[surfname][lyrname][2]
            newsurfplt = self.axes.add_artist(surfplt)
            # update the object with the newly added artist
            self.surface_objects[surfname][lyrname][2] = newsurfplt
            self._add_to_active_layers(surfname, lyrname)

    def _hide_surface_layer(self, surfname: str, lyrname: str):
        """
        Hide the surface layer that corresponds to the given names.

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        """

        surfplt = self.surface_objects[surfname][lyrname][2]
        surfplt.remove()
        self._remove_from_active_layers(surfname, lyrname)
        self.refresh_screen()

    def _add_to_active_layers(self, surfname: str, lyrname: str):
        """
        Add the surface layer to the active layers dict

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        """

        if surfname in self.active_layers:
            self.active_layers[surfname].append(lyrname)
        else:
            self.active_layers[surfname] = [lyrname]

    def _add_to_surface_objects(self, surfname: str, lyrname: str, data: list):
        """
        Add the surface layer data to the surface objects dict

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        data
            list of [2dim y values for the grid, 2dim x values for the grid, matplotlib.collections.QuadMesh]
        """

        if surfname in self.surface_objects:
            self.surface_objects[surfname][lyrname] = data
        else:
            self.surface_objects[surfname] = {lyrname: data}

    def _remove_from_active_layers(self, surfname: str, lyrname: str):
        """
        Remove the surface layer from the active layers dict

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        """

        if surfname in self.active_layers:
            if lyrname in self.active_layers[surfname]:
                self.active_layers[surfname].remove(lyrname)

    def _remove_from_surface_objects(self, surfname, lyrname):
        """
        Remove the surface layer from the surface objects dict

        Parameters
        ----------
        surfname
            path to the surface that is used as a name
        lyrname
            band layer name for the provided data
        """

        if surfname in self.surface_objects:
            if lyrname in self.surface_objects[surfname]:
                self.surface_objects[surfname].pop(lyrname)

    def change_line_colors(self, line_names: list, color: str):
        """
        Change the provided line names to the provided color

        Parameters
        ----------
        line_names
            list of line names to use as keys in the line objects dict
        color
            string color identifier, ex: 'r' or 'red'
        """

        for line in line_names:
            lne = self.line_objects[line][2]
            lne.set_color(color)
            self.selected_line_objects.append(lne)
        self.refresh_screen()

    def reset_line_colors(self):
        """
        Reset all lines back to the default color
        """

        for lne in self.selected_line_objects:
            lne.set_color('b')
        self.selected_line_objects = []
        self.refresh_screen()

    def _line_select_callback(self, eclick: MouseEvent, erelease: MouseEvent):
        """
        Handle the return of the Matplotlib RectangleSelector, provides an event with the location of the click and
        an event with the location of the release

        Parameters
        ----------
        eclick
            MouseEvent with the position of the initial click
        erelease
            MouseEvent with the position of the final release of the mouse button
        """

        x1, y1 = eclick.xdata, eclick.ydata
        x2, y2 = erelease.xdata, erelease.ydata
        self.rs.set_visible(False)

        # set the visible property back to True so that the next move event shows the box
        self.rs.visible = True

        # signal with min lat, max lat, min lon, max lon
        self.box_select.emit(y1, y2, x1, x2)
        # print("(%3.2f, %3.2f) --> (%3.2f, %3.2f)" % (x1, y1, x2, y2))

    def set_extents_from_lines(self):
        """
        Set the maximum extent based on the line_object coordinates
        """

        lats = []
        lons = []
        for ln in self.line_objects:
            lats.append(self.line_objects[ln][0])
            lons.append(self.line_objects[ln][1])

        if not lats or not lons:
            self.set_extent(90, -90, 100, -100)
        else:
            lats = np.concatenate(lats)
            lons = np.concatenate(lons)

            self.set_extent(np.max(lats), np.min(lats), np.max(lons), np.min(lons))
        self.refresh_screen()

    def set_extents_from_surfaces(self):
        """
        Set the maximum extent based on the surface_objects coordinates
        """

        lats = []
        lons = []
        for surf in self.surface_objects:
            for lyrs in self.surface_objects[surf]:
                lats.append(self.surface_objects[surf][lyrs][0])
                lons.append(self.surface_objects[surf][lyrs][1])

        if not lats or not lons:
            self.set_extent(90, -90, 100, -100)
        else:
            lats = np.concatenate(lats)
            lons = np.concatenate(lons)

            self.set_extent(np.max(lats), np.min(lats), np.max(lons), np.min(lons))
        self.refresh_screen()

    def clear(self):
        """
        Clear all loaded data including surfaces and lines and refresh the screen
        """

        self.line_objects = {}
        self.surface_objects = {}
        self.active_layers = {}
        self.data_extents = {'min_lat': 999, 'max_lat': -999, 'min_lon': 999, 'max_lon': -999}
        self.selected_line_objects = []
        self.set_extent(90, -90, 100, -100)
        self.refresh_screen()

    def refresh_screen(self):
        """
        Reset to the original zoom/extents
        """

        self.axes.relim()
        self.axes.autoscale_view()

        self.fig.canvas.draw_idle()
        self.fig.canvas.flush_events()
