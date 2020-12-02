# have to import PySide2 first so the matplotlib backend figures out we want PySide2
from PySide2 import QtCore, QtGui, QtWidgets

# apparently there is some problem with PySide2 + Pyinstaller, for future reference
# https://stackoverflow.com/questions/56182256/figurecanvas-not-interpreted-as-qtwidget-after-using-pyinstaller/62055972#62055972
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg, NavigationToolbar2QT
from matplotlib.figure import Figure
from matplotlib.widgets import RectangleSelector

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import sys
import numpy as np

# see block plots for surface maybe
# https://scitools.org.uk/cartopy/docs/v0.13/matplotlib/advanced_plotting.html


class Kluster2dview(FigureCanvasQTAgg):
    """
    Map view using cartopy/matplotlib to view lines and surfaces with a map context.
    """
    box_select = QtCore.Signal(float, float, float, float)

    def __init__(self, parent=None, width=5, height=4, dpi=100, map_proj=ccrs.PlateCarree()):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.map_proj = map_proj
        self.axes = self.fig.add_subplot(projection=map_proj)
        # self.axes.coastlines(resolution='10m')
        self.fig.add_axes(self.axes)
        #self.fig.subplots_adjust(left=0, right=1, bottom=0, top=1)

        self.axes.gridlines(draw_labels=True, crs=self.map_proj)
        self.axes.add_feature(cfeature.LAND)
        self.axes.add_feature(cfeature.COASTLINE)

        self.line_objects = {}
        self.surface_objects = {}
        self.active_layers = {}
        self.data_extents = {'min_lat': 999, 'max_lat': -999, 'min_lon': 999, 'max_lon': -999}
        self.selected_line_objects = []

        super(Kluster2dview, self).__init__(self.fig)

        self.navi_toolbar = NavigationToolbar2QT(self.fig.canvas, self)
        self.rs = RectangleSelector(self.axes, self.line_select_callback, drawtype='box', useblit=False,
                                    button=[1], minspanx=5, minspany=5, spancoords='pixels', interactive=True)
        self.set_extent(40, -40, 30, -30)

    def set_extent(self, max_lat, min_lat, max_lon, min_lon, buffer=True):
        self.data_extents['min_lat'] = np.min([min_lat, self.data_extents['min_lat']])
        self.data_extents['max_lat'] = np.max([max_lat, self.data_extents['max_lat']])
        self.data_extents['min_lon'] = np.min([min_lon, self.data_extents['min_lon']])
        self.data_extents['max_lon'] = np.max([max_lon, self.data_extents['max_lon']])

        if self.data_extents['min_lat'] != 999 and self.data_extents['max_lat'] != -999 and self.data_extents[
                             'min_lon'] != 999 and self.data_extents['max_lon'] != -999:
            if buffer:
                lat_buffer = np.max([(max_lat - min_lat) * 1.5, 1])
                lon_buffer = np.max([(max_lon - min_lon) * 1.5, 1])
            else:
                lat_buffer = 0
                lon_buffer = 0
            self.axes.set_extent([np.clip(min_lon - lon_buffer, -180, 180), np.clip(max_lon + lon_buffer, -180, 180),
                                  np.clip(min_lat - lat_buffer, -90, 90), np.clip(max_lat + lat_buffer, -90, 90)],
                                 crs=ccrs.Geodetic())

    def add_line(self, line_name, lats, lons):
        lne = self.axes.plot(lons, lats, color='blue', linewidth=2, transform=ccrs.Geodetic())
        self.line_objects[line_name] = [lats, lons, lne[0]]
        self.refresh_screen()

    def remove_line(self, line_name):
        if line_name in self.line_objects:
            lne = self.line_objects[line_name][2]
            lne.remove()
            self.line_objects.pop(line_name)
        self.refresh_screen()

    def add_surface(self, surfname, lyrname, surfx, surfy, surfz, surf_crs):
        try:
            addlyr = True
            if lyrname in self.active_layers[surfname]:
                addlyr = False
        except KeyError:
            addlyr = True

        if addlyr:
            self._add_surface_layer(surfname, lyrname, surfx, surfy, surfz, surf_crs)
            self.refresh_screen()

    def hide_surface(self, surfname, lyrname):
        try:
            hidelyr = True
            if lyrname not in self.active_layers[surfname]:
                hidelyr = False
        except KeyError:
            hidelyr = False

        if hidelyr:
            self._hide_surface_layer(surfname, lyrname)

    def remove_surface(self, surfname):
        if surfname in self.surface_objects:
            for lyr in self.surface_objects[surfname]:
                self.hide_surface(surfname, lyr)
                surf = self.surface_objects[surfname][lyr][2]
                surf.remove()
                self.surface_objects.pop(surfname)
        self.refresh_screen()

    def _add_surface_layer(self, surfname, lyrname, surfx, surfy, surfz, surf_crs):
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
            surfplt = self.axes.pcolormesh(lons, lats, surfz.T, vmin=vmin, vmax=vmax, transform=self.map_proj)
            self._add_to_active_layers(surfname, lyrname)
            self._add_to_surface_objects(surfname, lyrname, [lats, lons, surfplt])
            if not self.line_objects and not self.surface_objects:  # if this is the first thing you are loading, jump to it's extents
                self.set_extents_from_surfaces()
        else:
            surfplt = self.surface_objects[surfname][lyrname][2]
            newsurfplt = self.axes.add_artist(surfplt)
            # update the object with the newly added artist
            self.surface_objects[surfname][lyrname][2] = newsurfplt
            self._add_to_active_layers(surfname, lyrname)

    def _hide_surface_layer(self, surfname, lyrname):
        surfplt = self.surface_objects[surfname][lyrname][2]
        surfplt.remove()
        self._remove_from_active_layers(surfname, lyrname)
        self.refresh_screen()

    def _add_to_active_layers(self, surfname, lyrname):
        if surfname in self.active_layers:
            self.active_layers[surfname].append(lyrname)
        else:
            self.active_layers[surfname] = [lyrname]

    def _add_to_surface_objects(self, surfname, lyrname, data):
        if surfname in self.surface_objects:
            self.surface_objects[surfname][lyrname] = data
        else:
            self.surface_objects[surfname] = {lyrname: data}

    def _remove_from_active_layers(self, surfname, lyrname):
        if surfname in self.active_layers:
            if lyrname in self.active_layers[surfname]:
                self.active_layers[surfname].remove(lyrname)

    def _remove_from_surface_objects(self, surfname, lyrname):
        if surfname in self.surface_objects:
            if lyrname in self.surface_objects[surfname]:
                self.surface_objects[surfname].pop(lyrname)

    def change_line_colors(self, line_names, color):
        for line in line_names:
            lne = self.line_objects[line][2]
            lne.set_color(color)
            self.selected_line_objects.append(lne)
        self.refresh_screen()

    def reset_colors(self):
        for lne in self.selected_line_objects:
            lne.set_color('b')
        self.selected_line_objects = []
        self.refresh_screen()

    def line_select_callback(self, eclick, erelease):
        x1, y1 = eclick.xdata, eclick.ydata
        x2, y2 = erelease.xdata, erelease.ydata
        self.rs.set_visible(False)

        # set the visible property back to True so that the next move event shows the box
        self.rs.visible = True

        # signal with min lat, max lat, min lon, max lon
        self.box_select.emit(y1, y2, x1, x2)
        # print("(%3.2f, %3.2f) --> (%3.2f, %3.2f)" % (x1, y1, x2, y2))

    def set_extents_from_lines(self):
        lats = []
        lons = []
        for ln in self.line_objects:
            lats.append(self.line_objects[ln][0])
            lons.append(self.line_objects[ln][1])

        if not lats or not lons:
            self.set_extent(40, -40, 30, -30)
        else:
            lats = np.concatenate(lats)
            lons = np.concatenate(lons)

            self.set_extent(np.max(lats), np.min(lats), np.max(lons), np.min(lons))
        self.refresh_screen()

    def set_extents_from_surfaces(self):
        lats = []
        lons = []
        for surf in self.surface_objects:
            for lyrs in self.surface_objects[surf]:
                lats.append(self.surface_objects[surf][lyrs][0])
                lons.append(self.surface_objects[surf][lyrs][1])

        if not lats or not lons:
            self.set_extent(40, -40, 30, -30)
        else:
            lats = np.concatenate(lats)
            lons = np.concatenate(lons)

            self.set_extent(np.max(lats), np.min(lats), np.max(lons), np.min(lons))
        self.refresh_screen()

    def refresh_screen(self):
        self.axes.relim()
        self.axes.autoscale_view()

        self.fig.canvas.draw()
        self.fig.canvas.flush_events()


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    f = Kluster2dview()
    #f.add_line('test', [40, 45, 50], [-10, 10, 20])
    f.show()
    sys.exit(app.exec_())
