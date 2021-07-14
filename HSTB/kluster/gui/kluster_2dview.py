from HSTB.kluster.gui.backends._qt import qgis_enabled
if qgis_enabled:
    backend = 'qgis'
    from HSTB.kluster.gui.backends._qgis import *
else:
    raise EnvironmentError('QGIS not found, Unable to run Kluster')
    # backend = 'cartopy'
    # from HSTB.kluster.gui.backends._cartopy import *


class Kluster2dview(MapView):
    """
    Generate 2dview from one of the backends, _qgis offers the most functionality and is the preferred backend
    """

    def __init__(self, parent=None, settings=None):
        super().__init__(parent=parent, settings=settings)

    def set_background(self, layername: str, transparency: float, surface_transparency: float):
        super().set_background(layername, transparency, surface_transparency)

    def set_extent(self, max_lat: float, min_lat: float, max_lon: float, min_lon: float, buffer: bool = True):
        super().set_extent(max_lat, min_lat, max_lon, min_lon, buffer)

    def add_line(self, line_name: str, lats: np.ndarray, lons: np.ndarray, refresh: bool = False):
        super().add_line(line_name, lats, lons, refresh)

    def remove_line(self, line_name, refresh=False):
        super().remove_line(line_name, refresh)

    def add_surface(self, data_block: list):
        if backend == 'qgis':
            add_surface, surface_layer_name, data, geo_transform, crs, resolution = data_block
            super().add_surface(add_surface, surface_layer_name, data, geo_transform, crs, resolution)
        elif backend == 'cartopy':
            add_surface, surface_layer_name, x, y, z, crs = data_block
            super().add_surface(add_surface, surface_layer_name, x, y, z, crs)

    def hide_surface(self, surfname, lyrname, resolution):
        super().hide_surface(surfname, lyrname, resolution)

    def show_surface(self, surfname: str, lyrname: str, resolution):
        return super().show_surface(surfname, lyrname, resolution)

    def remove_surface(self, surfname, resolution):
        super().remove_surface(surfname, resolution)

    def change_line_colors(self, line_names, color):
        super().change_line_colors(line_names, color)

    def reset_line_colors(self):
        super().reset_line_colors()

    def set_extents_from_lines(self, subset_lines: list = None):
        super().set_extents_from_lines(subset_lines)

    def set_extents_from_surfaces(self, subset_surf: str = None, resolution: float = None):
        super().set_extents_from_surfaces(subset_surf, resolution)

    def clear(self):
        super().clear()

    def refresh_screen(self):
        super().refresh_screen()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    f = Kluster2dview()
    f.show()
    sys.exit(app.exec_())
