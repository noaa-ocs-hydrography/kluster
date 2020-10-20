from PySide2 import QtWidgets
import pyqtgraph.opengl as gl
import numpy as np
import sys

from matplotlib import cm


# open3d is nice, but kind of rigid in how it is built.  Not very changeable.  Also QT support might be coming but is
#  not there yet.  They are using this im gui thing?
# https://github.com/intel-isl/Open3D/issues/1161

# this instead?
# https://pyqtgraph.readthedocs.io/en/latest/how_to_use.html#embedding-widgets-inside-pyqt-applications

# mayavi?
# https://docs.enthought.com/mayavi/mayavi/auto/example_qt_embedding.html#example-qt-embedding

# this has basic plotting and is good for dask
# https://hvplot.holoviz.org/user_guide/index.html

# holoviz for plots straight from dask
# http://holoviews.org/user_guide/Dashboards.html

# dash + datashader
# https://github.com/plotly/plotly.py/issues/1266

# https://doc.qt.io/qt-5/qtdatavisualization-scatter-example.html


class Kluster3dview(gl.GLViewWidget):
    """
    Currently using pyqtgraph opengl widgets for 3d plotting.  It is pretty basic, and I haven't spent much time on
    this.  This is sort of a placeholder for what we might want eventually.

    Can plot points and surfaces.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.opts['distance'] = 20
        self.setWindowTitle('kluster 3dview')

        self.grid = gl.GLGridItem()
        self.addItem(self.grid)

        self.ptstore = []
        self.surfstore = []

    def add_point_dataset(self, x, y, z):
        """
        Plot the provided xyz numpy arrays.  Centers the data in the plot using translate.

        Store the plot object to self.ptstore so we can remove it later.

        Parameters
        ----------
        x: numpy array, x value
        y: numpy array, y value
        z: numpy array, z value

        """
        pts = np.c_[x, y, z]

        cmap = cm.get_cmap('viridis', 100)
        normz = (z/np.max(z) * 99).astype(np.int16)
        color = cmap.colors[normz]

        scatterplt = gl.GLScatterPlotItem(pos=pts, color=color, size=1)
        scatterplt.translate(-np.nanmean(x), -np.nanmean(y), -np.nanmean(z))
        scatterplt.setData()

        self.ptstore.append(scatterplt)
        self.addItem(self.ptstore[-1])

    def add_surface_dataset(self, x, y, z):
        """
        Plot the provided xyz node locations as a surface.  Centers the data in the plot using translate

        Store the plot object to self.ptstore so we can remove it later.

        Parameters
        ----------
        x: numpy array, x value
        y: numpy array, y value
        z: numpy array, z value

        """
        surfplt = gl.GLSurfacePlotItem(x=x, y=y, z=z, shader='normalColor')
        surfplt.translate(-np.nanmean(x), -np.nanmean(y), -np.nanmean(z))
        surfplt.setData()

        self.surfstore.append(surfplt)
        self.addItem(self.surfstore[-1])

    def clear_plot_area(self):
        """
        Clear all the plotted surfaces/points that were stored to the stores.
        """
        for sf in self.surfstore:
            self.removeItem(sf)
        for pd in self.ptstore:
            self.removeItem(pd)
        self.ptstore = []
        self.surfstore = []


if __name__ == '__main__':
    app = QtWidgets.QApplication()

    try:
        test_window_one = Kluster3dview()
        from HSTB.kluster.fqpr_convenience import reload_data
        fq = reload_data(r"C:\collab\dasktest\data_dir\hassler_acceptance\refsurf\converted")
        test_window_one.add_point_dataset(fq.soundings.x.values, fq.soundings.y.values, fq.soundings.z.values)
        fq.client.close()
        test_window_one.show()

        test_window_two = Kluster3dview()
        from HSTB.kluster.fqpr_surface import BaseSurface
        surf = BaseSurface(from_file=r"C:\collab\dasktest\data_dir\hassler_acceptance\refsurf\converted\surf.npz")
        x, y, z, valid = surf.return_surf_xyz()
        test_window_two.add_surface_dataset(x, y, z)
        test_window_two.show()

    except AttributeError:  # cant find the folder, so use this test data
        # use some non-(0,0,0) centered data to test the translation
        test_window_one = Kluster3dview()
        test_window_one.add_point_dataset(np.array([2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007]),
                                          np.array([2001, 2004, 2005, 2006, 2007, 2008, 2009, 2010]),
                                          np.array([2002, 2007, 2008, 2009, 2010, 2011, 2012, 2013]))
        test_window_one.show()

        test_window_two = Kluster3dview()
        test_window_two.add_surface_dataset(np.array([2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007]),
                                            np.array([2001, 2004, 2005, 2006, 2007, 2008, 2009, 2010]),
                                            np.random.random((8,8)))
        test_window_two.show()

    sys.exit(app.exec_())
