import os
import logging
from datetime import datetime, timezone

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import ManageDialog
from matplotlib.backends.backend_qt5agg import (FigureCanvasQTAgg, NavigationToolbar2QT as NavigationToolbar)
import matplotlib.pyplot as plt


class ManageSurfaceDialog(ManageDialog):
    """
    Dialog contains a summary of the surface data and some options for altering the data contained within.
    """
    update_surface = Signal(str)

    def __init__(self, parent=None, settings=None):
        super().__init__(parent, 'Manage Surface', 'ManageSurfaceDialog', settings)

        self.calcdropdown.addItems(['area, sq nm', 'area, sq meters'])

    def populate(self, surf):
        """
        Examine the surface and populate the controls with the correct data
        """

        self.surf = surf
        self.managelabel.setText('Manage: {}'.format(os.path.split(surf.output_folder)[1]))
        self.basicdata.setText(surf.__repr__())
        allplots = ['Histogram, Density (count)', 'Histogram, Density (sq meters)']
        if self.surf.is_backscatter:
            allplots += ['Histogram, Intensity (dB)']
        else:
            allplots += ['Histogram, Depth (meters)', 'Depth vs Density (count)', 'Depth vs Density (sq meters)']
            if 'vertical_uncertainty' in self.surf.layer_names:
                allplots += ['Histogram, vertical uncertainty (2 sigma, meters)',
                             'Histogram, horizontal uncertainty (2 sigma, meters)']
            elif 'total_uncertainty' in self.surf.layer_names:
                allplots += ['Histogram, total uncertainty (2 sigma, meters)']
        allplots.sort()
        self.plotdropdown.addItems(allplots)

    def plot_tooltip_config(self, e):
        plotname = self.plotdropdown.currentText()
        if plotname == 'Histogram, Intensity (dB)':
            self.plotdropdown.setToolTip('Compute a histogram of the Intensity layer values in the gridded data in this surface')
        elif plotname == 'Histogram, Depth (meters)':
            self.plotdropdown.setToolTip('Compute a histogram of the Depth layer values in the gridded data in this surface')
        elif plotname in ['Histogram, Density (count)']:
            self.plotdropdown.setToolTip('Compute a histogram of soundings per cell across all tiles in the grid')
        elif plotname in ['Histogram, Density (sq meters)']:
            self.plotdropdown.setToolTip('Compute a histogram of soundings per cell per square meter across all tiles in the grid')
        elif plotname in ['Depth vs Density (count)']:
            self.plotdropdown.setToolTip('Plot the average depth vs density, where density is the number of soundings per cell')
        elif plotname in ['Depth vs Density (sq meters)']:
            self.plotdropdown.setToolTip('Plot the average depth vs density, where density is the density per cell per square meter')
        elif plotname in ['Histogram, vertical uncertainty (2 sigma, meters)']:
            self.plotdropdown.setToolTip('Compute a histogram of the Vertical Uncertainty layer values in the gridded data in this surface')
        elif plotname in ['Histogram, horizontal uncertainty (2 sigma, meters)']:
            self.plotdropdown.setToolTip('Compute a histogram of the Horizontal Uncertainty layer values in the gridded data in this surface')
        elif plotname in ['Histogram, total uncertainty (2 sigma, meters)']:
            self.plotdropdown.setToolTip('Compute a histogram of the Total Uncertainty layer values in the gridded data in this surface')

    def calc_tooltip_config(self, e):
        calcname = self.calcdropdown.currentText()
        if calcname == 'area, sq nm':
            self.calcdropdown.setToolTip('Calculate the total area of the grid (only the populated cells) in square nautical miles')
        elif calcname == 'area, sq meters':
            self.calcdropdown.setToolTip('Calculate the total area of the grid (only the populated cells) in square meters')

    def run_tooltip_config(self, e):
        pass

    def calculate_statistic(self, e):
        stat = self.calcdropdown.currentText()
        if stat == 'area, sq nm':
            self.calcanswer.setText(str(round(self.surf.coverage_area_square_nm, 3)))
        elif stat == 'area, sq meters':
            self.calcanswer.setText(str(round(self.surf.coverage_area_square_meters, 3)))
        else:
            raise ValueError(f'Unrecognized input for calculating statistic: {stat}')

    def generate_plot(self, e):
        bincount = int(self.bincount.text())
        plotname = self.plotdropdown.currentText()
        plt.figure()
        if plotname in ['Histogram, Intensity (dB)', 'Histogram, Depth (meters)']:
            self.surf.plot_z_histogram(number_of_bins=bincount)
        elif plotname in ['Histogram, Density (count)']:
            self.surf.plot_density_histogram(number_of_bins=bincount)
        elif plotname in ['Histogram, Density (sq meters)']:
            self.surf.plot_density_per_square_meter_histogram(number_of_bins=bincount)
        elif plotname in ['Depth vs Density (count)']:
            self.surf.plot_density_vs_depth()
        elif plotname in ['Depth vs Density (sq meters)']:
            self.surf.plot_density_per_square_meter_vs_depth()
        elif plotname in ['Histogram, vertical uncertainty (2 sigma, meters)']:
            self.surf.plot_vertical_uncertainty_histogram(number_of_bins=bincount)
        elif plotname in ['Histogram, horizontal uncertainty (2 sigma, meters)']:
            self.surf.plot_horizontal_uncertainty_histogram(number_of_bins=bincount)
        elif plotname in ['Histogram, total uncertainty (2 sigma, meters)']:
            self.surf.plot_total_uncertainty_histogram(number_of_bins=bincount)
        # set always on top
        plt.gcf().canvas.manager.window.setWindowFlags(self.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
        plt.gcf().canvas.manager.window.show()

    def run_function(self, e):
        pass


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ManageSurfaceDialog()
    # from HSTB.kluster.fqpr_convenience import reload_surface
    # dlog.populate(reload_surface(r"C:\collab\dasktest\data_dir\outputtest\tj_patch_test_710_20220104_203429"))
    dlog.show()
    app.exec_()
