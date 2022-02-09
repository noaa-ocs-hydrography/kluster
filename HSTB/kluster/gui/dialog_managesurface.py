import os
from datetime import datetime, timezone

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from matplotlib.backends.backend_qt5agg import (FigureCanvasQTAgg, NavigationToolbar2QT as NavigationToolbar)
import matplotlib.pyplot as plt


class ManageSurfaceDialog(QtWidgets.QWidget):
    """
    Dialog contains a summary of the surface data and some options for altering the data contained within.
    """
    update_surface = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setMinimumWidth(500)
        self.setMinimumHeight(400)

        self.setWindowTitle('Manage Surface')
        layout = QtWidgets.QVBoxLayout()

        self.basicdata = QtWidgets.QTextEdit()
        self.basicdata.setReadOnly(True)
        self.basicdata.setText('')
        layout.addWidget(self.basicdata)

        self.managelabel = QtWidgets.QLabel('Manage: ')
        layout.addWidget(self.managelabel)

        calclayout = QtWidgets.QHBoxLayout()
        self.calcbutton = QtWidgets.QPushButton('Calculate')
        calclayout.addWidget(self.calcbutton)
        self.calcdropdown = QtWidgets.QComboBox()
        self.calcdropdown.addItems(['area, sq nm', 'area, sq meters'])
        calclayout.addWidget(self.calcdropdown)
        self.calcanswer = QtWidgets.QLineEdit('')
        self.calcanswer.setReadOnly(True)
        calclayout.addWidget(self.calcanswer)
        layout.addLayout(calclayout)

        plotlayout = QtWidgets.QHBoxLayout()
        self.plotbutton = QtWidgets.QPushButton('Plot')
        plotlayout.addWidget(self.plotbutton)
        self.plotdropdown = QtWidgets.QComboBox()
        szepolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Preferred)
        szepolicy.setHorizontalStretch(2)
        self.plotdropdown.setSizePolicy(szepolicy)
        plotlayout.addWidget(self.plotdropdown)
        layout.addLayout(plotlayout)

        self.calcbutton.clicked.connect(self.calculate_statistic)
        self.plotbutton.clicked.connect(self.generate_plot)

        self.setLayout(layout)
        self.surf = None

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
        allplots.sort()
        self.plotdropdown.addItems(allplots)

    def calculate_statistic(self, e):
        stat = self.calcdropdown.currentText()
        if stat == 'area, sq nm':
            self.calcanswer.setText(str(round(self.surf.coverage_area_square_nm, 3)))
        elif stat == 'area, sq meters':
            self.calcanswer.setText(str(round(self.surf.coverage_area_square_meters, 3)))
        else:
            raise ValueError(f'Unrecognized input for calculating statistic: {stat}')

    def generate_plot(self, e):
        plotname = self.plotdropdown.currentText()
        plt.figure()
        if plotname in ['Histogram, Intensity (dB)', 'Histogram, Depth (meters)']:
            self.surf.plot_z_histogram()
        elif plotname in ['Histogram, Density (count)']:
            self.surf.plot_density_histogram()
        elif plotname in ['Histogram, Density (sq meters)']:
            self.surf.plot_density_per_square_meter_histogram()
        elif plotname in ['Depth vs Density (count)']:
            self.surf.plot_density_vs_depth()
        elif plotname in ['Depth vs Density (sq meters)']:
            self.surf.plot_density_per_square_meter_vs_depth()
        elif plotname in ['Histogram, vertical uncertainty (2 sigma, meters)']:
            self.surf.plot_vertical_uncertainty_histogram()
        elif plotname in ['Histogram, horizontal uncertainty (2 sigma, meters)']:
            self.surf.plot_horizontal_uncertainty_histogram()


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
