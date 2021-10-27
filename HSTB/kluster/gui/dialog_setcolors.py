from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster import kluster_variables


class ColorRanges(QtWidgets.QDialog):
    """
    Widget that allows you to manually start the dask client if you need to run it in a specific way.  If you don't
    use this, we just autostart a default LocalCluster.
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Set Color Ranges')

        self.ranges_layout = QtWidgets.QVBoxLayout()

        self.depth_box = QtWidgets.QGroupBox('Override Depth Ranges')
        self.depth_box.setCheckable(True)
        self.depth_box.setChecked(False)
        self.depth_layout = QtWidgets.QHBoxLayout()
        self.mindepth_lbl = QtWidgets.QLabel('Minimum')
        self.depth_layout.addWidget(self.mindepth_lbl)
        self.mindepth = QtWidgets.QLineEdit('0.0')
        self.depth_layout.addWidget(self.mindepth)
        self.maxdepth_lbl = QtWidgets.QLabel('Maximum')
        self.depth_layout.addWidget(self.maxdepth_lbl)
        self.maxdepth = QtWidgets.QLineEdit('0.0')
        self.depth_layout.addWidget(self.maxdepth)
        self.depth_box.setLayout(self.depth_layout)
        self.ranges_layout.addWidget(self.depth_box)

        self.density_box = QtWidgets.QGroupBox('Override Density Ranges')
        self.density_box.setCheckable(True)
        self.density_box.setChecked(False)
        self.density_layout = QtWidgets.QHBoxLayout()
        self.mindensity_lbl = QtWidgets.QLabel('Minimum')
        self.density_layout.addWidget(self.mindensity_lbl)
        self.mindensity = QtWidgets.QLineEdit('0')
        self.density_layout.addWidget(self.mindensity)
        self.maxdensity_lbl = QtWidgets.QLabel('Maximum')
        self.density_layout.addWidget(self.maxdensity_lbl)
        self.maxdensity = QtWidgets.QLineEdit('0')
        self.density_layout.addWidget(self.maxdensity)
        self.density_box.setLayout(self.density_layout)
        self.ranges_layout.addWidget(self.density_box)

        self.vunc_box = QtWidgets.QGroupBox('Override Vertical Uncertainty Ranges')
        self.vunc_box.setCheckable(True)
        self.vunc_box.setChecked(False)
        self.vunc_layout = QtWidgets.QHBoxLayout()
        self.minvunc_lbl = QtWidgets.QLabel('Minimum')
        self.vunc_layout.addWidget(self.minvunc_lbl)
        self.minvunc = QtWidgets.QLineEdit('0.0')
        self.vunc_layout.addWidget(self.minvunc)
        self.maxvunc_lbl = QtWidgets.QLabel('Maximum')
        self.vunc_layout.addWidget(self.maxvunc_lbl)
        self.maxvunc = QtWidgets.QLineEdit('0.0')
        self.vunc_layout.addWidget(self.maxvunc)
        self.vunc_box.setLayout(self.vunc_layout)
        self.ranges_layout.addWidget(self.vunc_box)

        self.hunc_box = QtWidgets.QGroupBox('Override Horizontal Uncertainty Ranges')
        self.hunc_box.setCheckable(True)
        self.hunc_box.setChecked(False)
        self.hunc_layout = QtWidgets.QHBoxLayout()
        self.minhunc_lbl = QtWidgets.QLabel('Minimum')
        self.hunc_layout.addWidget(self.minhunc_lbl)
        self.minhunc = QtWidgets.QLineEdit('0.0')
        self.hunc_layout.addWidget(self.minhunc)
        self.maxhunc_lbl = QtWidgets.QLabel('Maximum')
        self.hunc_layout.addWidget(self.maxhunc_lbl)
        self.maxhunc = QtWidgets.QLineEdit('0.0')
        self.hunc_layout.addWidget(self.maxhunc)
        self.hunc_box.setLayout(self.hunc_layout)
        self.ranges_layout.addWidget(self.hunc_box)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { " + kluster_variables.error_color + "; }")
        self.ranges_layout.addWidget(self.status_msg)

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.button_layout.addWidget(self.ok_button)
        self.button_layout.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.button_layout.addWidget(self.cancel_button)
        self.button_layout.addStretch(1)
        self.ranges_layout.addLayout(self.button_layout)

        self.setLayout(self.ranges_layout)
        self.cancelled = False

        self.ok_button.clicked.connect(self.ok_dialog)
        self.cancel_button.clicked.connect(self.cancel_dialog)

    def ok_dialog(self):
        """
        Start a new dask client with the options you have here.  Save to the cl attribute so the main window can pull
        it out after user hits OK.
        """

        try:
            float(self.mindepth.text())
        except:
            self.status_msg.setText('Minimum Depth {} is not a valid integer or floating point number'.format(self.mindepth.text()))
            return
        try:
            float(self.maxdepth.text())
        except:
            self.status_msg.setText('Maximum Depth {} is not a valid integer or floating point number'.format(self.maxdepth.text()))
            return
        try:
            int(self.mindensity.text())
        except:
            self.status_msg.setText('Minimum Density {} is not a valid integer'.format(self.mindensity.text()))
            return
        try:
            int(self.maxdensity.text())
        except:
            self.status_msg.setText('Maximum Density {} is not a valid integer'.format(self.maxdensity.text()))
            return
        try:
            float(self.minvunc.text())
        except:
            self.status_msg.setText('Minimum Vertical Uncertainty {} is not a valid integer or floating point number'.format(self.minvunc.text()))
            return
        try:
            float(self.maxvunc.text())
        except:
            self.status_msg.setText('Maximum Vertical Uncertainty {} is not a valid integer or floating point number'.format(self.maxvunc.text()))
            return
        try:
            float(self.minhunc.text())
        except:
            self.status_msg.setText('Minimum Horizontal Uncertainty {} is not a valid integer or floating point number'.format(self.minhunc.text()))
            return
        try:
            float(self.maxhunc.text())
        except:
            self.status_msg.setText('Maximum Horizontal Uncertainty {} is not a valid integer or floating point number'.format(self.maxhunc.text()))
            return
        self.cancelled = False
        self.accept()

    def cancel_dialog(self):
        self.cancelled = True
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ColorRanges()
    dlog.show()
    if dlog.exec_():
        pass
