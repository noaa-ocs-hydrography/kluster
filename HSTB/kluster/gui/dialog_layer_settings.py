import numpy as np
import logging

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import SaveStateDialog


class LayerSettingsDialog(SaveStateDialog):
    """
    Dialog contains all layer settings for the 2d view.
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='layer_settings')

        self.setWindowTitle('Layer Settings')
        layout = QtWidgets.QVBoxLayout()

        self.layer_msg = QtWidgets.QLabel('Background Layer:')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.layer_dropdown = QtWidgets.QComboBox()
        self.layer_dropdown.addItems(['None', 'Default', 'VDatum Coverage (VDatum required)', 'OpenStreetMap (internet required)',
                                      'Satellite (internet required)', 'NOAA RNC (internet required)',
                                      'NOAA ENC (internet required)',
                                      'NOAA Chart Display Service (internet required)',
                                      'GEBCO Grid (internet required)',
                                      'EMODnet Bathymetry (internet required)',
                                      'NowCoast Global Sea Surface Temp (internet required)',
                                      'NowCoast High Resolution Sea Surface Temp (internet required)'])
        self.hlayout_one.addWidget(self.layer_dropdown)
        self.hlayout_one.addStretch(1)

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.transparency_label = QtWidgets.QLabel('Background Transparency: ')
        self.hlayout_two.addWidget(self.transparency_label)
        self.hlayout_two.addStretch(1)
        self.transparency = QtWidgets.QLineEdit('0')
        self.hlayout_two.addWidget(self.transparency)
        self.transparency_sign_label = QtWidgets.QLabel('%')
        self.hlayout_two.addWidget(self.transparency_sign_label)

        self.hlayout_six = QtWidgets.QHBoxLayout()
        self.hlayout_six.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_six.addWidget(self.ok_button)
        self.hlayout_six.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_six.addWidget(self.cancel_button)
        self.hlayout_six.addStretch(1)

        layout.addWidget(self.layer_msg)
        layout.addLayout(self.hlayout_one)
        layout.addLayout(self.hlayout_two)
        layout.addStretch()
        layout.addLayout(self.hlayout_six)
        self.setLayout(layout)

        self.canceled = False
        self.color_ranges = {}

        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)

        self.read_settings()

    def return_layer_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if not self.canceled:
            try:
                transp = int(self.transparency.text()) / 100
                transp = np.clip(transp, 0, 1)
            except ValueError:
                self.print('Layer_Settings: transparency={} is invalid, must be an integer between 0 and 100, defaulting to 0'.format(self.transparency.text()), logging.WARNING)
                transp = 0
            opts = {'layer_background': self.layer_dropdown.currentText(),
                    'layer_transparency': transp}
        else:
            opts = None
        return opts

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        self.print('Layer settings saved', logging.INFO)
        self.canceled = False
        self.save_settings()
        self.accept()

    def cancel(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()

    def save_settings(self):
        """
        Save the settings to the Qsettings registry

        Overriding the basic savestatedialog stuff with this custom code
        """
        settings = self.settings_object
        new_sets = self.return_layer_options()
        settings.setValue('Kluster/layer_settings_background', new_sets['layer_background'])
        settings.setValue('Kluster/layer_settings_transparency', new_sets['layer_transparency'])

    def read_settings(self):
        """
        Read from the Qsettings registry

        Overriding the basic savestatedialog stuff with this custom code
        """
        settings = self.settings_object

        try:
            self.layer_dropdown.setCurrentText(settings.value('Kluster/layer_settings_background'))
            if settings.value('Kluster/layer_settings_transparency'):
                self.transparency.setText(str(min(100, int(float(settings.value('Kluster/layer_settings_transparency')) * 100))))
        except AttributeError:
            # no settings exist yet for this app, .lower failed
            pass


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = LayerSettingsDialog()
    dlog.show()
    if dlog.exec_():
        print(dlog.return_layer_options())
