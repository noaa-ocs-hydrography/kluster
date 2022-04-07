import numpy as np

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
                                      'EMODnet Bathymetry (internet required)'])
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

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.surf_transparency_label = QtWidgets.QLabel('Surface Transparency: ')
        self.hlayout_three.addWidget(self.surf_transparency_label)
        self.hlayout_three.addStretch(1)
        self.surf_transparency = QtWidgets.QLineEdit('0')
        self.hlayout_three.addWidget(self.surf_transparency)
        self.surf_transparency_sign_label = QtWidgets.QLabel('%')
        self.hlayout_three.addWidget(self.surf_transparency_sign_label)

        self.hlayout_four = QtWidgets.QHBoxLayout()
        self.color_range_label = QtWidgets.QLabel('Color Ranges: ')
        self.hlayout_four.addWidget(self.color_range_label)
        self.hlayout_four.addStretch(1)
        self.color_range_select = QtWidgets.QComboBox()
        self.hlayout_four.addWidget(self.color_range_select)

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.color_range_box = QtWidgets.QGroupBox('Override')
        self.color_range_box.setCheckable(True)
        self.color_range_box.setChecked(False)
        self.range_group_layout = QtWidgets.QVBoxLayout()
        self.minrange_layout = QtWidgets.QHBoxLayout()
        self.color_range_min_label = QtWidgets.QLabel('Minimum')
        self.minrange_layout.addWidget(self.color_range_min_label)
        self.color_range_min_value = QtWidgets.QLineEdit('0.0')
        self.minrange_layout.addWidget(self.color_range_min_value)
        self.minrange_layout.addStretch()
        self.range_group_layout.addLayout(self.minrange_layout)
        self.maxrange_layout = QtWidgets.QHBoxLayout()
        self.color_range_max_label = QtWidgets.QLabel('Maximum')
        self.maxrange_layout.addWidget(self.color_range_max_label)
        self.color_range_max_value = QtWidgets.QLineEdit('0.0')
        self.maxrange_layout.addWidget(self.color_range_max_value)
        self.maxrange_layout.addStretch()
        self.range_group_layout.addLayout(self.maxrange_layout)
        self.color_range_box.setLayout(self.range_group_layout)
        self.hlayout_five.addWidget(self.color_range_box)

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
        layout.addLayout(self.hlayout_three)
        layout.addLayout(self.hlayout_four)
        layout.addLayout(self.hlayout_five)
        layout.addStretch()
        layout.addLayout(self.hlayout_six)
        self.setLayout(layout)

        self.canceled = False
        self.color_ranges = {}

        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)
        self.color_range_select.currentTextChanged.connect(self._update_color_ranges)
        self.color_range_box.clicked.connect(self._update_override_box)
        self.color_range_min_value.textChanged.connect(self._update_min_val)
        self.color_range_max_value.textChanged.connect(self._update_max_val)

        self.read_settings()
        # self.resize(600, 200)

    def set_color_ranges(self, color_ranges: dict):
        self.color_ranges = color_ranges
        self.color_range_select.clear()
        self.color_range_select.addItems(list(color_ranges.keys()))
        self._update_color_ranges(None)

    def _update_color_ranges(self, e):
        cur_band = self.color_range_select.currentText()
        if cur_band:
            try:
                data = self.color_ranges[cur_band]
                override, minval, maxval = data[0:3]
            except:
                print('dialog_layer_settings: ERROR - Unable to load data for band {}'.format(cur_band))
                return
            self.color_range_box.setChecked(override)
            self.color_range_min_value.setText(str(minval))
            self.color_range_max_value.setText(str(maxval))

    def _update_override_box(self, e):
        cur_band = self.color_range_select.currentText()
        override = self.color_range_box.isChecked()
        self.color_ranges[cur_band][0] = override
        if override:
            self.color_range_min_value.setDisabled(False)
            self.color_range_max_value.setDisabled(False)
        else:
            self.color_range_min_value.setDisabled(True)
            self.color_range_max_value.setDisabled(True)

    def _update_min_val(self, e):
        cur_band = self.color_range_select.currentText()
        minval = self.color_range_min_value.text()
        self.color_ranges[cur_band][1] = float(minval)

    def _update_max_val(self, e):
        cur_band = self.color_range_select.currentText()
        maxval = self.color_range_max_value.text()
        self.color_ranges[cur_band][2] = float(maxval)

    def return_layer_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if not self.canceled:
            try:
                transp = int(self.transparency.text()) / 100
                transp = np.clip(transp, 0, 1)
            except ValueError:
                print('Layer_Settings: transparency={} is invalid, must be an integer between 0 and 100, defaulting to 0'.format(self.transparency.text()))
                transp = 0
            try:
                surf_transp = int(self.surf_transparency.text()) / 100
                surf_transp = np.clip(surf_transp, 0, 1)
            except ValueError:
                print('Layer_Settings: surface_transparency={} is invalid, must be an integer between 0 and 100, defaulting to 0'.format(self.surf_transparency.text()))
                surf_transp = 0
            opts = {'layer_background': self.layer_dropdown.currentText(),
                    'layer_transparency': transp,
                    'surface_transparency': surf_transp,
                    'color_ranges': self.color_ranges}
        else:
            opts = None
        return opts

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        print('Layer settings saved')
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
        settings.setValue('Kluster/layer_settings_surfacetransparency', new_sets['surface_transparency'])

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
            if settings.value('Kluster/layer_settings_surfacetransparency'):
                self.surf_transparency.setText(str(min(100, int(float(settings.value('Kluster/layer_settings_surfacetransparency')) * 100))))
        except AttributeError:
            # no settings exist yet for this app, .lower failed
            pass


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = LayerSettingsDialog()
    dlog.set_color_ranges({'Depth': [False, 1, 10], 'Uncertainty': [True, 2.3, 5]})
    dlog.show()
    if dlog.exec_():
        print(dlog.return_layer_options())
