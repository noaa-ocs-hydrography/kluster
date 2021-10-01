import os
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled
if qgis_enabled:
    os.environ['PYDRO_GUI_FORCE_PYQT'] = 'True'
    from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui

from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables
from HSTB.kluster.modules.georeference import set_vyperdatum_vdatum_path


class SettingsDialog(SaveStateDialog):
    """
    Dialog contains all the processing steps post-conversion.  Use return_processing_options to get the kwargs to feed
    the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='settings')

        self.setWindowTitle('Settings')
        layout = QtWidgets.QVBoxLayout()

        self.parallel_write = QtWidgets.QCheckBox('Enable Parallel Writes')
        self.parallel_write.setChecked(True)
        self.parallel_write.setToolTip('If checked, Kluster will write to the hard drive in parallel, disabling this ' +
                                       'is a useful step in troubleshooting PermissionErrors.')

        self.keep_waterline_changes = QtWidgets.QCheckBox('Retain Waterline Changes')
        self.keep_waterline_changes.setChecked(True)
        self.keep_waterline_changes.setToolTip('If checked (only applicable if you are using a Vessel File), Kluster will save all ' +
                                               'waterline changes in later multibeam files to the vessel file.  \nUncheck this if you ' +
                                               'do not want changes in waterline to be new entries in the vessel file.')

        self.force_coordinate_match = QtWidgets.QCheckBox('Force all days to have the same Coordinate System')
        self.force_coordinate_match.setChecked(True)
        self.force_coordinate_match.setToolTip('By default, Kluster will assign an automatic UTM zone number to each day of data.  If you ' +
                                               'have data that crosses UTM zones, you might find that a project \ncontains data with ' +
                                               'different coordinate systems.  Check this box if you want to force all days in a project ' +
                                               '\nto have the same coordinate system as the first Converted entry in the Project Tree list.  use_epsg in project ' +
                                               'settings will ignore this.')

        self.hlayout_one_one = QtWidgets.QHBoxLayout()
        self.auto_processing_mode_label = QtWidgets.QLabel('Process Mode: ')
        self.hlayout_one_one.addWidget(self.auto_processing_mode_label)
        self.auto_processing_mode = QtWidgets.QComboBox()
        autooptions = ['normal', 'convert_only', 'concatenate']
        self.auto_processing_mode.addItems(autooptions)
        self.auto_processing_mode.setToolTip('Controls the processing actions that appear when new data is added or settings are changed.\n' +
                                             'See the following mode explanations for the currently available options\n\n' +
                                             'normal = data is converted and processed as it comes in, where each line added would reprocess the whole day\n' +
                                             'convert only = data is only converted, data is never automatically processed\n' +
                                             'concatenate = data is converted as lines are added and each line is processed individually.  Similar to normal\n' +
                                             '  mode but more efficient if you are adding lines as they are acquired, normal mode would do a full reprocess of\n' +
                                             '  the day after each new line is added')
        self.hlayout_one_one.addWidget(self.auto_processing_mode)
        self.hlayout_one_one.addStretch()

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.vdatum_label = QtWidgets.QLabel('VDatum Directory')
        self.hlayout_one.addWidget(self.vdatum_label)
        self.vdatum_text = QtWidgets.QLineEdit('', self)
        self.vdatum_text.setReadOnly(True)
        self.vdatum_text.setToolTip('Optional, this is required if you are using the "NOAA MLLW" or "NOAA MHW" vertical reference options.')
        self.hlayout_one.addWidget(self.vdatum_text)
        self.browse_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_one.addWidget(self.browse_button)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { " + kluster_variables.error_color + "; }")

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.hlayout_five.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_five.addWidget(self.ok_button)
        self.hlayout_five.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_five.addWidget(self.cancel_button)
        self.hlayout_five.addStretch(1)

        layout.addWidget(self.parallel_write)
        layout.addWidget(self.keep_waterline_changes)
        layout.addWidget(self.force_coordinate_match)
        layout.addLayout(self.hlayout_one_one)
        layout.addLayout(self.hlayout_one)
        layout.addStretch()
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.vdatum_pth = None

        self.canceled = False

        self.browse_button.clicked.connect(self.vdatum_browse)
        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)

        self.text_controls = [['vdatum_directory', self.vdatum_text], ['auto_processing_mode', self.auto_processing_mode]]
        self.checkbox_controls = [['enable_parallel_writes', self.parallel_write], ['keep_waterline_changes', self.keep_waterline_changes],
                                  ['force_coordinate_match', self.force_coordinate_match]]

        self.read_settings()
        self._refresh_error_message()
        self.resize(600, 150)

    def return_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if not self.canceled:
            opts = {'write_parallel': self.parallel_write.isChecked(),
                    'keep_waterline_changes': self.keep_waterline_changes.isChecked(),
                    'force_coordinate_match': self.force_coordinate_match.isChecked(),
                    'vdatum_directory': self.vdatum_pth,
                    'autoprocessing_mode': self.auto_processing_mode.currentText()}
            if self.vdatum_pth:
                set_vyperdatum_vdatum_path(self.vdatum_pth)
        else:
            opts = None
        return opts

    def vdatum_browse(self):
        # dirpath will be None or a string
        msg, self.vdatum_pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='Kluster',
                                                                Title='Select Vdatum directory', AppName='\\reghelp')
        if self.vdatum_pth:
            self.vdatum_text.setText(self.vdatum_pth)
        self._refresh_error_message()

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        print('General settings saved')
        self.canceled = False
        self.save_settings()
        self.accept()

    def cancel(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()

    def read_settings(self):
        super().read_settings()
        self.vdatum_pth = self.vdatum_text.text()

    def _refresh_error_message(self):
        err = False
        if self.vdatum_pth:
            expected_vdatum_path = os.path.join(self.vdatum_pth, 'vdatum.bat')
            if not os.path.exists(expected_vdatum_path):
                self.status_msg.setText('VDatum Directory: Unable to find {}'.format(expected_vdatum_path))
                err = True
        if not err:
            self.status_msg.setText('')


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = SettingsDialog()
    dlog.show()
    if dlog.exec_():
        pass
