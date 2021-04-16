import os
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled
if qgis_enabled:
    os.environ['PYDRO_GUI_FORCE_PYQT'] = 'True'
    from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui

from HSTB.shared import RegistryHelpers
from HSTB.kluster import kluster_variables


class SettingsDialog(QtWidgets.QDialog):
    """
    Dialog contains all the processing steps post-conversion.  Use return_processing_options to get the kwargs to feed
    the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
    def __init__(self, parent=None, settings=None):
        super().__init__(parent)
        self.external_settings = settings

        self.setWindowTitle('Settings')
        layout = QtWidgets.QVBoxLayout()

        self.parallel_write = QtWidgets.QCheckBox('Enable Parallel Writes')
        self.parallel_write.setChecked(True)

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.vdatum_label = QtWidgets.QLabel('VDatum Directory')
        self.hlayout_one.addWidget(self.vdatum_label)
        self.vdatum_text = QtWidgets.QLineEdit('', self)
        self.vdatum_text.setReadOnly(True)
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

        self.read_settings()
        self._refresh_error_message()
        self.resize(600, 150)

    @property
    def settings_object(self):
        if self.external_settings:
            return self.external_settings
        else:
            return QtCore.QSettings("NOAA", "Kluster")

    def return_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if not self.canceled:
            opts = {'parallel_write': self.parallel_write.isChecked(), 'vdatum_directory': self.vdatum_pth}
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

    def save_settings(self):
        """
        Save the settings to the Qsettings registry
        """
        settings = self.settings_object
        settings.setValue('Kluster/settings_enable_parallel_writes', self.parallel_write.isChecked())
        settings.setValue('Kluster/settings_vdatum_directory', self.vdatum_text.text())

    def read_settings(self):
        """
        Read from the Qsettings registry
        """
        settings = self.settings_object

        try:
            try:
                self.parallel_write.setChecked(settings.value('Kluster/settings_enable_parallel_writes').lower() == 'true')
            except:
                self.parallel_write.setChecked(settings.value('Kluster/settings_enable_parallel_writes'))
            self.vdatum_text.setText(settings.value('Kluster/settings_vdatum_directory'))
            self.vdatum_pth = settings.value('Kluster/settings_vdatum_directory')
        except AttributeError:
            # no settings exist yet for this app, .lower failed
            pass

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
