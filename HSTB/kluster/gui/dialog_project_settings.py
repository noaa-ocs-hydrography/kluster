from PySide2 import QtWidgets, QtCore


class ProjectSettingsDialog(QtWidgets.QDialog):
    """
    Dialog contains all the processing steps post-conversion.  Use return_processing_options to get the kwargs to feed
    the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Project Settings')
        layout = QtWidgets.QVBoxLayout()

        self.coord_msg = QtWidgets.QLabel('Coordinate System:')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.epsg_radio = QtWidgets.QRadioButton('From EPSG')
        self.hlayout_one.addWidget(self.epsg_radio)
        self.epsg_val = QtWidgets.QLineEdit('', self)
        self.epsg_val.setMaximumWidth(80)
        self.hlayout_one.addWidget(self.epsg_val)
        self.hlayout_one.addStretch(1)

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.auto_utm_radio = QtWidgets.QRadioButton('Auto UTM')
        self.auto_utm_radio.setChecked(True)
        self.hlayout_two.addWidget(self.auto_utm_radio)
        self.auto_utm_val = QtWidgets.QComboBox()
        self.auto_utm_val.addItems(['NAD83', 'WGS84'])
        self.auto_utm_val.setMaximumWidth(100)
        self.hlayout_two.addWidget(self.auto_utm_val)
        self.hlayout_two.addStretch(1)

        self.vertref_msg = QtWidgets.QLabel('Vertical Reference:')

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.georef_vertref = QtWidgets.QComboBox()
        self.georef_vertref.addItems(['waterline', 'ellipse'])
        self.georef_vertref.setMaximumWidth(100)
        self.hlayout_three.addWidget(self.georef_vertref)
        self.hlayout_three.addStretch(1)

        self.hlayout_five = QtWidgets.QHBoxLayout()
        self.hlayout_five.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_five.addWidget(self.ok_button)
        self.hlayout_five.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_five.addWidget(self.cancel_button)
        self.hlayout_five.addStretch(1)

        layout.addWidget(self.coord_msg)
        layout.addLayout(self.hlayout_one)
        layout.addLayout(self.hlayout_two)
        layout.addWidget(self.vertref_msg)
        layout.addLayout(self.hlayout_three)
        layout.addStretch()
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.canceled = False

        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)

        self.resize(600, 500)

    def return_processing_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if not self.canceled:
            if self.epsg_val.text():
                try:
                    epsg = int(self.epsg_val.text())
                except:
                    print('dialog_project_settings: EPSG must be an integer, received: {}'.format(self.epsg_val.text()))
            else:
                epsg = None
            opts = {'use_epsg': self.epsg_radio.isChecked(), 'epsg': epsg,
                    'use_coord': self.auto_utm_radio.isChecked(), 'coord_system': self.auto_utm_val.currentText(),
                    'vert_ref': self.georef_vertref.currentText()}
        else:
            opts = None
        return opts

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        self.canceled = False
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
        settings = QtCore.QSettings("NOAA", "Kluster")
        settings.setValue('Kluster/proj_settings_epsgradio', self.epsg_radio.isChecked())
        settings.setValue('Kluster/proj_settings_epsgval', self.epsg_val.text())
        settings.setValue('Kluster/proj_settings_utmradio', self.auto_utm_radio.isChecked())
        settings.setValue('Kluster/proj_settings_utmval', self.auto_utm_val.currentText())
        settings.setValue('Kluster/proj_settings_vertref', self.georef_vertref.currentText())

    def read_settings(self):
        """
        Read from the Qsettings registry
        """
        settings = QtCore.QSettings("NOAA", "Kluster")

        try:
            self.epsg_radio.setChecked(settings.value('Kluster/proj_settings_epsgradio').lower() == 'true')
            self.epsg_val.setText(settings.value('Kluster/proj_settings_epsgval'))
            self.auto_utm_radio.setChecked(settings.value('Kluster/proj_settings_utmradio').lower() == 'true')
            self.auto_utm_val.setCurrentText(settings.value('Kluster/proj_settings_utmval'))
            self.georef_vertref.setCurrentText(settings.value('Kluster/proj_settings_vertref'))
        except AttributeError:
            # no settings exist yet for this app, .lower failed
            pass


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    dlog = ProjectSettingsDialog()
    dlog.show()
    if dlog.exec_():
        pass
