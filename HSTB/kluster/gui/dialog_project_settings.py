import os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster import kluster_variables


class ProjectSettingsDialog(SaveStateDialog):
    """
    Dialog contains all the processing steps post-conversion.  Use return_processing_options to get the kwargs to feed
    the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='proj_settings')

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
        self.auto_utm_val.addItems(kluster_variables.coordinate_systems)
        self.auto_utm_val.setCurrentIndex(kluster_variables.coordinate_systems.index(kluster_variables.default_coordinate_system))
        self.auto_utm_val.setMaximumWidth(100)
        self.hlayout_two.addWidget(self.auto_utm_val)
        self.hlayout_two.addStretch(1)

        self.vertref_msg = QtWidgets.QLabel('Vertical Reference:')

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.georef_vertref = QtWidgets.QComboBox()
        self.georef_vertref.addItems(kluster_variables.vertical_references)
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

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { " + kluster_variables.error_color + "; }")
        self.status_msg.setAlignment(QtCore.Qt.AlignCenter)
        self.statusbox = QtWidgets.QHBoxLayout()
        self.statusbox.addWidget(self.status_msg)

        layout.addWidget(self.coord_msg)
        layout.addLayout(self.hlayout_one)
        layout.addLayout(self.hlayout_two)
        layout.addWidget(self.vertref_msg)
        layout.addLayout(self.hlayout_three)
        layout.addStretch()
        layout.addLayout(self.statusbox)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.canceled = False

        self.ok_button.clicked.connect(self.start)
        self.cancel_button.clicked.connect(self.cancel)
        self.georef_vertref.currentTextChanged.connect(self.find_vdatum)

        self.text_controls = [['epsgval', self.epsg_val], ['utmval', self.auto_utm_val], ['vertref', self.georef_vertref]]
        self.checkbox_controls = [['epsgradio', self.epsg_radio], ['utmradio', self.auto_utm_radio]]

        self.read_settings()
        self.resize(600, 200)

    def find_vdatum(self):
        """
        Adds a status message telling you if NOAA MLLW/MHW is a valid option based on whether or not we can
        find vdatum successfully
        """
        self.status_msg.setStyleSheet("QLabel { " + kluster_variables.error_color + "; }")
        curr_vert = self.georef_vertref.currentText()
        if curr_vert in kluster_variables.vdatum_vertical_references:
            vdatum = self.settings_object.value('Kluster/settings_vdatum_directory')
            if vdatum:
                if os.path.exists(vdatum):
                    if os.path.exists(os.path.join(vdatum, 'vdatum.jar')):
                        self.status_msg.setStyleSheet("QLabel { " + kluster_variables.pass_color + "; }")
                        self.status_msg.setText('Found VDatum at {}'.format(vdatum))
                    else:
                        self.status_msg.setText('Unable to find vdatum.jar at {}'.format(vdatum))
                else:
                    self.status_msg.setText('Unable to find vdatum folder at {}'.format(vdatum))
            else:
                self.status_msg.setText('VDatum folder not set')
        else:
            self.status_msg.setText('')

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
                epsg = ''
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
        print('Project settings saved')
        self.canceled = False
        self.save_settings()
        self.accept()

    def cancel(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ProjectSettingsDialog()
    dlog.show()
    if dlog.exec_():
        pass
