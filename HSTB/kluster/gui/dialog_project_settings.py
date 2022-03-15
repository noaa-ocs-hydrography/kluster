import os
from pyproj import CRS

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster import kluster_variables
from HSTB.kluster.fqpr_helpers import epsg_determinator

geo_datum_descrip = [f'{k} = EPSG:{epsg_determinator(k)}' for k in kluster_variables.geographic_coordinate_systems]
proj_datum_descrip = []
for projcoord in kluster_variables.coordinate_systems:
    try:
        coordescrip = f'{projcoord} = EPSG:{str(epsg_determinator(projcoord, zone=4, hemisphere="n"))[:-2] + "xx"}'
    except ValueError:
        coordescrip = f'{projcoord} = EPSG:{str(epsg_determinator(projcoord, zone=54, hemisphere="n"))[:-2] + "xx"}'
    proj_datum_descrip.append(coordescrip)


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

        self.incoord_group = QtWidgets.QGroupBox('Input Coordinate System:')
        self.incoord_layout = QtWidgets.QVBoxLayout()

        self.infromdata_radio = QtWidgets.QRadioButton('From multibeam data')
        self.infromdata_radio.setChecked(True)
        self.infromdata_radio.setToolTip('Uses the input coordinate system determined automatically from the raw multibeam data.  This is the default option, you should only change this '
                                         'if you know that the multibeam is not writing the correct datum to file.  Ignored if SBET datum exists.')
        self.incoord_layout.addWidget(self.infromdata_radio)

        inepsg_tooltip = 'Generates a new input coordinate system from EPSG code, ignored if SBET datum exists.  Only use if you have to overwrite the multibeam data datum description.'
        self.hlayout_zero_one = QtWidgets.QHBoxLayout()
        self.inepsg_radio = QtWidgets.QRadioButton('From EPSG')
        self.inepsg_radio.setToolTip(inepsg_tooltip)
        self.hlayout_zero_one.addWidget(self.inepsg_radio)
        self.inepsg_val = QtWidgets.QLineEdit('', self)
        self.inepsg_val.setToolTip(inepsg_tooltip)
        self.hlayout_zero_one.addWidget(self.inepsg_val)
        self.incoord_layout.addLayout(self.hlayout_zero_one)

        indropdown_tooltip = f'Generates a new input coordinate system from coordinate system description, ignored if SBET datum exists.\n\n{geo_datum_descrip}'
        self.hlayout_zero_two = QtWidgets.QHBoxLayout()
        self.indropdown_radio = QtWidgets.QRadioButton('From identifier')
        self.indropdown_radio.setToolTip(indropdown_tooltip)
        self.hlayout_zero_two.addWidget(self.indropdown_radio)
        self.indropdown_val = QtWidgets.QComboBox()
        self.indropdown_val.addItems(kluster_variables.geographic_coordinate_systems)
        self.indropdown_val.setCurrentIndex(kluster_variables.geographic_coordinate_systems.index(kluster_variables.default_coordinate_system))
        self.indropdown_val.setToolTip(indropdown_tooltip)
        self.hlayout_zero_two.addWidget(self.indropdown_val)
        self.incoord_layout.addLayout(self.hlayout_zero_two)

        self.incoord_group.setLayout(self.incoord_layout)

        self.outcoord_group = QtWidgets.QGroupBox('Output Coordinate System:')
        self.outcoord_layout = QtWidgets.QVBoxLayout()

        outepsg_tooltip = 'Generates a new output coordinate system from EPSG code.'
        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.epsg_radio = QtWidgets.QRadioButton('From EPSG')
        self.epsg_radio.setToolTip(outepsg_tooltip)
        self.hlayout_one.addWidget(self.epsg_radio)
        self.epsg_val = QtWidgets.QLineEdit('', self)
        self.epsg_val.setToolTip(outepsg_tooltip)
        self.hlayout_one.addWidget(self.epsg_val)
        self.outcoord_layout.addLayout(self.hlayout_one)

        autoutm_tooltip = f'Build the correct EPSG code for the output coordinate system from datum description and known hemisphere/zone number.\n\n{proj_datum_descrip}'
        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.auto_utm_radio = QtWidgets.QRadioButton('Auto UTM')
        self.auto_utm_radio.setChecked(True)
        self.auto_utm_radio.setToolTip(autoutm_tooltip)
        self.hlayout_two.addWidget(self.auto_utm_radio)
        self.auto_utm_val = QtWidgets.QComboBox()
        self.auto_utm_val.setToolTip(autoutm_tooltip)
        self.auto_utm_val.addItems(kluster_variables.coordinate_systems)
        self.auto_utm_val.setCurrentIndex(kluster_variables.coordinate_systems.index(kluster_variables.default_coordinate_system))
        self.hlayout_two.addWidget(self.auto_utm_val)
        self.outcoord_layout.addLayout(self.hlayout_two)

        self.outcoord_group.setLayout(self.outcoord_layout)

        self.vertref_msg = QtWidgets.QLabel('Vertical Reference:')

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.georef_vertref = QtWidgets.QComboBox()
        self.georef_vertref.addItems(kluster_variables.vertical_references)
        self.georef_vertref.setToolTip('Set the vertical reference used in georeferencing, this determines the zero point for all depths generated in Kluster.')
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
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
        self.status_msg.setAlignment(QtCore.Qt.AlignCenter)
        self.statusbox = QtWidgets.QHBoxLayout()
        self.statusbox.addWidget(self.status_msg)

        layout.addWidget(self.incoord_group)
        layout.addStretch()
        layout.addWidget(self.outcoord_group)
        layout.addStretch()
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
        self.epsg_val.textChanged.connect(self.validate_epsg)
        self.inepsg_val.textChanged.connect(self.validate_epsg)

        self.text_controls = [['epsgval', self.epsg_val], ['utmval', self.auto_utm_val], ['vertref', self.georef_vertref],
                              ['inepsg_val', self.inepsg_val], ['indropdown_val', self.indropdown_val]]
        self.checkbox_controls = [['infromdata_radio', self.infromdata_radio], ['inepsg_radio', self.inepsg_radio],
                                  ['indropdown_radio', self.indropdown_radio], ['epsg_radio', self.epsg_radio],
                                  ['auto_utm_radio', self.auto_utm_radio]]

        self.read_settings()
        self.validate_epsg()
        self.find_vdatum()

    def validate_epsg(self):
        self.status_msg.setText('')
        if self.epsg_radio.isChecked():
            epsg = ''
            try:
                epsg = int(self.epsg_val.text())
                ecrs = CRS.from_epsg(epsg)
                if not ecrs.is_projected:
                    self.status_msg.setText(f'ERROR: must be a projected CRS, Output EPSG:{epsg}')
                elif ecrs.coordinate_system.axis_list[0].unit_name not in ['meters', 'metre', 'metres']:
                    self.status_msg.setText(f'ERROR: CRS must be in units of meters, found {ecrs.coordinate_system.axis_list[0].unit_name}, Output EPSG:{epsg}')
                else:
                    self.status_msg.setText('')
            except:
                self.status_msg.setText(f'Unknown Error: Unable to generate new CRS from Output EPSG:{epsg}')
        else:
            self.status_msg.setText('')
        if self.status_msg.text():
            return
        if self.inepsg_radio.isChecked():
            epsg = ''
            try:
                epsg = int(self.inepsg_val.text())
                ecrs = CRS.from_epsg(epsg)
                if ecrs.is_projected:
                    self.status_msg.setText(f'ERROR: must be a Geographic CRS, Input EPSG:{epsg}')
                elif ecrs.coordinate_system.axis_list[0].unit_name not in ['degree', 'degrees']:
                    self.status_msg.setText(f'ERROR: CRS must be in units of degrees, found {ecrs.coordinate_system.axis_list[0].unit_name}, Input EPSG:{epsg}')
                else:
                    self.status_msg.setText('')
            except:
                self.status_msg.setText(f'Unknown Error: Unable to generate new CRS from Input EPSG:{epsg}')

    def find_vdatum(self):
        """
        Adds a status message telling you if NOAA MLLW/MHW is a valid option based on whether or not we can
        find vdatum successfully
        """
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
        curr_vert = self.georef_vertref.currentText()
        if curr_vert in kluster_variables.vdatum_vertical_references:
            vdatum = self.settings_object.value('Kluster/settings_vdatum_directory')
            if vdatum:
                if os.path.exists(vdatum):
                    if os.path.exists(os.path.join(vdatum, 'vdatum.jar')):
                        self.status_msg.setText('')
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
                epsg = int(self.epsg_val.text())
            else:
                epsg = ''
            if self.infromdata_radio.isChecked():
                inepsg = None
            elif self.inepsg_radio.isChecked():
                inepsg = str(self.inepsg_val.text())
            else:
                inepsg = str(self.indropdown_val.currentText())
            opts = {'use_epsg': self.epsg_radio.isChecked(), 'epsg': epsg,
                    'use_coord': self.auto_utm_radio.isChecked(), 'coord_system': self.auto_utm_val.currentText(),
                    'vert_ref': self.georef_vertref.currentText(), 'input_datum': inepsg}
        else:
            opts = None
        return opts

    def start(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """

        self.validate_epsg()
        if self.status_msg.text():
            return
        self.find_vdatum()
        if self.status_msg.text():
            return
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
        print(dlog.return_processing_options())
