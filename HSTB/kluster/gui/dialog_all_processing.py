from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.kluster import kluster_variables

# DEPRECATED AS WE HAVE MOVED TO FQPRINTELLIGENCE/FQPRACTIONS TO CONTROL processing


class AllProcessingDialog(QtWidgets.QDialog):
    """
    Dialog contains all the processing steps post-conversion.  Use return_processing_options to get the kwargs to feed
    the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('All Processing')
        layout = QtWidgets.QVBoxLayout()

        self.input_msg = QtWidgets.QLabel('Run processing on the following:')

        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.hlayout_zero.addWidget(self.input_fqpr)

        self.start_msg = QtWidgets.QLabel('Check processes you want to include:')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.ovec_check = QtWidgets.QGroupBox('Compute Orientation')
        self.ovec_check.setCheckable(True)
        self.ovec_check.setChecked(True)
        self.hlayout_one.addWidget(self.ovec_check)

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.bvec_check = QtWidgets.QGroupBox('Compute Beam Vectors')
        self.bvec_check.setCheckable(True)
        self.bvec_check.setChecked(True)
        self.hlayout_two.addWidget(self.bvec_check)

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.svcorr_check = QtWidgets.QGroupBox('Sound Velocity Correct')
        self.svcorr_check.setCheckable(True)
        self.svcorr_check.setChecked(True)
        self.svopts = QtWidgets.QVBoxLayout()
        self.sv_msg = QtWidgets.QLabel('(Optional) Additional .svp files')
        self.svopts.addWidget(self.sv_msg)

        self.hlayout_three_one = QtWidgets.QHBoxLayout()
        self.svfiles = BrowseListWidget(self)
        self.svfiles.setup(registry_key='kluster', app_name='klusterbrowse', supported_file_extension='.svp',
                           multiselect=True, filebrowse_title='Select .svp files',
                           filebrowse_filter='Caris svp files (*.svp)')
        self.hlayout_three_one.addWidget(self.svfiles)
        self.svopts.addLayout(self.hlayout_three_one)
        self.svcorr_check.setLayout(self.svopts)
        self.hlayout_three.addWidget(self.svcorr_check)

        self.hlayout_four = QtWidgets.QHBoxLayout()
        self.georef_check = QtWidgets.QGroupBox('Georeference Soundings')
        self.georef_check.setCheckable(True)
        self.georef_check.setChecked(True)
        self.georefopts = QtWidgets.QVBoxLayout()
        self.coord_msg = QtWidgets.QLabel('Coordinate System:')
        self.georefopts.addWidget(self.coord_msg)

        self.hlayout_four_one = QtWidgets.QHBoxLayout()
        self.epsg_radio = QtWidgets.QRadioButton('From EPSG')
        self.hlayout_four_one.addWidget(self.epsg_radio)
        self.epsg_val = QtWidgets.QLineEdit('', self)
        self.epsg_val.setMaximumWidth(80)
        self.hlayout_four_one.addWidget(self.epsg_val)
        self.hlayout_four_one.addStretch(1)
        self.georefopts.addLayout(self.hlayout_four_one)

        self.hlayout_four_two = QtWidgets.QHBoxLayout()
        self.auto_utm_radio = QtWidgets.QRadioButton('Auto UTM')
        self.auto_utm_radio.setChecked(True)
        self.hlayout_four_two.addWidget(self.auto_utm_radio)
        self.auto_utm_val = QtWidgets.QComboBox()
        self.auto_utm_val.addItems(['NAD83', 'WGS84'])
        self.auto_utm_val.setMaximumWidth(100)
        self.hlayout_four_two.addWidget(self.auto_utm_val)
        self.hlayout_four_two.addStretch(1)
        self.georefopts.addLayout(self.hlayout_four_two)

        self.hlayout_four_three = QtWidgets.QHBoxLayout()
        self.vertref_msg = QtWidgets.QLabel('Vertical Reference:')
        self.hlayout_four_three.addWidget(self.vertref_msg)
        self.georef_vertref = QtWidgets.QComboBox()
        self.georef_vertref.addItems(['waterline', 'ellipse'])
        self.georef_vertref.setMaximumWidth(100)
        self.hlayout_four_three.addWidget(self.georef_vertref)
        self.hlayout_four_three.addStretch(1)
        self.georefopts.addLayout(self.hlayout_four_three)
        self.georef_check.setLayout(self.georefopts)
        self.hlayout_four.addWidget(self.georef_check)

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

        layout.addWidget(self.input_msg)
        layout.addLayout(self.hlayout_zero)
        layout.addWidget(self.start_msg)
        layout.addLayout(self.hlayout_one)
        layout.addLayout(self.hlayout_two)
        layout.addLayout(self.hlayout_three)
        layout.addLayout(self.hlayout_four)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.addtl_cast_files = []
        self.fqpr_inst = []
        self.canceled = False

        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.svfiles.files_updated.connect(self.update_cast_files)
        self.ok_button.clicked.connect(self.start_processing)
        self.cancel_button.clicked.connect(self.cancel_processing)

        self.resize(600, 500)

    def _event_update_fqpr_instances(self):
        """
        Method for connecting the input_fqpr signal with the update_fqpr_instances
        """

        self.update_fqpr_instances()

    def update_fqpr_instances(self, addtl_files=None):
        """
        Used through kluster_main to update the list_widget with new fqpr processed instances to process

        Parameters
        ----------
        addtl_files: optional, list, list of folder paths to converted data (string folder paths)

        """
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = [self.input_fqpr.list_widget.item(i).text() for i in range(self.input_fqpr.list_widget.count())]

    def return_processing_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if not self.canceled:
            opts = {'fqpr_inst': self.fqpr_inst, 'run_orientation': self.ovec_check.isChecked(),
                    'run_beam_vec': self.bvec_check.isChecked(), 'run_svcorr': self.svcorr_check.isChecked(),
                    'add_cast_files': self.addtl_cast_files, 'run_georef': self.georef_check.isChecked(),
                    'use_epsg': self.epsg_radio.isChecked(), 'epsg': self.epsg_val.text(),
                    'use_coord': self.auto_utm_radio.isChecked(), 'coord_system': self.auto_utm_val.currentText(),
                    'vert_ref': self.georef_vertref.currentText()}
        else:
            opts = None
        return opts

    def update_cast_files(self):
        """
        Populate self.addtl_cast_files with new cast files from the list widget
        """
        self.addtl_cast_files = [self.svfiles.list_widget.item(i).text() for i in range(self.svfiles.list_widget.count())]

    def start_processing(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        if not self.ovec_check.isChecked() and not self.bvec_check.isChecked() and not self.svcorr_check.isChecked() and not self.georef_check.isChecked():
            self.status_msg.setText('Error: You must select at least one processing option to proceed')
        else:
            self.canceled = False
            self.accept()

    def cancel_processing(self):
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
    dlog = AllProcessingDialog()
    dlog.show()
    if dlog.exec_():
        pass
