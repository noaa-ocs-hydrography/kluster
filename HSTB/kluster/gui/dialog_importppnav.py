from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from datetime import datetime

from HSTB.shared import RegistryHelpers
from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.kluster import kluster_variables


class ImportPostProcNavigationDialog(QtWidgets.QDialog):
    """
    Dialog contains all the options related to importing post processed navigation (POSPac SBET).  Use
    return_processing_options to get the kwargs to feed the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Import Post Processed Navigation')
        layout = QtWidgets.QVBoxLayout()

        self.input_msg = QtWidgets.QLabel('Apply to the following:')

        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.hlayout_zero.addWidget(self.input_fqpr)

        self.sbet_msg = QtWidgets.QLabel('POSPac SBET Files')

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.sbetfiles = BrowseListWidget(self)
        self.sbetfiles.setup(registry_key='kluster', app_name='klusterbrowse', supported_file_extension=['.out', '.sbet'],
                             multiselect=True, filebrowse_title='Select SBET files',
                             filebrowse_filter='POSPac SBET files (*.out;*.sbet)')
        self.hlayout_two.addWidget(self.sbetfiles)

        self.smrmsg_msg = QtWidgets.QLabel('POSPac SMRMSG Files')

        self.hlayout_three = QtWidgets.QHBoxLayout()
        self.smrmsgfiles = BrowseListWidget(self)
        self.smrmsgfiles.setup(registry_key='kluster', app_name='klusterbrowse', supported_file_extension=['.out', '.smrmsg'],
                               multiselect=True, filebrowse_title='Select SMRMSG files',
                               filebrowse_filter='POSPac SMRMSG files (*.out;*.smrmsg)')
        self.hlayout_three.addWidget(self.smrmsgfiles)

        self.log_check = QtWidgets.QGroupBox('Load from POSPac export log')
        self.log_check.setCheckable(True)
        self.log_check.setChecked(True)
        self.logopts = QtWidgets.QVBoxLayout()
        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.log_file = QtWidgets.QLineEdit('', self)
        self.log_file.setReadOnly(True)
        self.hlayout_one.addWidget(self.log_file)
        self.browse_button = QtWidgets.QPushButton("Browse", self)
        self.hlayout_one.addWidget(self.browse_button)
        self.logopts.addLayout(self.hlayout_one)
        self.log_check.setLayout(self.logopts)

        self.override_check = QtWidgets.QGroupBox('Manually set metadata')
        self.override_check.setCheckable(True)
        self.override_check.setChecked(False)
        self.overrideopts = QtWidgets.QVBoxLayout()
        self.hlayout_four_one = QtWidgets.QHBoxLayout()
        self.caltext = QtWidgets.QLabel('Date of SBET')
        self.hlayout_four_one.addWidget(self.caltext)
        self.calendar_widget = QtWidgets.QDateEdit()
        self.calendar_widget.setCalendarPopup(True)
        currdate = datetime.now()
        self.calendar_widget.setDate(QtCore.QDate(currdate.year, currdate.month, currdate.day))
        self.hlayout_four_one.addWidget(self.calendar_widget)
        self.hlayout_four_two = QtWidgets.QHBoxLayout()
        self.datumtext = QtWidgets.QLabel('Coordinate System')
        self.hlayout_four_two.addWidget(self.datumtext)
        self.datum_val = QtWidgets.QComboBox()
        self.datum_val.addItems(['NAD83', 'WGS84'])
        self.hlayout_four_two.addWidget(self.datum_val)
        self.overrideopts.addLayout(self.hlayout_four_one)
        self.overrideopts.addLayout(self.hlayout_four_two)
        self.override_check.setLayout(self.overrideopts)

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")

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
        layout.addWidget(self.sbet_msg)
        layout.addLayout(self.hlayout_two)
        layout.addWidget(self.smrmsg_msg)
        layout.addLayout(self.hlayout_three)
        layout.addWidget(self.log_check)
        layout.addWidget(self.override_check)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.log_file_path = ''
        self.sbet_files = []
        self.smrmsg_files = []
        self.fqpr_inst = []
        self.canceled = False

        self.browse_button.clicked.connect(self.file_browse)
        self.log_check.clicked.connect(self.log_override_checked)
        self.override_check.clicked.connect(self.log_override_checked)
        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.sbetfiles.files_updated.connect(self.update_sbet_files)
        self.smrmsgfiles.files_updated.connect(self.update_smrmsg_files)
        self.ok_button.clicked.connect(self.start_processing)
        self.cancel_button.clicked.connect(self.cancel_processing)

        self.resize(600, 500)

    def file_browse(self):
        # dirpath will be None or a string
        msg, self.log_file_path = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='Kluster',
                                                                        Title='Select POSPac Export Log',
                                                                        AppName='\\reghelp', bSave=False,
                                                                        fFilter="Log Files | *.txt;*.log")
        if self.log_file_path is not None:
            self.log_file.setText(self.log_file_path)

    def log_override_checked(self):
        """
        Either override or export log is required, but not both.  Make it so only one can be checked at a time
        """
        if self.sender() == self.log_check:
            if self.log_check.isChecked():
                self.override_check.setChecked(False)
            else:
                self.override_check.setChecked(True)
        else:
            if self.override_check.isChecked():
                self.log_check.setChecked(False)
            else:
                self.log_check.setChecked(True)

    def _event_update_fqpr_instances(self):
        """
        Method for connecting the input_fqpr signal with the update_fqpr_instances
        """

        self.update_fqpr_instances()

    def update_fqpr_instances(self, addtl_files=None):
        """
        Used through kluster_main to update the list_widget with new multibeam files to process

        Parameters
        ----------
        addtl_files: optional, list, list of multibeam files (string file paths)

        """
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = self.input_fqpr.return_all_items()

    def update_sbet_files(self):
        """
        Populate self.sbet_files with new sbet files from the list widget
        """
        self.sbet_files = self.sbetfiles.return_all_items()

    def update_smrmsg_files(self):
        """
        Populate self.smrmsg_files with new smrmsg files from the list widget
        """
        self.smrmsg_files = self.smrmsgfiles.return_all_items()

    def return_processing_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """
        if self.log_check.isChecked():
            logfiles = [self.log_file_path] * len(self.sbet_files)
            weekstart_week = None
            weekstart_year = None
            override_datum = None
        else:
            logfiles = None
            weekstart_year, weekstart_week, dy = datetime.strptime(self.calendar_widget.text(), '%m/%d/%Y').isocalendar()
            override_datum = self.datum_val.currentText()

        if not self.smrmsg_files:
            self.smrmsg_files = None

        # always overwrite when the user uses the manual import with this dialog
        if not self.canceled:
            opts = {'fqpr_inst': self.fqpr_inst, 'navfiles': self.sbet_files, 'errorfiles': self.smrmsg_files,
                    'logfiles': logfiles, 'weekstart_year': weekstart_year, 'weekstart_week': weekstart_week,
                    'override_datum': override_datum, 'overwrite': True}
        else:
            opts = None
        return opts

    def start_processing(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        if not self.fqpr_inst and not self.sbet_files:
            self.status_msg.setText('Error: You must select source data and sbet files to continue')
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
    dlog = ImportPostProcNavigationDialog()
    dlog.show()
    if dlog.exec_():
        pass
