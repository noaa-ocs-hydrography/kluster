from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from datetime import datetime

from HSTB.shared import RegistryHelpers
from HSTB.kluster.gui.common_widgets import BrowseListWidget, SaveStateDialog
from HSTB.kluster import kluster_variables


class OverwriteNavigationDialog(SaveStateDialog):
    """
    Dialog contains all the options related to overwriting the navigation with POSMV files.  Use
    return_processing_options to get the kwargs to feed the fqpr_convenience.process_multibeam function.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """
    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='OverwriteNavigationDialog')

        self.setWindowTitle('Overwrite Navigation')
        layout = QtWidgets.QVBoxLayout()

        self.input_msg = QtWidgets.QLabel('Apply to the following:')

        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.hlayout_zero.addWidget(self.input_fqpr)

        self.posmv_msg = QtWidgets.QLabel('POSMV Files')

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.posmvfiles = BrowseListWidget(self)
        self.posmvfiles.setup(registry_key='kluster', app_name='klusterbrowse',
                              multiselect=True, filebrowse_title='Select POS MV files')
        self.hlayout_two.addWidget(self.posmvfiles)

        self.override_check = QtWidgets.QGroupBox('Manually set metadata')
        self.override_check.setCheckable(False)
        self.override_check.setChecked(True)
        self.overrideopts = QtWidgets.QVBoxLayout()
        self.hlayout_four_one = QtWidgets.QHBoxLayout()
        self.caltext = QtWidgets.QLabel('Date of POS MV File')
        self.hlayout_four_one.addWidget(self.caltext)
        self.calendar_widget = QtWidgets.QDateEdit()
        self.calendar_widget.setCalendarPopup(True)
        currdate = datetime.now()
        self.calendar_widget.setDate(QtCore.QDate(currdate.year, currdate.month, currdate.day))
        self.hlayout_four_one.addWidget(self.calendar_widget)
        self.hlayout_four_two = QtWidgets.QHBoxLayout()
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
        layout.addWidget(self.posmv_msg)
        layout.addLayout(self.hlayout_two)
        layout.addWidget(self.override_check)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_five)
        self.setLayout(layout)

        self.pos_files = []
        self.fqpr_inst = []
        self.canceled = False

        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.posmvfiles.files_updated.connect(self.update_posmv_files)
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
        Used through kluster_main to update the list_widget with new multibeam files to process

        Parameters
        ----------
        addtl_files: optional, list, list of multibeam files (string file paths)

        """
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = self.input_fqpr.return_all_items()

    def update_posmv_files(self):
        """
        Populate self.pos_files with new pos files from the list widget
        """
        self.pos_files = self.posmvfiles.return_all_items()

    def return_processing_options(self):
        """
        Return a dict of processing options to feed to fqpr_convenience.process_multibeam

        """

        weekstart_year, weekstart_week, dy = datetime.strptime(self.calendar_widget.text(), '%m/%d/%Y').isocalendar()

        # always overwrite when the user uses the manual import with this dialog
        if not self.canceled:
            opts = {'fqpr_inst': self.fqpr_inst, 'navfiles': self.pos_files, 'weekstart_year': weekstart_year,
                    'weekstart_week': weekstart_week, 'overwrite': True}
        else:
            opts = None
        return opts

    def start_processing(self):
        """
        Dialog completes if the specified widgets are populated, use return_processing_options to get access to the
        settings the user entered into the dialog.
        """
        if not self.fqpr_inst and not self.pos_files:
            self.status_msg.setText('Error: You must select source data and pos mv files to continue')
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
    dlog = OverwriteNavigationDialog()
    dlog.show()
    if dlog.exec_():
        pass
