import logging

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.gui.common_widgets import BrowseListWidget
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster import kluster_variables


class FilterDialog(SaveStateDialog):
    """
    Dialog allows for providing fqpr data for filtering, and the selection of a filter algorithm.  Will
    spawn a separate dialog for the parameters required by the filter.

    fqpr = fully qualified ping record, the term for the datastore in kluster
    """

    def __init__(self, filters: list, filter_descrip: dict, parent=None, settings=None):
        super().__init__(parent, settings, widgetname='filter')

        self.setWindowTitle('Filter Soundings')
        layout = QtWidgets.QVBoxLayout()

        self.basic_filter_group = QtWidgets.QGroupBox('Use the following datasets:')
        self.basic_filter_group.setCheckable(True)
        self.basic_filter_group.setChecked(True)
        self.basic_filter_group.setToolTip('Will run the filter on all datasets in the list.')
        self.hlayout_zero = QtWidgets.QHBoxLayout()
        self.input_fqpr = BrowseListWidget(self)
        self.input_fqpr.sizeHint()
        self.input_fqpr.setup(mode='directory', registry_key='kluster', app_name='klusterbrowse',
                              filebrowse_title='Select input processed folder')
        self.input_fqpr.setMinimumWidth(600)
        self.hlayout_zero.addWidget(self.input_fqpr)
        self.basic_filter_group.setLayout(self.hlayout_zero)

        self.line_filter = QtWidgets.QCheckBox('Use the selected lines')
        self.line_filter.setChecked(False)
        self.line_filter.setToolTip('Will only run the filter on the selected lines.')

        self.points_view_filter = QtWidgets.QCheckBox('Use the points in Points View')
        self.points_view_filter.setChecked(False)
        self.points_view_filter.setToolTip('Will only run on the points shown in Points View.  Note that you must have the datasets selected (they must be shown in the "Use the following datasets" box)')

        self.hlayout_one = QtWidgets.QHBoxLayout()
        self.start_msg = QtWidgets.QLabel('Run filter: ')
        self.hlayout_one.addWidget(self.start_msg)
        self.filter_opts = QtWidgets.QComboBox()
        self.filter_opts.addItems(filters)
        # self.filter_opts.setToolTip('Select one of the filter files found in the plugins/filters directory or the external filter directory')
        self.hlayout_one.addWidget(self.filter_opts)
        self.hlayout_one.addStretch()

        self.hlayout_one_two = QtWidgets.QHBoxLayout()
        self.save_to_disk_checkbox = QtWidgets.QCheckBox('Save to disk')
        self.save_to_disk_checkbox.setChecked(True)
        self.save_to_disk_checkbox.setVisible(False)
        self.save_to_disk_checkbox.setToolTip('Points View filter allows you to run the filter and only change the points in the Points '
                                              'View.  Uncheck this box if you want to skip saving changes to disk and only affect the Points View.')
        self.hlayout_one_two.addWidget(self.save_to_disk_checkbox)
        self.hlayout_one_two.addStretch()

        self.status_msg = QtWidgets.QLabel('')
        self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.hlayout_two.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_two.addWidget(self.ok_button)
        self.hlayout_two.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_two.addWidget(self.cancel_button)
        self.hlayout_two.addStretch(1)

        layout.addWidget(self.basic_filter_group)
        layout.addWidget(self.line_filter)
        layout.addWidget(self.points_view_filter)
        layout.addWidget(QtWidgets.QLabel(' '))
        layout.addLayout(self.hlayout_one)
        layout.addLayout(self.hlayout_one_two)
        layout.addWidget(self.status_msg)
        layout.addLayout(self.hlayout_two)
        self.setLayout(layout)

        self.fqpr_inst = []
        self.canceled = False
        self.filter_descrip = filter_descrip

        self.basic_filter_group.toggled.connect(self._handle_basic_checked)
        self.line_filter.toggled.connect(self._handle_line_checked)
        self.points_view_filter.toggled.connect(self._handle_points_checked)
        self.input_fqpr.files_updated.connect(self._event_update_fqpr_instances)
        self.filter_opts.currentTextChanged.connect(self._event_update_status)
        self.ok_button.clicked.connect(self.start_filter)
        self.cancel_button.clicked.connect(self.cancel_filter)

        self.text_controls = [['filter_ops', self.filter_opts]]
        self.checkbox_controls = [['basic_filter_group', self.basic_filter_group], ['line_filter', self.line_filter],
                                  ['points_view_filter', self.points_view_filter], ['save_to_disk_checkbox', self.save_to_disk_checkbox]]
        self.read_settings()
        self._event_update_status(self.filter_opts.currentText())

    def _handle_basic_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_filter.setChecked(False)
            self.points_view_filter.setChecked(False)
            self.save_to_disk_checkbox.setVisible(False)
            self.status_msg.setStyleSheet("QLabel { color: " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _handle_line_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.basic_filter_group.setChecked(False)
            self.points_view_filter.setChecked(False)
            self.save_to_disk_checkbox.setVisible(False)
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _handle_points_checked(self, evt):
        """
        Ensure only one group at a time is selected
        """

        if evt:
            self.line_filter.setChecked(False)
            self.basic_filter_group.setChecked(False)
            self.save_to_disk_checkbox.setVisible(True)
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.pass_color + "; }")
            self.status_msg.setText('')

    def _event_update_fqpr_instances(self):
        """
        Method for connecting the input_fqpr signal with the update_fqpr_instances
        """

        self.update_fqpr_instances()

    def _event_update_status(self, combobox_text: str):
        """
        Update the status message if an Error presents itself.  Also controls the OK button, to prevent kicking off
        a process if we know it isn't going to work

        Parameters
        ----------
        combobox_text
            value of the combobox as text
        """

        try:
            curdescrip = self.filter_descrip[combobox_text]
            descrip = 'Select one of the filter files found in the plugins/filters directory or the external filter directory\n\n'
            descrip += f'{combobox_text}: {curdescrip}'
            self.filter_opts.setToolTip(descrip)
        except:
            self.print(f'Unable to load description for filter {combobox_text}', logging.WARNING)
        self.status_msg.setText('')
        self.ok_button.setEnabled(True)

    def update_fqpr_instances(self, addtl_files=None):
        """
        Used through kluster_main to update the list_widget with new multibeam files to process

        Parameters
        ----------
        addtl_files: optional, list, list of multibeam files (string file paths)

        """
        if addtl_files is not None:
            self.input_fqpr.add_new_files(addtl_files)
        self.fqpr_inst = [self.input_fqpr.list_widget.item(i).text() for i in range(self.input_fqpr.list_widget.count())]
        if self.points_view_filter.isChecked():
            self._handle_points_checked(True)

    def start_filter(self):
        """
        Dialog completes if the specified widgets are populated
        """
        if self.basic_filter_group.isChecked() and not self.fqpr_inst:
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: No data provided')
        elif not self.basic_filter_group.isChecked() and not self.line_filter.isChecked() and not self.points_view_filter.isChecked():
            self.status_msg.setStyleSheet("QLabel { color : " + kluster_variables.error_color + "; }")
            self.status_msg.setText('Error: You must select one of the three filter modes (filter datasets, filter lines, filter points)')
        else:
            self.canceled = False
            self.save_settings()
            self.accept()

    def cancel_filter(self):
        """
        Dialog completes, use self.canceled to get the fact that it cancelled
        """
        self.canceled = True
        self.accept()


class AdditionalFilterOptionsDialog(SaveStateDialog):
    def __init__(self, parent=None, title='', settings=None, controls=None):
        super().__init__(parent, settings, widgetname='AdditionalFilterOptionsDialog')
        self.setWindowTitle(title)
        layout = QtWidgets.QVBoxLayout()
        self.controls = []
        self.labels = []

        for cntrl in controls:
            cntrl_type, cntrl_name, cntrl_val, cntrl_properties = cntrl
            hbox = QtWidgets.QHBoxLayout()
            hbox.addWidget(QtWidgets.QLabel(cntrl_name))
            self.labels.append(cntrl_name)
            if cntrl_type == 'float':
                newcntrl = QtWidgets.QDoubleSpinBox()
            elif cntrl_type == 'int':
                newcntrl = QtWidgets.QSpinBox()
            elif cntrl_type == 'text':
                newcntrl = QtWidgets.QLineEdit()
            elif cntrl_type == 'checkbox':
                newcntrl = QtWidgets.QCheckBox()
            elif cntrl_type == 'combobox':
                newcntrl = QtWidgets.QComboBox()
            else:
                raise NotImplementedError(f'AdditionalFilterOptionsDialog: type {cntrl_type} not currently supported')
            # set properties before value, as some properties affect the validation and formatting of the value set
            for propkey, propvalue in cntrl_properties.items():
                try:
                    newcntrl.setProperty(propkey, propvalue)
                except:
                    self.print(f'AdditionalFilterOptionsDialog: Error: unable to set property {propkey}:{propvalue} on {newcntrl}', logging.ERROR)
            if cntrl_type == 'float':
                newcntrl.setValue(cntrl_val)
            elif cntrl_type == 'int':
                newcntrl.setValue(cntrl_val)
            elif cntrl_type == 'text':
                newcntrl.setText(cntrl_val)
            elif cntrl_type == 'checkbox':
                newcntrl.setChecked(cntrl_val)
            elif cntrl_type == 'combobox':
                newcntrl.addItems(cntrl_val)

            hbox.addWidget(newcntrl)
            self.controls.append(newcntrl)
            layout.addLayout(hbox)

        self.hlayout_two = QtWidgets.QHBoxLayout()
        self.hlayout_two.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('OK', self)
        self.hlayout_two.addWidget(self.ok_button)
        self.hlayout_two.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.hlayout_two.addWidget(self.cancel_button)
        self.hlayout_two.addStretch(1)
        layout.addLayout(self.hlayout_two)

        self.setLayout(layout)
        self.canceled = False

        self.ok_button.clicked.connect(self.ok_pressed)
        self.cancel_button.clicked.connect(self.cancel)

    def return_kwargs(self):
        kwargs = {}
        for lbl, cntrl in zip(self.labels, self.controls):
            try:
                kwargs[lbl] = cntrl.value()
            except:
                try:
                    kwargs[lbl] = cntrl.text()
                except:
                    try:
                        kwargs[lbl] = cntrl.currentText()
                    except:
                        try:
                            kwargs[lbl] = cntrl.isChecked()
                        except:
                            raise ValueError(f'AdditionalFilterOptionsDialog: Unable to return value from {lbl}:{cntrl}')
        return kwargs

    def ok_pressed(self):
        self.canceled = False
        self.accept()

    def cancel(self):
        self.canceled = True
        self.accept()


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = FilterDialog(['test'], {'test': 'test'})
    dlog.show()
    if dlog.exec_():
        pass
