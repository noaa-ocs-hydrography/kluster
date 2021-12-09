from datetime import datetime

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster import kluster_variables


class PrePatchDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Patch Test')
        self.setMinimumWidth(950)
        self.setMinimumHeight(200)

        self.main_layout = QtWidgets.QVBoxLayout()

        self.instructions_layout = QtWidgets.QVBoxLayout()
        self.instructions = QtWidgets.QLabel('Select the installation parameter entries you would like to use in the patch test.')
        self.instructions_layout.addWidget(self.instructions)
        self.instructions_two = QtWidgets.QLabel('The selected entries MUST match in roll, pitch, heading, X, Y, Z, latency, serial number and head')
        self.instructions_layout.addWidget(self.instructions_two)
        self.instructions_three = QtWidgets.QLabel('Data processed with the selected entries will be included in the Patch Test.  Otherwise, the data will be excluded')
        self.instructions_layout.addWidget(self.instructions_three)
        self.main_layout.addLayout(self.instructions_layout)

        self.xyzrph_list = XyzrphList(self)
        self.main_layout.addWidget(self.xyzrph_list)

        self.hlayout_msg = QtWidgets.QHBoxLayout()
        self.warning_message = QtWidgets.QLabel('', self)
        self.warning_message.setStyleSheet("{};".format(kluster_variables.error_color))
        self.hlayout_msg.addWidget(self.warning_message)
        self.main_layout.addLayout(self.hlayout_msg)

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('Load', self)
        self.button_layout.addWidget(self.ok_button)
        self.button_layout.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.button_layout.addWidget(self.cancel_button)
        self.button_layout.addStretch(1)
        self.main_layout.addLayout(self.button_layout)

        self.setLayout(self.main_layout)
        self.canceled = False

        self.ok_button.clicked.connect(self.return_selected)
        self.cancel_button.clicked.connect(self.cancel_patch)

        self.fqprs = []
        self.serial_numbers = []
        self.time_segments = []
        self.xyzrec = []
        self.sysids = []
        self.headindexes = []
        self.timestamps = []
        self.timestamps_formatted = []
        self.headindexes_formatted = []
        self.roll = []
        self.pitch = []
        self.heading = []
        self.x_lever = []
        self.y_lever = []
        self.z_lever = []
        self.latency = []
        self.prefixes = []

        self.selected_data = None

    def add_data(self, datablock):
        for fq, serialnum, fq_time_segs, xyzrec, sysid, head, vfname in datablock:
            self.fqprs.append(fq)
            self.serial_numbers.append(serialnum)
            self.time_segments.append(fq_time_segs)
            self.xyzrec.append(xyzrec)
            self.sysids.append(sysid)
            self.headindexes.append(head)
            tstmp = list(xyzrec['waterline'].keys())[0]
            tstmp_fmt = datetime.fromtimestamp(int(tstmp)).strftime('%m/%d/%Y %H%M')
            if fq.multibeam.is_dual_head():
                if int(head) == 0:
                    head_fmt = 'PORT'
                    roll = xyzrec['rx_port_r'][tstmp]
                    pitch = xyzrec['rx_port_p'][tstmp]
                    heading = xyzrec['rx_port_h'][tstmp]
                    xlever = xyzrec['rx_port_x'][tstmp]
                    ylever = xyzrec['rx_port_y'][tstmp]
                    zlever = xyzrec['rx_port_z'][tstmp]
                    self.prefixes = ['rx_port_r', 'rx_port_p', 'rx_port_h', 'rx_port_x', 'rx_port_y', 'rx_port_z', 'latency']
                elif int(head) == 1:
                    head_fmt = 'STARBOARD'
                    roll = xyzrec['rx_stbd_r'][tstmp]
                    pitch = xyzrec['rx_stbd_p'][tstmp]
                    heading = xyzrec['rx_stbd_h'][tstmp]
                    xlever = xyzrec['rx_stbd_x'][tstmp]
                    ylever = xyzrec['rx_stbd_y'][tstmp]
                    zlever = xyzrec['rx_stbd_z'][tstmp]
                    self.prefixes = ['rx_stbd_r', 'rx_stbd_p', 'rx_stbd_h', 'rx_stbd_x', 'rx_stbd_y', 'rx_stbd_z', 'latency']
                else:
                    raise NotImplementedError(
                        'Only head indices 0 and 1 supported, we expect max 2 heads, got: {}'.format(head))
            else:
                roll = xyzrec['rx_r'][tstmp]
                pitch = xyzrec['rx_p'][tstmp]
                heading = xyzrec['rx_h'][tstmp]
                xlever = xyzrec['rx_x'][tstmp]
                ylever = xyzrec['rx_y'][tstmp]
                zlever = xyzrec['rx_z'][tstmp]
                self.prefixes = ['rx_r', 'rx_p', 'rx_h', 'rx_x', 'rx_y', 'rx_z', 'latency']
                head_fmt = 'N/A'
            latency = xyzrec['latency'][tstmp]
            self.timestamps.append(tstmp)
            self.timestamps_formatted.append(tstmp_fmt)
            self.headindexes_formatted.append(head_fmt)
            self.roll.append(roll)
            self.pitch.append(pitch)
            self.heading.append(heading)
            self.x_lever.append(xlever)
            self.y_lever.append(ylever)
            self.z_lever.append(zlever)
            self.latency.append(latency)
            self.xyzrph_list.add_line([sysid, serialnum, tstmp_fmt, tstmp, head_fmt, roll, pitch, heading, xlever,
                                       ylever, zlever, latency])

    def return_selected(self):
        self.canceled = False
        selected_rows, err, msg = self._get_selected_data()
        if err:
            self.err_message(msg)
        else:
            self.selected_data = selected_rows
            self.accept()

    def _get_selected_data(self):
        selected_rows = []
        for row in range(self.xyzrph_list.rowCount()):
            chkd = self.xyzrph_list.cellWidget(row, 0).isChecked()
            if chkd:
                selected_rows.append(row)
        err = False
        msg = ''
        if len(selected_rows) > 1:
            base_sel_row = selected_rows[0]
            for i in range(len(selected_rows) - 1):
                next_sel = selected_rows[i + 1]
                if self.roll[base_sel_row] != self.roll[next_sel]:
                    msg = 'Roll does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.pitch[base_sel_row] != self.pitch[next_sel]:
                    msg = 'Pitch does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.heading[base_sel_row] != self.heading[next_sel]:
                    msg = 'Heading does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.x_lever[base_sel_row] != self.x_lever[next_sel]:
                    msg = 'X Lever Arm does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.y_lever[base_sel_row] != self.y_lever[next_sel]:
                    msg = 'Y Lever Arm does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.z_lever[base_sel_row] != self.z_lever[next_sel]:
                    msg = 'Z Lever Arm does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.latency[base_sel_row] != self.latency[next_sel]:
                    msg = 'Latency does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.serial_numbers[base_sel_row] != self.serial_numbers[next_sel]:
                    msg = 'Serial Number does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
                elif self.headindexes[base_sel_row] != self.headindexes[next_sel]:
                    msg = 'Sonar Head does not match between rows {} and {}!'.format(base_sel_row + 1, next_sel + 1)
        if msg:
            err = True
        return selected_rows, err, msg

    def return_final_data(self):
        total_fqpr = [self.fqprs[self.selected_data[0]]]
        total_systemids = [self.sysids[self.selected_data[0]]]
        total_timestamps = [self.timestamps[self.selected_data[0]]]
        total_dates = [self.timestamps_formatted[self.selected_data[0]]]
        total_timesegments = [self.time_segments[self.selected_data[0]]]
        for idx in self.selected_data[1:]:
            if self.fqprs[idx] not in total_fqpr:
                total_fqpr += [self.fqprs[idx]]
                total_timesegments += [self.time_segments[idx]]
            total_systemids += [self.sysids[idx]]
            total_timestamps += [self.timestamps[idx]]
            total_dates += [self.timestamps_formatted[idx]]

        # use copies of the fqpr object to ensure we do not alter the currently loaded data during the patch test
        total_fqpr = [fq.copy() for fq in total_fqpr]
        model_number = total_fqpr[0].sonar_model
        serial_number = self.serial_numbers[self.selected_data[0]]
        head = self.headindexes[self.selected_data[0]]
        roll = self.roll[self.selected_data[0]]
        pitch = self.pitch[self.selected_data[0]]
        heading = self.heading[self.selected_data[0]]
        xlever = self.x_lever[self.selected_data[0]]
        ylever = self.y_lever[self.selected_data[0]]
        zlever = self.z_lever[self.selected_data[0]]
        latency = self.latency[self.selected_data[0]]
        return [total_fqpr, total_systemids, model_number, serial_number, total_dates, total_timestamps, total_timesegments, \
               head, roll, pitch, heading, xlever, ylever, zlever, latency]

    def cancel_patch(self):
        self.canceled = True
        self.accept()

    def err_message(self, text: str = ''):
        if text:
            self.warning_message.setText('ERROR: ' + text)
        else:
            self.warning_message.setText('')


class XyzrphList(QtWidgets.QTableWidget):
    def __init__(self, parent):
        super().__init__(parent)

        # makes it so no editing is possible with the table
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        self.headr = ['', 'Container', r'S/N', 'Record Time', 'Record Time UTC', 'Head', 'Roll', 'Pitch', 'Heading', 'X Lever', 'Y Lever', 'Z Lever', 'Latency']
        self.setColumnCount(13)
        self.setHorizontalHeaderLabels(self.headr)
        self.setColumnWidth(0, 20)
        self.setColumnWidth(1, 170)
        self.setColumnWidth(2, 50)
        self.setColumnWidth(3, 110)
        self.setColumnWidth(4, 110)
        self.setColumnWidth(5, 50)
        self.setColumnWidth(6, 50)
        self.setColumnWidth(7, 50)
        self.setColumnWidth(8, 70)
        self.setColumnWidth(9, 50)
        self.setColumnWidth(10, 50)
        self.setColumnWidth(11, 50)
        self.setColumnWidth(12, 60)
        self.row_full_attribution = []

    def setup_table(self):
        self.clearContents()
        self.setRowCount(0)
        self.row_full_attribution = []

    def add_line(self, line_data: list):
        if line_data:
            next_row = self.rowCount()
            self.insertRow(next_row)
            self.row_full_attribution.append(line_data)
            for column_index, _ in enumerate(self.headr):
                if column_index == 0:
                    item = QtWidgets.QCheckBox()
                    self.setCellWidget(next_row, column_index, item)
                else:
                    item = QtWidgets.QTableWidgetItem(str(line_data[column_index - 1]))
                    self.setItem(next_row, column_index, item)


class PatchSpinBox(QtWidgets.QDoubleSpinBox):
    def __init__(self):
        super().__init__()
        self.setMinimum(-999.999)
        self.setMaximum(999.999)
        self.setDecimals(3)
        self.setSingleStep(0.01)
        self.setMaximumWidth(65)


class ManualPatchTestWidget(QtWidgets.QWidget):
    new_offsets_angles = Signal(float, float, float, float, float, float, float)

    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)

        self.setWindowTitle('Patch Test')
        self.setMinimumWidth(450)
        self.setMinimumHeight(330)

        self.main_layout = QtWidgets.QVBoxLayout()

        config_layout = QtWidgets.QHBoxLayout()
        config_layout_labels = QtWidgets.QVBoxLayout()
        config_layout_controls = QtWidgets.QVBoxLayout()
        configtxt = QtWidgets.QLabel('Vessel File: ')
        self.config_name = QtWidgets.QLabel('')
        sourcestxt = QtWidgets.QLabel('Sources: ')
        self.sources = QtWidgets.QLabel('')
        self.serial_descrip = QtWidgets.QLabel('S/N: ')
        self.serial_select = QtWidgets.QLabel()
        self.model_descrip = QtWidgets.QLabel('Model: ')
        self.model_select = QtWidgets.QLabel('')
        time_descrip = QtWidgets.QLabel('UTC Date: ')
        self.time_select = QtWidgets.QLabel()
        timestamp_descrip = QtWidgets.QLabel('UTC Timestamp: ')
        self.timestamp_select = QtWidgets.QLabel()

        for lbl in [configtxt, sourcestxt, self.model_descrip, self.serial_descrip, time_descrip, timestamp_descrip]:
            config_layout_labels.addWidget(lbl)
        for widg in [self.config_name, self.sources, self.model_select, self.serial_select, self.time_select, self.timestamp_select]:
            config_layout_controls.addWidget(widg)
        config_layout.addLayout(config_layout_labels)
        config_layout.addLayout(config_layout_controls)
        config_layout.addStretch()

        attdevices_layout = QtWidgets.QHBoxLayout()

        roll_layout = QtWidgets.QVBoxLayout()
        self.xlever_label = QtWidgets.QLabel('X Lever Arm (+ Forward)')
        roll_layout.addWidget(self.xlever_label)
        self.xlever_spinbox = PatchSpinBox()
        roll_layout.addWidget(self.xlever_spinbox)
        self.roll_label = QtWidgets.QLabel('Roll (+ Port Up)')
        roll_layout.addWidget(self.roll_label)
        self.roll_spinbox = PatchSpinBox()
        roll_layout.addWidget(self.roll_spinbox)
        attdevices_layout.addLayout(roll_layout)

        attdevices_layout.addStretch()
        pitch_layout = QtWidgets.QVBoxLayout()
        self.ylever_label = QtWidgets.QLabel('Y Lever Arm (+ Starboard)')
        pitch_layout.addWidget(self.ylever_label)
        self.ylever_spinbox = PatchSpinBox()
        pitch_layout.addWidget(self.ylever_spinbox)
        self.pitch_label = QtWidgets.QLabel('Pitch (+ Bow Up)')
        pitch_layout.addWidget(self.pitch_label)
        self.pitch_spinbox = PatchSpinBox()
        pitch_layout.addWidget(self.pitch_spinbox)
        attdevices_layout.addLayout(pitch_layout)
        attdevices_layout.addStretch()

        heading_layout = QtWidgets.QVBoxLayout()
        self.zlever_label = QtWidgets.QLabel('Z Lever Arm (+ Down)')
        heading_layout.addWidget(self.zlever_label)
        self.zlever_spinbox = PatchSpinBox()
        heading_layout.addWidget(self.zlever_spinbox)
        self.heading_label = QtWidgets.QLabel('Heading (+ Clockwise)')
        heading_layout.addWidget(self.heading_label)
        self.heading_spinbox = PatchSpinBox()
        heading_layout.addWidget(self.heading_spinbox)
        attdevices_layout.addLayout(heading_layout)
        attdevices_layout.addStretch()

        latencydevices_layout = QtWidgets.QHBoxLayout()
        latency_layout = QtWidgets.QVBoxLayout()
        self.latency_label = QtWidgets.QLabel('Latency (seconds)')
        latency_layout.addWidget(self.latency_label)
        self.latency_spinbox = PatchSpinBox()
        latency_layout.addWidget(self.latency_spinbox)
        latencydevices_layout.addLayout(latency_layout)
        latencydevices_layout.addStretch()

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.update_button = QtWidgets.QPushButton('Update', self)
        self.button_layout.addWidget(self.update_button)
        self.button_layout.addStretch(1)
        self.button_layout.addStretch(1)

        self.instructions = QtWidgets.QLabel('Update will adjust the data displayed in Points View.  Will not save the changes to disk.')

        self.main_layout.addLayout(config_layout)
        self.main_layout.addStretch()
        self.main_layout.addLayout(attdevices_layout)
        self.main_layout.addLayout(latencydevices_layout)
        self.main_layout.addStretch()
        self.main_layout.addWidget(self.instructions)
        self.main_layout.addLayout(self.button_layout)

        self.setLayout(self.main_layout)

        self.update_button.clicked.connect(self.update_data)

    def populate(self, vesselfile: str, sources: str, model: str, serialnum: str, utcdate: str, utctimestamp: str,
                 roll: float, pitch: float, heading: float, xlever: float, ylever: float, zlever: float, latency: float):
        self.config_name.setText(str(vesselfile))
        self.sources.setText(str(sources))
        self.model_select.setText(str(model))
        self.serial_select.setText(str(serialnum))
        self.time_select.setText(str(utcdate))
        self.timestamp_select.setText(str(utctimestamp))
        self.roll_spinbox.setValue(float(roll))
        self.pitch_spinbox.setValue(float(pitch))
        self.heading_spinbox.setValue(float(heading))
        self.xlever_spinbox.setValue(float(xlever))
        self.ylever_spinbox.setValue(float(ylever))
        self.zlever_spinbox.setValue(float(zlever))
        self.latency_spinbox.setValue(float(latency))

    def update_data(self):
        self.new_offsets_angles.emit(self.roll_spinbox.value(), self.pitch_spinbox.value(), self.heading_spinbox.value(),
                                     self.xlever_spinbox.value(), self.ylever_spinbox.value(), self.zlever_spinbox.value(),
                                     self.latency_spinbox.value())


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = ManualPatchTestWidget()
    dlog.show()
    app.exec_()
