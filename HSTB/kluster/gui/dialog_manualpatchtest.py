from datetime import datetime

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster import kluster_variables


class PrePatchDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Patch Test')
        self.setMinimumWidth(800)
        self.setMinimumHeight(200)

        self.main_layout = QtWidgets.QVBoxLayout()

        self.instructions_layout = QtWidgets.QVBoxLayout()
        self.instructions = QtWidgets.QLabel('Select the installation parameter entries you would like to use in the patch test.')
        self.instructions_layout.addWidget(self.instructions)
        self.instructions_two = QtWidgets.QLabel('The selected entries MUST match in roll, pitch, heading, latency, serial number and head')
        self.instructions_layout.addWidget(self.instructions_two)
        self.instructions_three = QtWidgets.QLabel('Data processed with the selected entries will be included in the Patch Test.  Otherwise, the data will be excluded')
        self.instructions_layout.addWidget(self.instructions_three)
        self.main_layout.addLayout(self.instructions_layout)

        self.xyzrph_list = XyzrphList(self)
        self.main_layout.addWidget(self.xyzrph_list)

        self.button_layout = QtWidgets.QHBoxLayout()
        self.button_layout.addStretch(1)
        self.ok_button = QtWidgets.QPushButton('Run', self)
        self.button_layout.addWidget(self.ok_button)
        self.button_layout.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton('Cancel', self)
        self.button_layout.addWidget(self.cancel_button)
        self.button_layout.addStretch(1)
        self.main_layout.addLayout(self.button_layout)

        self.hlayout_msg = QtWidgets.QHBoxLayout()
        self.warning_message = QtWidgets.QLabel('', self)
        self.warning_message.setStyleSheet("{};".format(kluster_variables.error_color))
        self.hlayout_msg.addWidget(self.warning_message)
        self.main_layout.addLayout(self.hlayout_msg)

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
        self.latency = []

        self.selected_data = None

    def add_data(self, datablock):
        for fq, serialnum, fq_time_segs, xyzrec, sysid, head in datablock:
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
                elif int(head) == 1:
                    head_fmt = 'STARBOARD'
                    roll = xyzrec['rx_stbd_r'][tstmp]
                    pitch = xyzrec['rx_stbd_p'][tstmp]
                    heading = xyzrec['rx_stbd_h'][tstmp]
                else:
                    raise NotImplementedError(
                        'Only head indices 0 and 1 supported, we expect max 2 heads, got: {}'.format(head))
            else:
                roll = xyzrec['rx_r'][tstmp]
                pitch = xyzrec['rx_p'][tstmp]
                heading = xyzrec['rx_h'][tstmp]
                head_fmt = 'N/A'
            latency = xyzrec['latency'][tstmp]
            self.timestamps.append(tstmp)
            self.timestamps_formatted.append(tstmp_fmt)
            self.headindexes_formatted.append(head_fmt)
            self.roll.append(roll)
            self.pitch.append(pitch)
            self.heading.append(heading)
            self.latency.append(latency)
            self.xyzrph_list.add_line([sysid, serialnum, tstmp_fmt, tstmp, head_fmt, roll, pitch, heading, latency])

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
                    msg = 'Roll does not match between rows {} and {}!'.format(base_sel_row, next_sel)
                elif self.pitch[base_sel_row] != self.pitch[next_sel]:
                    msg = 'Pitch does not match between rows {} and {}!'.format(base_sel_row, next_sel)
                elif self.heading[base_sel_row] != self.heading[next_sel]:
                    msg = 'Heading does not match between rows {} and {}!'.format(base_sel_row, next_sel)
                elif self.latency[base_sel_row] != self.latency[next_sel]:
                    msg = 'Latency does not match between rows {} and {}!'.format(base_sel_row, next_sel)
                elif self.serial_numbers[base_sel_row] != self.serial_numbers[next_sel]:
                    msg = 'Serial Number does not match between rows {} and {}!'.format(base_sel_row, next_sel)
                elif self.headindexes[base_sel_row] != self.headindexes[next_sel]:
                    msg = 'Sonar Head does not match between rows {} and {}!'.format(base_sel_row, next_sel)
        if msg:
            err = True
        return selected_rows, err, msg

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

        self.headr = ['', 'Container', r'S/N', 'Record Time', 'Record Time UTC', 'Head', 'Roll', 'Pitch', 'Heading', 'Latency']
        self.setColumnCount(10)
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
        self.setColumnWidth(9, 60)
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


class ManualPatchTestWidget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(self, parent)

        self.setWindowTitle('Patch Test')
        self.setMinimumWidth(900)
        self.setMinimumHeight(400)


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = PrePatchDialog()

    dlog.show()
    if dlog.exec_():
        pass
