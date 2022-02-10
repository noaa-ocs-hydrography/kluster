from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.gui.common_widgets import SaveStateDialog
from HSTB.kluster import kluster_variables


class PatchTestDialog(SaveStateDialog):
    patch_query = Signal(str)  # submit new query to main for data

    def __init__(self, parent=None, title='', settings=None):
        super().__init__(parent, settings, widgetname='patchtestdialog')

        self.setWindowTitle('Patch Test')
        self.setMinimumWidth(900)
        self.setMinimumHeight(400)

        self.main_layout = QtWidgets.QVBoxLayout()

        self.listlayout = QtWidgets.QHBoxLayout()
        self.leftlayout = QtWidgets.QVBoxLayout()
        self.choose_layout = QtWidgets.QHBoxLayout()
        self.from_selected_lines = QtWidgets.QRadioButton('Use selected lines')
        self.from_selected_lines.setChecked(True)
        self.choose_layout.addWidget(self.from_selected_lines)
        self.from_points_view = QtWidgets.QRadioButton('Use Points View selection')
        self.from_points_view.setChecked(False)
        self.from_points_view.setDisabled(True)
        self.choose_layout.addWidget(self.from_points_view)
        self.choose_layout.addStretch()
        self.leftlayout.addLayout(self.choose_layout)

        self.button_layout = QtWidgets.QHBoxLayout()
        self.analyze_button = QtWidgets.QPushButton('Analyze')
        self.button_layout.addWidget(self.analyze_button)
        self.button_layout.addStretch()
        self.leftlayout.addLayout(self.button_layout)

        self.line_list = LineList(self)
        self.leftlayout.addWidget(self.line_list)

        self.rightlayout = QtWidgets.QHBoxLayout()
        self.explanation = QtWidgets.QTextEdit('', self)
        self.explanation.setMinimumWidth(150)
        self.rightlayout.addWidget(self.explanation)

        self.listlayout.addLayout(self.leftlayout)
        self.listlayout.addLayout(self.rightlayout)
        self.main_layout.addLayout(self.listlayout)

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
        self.warning_message.setStyleSheet("color : {};".format(kluster_variables.error_color))
        self.hlayout_msg.addWidget(self.warning_message)
        self.main_layout.addLayout(self.hlayout_msg)

        self.setLayout(self.main_layout)
        self.canceled = False
        self.return_pairs = None

        self.from_selected_lines.clicked.connect(self.radio_selected)
        self.from_points_view.clicked.connect(self.radio_selected)
        self.analyze_button.clicked.connect(self.analyze_data)
        self.ok_button.clicked.connect(self.return_patch_test_data)
        self.cancel_button.clicked.connect(self.cancel_patch)

        self.text_controls = []
        self.checkbox_controls = [['from_points_view', self.from_points_view], ['from_selected_lines', self.from_selected_lines]]
        self.read_settings()
        self._set_explanation()

    @property
    def row_full_attribution(self):
        return self.line_list.final_attribution

    def _set_explanation(self):
        msg = 'Based on "Computation of Calibration Parameters for Multibeam Echo Sounders Using the Least Squares Method"'
        msg += ', by Jan Terje Bjorke\n\nCompute new offsets/angles for the data provided using this automated least squares'
        msg += ' adjustment.'
        self.explanation.setText(msg)

    def err_message(self, text: str = ''):
        if text:
            self.warning_message.setText('ERROR: ' + text)
        else:
            self.warning_message.setText('')

    def analyze_data(self):
        self.err_message()
        if self.from_selected_lines.isChecked():
            self.patch_query.emit('lines')
        elif self.from_points_view.isChecked():
            self.patch_query.emit('pointsview')

    def radio_selected(self, ev):
        if self.from_selected_lines.isChecked():
            self.from_points_view.setChecked(False)
        elif self.from_points_view.isChecked():
            self.from_selected_lines.setChecked(False)

    def add_line(self, line_data: list):
        self.line_list.add_line(line_data)

    def validate_pairs(self):
        pair_dict, err, msg = self.line_list.form_pairs()
        if err:
            self.err_message(msg)
        return err, pair_dict

    def return_patch_test_data(self):
        self.canceled = False
        err, pairdict = self.validate_pairs()
        if not err:
            self.return_pairs = pairdict
            self.save_settings()
            self.accept()

    def cancel_patch(self):
        self.canceled = True
        self.accept()

    def clear(self):
        self.line_list.setup_table()


class LineList(QtWidgets.QTableWidget):
    def __init__(self, parent):
        super().__init__(parent)

        self.setDragEnabled(True)  # enable support for dragging table items
        self.setAcceptDrops(True)  # enable drop events
        self.viewport().setAcceptDrops(True)  # viewport is the total rendered area, this is recommended from my reading
        self.setDragDropOverwriteMode(False)  # False makes sure we don't overwrite rows on dragging
        self.setDropIndicatorShown(True)

        self.setSortingEnabled(True)
        # ExtendedSelection - allows multiselection with shift/ctrl
        self.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.setDragDropMode(QtWidgets.QAbstractItemView.DragDrop)

        # makes it so no editing is possible with the table
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        self.headr = ['Pair', 'Line Name', 'Heading']
        self.setColumnCount(3)
        self.setHorizontalHeaderLabels(self.headr)
        self.setColumnWidth(0, 40)
        self.setColumnWidth(1, 299)
        self.setColumnWidth(2, 80)
        self.row_full_attribution = {}

    @property
    def final_attribution(self):
        curdata = self.row_full_attribution
        actual_lines = []
        for row in range(self.rowCount()):  # update the pair numbers from the table comboboxes first
            pair_num = int(self.cellWidget(row, 0).currentText())
            line_name = str(self.item(row, 1).text())
            curdata[line_name][0] = pair_num
            actual_lines.append(line_name)
        dropthese = []
        for lname in curdata.keys():
            if lname not in actual_lines:
                dropthese.append(lname)
        for lname in dropthese:
            curdata.pop(lname)
        return curdata

    def keyReleaseEvent(self, e):
        """
        Catch keyboard driven events to delete entries or select new rows

        Parameters
        ----------
        e: QEvent generated on keyboard key release

        """
        if e.matches(QtGui.QKeySequence.Delete) or e.matches(QtGui.QKeySequence.Back):
            rows = sorted(set(item.row() for item in self.selectedItems()))
            for row in rows:
                self.removeRow(row)

    def dragEnterEvent(self, e):
        """
        Catch mouse drag enter events to block things not move/read related

        Parameters
        ----------
        e: QEvent which is sent to a widget when a drag and drop action enters it

        """
        if e.source() == self:  # allow MIME type files, have a 'file://', 'http://', etc.
            e.accept()
        else:
            e.ignore()

    def dragMoveEvent(self, e):
        """
        Catch mouse drag enter events to block things not move/read related

        Parameters
        ----------
        e: QEvent which is sent while a drag and drop action is in progress

        """
        if e.source() == self:
            e.accept()
        else:
            e.ignore()

    def dropEvent(self, e):
        """
        On drag and drop, handle either reordering of rows or incoming new data from zarr store

        Parameters
        ----------
        e: QEvent which is sent when a drag and drop action is completed

        """
        if not e.isAccepted() and e.source() == self:
            e.setDropAction(QtCore.Qt.MoveAction)
            drop_row = self.drop_on(e)
            self.custom_move_row(drop_row)
        else:
            e.ignore()

    def drop_on(self, e):
        """
        Returns the integer row index of the insertion point on drag and drop

        Parameters
        ----------
        e: QEvent which is sent when a drag and drop action is completed

        Returns
        -------
        int: row index

        """
        index = self.indexAt(e.pos())
        if not index.isValid():
            return self.rowCount()
        return index.row() + 1 if self.is_below(e.pos(), index) else index.row()

    def is_below(self, pos, index):
        """
        Using the event position and the row rect shape, figure out if the new row should go above the index row or
        below.

        Parameters
        ----------
        pos: position of the cursor at the event time
        index: row index at the cursor

        Returns
        -------
        bool: True if new row should go below, False otherwise

        """
        rect = self.visualRect(index)
        margin = 2
        if pos.y() - rect.top() < margin:
            return False
        elif rect.bottom() - pos.y() < margin:
            return True
        return rect.contains(pos, True) and pos.y() >= rect.center().y()

    def custom_move_row(self, drop_row):
        """
        Something I stole from someone online.  Will get the row indices of the selected rows and insert those rows
        at the drag-n-drop mouse cursor location.  Will even account for relative cursor position to the center
        of the row, see is_below.

        Parameters
        ----------
        drop_row: int, row index of the insertion point for the drag and drop

        """

        self.setSortingEnabled(False)
        rows = sorted(set(item.row() for item in self.selectedItems()))  # pull all the selected rows
        rows_to_move = [[QtWidgets.QTableWidgetItem(self.item(row_index, column_index)) for column_index in
                         range(self.columnCount())] for row_index in rows]  # get the data for the rows

        for row_index in reversed(rows):
            self.removeRow(row_index)
            if row_index < drop_row:
                drop_row -= 1

        for row_index, data in enumerate(rows_to_move):
            row_index += drop_row
            self.insertRow(row_index)
            for column_index, column_data in enumerate(data):
                self.setItem(row_index, column_index, column_data)

        for row_index in range(len(rows_to_move)):
            for i in range(int(len(self.headr))):
                self.item(drop_row + row_index, i).setSelected(True)
        self.setSortingEnabled(True)

    def setup_table(self):
        self.clearContents()
        self.setRowCount(0)
        self.row_full_attribution = {}

    def add_line(self, line_data: list):
        if line_data:
            self.setSortingEnabled(False)
            pair_number, linename, heading = line_data
            if linename in self.row_full_attribution:
                raise Exception("ERROR: PatchTest - Unable to add line {} when this line already exists".format(linename))
            self.row_full_attribution[linename] = [pair_number, heading]
            next_row = self.rowCount()
            self.insertRow(next_row)

            for column_index, column_data in enumerate(line_data):
                if column_index == 0:
                    item = QtWidgets.QComboBox()
                    item.addItems([str(i) for i in range(0, 15)])
                    item.setCurrentText(str(column_data))
                    self.setCellWidget(next_row, column_index, item)
                else:
                    if column_index == 2:  # heading
                        item = QtWidgets.QTableWidgetItem('{:3.3f}'.format(float(column_data)).zfill(7))
                    else:
                        item = QtWidgets.QTableWidgetItem(str(column_data))
                    self.setItem(next_row, column_index, item)
            self.setSortingEnabled(True)

    def form_pairs(self):
        pair_dict = {}
        az_dict = {}
        err = False
        msg = ''
        for lname, ldata in self.final_attribution.items():
            pair_index = int(ldata[0])
            azimuth = float(ldata[1])
            if pair_index in pair_dict:
                pair_dict[pair_index].append(lname)
                az_dict[pair_index].append(azimuth)
            else:
                pair_dict[pair_index] = [lname]
                az_dict[pair_index] = [azimuth]
        for pair_cnt, pair_lines in pair_dict.items():
            if len(pair_lines) > 2:
                msg = 'Pair {} has {} lines, can only have 2'.format(pair_cnt, len(pair_lines))
                err = True
            elif len(pair_lines) < 2:
                msg = 'Pair {} has less than 2 lines, each pair must have 2 lines'.format(pair_cnt)
                err = True
        for pairidx, az_list in az_dict.items():  # tack on the lowest azimuth
            pair_dict[pairidx].append(min(az_list))
        return pair_dict, err, msg


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    dlog = PatchTestDialog()
    dlog.add_line([1, 'tstline', 0.0])
    dlog.add_line([2, 'tstline2', 180.0])
    dlog.show()
    if dlog.exec_():
        pass
