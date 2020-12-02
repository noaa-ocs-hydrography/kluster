import sys
import os
import pprint
from collections import OrderedDict
from datetime import datetime

from PySide2 import QtCore, QtGui, QtWidgets


class KlusterExplorer(QtWidgets.QTableWidget):
    """
    Instance of QTableWidget designed to display the converted Kluster attribution on a line by line basis.  Allows for
    some drag and drop sorting and other features.  Selecting a row will feed the attribution for that converted
    zarr object to the KlusterAttribution object.

    """
    # signals must be defined on the class, not the instance of the class
    row_selected = QtCore.Signal(object)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.setObjectName('kluster_explorer')

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

        self.cellDoubleClicked.connect(self.view_full_attribution)
        self.cellClicked.connect(self.update_attribution)

        self.setColumnCount(6)
        self.headr = ['Name', 'Survey Identifier', 'EPSG', 'Min Time', 'Max Time', 'Source']
        self.setHorizontalHeaderLabels(self.headr)
        self.horizontalHeader().setStretchLastSection(True)
        self.setColumnWidth(0, 250)
        self.setColumnWidth(1, 150)
        self.setColumnWidth(2, 80)
        self.setColumnWidth(3, 150)
        self.setColumnWidth(4, 150)
        self.setColumnWidth(5, 200)

        self.row_full_attribution = {}
        self.row_translated_attribution = {}

    def keyReleaseEvent(self, e):
        """
        Catch keyboard driven events to delete entries

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

    def update_attribution(self, row, column):
        """
        Get the line name and emit the attribution for that project.  Get's picked up by the KlusterAttribution widget
        for display.  See kluster_main.

        Parameters
        ----------
        row: int, row number
        column: int, column number

        """
        linename = None
        try:
            linename = self.item(row, 0).text()
            attribution = self.row_full_attribution[linename]
            self.row_selected.emit(attribution)
        except (AttributeError, KeyError):
            print('Kluster_Explorer: Unable to find attribution for line {}'.format(linename))

    def view_full_attribution(self, row, column):
        """
        On double click, this will bring up a message box containing the full attribution for the converted object in
        a message box.

        It's pretty ugly at this point, will need to make something better from this in the future.

        Parameters
        ----------
        row: int, row number
        column: int, column number

        """
        name_item = self.item(row, 0)
        linename = name_item.text()

        info = QtWidgets.QMessageBox()
        info.setWindowTitle('Full Attribution')
        info.setText(pprint.pformat(self.row_full_attribution[linename]))
        info.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
        result = info.exec_()

    def translate_fqpr_attribution(self, attrs):
        """
        Gets the attribution from the provided fqpr_generation.FPQR object and translates it for viewing

        Will return the selection of attributes that are needed to populate the table

        Parameters
        ----------
        attrs: dict, fqpr_generation.FPQR attribution

        Returns
        -------
        translated_attrs: list of OrderedDict, attributes by line that match self.headr order
        attrs: OrderedDict, raw attribution from the zarr store

        """
        translated_attrs = []
        if 'multibeam_files' in attrs:
            line_names = list(attrs['multibeam_files'].items())
            for cnt, ln in enumerate(line_names):
                # ln is tuple like ('0015_20200304_070725_S250.all', [1583305645.423, 1583305889.905])
                newline_attr = OrderedDict([(h, '') for h in self.headr])
                newline_attr['Source'] = os.path.split(attrs['output_path'])[1]
                newline_attr['Name'] = ln[0]
                if 'survey_number' in attrs:
                    newline_attr['Survey Identifier'] = ', '.join(attrs['survey_number'])
                else:
                    newline_attr['Survey Identifier'] = ''
                min_line_time = ln[1][0]
                max_line_time = ln[1][1]
                newline_attr['Min Time'] = datetime.utcfromtimestamp(min_line_time).strftime('%D %H%M%S')
                newline_attr['Max Time'] = datetime.utcfromtimestamp(max_line_time).strftime('%D %H%M%S')
                if 'xyz_crs' in attrs:
                    newline_attr['EPSG'] = str(attrs['xyz_crs'])
                translated_attrs.append(newline_attr)

        return translated_attrs, attrs

    def build_line_attribution(self, linename, raw_attrs):
        """
        Uses line name and attribution for the project that line is associated with.

        Returns translated attribution for that line.  If it is the first time seeing this line, will build out
        all the line attribution for all lines in raw_attrs

        Parameters
        ----------
        linename: str, line name
        raw_attrs: dict, attribution of fqpr_generation.Fqpr instance that the linename is in

        """
        if linename in self.row_translated_attribution:
            line_data = self.row_translated_attribution[linename]
        else:
            line_data = None
            data, raw_attribution = self.translate_fqpr_attribution(raw_attrs)
            for line in data:
                self.row_full_attribution[line['Name']] = raw_attribution
                self.row_translated_attribution[line['Name']] = line
                if line['Name'] == linename:
                    line_data = line
            if line_data is None:
                print('build_line_attribution: Unable to find attribution for line {}'.format(linename))
        return line_data

    def populate_explorer(self, linename, raw_attrs):
        """
        Uses line name and attribution for the project that line is associated with.

        Returns translated attribution for that line.  If it is the first time seeing this line, will build out
        all the line attribution for all lines in raw_attrs

        Parameters
        ----------
        linename: str, line name
        raw_attrs: dict, attribution of fqpr_generation.Fqpr instance that the linename is in

        """
        line_data = self.build_line_attribution(linename, raw_attrs)
        if line_data is not None:
            next_row = self.rowCount()
            self.insertRow(next_row)

            for column_index, column_data in enumerate(line_data):
                item = QtWidgets.QTableWidgetItem(line_data[column_data])

                if self.headr[column_index] == 'Source':
                    item.setToolTip(raw_attrs['output_path'])

                self.setItem(next_row, column_index, item)

    def clear_explorer_data(self):
        """
        Clear out the data but keep the headers.  Also set the row count to zero so that the next insertRow call is
        at the first line.

        """
        self.clearContents()
        self.setRowCount(0)


class KlusterAttribution(QtWidgets.QTableWidget):
    """
    Instance of QTableWidget designed to display the full attribution in a scrollable sheet

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.setObjectName('kluster_attribution')

        self.setDragEnabled(False)
        self.setAcceptDrops(False)
        self.viewport().setAcceptDrops(False)
        self.setDropIndicatorShown(True)

        self.setSortingEnabled(True)
        self.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.setDragDropMode(QtWidgets.QAbstractItemView.NoDragDrop)

        # makes it so no editing is possible with the table
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        self.setColumnCount(2)
        self.headr = ['Attribute', 'Value']
        self.setColumnWidth(0, 100)
        self.setColumnWidth(1, 250)

        self.setHorizontalHeaderLabels(self.headr)
        self.horizontalHeader().setStretchLastSection(True)

    def display_file_attribution(self, attrs):
        """
        Kluster Explorer will activate this method on clicking one of the rows of data.  This method will take the
        attribution assigned to the converted object that holds that line and populate this table with a key/value per
        row type view.

        Parameters
        ----------
        attrs: list, list of attributes from the zarr datastore

        """
        self.setRowCount(0)
        rowoffset = 0
        for cnt, a in enumerate(attrs):
            if type(attrs[a]) is not dict:
                self.insertRow(cnt + rowoffset)
                att_item = QtWidgets.QTableWidgetItem(a)
                self.setItem(cnt + rowoffset, 0, att_item)

                value = pprint.pformat(attrs[a])
                val_item = QtWidgets.QTableWidgetItem(value)
                val_item.setToolTip(value)
                self.setItem(cnt + rowoffset, 1, val_item)
            else:
                hdr_row = cnt + rowoffset
                dictdata = attrs[a]
                for k, v in dictdata.items():
                    self.insertRow(cnt + rowoffset)
                    if hdr_row == cnt + rowoffset:
                        att_item = QtWidgets.QTableWidgetItem(a)
                        self.setItem(hdr_row, 0, att_item)

                    dat = '{}: {}'.format(k, v)
                    val_item = QtWidgets.QTableWidgetItem(dat)
                    val_item.setToolTip(dat)
                    self.setItem(cnt + rowoffset, 1, val_item)
                    rowoffset += 1
                rowoffset -= 1

    def clear_attribution_data(self):
        """
        Clear out the data but keep the headers.  Also set the row count to zero so that the next insertRow call is
        at the first line.

        """
        self.clearContents()
        self.setRowCount(0)


class MyTestWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the KlusterExplorer for testing
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.resize(1220, 450)
        self.setWindowTitle('Kluster Explorer')
        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        self.k_explorer = KlusterExplorer(self)
        self.k_explorer.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        self.k_explorer.setMinimumWidth(500)
        layout.addWidget(self.k_explorer)

        # test filling a couple of rows with data
        for j in range(2):
            self.k_explorer.insertRow(j)
            for i in range(6):
                item = QtWidgets.QTableWidgetItem(str(i))
                if j == 1 and i == 1:
                    # item.setFlags(QtCore.Qt.ItemIsUserCheckable | QtCore.Qt.ItemIsEnabled)
                    item.setCheckState(QtCore.Qt.Checked)
                self.k_explorer.setItem(j, i, item)

        self.k_attribution = KlusterAttribution(self)
        self.k_attribution.setSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Expanding)
        self.k_attribution.setMinimumWidth(300)
        layout.addWidget(self.k_attribution)

        self.k_explorer.row_selected.connect(self.k_attribution.display_file_attribution)

        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    test_window = MyTestWindow()
    test_window.show()
    sys.exit(app.exec_())
