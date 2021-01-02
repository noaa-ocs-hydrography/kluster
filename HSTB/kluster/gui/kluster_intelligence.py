import sys, os
from datetime import datetime
from collections import OrderedDict

from PySide2 import QtGui, QtCore, QtWidgets

from HSTB.shared import RegistryHelpers
from HSTB.kluster.gui import kluster_output_window, kluster_projectview
from HSTB.kluster import fqpr_project, fqpr_intelligence, monitor


class ActionTab(QtWidgets.QWidget):
    """
    Action tab displays all of the actions we have queued up from the intelligence module, shows the text/tooltip
    attributes that we built in the kluster_intelligence module
    """
    def __init__(self, parent=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.parent = parent
        self.vlayout = QtWidgets.QVBoxLayout()
        self.table = QtWidgets.QTableWidget()
        self.vlayout.addWidget(self.table)
        self.setLayout(self.vlayout)

        self.table.setColumnCount(3)
        self.table.setColumnWidth(0, 350)
        self.table.setColumnWidth(1, 200)
        self.table.setColumnWidth(2, 200)

        self.table.setHorizontalHeaderLabels(['Action', 'Progress', 'Function'])
        self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)

        self.setMinimumHeight(600)

        self.actions = []

    def update_actions(self, actions: list):
        """
        Currently whenever we receive new actions, we clear out all the actions and start over

        Parameters
        ----------
        actions
            list of fqpr_intelligence.FqprAction objects
        """

        self.actions = actions
        self.clear_actions()
        for cnt, action in enumerate(self.actions):
            self.add_action(cnt, action)

    def clear_actions(self):
        self.table.clearContents()
        self.table.setRowCount(0)

    def add_action(self, rowcnt, action):
        self.table.insertRow(rowcnt)
        att_item = QtWidgets.QTableWidgetItem(action.text)
        att_item.setToolTip(action.tooltip_text)
        self.table.setItem(rowcnt, 0, att_item)

    def execute_action(self):
        pass


class IntelViewer(QtWidgets.QTableWidget):
    """
    QTableWidget to display the data from an fqpr_intelligence IntelModule.
    """

    file_added = QtCore.Signal(str)
    remove_by_uniqueid = QtCore.Signal(int)
    show_in_explorer = QtCore.Signal(int)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headr = []
        # self.setStyleSheet('font: 10.5pt "Consolas";')

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

        self.right_click_menu = None
        self.setup_menu()
        self.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_right_click_menu)

    def setup_menu(self):
        """
        Build the right click menu for added lines
        """

        self.right_click_menu = QtWidgets.QMenu('menu', self)

        close_dat = QtWidgets.QAction('Close', self)
        close_dat.triggered.connect(self.right_click_close_row)
        show_explorer = QtWidgets.QAction('Open Explorer', self)
        show_explorer.triggered.connect(self.show_file_in_explorer)

        self.right_click_menu.addAction(close_dat)
        self.right_click_menu.addAction(show_explorer)

    def show_right_click_menu(self):
        """
        Generate a close option when you right click as well as an open in explorer option
        """

        if self.selectedItems():  # a row is selected
            self.right_click_menu.exec_(QtGui.QCursor.pos())

    def right_click_close_row(self, event: QtCore.QEvent):
        """
        On right clicking an added file and selecting 'Close', remove the file from the table and from the attached
        intelligence module

        Parameters
        ----------
        event
            triggered event
        """

        itms = self.selectedItems()
        if self.headr[-1] != 'unique_id':
            print('Error: context menu requires "unique_id" as the last element')
            return
        idx = len(self.headr) - 1
        while idx <= len(itms):
            uid = itms[idx].text()
            self.remove_by_uniqueid.emit(int(uid))
            idx += len(self.headr)

    def show_file_in_explorer(self, event: QtCore.QEvent):
        """
        On right clicking an added file and selecting 'Open Explorer', open the containing file in explorer

        Parameters
        ----------
        event
            triggered event
        """

        itms = self.selectedItems()
        if self.headr[-1] != 'unique_id':
            print('Error: context menu requires "unique_id" as the last element')
            return
        idx = len(self.headr) - 1
        while idx <= len(itms):
            uid = itms[idx].text()
            self.show_in_explorer.emit(int(uid))
            idx += len(self.headr)

    def dragEnterEvent(self, event: QtCore.QEvent):
        """
        Catch mouse drag enter events to block things not move/read related

        Parameters
        ----------
        event
            QEvent which is sent to a widget when a drag and drop action enters it
        """

        event.accept()

    def dragMoveEvent(self, event: QtCore.QEvent):
        """
        Catch mouse drag enter events to block things not move/read related

        Parameters
        ----------
        event
            QEvent which is sent while a drag and drop action is in progress
        """

        event.accept()

    def dropEvent(self, event: QtCore.QEvent):
        """
        On drag and drop, handle either reordering of rows or incoming new data from file

        For incoming new file, trigger a file_added event when dragging and dropping a file in the IntelViewer

        Parameters
        ----------
        event
            QEvent which is sent when a drag and drop action is completed
        """

        if not event.isAccepted() and event.source() == self:
            event.setDropAction(QtCore.Qt.MoveAction)
            drop_row = self._drop_row_index(event)
            self.custom_move_row(drop_row)
        elif event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                self.file_added.emit(url.toLocalFile())
        else:
            print('Unrecognized input: {}'.format(event.source()))

    def _drop_row_index(self, event: QtCore.QEvent):
        """
        Returns the integer row index of the insertion point on drag and drop

        Parameters
        ----------
        event
            QEvent which is sent when a drag and drop action is completed

        Returns
        -------
        int
            row index
        """

        index = self.indexAt(event.pos())
        if not index.isValid():
            return self.rowCount()
        return index.row() + 1 if self._is_below(event.pos(), index) else index.row()

    def _is_below(self, pos: QtCore.QPoint, index: int):
        """
        Using the event position and the row rect shape, figure out if the new row should go above the index row or
        below.

        Parameters
        ----------
        pos
            position of the cursor at the event time
        index
            row index at the cursor

        Returns
        -------
        bool
            True if new row should go below, False otherwise
        """

        rect = self.visualRect(index)
        margin = 2
        if pos.y() - rect.top() < margin:
            return False
        elif rect.bottom() - pos.y() < margin:
            return True
        return rect.contains(pos, True) and pos.y() >= rect.center().y()

    def custom_move_row(self, drop_row: int):
        """
        Something I stole from someone online.  Will get the row indices of the selected rows and insert those rows
        at the drag-n-drop mouse cursor location.  Will even account for relative cursor position to the center
        of the row, see _is_below.

        Parameters
        ----------
        drop_row
            row index of the insertion point for the drag and drop
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
                itm = self.item(drop_row + row_index, i)
                if itm is not None:
                    itm.setSelected(True)

    def update_from_dict(self, dict_attributes: OrderedDict):
        """
        Add a new row to the table, where the column values are the matching keys between dict_attributes and the
        self.headr.

        Parameters
        ----------
        dict_attributes
            new row to be added
        """

        if dict_attributes and self.headr:  # headr is only populated when extending this class
            next_row = self.rowCount()
            self.insertRow(next_row)
            for col_index, ky in enumerate(self.headr):
                data = dict_attributes[ky]
                if isinstance(data, datetime):
                    data = data.strftime('%D %H:%M:%S')
                elif isinstance(data, list):
                    for cnt, d in enumerate(data):
                        if isinstance(d, datetime):
                            data[cnt] = d.strftime('%D %H:%M:%S')
                data_item = QtWidgets.QTableWidgetItem(str(data))
                self.setItem(next_row, col_index, data_item)

    def remove_row(self, unique_id: int):
        """
        Remove a row based on the provided unique_id

        Parameters
        ----------
        unique_id
            unique id for the row to be removed
        """
        uid_column_index = self.headr.index('unique_id')
        total_rows = self.rowCount()
        remove_these_rows = []
        for i in range(total_rows):
            if self.item(i, uid_column_index).text() == str(unique_id):
                remove_these_rows.append(i)
        for i in sorted(remove_these_rows, reverse=True):  # remove from bottom up to not mess up index
            self.removeRow(i)


class MultibeamIntel(IntelViewer):
    """
    IntelViewer specific for multibeam files, with multibeam specific attribution
    """

    def __init__(self):
        super().__init__()
        self.headr = ['file_name', 'type', 'data_start_time_utc', 'data_end_time_utc', 'primary_serial_number',
                      'secondary_serial_number', 'sonar_model_number',
                      'last_modified_time_utc', 'created_time_utc', 'file_size_kb', 'time_added', 'unique_id']
        self.setColumnCount(len(self.headr))
        self.setHorizontalHeaderLabels(self.headr)

        hdr = self.horizontalHeader()
        for i in range(0, len(self.headr)):
            hdr.setSectionResizeMode(i, QtWidgets.QHeaderView.ResizeToContents)


class NavIntel(IntelViewer):
    """
    IntelViewer specific for post processed navigation (SBET) files, with SBET specific attribution
    """

    def __init__(self):
        super().__init__()
        self.headr = ['file_name', 'type', 'weekly_seconds_start', 'weekly_seconds_end', 'last_modified_time_utc',
                      'created_time_utc', 'file_size_kb', 'time_added', 'unique_id']
        self.setColumnCount(len(self.headr))
        self.setHorizontalHeaderLabels(self.headr)

        hdr = self.horizontalHeader()
        for i in range(0, len(self.headr)):
            hdr.setSectionResizeMode(i, QtWidgets.QHeaderView.ResizeToContents)


class NavErrorIntel(IntelViewer):
    """
    IntelViewer specific for post processed nav error (SMRMSG) files, with SMRMSG specific attribution
    """

    def __init__(self):
        super().__init__()
        self.headr = ['file_name', 'type', 'weekly_seconds_start', 'weekly_seconds_end', 'last_modified_time_utc',
                      'created_time_utc', 'file_size_kb', 'time_added', 'unique_id']
        self.setColumnCount(len(self.headr))
        self.setHorizontalHeaderLabels(self.headr)

        hdr = self.horizontalHeader()
        for i in range(0, len(self.headr)):
            hdr.setSectionResizeMode(i, QtWidgets.QHeaderView.ResizeToContents)


class NavLogIntel(IntelViewer):
    """
    IntelViewer specific for sbet export log files, with log file specific attribution
    """

    def __init__(self):
        super().__init__()
        self.headr = ['file_name', 'input_sbet_file', 'exported_sbet_file', 'sample_rate_hertz', 'type',
                      'mission_date', 'datum', 'ellipsoid', 'last_modified_time_utc', 'created_time_utc',
                      'file_size_kb', 'time_added', 'unique_id']
        self.setColumnCount(len(self.headr))
        self.setHorizontalHeaderLabels(self.headr)

        hdr = self.horizontalHeader()
        for i in range(0, len(self.headr)):
            hdr.setSectionResizeMode(i, QtWidgets.QHeaderView.ResizeToContents)


class SvpIntel(IntelViewer):
    """
    IntelViewer specific for caris svp files, with svp file specific attribution
    """

    def __init__(self):
        super().__init__()
        self.headr = ['file_name', 'type', 'number_of_profiles', 'number_of_layers', 'julian_day',
                      'time_utc', 'latitude', 'longitude', 'source_epsg', 'utm_zone', 'utm_hemisphere',
                      'last_modified_time_utc', 'created_time_utc', 'file_size_kb', 'time_added', 'unique_id']
        self.setColumnCount(len(self.headr))
        self.setHorizontalHeaderLabels(self.headr)

        hdr = self.horizontalHeader()
        for i in range(0, len(self.headr)):
            hdr.setSectionResizeMode(i, QtWidgets.QHeaderView.ResizeToContents)


class IntelTab(QtWidgets.QTabWidget):
    """
    Tab widget holding all the intelligence viewer objects and associated objects (monitoring, etc.)
    """

    def __init__(self):
        super().__init__()
        self.setSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Preferred)
        # self.setStyleSheet(('font: 10.5pt "Consolas";'))


class IntelOutput(kluster_output_window.KlusterOutput):
    """
    KlusterOutput window for stdout/stderr
    """

    def __init__(self):
        super().__init__()
        self.setSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Preferred)


class MonitorPath(QtWidgets.QWidget):
    """
    Base widget for interacting with the fqpr_intelligence.DirectoryMonitor object.  Each instance of this class
    has a file browse button, status light, etc.
    """

    def __init__(self, parent: QtWidgets.QWidget = None):
        """
        initialize

        Parameters
        ----------
        parent
            MonitorDashboard
        """
        super().__init__()

        self.parent = parent
        self.hlayout = QtWidgets.QHBoxLayout()
        self.statuslight = QtWidgets.QCheckBox('')
        self.statuslight.setStyleSheet("background-color: black")
        self.statuslight.setCheckable(False)
        self.hlayout.addWidget(self.statuslight)
        self.fil_text = QtWidgets.QLineEdit('')
        self.fil_text.setMinimumWidth(400)
        self.fil_text.setReadOnly(True)
        self.hlayout.addWidget(self.fil_text)
        self.browse_button = QtWidgets.QPushButton("Browse")
        self.hlayout.addWidget(self.browse_button)
        self.start_button = QtWidgets.QPushButton('Start')
        self.hlayout.addWidget(self.start_button)
        self.stop_button = QtWidgets.QPushButton('Stop')
        self.hlayout.addWidget(self.stop_button)
        spcr = QtWidgets.QLabel('      ')
        self.hlayout.addWidget(spcr)
        self.include_subdirectories = QtWidgets.QCheckBox('Include Subdirectories')
        self.hlayout.addWidget(self.include_subdirectories)
        self.setLayout(self.hlayout)

        self.browse_button.clicked.connect(self.dir_browse)
        self.start_button.clicked.connect(self.start_monitoring)
        self.stop_button.clicked.connect(self.stop_monitoring)

        self.monitor = None

    def dir_browse(self):
        """
        As long as you aren't currently running the monitoring, this will get the directory you want to monitor
        """

        if not self.is_running():
            # dirpath will be None or a string
            msg, pth = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='klusterintel',
                                                        Title='Select directory to monitor', AppName='klusterintel')
            if pth is not None:
                self.fil_text.setText(pth)
        else:
            print('You have to stop monitoring before you can change the path')

    def return_monitoring_path(self):
        """
        Return the path we are monitoring

        Returns
        -------
        str
            directory path we are monitoring
        """

        return self.fil_text.displayText()

    def is_recursive(self):
        """
        Return whether or not the include_subdirectories checkbox is checked

        Returns
        -------
        bool
            True if checked
        """

        return self.include_subdirectories.isChecked()

    def is_running(self):
        """
        Return whether or not the monitor is running

        Returns
        -------
        bool
            True if the monitor is running
        """

        if self.monitor is not None:
            if self.monitor.watchdog_observer.is_alive():
                return True
        return False

    def start_monitoring(self):
        """
        Start a new DirectoryMonitor.  A stopped DirectoryMonitor will have to be re-instantiated, you can't restart
        a watchdog observer.

        Also sets the status light to green
        """

        pth = self.return_monitoring_path()
        is_recursive = self.is_recursive()
        if os.path.exists(pth):
            # you can't restart a watchdog observer, have to create a new one
            self.stop_monitoring()
            self.monitor = monitor.DirectoryMonitor(pth, is_recursive)
            if self.parent is not None:
                self.monitor.bind_to(self.parent.update_files)
            self.monitor.start()
            self.include_subdirectories.setEnabled(False)
            self.statuslight.setStyleSheet("background-color: green")
            print('Monitoring {}'.format(pth))

    def stop_monitoring(self):
        """
        If the DirectoryMonitor object is running, stop it
        """

        if self.is_running():
            self.monitor.stop()
            self.include_subdirectories.setEnabled(True)
            self.statuslight.setStyleSheet("background-color: black")
            print('No longer monitoring {}'.format(self.return_monitoring_path()))


class MonitorDashboard(QtWidgets.QWidget):
    """
    MonitorDashboard holds all the MonitorPath widgets in a vertical layout.  We bind the update_files method to the
    fqpr_intelligence.DirectoryMonitor object to receive files as they are created/destroyed.
    """

    def __init__(self, parent=None):
        super().__init__()
        self.parent = parent
        self.vlayout = QtWidgets.QVBoxLayout()
        self.total_files = []
        self.total_monitors = 20
        self.monitors = []
        self.file_buffer = []
        self.seen_files = []

        scroll = QtWidgets.QScrollArea()

        scroll_content = QtWidgets.QWidget()
        scroll_layout = QtWidgets.QVBoxLayout(scroll_content)
        scroll_content.setLayout(scroll_layout)

        for i in range(self.total_monitors):
            if self.parent is not None:
                self.monitors.append(MonitorPath(self))
            else:
                self.monitors.append(MonitorPath())
            scroll_layout.addWidget(self.monitors[i])

        scroll.setWidget(scroll_content)
        scroll.setWidgetResizable(True)
        self.vlayout.addWidget(scroll)
        scroll_layout.addStretch(1)

        self.setLayout(self.vlayout)

    def update_files(self, fil: str, file_event: str):
        """
        updates the KlusterIntelligence module of the new files found by the directory monitoring object

        mirrors the fqpr_intelligence.FqprIntel.handle_monitor_event method (for when you use the intelligence
        module outside of the gui)

        Parameters
        ----------
        fil
            absolute file path to the new file
        file_event
            event type, one of 'created', 'deleted'
        """

        if self.parent is not None:
            if fil not in self.seen_files and file_event == 'created':
                self.parent.add_new_file(fil)
            elif file_event == 'deleted':
                if fil in self.seen_files:
                    self.seen_files.remove(fil)
                self.parent.remove_file(fil)
        else:
            print('expected the KlusterIntelligence module passed as a parent to this class')


class KlusterIntelligence(QtWidgets.QMainWindow):
    def __init__(self):
        """
        Build out the dock widgets with the kluster widgets inside.  Will use QSettings object to retain size and
        position.
        """

        super().__init__()

        self.setWindowTitle('Kluster Intelligence')
        self.setDockNestingEnabled(True)

        self.iconpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images', 'kluster_img.ico')
        self.setWindowIcon(QtGui.QIcon(self.iconpath))

        self.widget_obj_names = []

        self.project = None

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.intelligence = fqpr_intelligence.FqprIntel(project=self.project)

        self.action_tab = ActionTab(self)
        self.monitor_dashboard = MonitorDashboard(self)
        self.project_view = kluster_projectview.KlusterProjectView(self)

        self.multibeam_intel = MultibeamIntel()
        self.nav_intel = NavIntel()
        self.naverror_intel = NavErrorIntel()
        self.navlog_intel = NavLogIntel()
        self.svp_intel = SvpIntel()

        self.lyout = QtWidgets.QGridLayout()
        self.top_widget = QtWidgets.QWidget()
        self.intel_tab = IntelTab()
        self.output_window = IntelOutput()

        self.setup_widgets()
        self.setup_signals()

        self.seen_files = []
        self.intelligence.bind_to(self.action_tab.update_actions)

    def sizeHint(self):
        return QtCore.QSize(1200, 600)

    def minimumSizeHint(self):
        return QtCore.QSize(600, 400)

    def set_project(self, project: fqpr_project.FqprProject):
        """
        Setting the project here means we also need to set the intelligence module project as well.  The intelligence
        module holds all of the data for us, we just ask it to update the gui when it receives/generates new information

        fqpr = fully qualified ping record, the term for the datastore in kluster
        Parameters
        ----------
        project
            new project instance to attach to the intelligence module

        Returns
        -------

        """
        self.project = project
        self.intelligence = fqpr_intelligence.FqprIntel(project=self.project)
        self.intelligence.bind_to(self.action_tab.update_actions)

    def setup_widgets(self):
        """
        Build out the initial positioning of the widgets.
        """

        # hide the central widget so that we can have an application with all dockable widgets
        self.setCentralWidget(self.top_widget)

        self.intel_tab.addTab(self.action_tab, 'Actions')
        self.intel_tab.addTab(self.project_view, 'Project')
        self.intel_tab.addTab(self.multibeam_intel, 'Multibeam')
        self.intel_tab.addTab(self.nav_intel, 'Processed Navigation')
        self.intel_tab.addTab(self.naverror_intel, 'Processed Nav Error')
        self.intel_tab.addTab(self.navlog_intel, 'Processed Nav Log')
        self.intel_tab.addTab(self.svp_intel, 'Processed SVP')
        self.intel_tab.addTab(self.monitor_dashboard, 'Monitor')

        self.lyout.addWidget(self.intel_tab, 0, 0, 2, 1)
        self.lyout.addWidget(self.output_window, 2, 0, 1, 1)

        self.setLayout(self.lyout)
        self.centralWidget().setLayout(self.lyout)

    def setup_signals(self):
        """
        Attach all the IntelViewer signals to the methods in this class
        """

        self.multibeam_intel.file_added.connect(self.add_new_file)
        self.multibeam_intel.remove_by_uniqueid.connect(self.remove_by_mouseclick)
        self.multibeam_intel.show_in_explorer.connect(self.show_in_explorer)
        self.svp_intel.file_added.connect(self.add_new_file)
        self.svp_intel.remove_by_uniqueid.connect(self.remove_by_mouseclick)
        self.svp_intel.show_in_explorer.connect(self.show_in_explorer)
        self.navlog_intel.file_added.connect(self.add_new_file)
        self.navlog_intel.remove_by_uniqueid.connect(self.remove_by_mouseclick)
        self.navlog_intel.show_in_explorer.connect(self.show_in_explorer)
        self.naverror_intel.file_added.connect(self.add_new_file)
        self.naverror_intel.remove_by_uniqueid.connect(self.remove_by_mouseclick)
        self.naverror_intel.show_in_explorer.connect(self.show_in_explorer)
        self.nav_intel.file_added.connect(self.add_new_file)
        self.nav_intel.remove_by_uniqueid.connect(self.remove_by_mouseclick)
        self.nav_intel.show_in_explorer.connect(self.show_in_explorer)

    def add_new_file(self, filepath: str):
        """
        On dragging in a new file (or through the directory monitoring object), add the file to the appropriate
        IntelViewer object

        Parameters
        ----------
        filepath
            absolute file path to the newly added file
        """

        if os.path.isdir(filepath):
            fils = [os.path.join(filepath, f) for f in os.listdir(filepath)]
        else:
            fils = [filepath]
        for f in fils:
            updated_type, new_data = self.intelligence.add_file(f)
            if updated_type == 'multibeam':
                self.multibeam_intel.update_from_dict(new_data)
            elif updated_type == 'svp':
                self.svp_intel.update_from_dict(new_data)
            elif updated_type == 'navigation':
                self.nav_intel.update_from_dict(new_data)
            elif updated_type == 'naverror':
                self.naverror_intel.update_from_dict(new_data)
            elif updated_type == 'navlog':
                self.navlog_intel.update_from_dict(new_data)

    def remove_file(self, filepath: str):
        """
        On right-click and removing a file (or removing it from a monitored folder), remove the file from the appropriate
        IntelViewer object

        Parameters
        ----------
        filepath
            absolute file path to the newly added file
        """

        if os.path.isdir(filepath):
            fils = [os.path.join(filepath, f) for f in os.listdir(filepath)]
        else:
            fils = [filepath]
        for f in fils:
            updated_type, unique_id = self.intelligence.remove_file(f)
            if updated_type == 'multibeam':
                self.multibeam_intel.remove_row(unique_id)
            elif updated_type == 'svp':
                self.svp_intel.remove_row(unique_id)
            elif updated_type == 'navigation':
                self.nav_intel.remove_row(unique_id)
            elif updated_type == 'naverror':
                self.naverror_intel.remove_row(unique_id)
            elif updated_type == 'navlog':
                self.navlog_intel.remove_row(unique_id)

    def return_filepath_from_unique_id(self, unique_id: int):
        """
        Use the unique id to look up the full file path

        Parameters
        ----------
        unique_id
            unique id for the row/file

        Returns
        -------
        str
            absolute file path to the file with this unique id
        """

        filepath = None
        if unique_id in list(self.intelligence.multibeam_intel.unique_id_reverse.keys()):
            filepath = self.intelligence.multibeam_intel.unique_id_reverse[unique_id]
        elif unique_id in list(self.intelligence.svp_intel.unique_id_reverse.keys()):
            filepath = self.intelligence.svp_intel.unique_id_reverse[unique_id]
        elif unique_id in list(self.intelligence.nav_intel.unique_id_reverse.keys()):
            filepath = self.intelligence.nav_intel.unique_id_reverse[unique_id]
        elif unique_id in list(self.intelligence.naverror_intel.unique_id_reverse.keys()):
            filepath = self.intelligence.naverror_intel.unique_id_reverse[unique_id]
        elif unique_id in list(self.intelligence.navlog_intel.unique_id_reverse.keys()):
            filepath = self.intelligence.navlog_intel.unique_id_reverse[unique_id]
        return filepath

    def remove_by_mouseclick(self, unique_id: int):
        """
        When we right click and select 'Close' on a row, we use the unique_id here to get the filepath that we then
        use to remove from the IntelView and IntelModule associated with that file type

        Parameters
        ----------
        unique_id
            unique id for the row/file
        """

        filepath = self.return_filepath_from_unique_id(unique_id)
        if filepath is not None:
            self.remove_file(filepath)
        else:
            print("Can't locate row by uniqueid {}".format(unique_id))

    def show_in_explorer(self, unique_id):
        """
        When we right click and select 'Open Explorer' on a row, we use the unique_id here to get the filepath that we
        then use to open Windows Explorer to show the containing folder

        Parameters
        ----------
        unique_id
            unique id for the row/file
        """

        filepath = self.return_filepath_from_unique_id(unique_id)
        if filepath is not None:
            os.startfile(os.path.dirname(filepath))
        else:
            print("Can't locate row by uniqueid {}".format(unique_id))


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    # Force the style to be the same on all OSs:
    app.setStyle("Fusion")

    # Now use a palette to switch to dark colors:
    palette = QtGui.QPalette()
    palette.setColor(QtGui.QPalette.Window, QtGui.QColor(53, 53, 53))
    palette.setColor(QtGui.QPalette.WindowText, QtCore.Qt.gray)
    palette.setColor(QtGui.QPalette.Base, QtGui.QColor(25, 25, 25))
    palette.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor(53, 53, 53))
    palette.setColor(QtGui.QPalette.ToolTipBase, QtCore.Qt.black)
    palette.setColor(QtGui.QPalette.ToolTipText, QtCore.Qt.gray)
    palette.setColor(QtGui.QPalette.Text, QtCore.Qt.gray)
    palette.setColor(QtGui.QPalette.Button, QtGui.QColor(53, 53, 53))
    palette.setColor(QtGui.QPalette.ButtonText, QtCore.Qt.gray)
    palette.setColor(QtGui.QPalette.BrightText, QtCore.Qt.red)
    palette.setColor(QtGui.QPalette.Link, QtGui.QColor(42, 130, 218))
    palette.setColor(QtGui.QPalette.Highlight, QtGui.QColor(42, 130, 218))
    palette.setColor(QtGui.QPalette.HighlightedText, QtCore.Qt.black)
    app.setPalette(palette)

    window = KlusterIntelligence()
    window.show()
    sys.exit(app.exec_())