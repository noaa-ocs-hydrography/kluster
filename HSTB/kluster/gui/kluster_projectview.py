from datetime import datetime, timezone
from PySide2 import QtCore, QtGui, QtWidgets
from collections import OrderedDict
import os

from HSTB.kluster.fqpr_generation import Fqpr
from HSTB.kluster.fqpr_project import FqprProject
from HSTB.kluster.gui.common_widgets import CollapsibleWidget
from HSTB.shared import RegistryHelpers


class MultibeamTable(QtWidgets.QWidget):
    """
    Contains the QTableWidget that displays all the information related to the multibeam files in the project
    """

    def __init__(self, multibeam_dict: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vlayout = QtWidgets.QVBoxLayout()
        self.table = QtWidgets.QTableWidget()
        self.vlayout.addWidget(self.table)
        self.setLayout(self.vlayout)

        self.table.setSortingEnabled(True)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setColumnCount(3)
        self.table.setColumnWidth(0, 350)
        self.table.setColumnWidth(1, 200)
        self.table.setColumnWidth(2, 200)

        self.table.setHorizontalHeaderLabels(['Multibeam File Name', 'Multibeam Start Time', 'Multibeam End Time'])
        self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)

        self.multibeam_dict = multibeam_dict
        self.populate()
        self.setMinimumHeight(600)
        self.vlayout.layout()

    def populate(self):
        for mbesfile, times in self.multibeam_dict.items():
            next_row = self.table.rowCount()
            self.table.insertRow(next_row)
            self.table.setItem(next_row, 0, QtWidgets.QTableWidgetItem(mbesfile))
            self.table.setItem(next_row, 1, QtWidgets.QTableWidgetItem(datetime.fromtimestamp(times[0], tz=timezone.utc).strftime('%c')))
            self.table.setItem(next_row, 2, QtWidgets.QTableWidgetItem(datetime.fromtimestamp(times[1], tz=timezone.utc).strftime('%c')))


class StatusTable(QtWidgets.QWidget):
    """
    Contains the QTableWidget that has all the information about the processed status of each sounding in the project
    """
    def __init__(self, status_dict: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vlayout = QtWidgets.QVBoxLayout()
        self.table = QtWidgets.QTableWidget()
        self.vlayout.addWidget(self.table)
        self.setLayout(self.vlayout)

        self.table.setSortingEnabled(True)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        self.status_dict = status_dict
        self.headr = ['SerialNumber_SectorNumber_Frequency'] + list(self.status_dict[list(self.status_dict.keys())[0]].keys())

        self.table.setColumnCount(len(self.headr))
        self.table.setColumnWidth(0, 250)
        for i in range(len(self.headr) - 1):
            self.table.setColumnWidth(i + 1, 100)

        self.table.setHorizontalHeaderLabels(self.headr)
        # self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)

        self.populate()
        self.setMinimumHeight(300)
        self.vlayout.layout()

    def populate(self):
        for sector, counts in self.status_dict.items():
            status_values = list(counts.values())
            next_row = self.table.rowCount()
            self.table.insertRow(next_row)
            self.table.setItem(next_row, 0, QtWidgets.QTableWidgetItem(sector))
            for cnt, val in enumerate(status_values):
                self.table.setItem(next_row, cnt + 1, QtWidgets.QTableWidgetItem(str(val)))


class LastRunTable(QtWidgets.QWidget):
    def __init__(self, lastrun_dict: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vlayout = QtWidgets.QVBoxLayout()
        self.table = QtWidgets.QTableWidget()
        self.vlayout.addWidget(self.table)
        self.setLayout(self.vlayout)

        self.table.setSortingEnabled(True)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        self.lastrun_dict = lastrun_dict
        self.headr = ['SerialNumber_SectorNumber_Frequency'] + [x[1:] + '_utc' for x in list(self.lastrun_dict[list(self.lastrun_dict.keys())[0]].keys())]

        self.table.setColumnCount(len(self.headr))
        self.table.setColumnWidth(0, 250)
        self.table.setColumnWidth(1, 160)
        self.table.setColumnWidth(2, 200)
        self.table.setColumnWidth(3, 215)
        self.table.setColumnWidth(4, 215)
        self.table.setColumnWidth(5, 230)
        self.table.setColumnWidth(6, 200)

        self.table.setHorizontalHeaderLabels(self.headr)
        # self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)

        self.populate()
        self.setMinimumHeight(300)
        self.vlayout.layout()

    def populate(self):
        for sector, counts in self.lastrun_dict.items():
            lastrun_values = list(counts.values())
            next_row = self.table.rowCount()
            self.table.insertRow(next_row)
            self.table.setItem(next_row, 0, QtWidgets.QTableWidgetItem(sector))
            for cnt, val in enumerate(lastrun_values):
                self.table.setItem(next_row, cnt + 1, QtWidgets.QTableWidgetItem(val))


class KlusterFqprView(QtWidgets.QWidget):
    def __init__(self, parent, fqpr_inst: Fqpr, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.parent = parent
        self.fqpr_inst = fqpr_inst
        self.dashboard_data = fqpr_inst.return_processing_dashboard()

        self.vlayout = QtWidgets.QVBoxLayout()
        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderHidden(True)

        mfile = QtWidgets.QTreeWidgetItem(['multibeam files'])
        l1 = QtWidgets.QTreeWidgetItem(mfile)
        self.mfile_table = MultibeamTable(self.dashboard_data['multibeam_files'])
        self.tree.setItemWidget(l1, 0, self.mfile_table)
        self.tree.addTopLevelItem(mfile)

        sstatus = QtWidgets.QTreeWidgetItem(['sounding status'])
        l2 = QtWidgets.QTreeWidgetItem(sstatus)
        self.soundingstatus_table = StatusTable(self.dashboard_data['sounding_status'])
        self.tree.setItemWidget(l2, 0, self.soundingstatus_table)
        self.tree.addTopLevelItem(sstatus)

        lrun = QtWidgets.QTreeWidgetItem(['last run process'])
        l3 = QtWidgets.QTreeWidgetItem(lrun)
        self.lastrun_table = LastRunTable(self.dashboard_data['last_run'])
        self.tree.setItemWidget(l3, 0, self.lastrun_table)
        self.tree.addTopLevelItem(lrun)

        self.vlayout.addWidget(self.tree)
        self.setLayout(self.vlayout)


class KlusterProjectView(QtWidgets.QWidget):
    """
    QTableWidget to display the data from an fqpr_intelligence IntelModule.
    """

    file_added = QtCore.Signal(str)

    def __init__(self, parent=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.parent = parent
        self.project_file = None
        self.project = None
        self.loaded_fqpr_views = []
        self.loaded_collapsible = []

        self.mainlayout = QtWidgets.QVBoxLayout()

        self.hlayout = QtWidgets.QHBoxLayout()
        self.fil_text = QtWidgets.QLineEdit('')
        self.fil_text.setMinimumWidth(400)
        self.fil_text.setReadOnly(True)
        self.hlayout.addWidget(self.fil_text)
        self.newproj_button = QtWidgets.QPushButton("New Project")
        self.hlayout.addWidget(self.newproj_button)
        self.openproj_button = QtWidgets.QPushButton("Open Project")
        self.hlayout.addWidget(self.openproj_button)
        self.mainlayout.addLayout(self.hlayout)

        scroll = QtWidgets.QScrollArea()
        scroll.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)

        scroll_content = QtWidgets.QWidget()
        scroll_layout = QtWidgets.QVBoxLayout(scroll_content)
        scroll_content.setLayout(scroll_layout)

        self.datalayout = QtWidgets.QVBoxLayout()
        scroll_layout.addLayout(self.datalayout)

        scroll.setWidget(scroll_content)
        scroll.setWidgetResizable(True)
        self.mainlayout.addWidget(scroll)

        self.setLayout(self.mainlayout)
        self.setMinimumSize(1000, 600)

        self.newproj_button.clicked.connect(self.new_project)
        self.openproj_button.clicked.connect(self.open_project)

    def _load_from_project(self):
        """
        Build out the gui from the loaded project.  Each fqpr instance gets it's own collapsible section
        """

        for fqpr_name, fqpr_inst in self.project.fqpr_instances.items():
            fqprview = KlusterFqprView(self, fqpr_inst)
            new_expand = CollapsibleWidget(self, fqpr_name, 100, set_expanded_height=800)
            new_layout = QtWidgets.QVBoxLayout()
            new_layout.addWidget(fqprview)
            new_expand.setContentLayout(new_layout)
            self.datalayout.addWidget(new_expand)

            self.loaded_fqpr_views.append(fqprview)
            self.loaded_collapsible.append(new_expand)
        self.datalayout.addStretch()
        self.datalayout.layout()

    def new_project(self):
        """
        Get the file path to a new project
        """

        # dirpath will be None or a string
        msg, pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='klusterintel', Title='Create a new Kluster project',
                                                         AppName='klusterintel', fFilter="*.json", bSave=True,
                                                         DefaultFile='kluster_project.json')
        if pth:
            # the project name is mandatory, just so that we can find it later, I ask for a file path for the project
            #    file and override the filename, kind of messy but works for now
            if os.path.exists(pth):
                os.remove(pth)
            directory, filename = os.path.split(pth)
            project_file = os.path.join(directory, 'kluster_project.json')

            self.fil_text.setText(project_file)
            self.project_file = project_file
            self.project = FqprProject(is_gui=True)
            self.project.new_project_from_directory(directory)
            if self.parent:
                self.parent.set_project(self.project)

            self._load_from_project()

    def open_project(self):
        """
        Get the file path to a new project
        """

        # dirpath will be None or a string
        msg, pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='klusterintel', Title='Open an existing Kluster project',
                                                         AppName='klusterintel', fFilter="*.json", bSave=False)
        if pth:
            self.fil_text.setText(pth)
            self.build_from_project(pth)

    def close_project(self):
        self.fil_text.setText('')
        self.project = None
        if self.parent:
            self.parent.set_project(self.project)

    def build_from_project(self, project_path: str):
        """
        Load from a new project file, will close the active project and repopulate the gui

        Parameters
        ----------
        project_path
            path to a kluster project, kluster_project.json file
        """

        if os.path.exists(project_path):
            self.clear_project()
            self.project_file = project_path
            self.project = FqprProject(is_gui=True)
            self.project.open_project(self.project_file, skip_dask=True)
            if self.parent:
                self.parent.set_project(self.project)

            self._load_from_project()
            print('Loaded {}'.format(project_path))
        else:
            print('Unable to load from file, does not exist: {}'.format(project_path))

    def clear_project(self):
        """
        Clear the datalayout layout widget
        """
        clear_layout(self.datalayout)


def clear_layout(data_layout: QtWidgets.QLayout):
    """
    Delete all widgets in a layout

    Parameters
    ----------
    data_layout
        layout we want to clear
    """

    while data_layout.count():
        child = data_layout.takeAt(0)
        if child.widget() is not None:
            child.widget().deleteLater()
        elif child.layout() is not None:
            clear_layout(child.layout())


class OutWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the KlusterProjectTree for testing

    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        self.k_view = KlusterProjectView()
        self.k_view.setObjectName('kluster_projectview')
        layout.addWidget(self.k_view)

        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()


if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication()
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())