from datetime import datetime
from PySide2 import QtCore, QtGui, QtWidgets
from collections import OrderedDict

from HSTB.kluster.fqpr_project import FqprProject
from HSTB.shared import RegistryHelpers


class KlusterFqprView(QtWidgets.QTableWidget):
    def __init__(self):
        pass


class KlusterProjectView(QtWidgets.QWidget):
    """
    QTableWidget to display the data from an fqpr_intelligence IntelModule.
    """

    file_added = QtCore.Signal(str)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_file = None
        self.project = None

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

        self.setLayout(self.mainlayout)

        self.newproj_button.clicked.connect(self.new_project)
        self.openproj_button.clicked.connect(self.open_project)

    def new_project(self):
        """
        Get the file path to a new project
        """

        # dirpath will be None or a string
        msg, pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='klusterintel', Title='Create a new Kluster project',
                                                         AppName='klusterintel', fFilter="*.json", bSave=True,
                                                         DefaultFile='kluster_project.json')
        if pth is not None:
            self.fil_text.setText(pth)

    def open_project(self):
        """
        Get the file path to a new project
        """

        # dirpath will be None or a string
        msg, pth = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='klusterintel', Title='Open an existing Kluster project',
                                                         AppName='klusterintel', fFilter="*.json", bSave=False)
        if pth is not None:
            self.fil_text.setText(pth)

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

        self.k_view = KlusterProjectView(self)
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