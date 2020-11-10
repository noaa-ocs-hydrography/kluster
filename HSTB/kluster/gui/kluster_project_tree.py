import sys

from PySide2 import QtCore, QtGui, QtWidgets

from HSTB.kluster.fqpr_project import FqprProject


class KlusterProjectTree(QtWidgets.QTreeView):
    """
    Tree widget to view the surfaces and converted data folders/lines associated with a FqprProject.

    """
    # signals must be defined on the class, not the instance of the class
    file_added = QtCore.Signal(object)
    fqpr_selected = QtCore.Signal(str)
    surface_selected = QtCore.Signal(str)
    line_selected = QtCore.Signal(str)
    all_lines_selected = QtCore.Signal(bool)
    surface_layer_selected = QtCore.Signal(str, str, bool)
    close_fqpr = QtCore.Signal(str)
    close_surface = QtCore.Signal(str)
    load_console_fqpr = QtCore.Signal(str)
    load_console_surface = QtCore.Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.model = QtGui.QStandardItemModel()
        self.setModel(self.model)
        self.setUniformRowHeights(True)
        self.setAcceptDrops(True)
        self.viewport().setAcceptDrops(True)  # viewport is the total rendered area, this is recommended from my reading
        self.setDropIndicatorShown(True)

        # ExtendedSelection - allows multiselection with shift/ctrl
        self.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        self.setDragDropMode(QtWidgets.QAbstractItemView.DragDrop)

        # makes it so no editing is possible with the table
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        # set up the context menu per item
        self.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.setup_menu()

        self.categories = ['Converted', 'Surfaces']
        self.tree_data = {}

        self.clicked.connect(self.item_selected)
        self.customContextMenuRequested.connect(self.show_context_menu)
        self.configure()

    def setup_menu(self):
        """
        Setup the menu that is generated on right clicking in the project tree.
        """
        self.right_click_menu = QtWidgets.QMenu('menu', self)
        close_dat = QtWidgets.QAction('Close', self)
        close_dat.triggered.connect(self.close_item_event)
        load_in_console = QtWidgets.QAction('Load in console', self)
        load_in_console.triggered.connect(self.load_in_console_event)
        self.right_click_menu.addAction(close_dat)
        self.right_click_menu.addAction(load_in_console)

    def show_context_menu(self):
        """
        Generate a close option when you right click on a mid level item (an fqpr instance or a fqpr surface instance).

        Emit the appropriate signal and let kluster_main handle the rest.
        """
        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        if mid_lvl_name in self.categories:
            self.right_click_menu.exec_(QtGui.QCursor.pos())

    def load_in_console_event(self, e):
        """
        We want the ability for the user to right click an object and load it in the console.  Here we emit the correct
        signal for the main to determine how to load it in the console.

        Parameters
        ----------
        e: QEvent on menu button click

        """
        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        if mid_lvl_name == 'Converted':
            self.load_console_fqpr.emit(sel_data)
        elif mid_lvl_name == 'Surfaces':
            self.load_console_surface.emit(sel_data)

    def close_item_event(self, e):
        """
        If user right clicks on a project tree item and selects close, triggers this event.  Emit signals depending
        on what kind of item the user selects.

        Parameters
        ----------
        e: QEvent on menu button click

        """
        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        if mid_lvl_name == 'Converted':
            self.close_fqpr.emit(sel_data)
        elif mid_lvl_name == 'Surfaces':
            self.close_surface.emit(sel_data)

    def dragEnterEvent(self, e):
        """
        Catch mouse drag enter events to block things not move/read related

        Parameters
        ----------
        e: QEvent which is sent to a widget when a drag and drop action enters it

        """
        if e.mimeData().hasUrls():  # allow MIME type files, have a 'file://', 'http://', etc.
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
        if e.mimeData().hasUrls():
            e.accept()
        else:
            e.ignore()

    def dropEvent(self, e):
        """
        On drag and drop, handle incoming new data from zarr store

        Parameters
        ----------
        e: QEvent which is sent when a drag and drop action is completed

        """
        if e.mimeData().hasUrls():
            e.setDropAction(QtCore.Qt.CopyAction)
            fils = [url.toLocalFile() for url in e.mimeData().urls()]
            self.file_added.emit(fils)
        else:
            e.ignore()

    def configure(self):
        """
        Clears all data currently in the tree and repopulates with loaded datasets and surfaces.

        """
        self.model.clear()
        self.model.setHorizontalHeaderLabels(['Project Tree'])
        for cnt, c in enumerate(self.categories):
            parent = QtGui.QStandardItem(c)
            self.tree_data[c] = [parent]
            self.model.appendRow(parent)
            self.setFirstColumnSpanned(cnt, self.rootIndex(), True)

    def _add_new_fqpr_from_proj(self, parent, line_data):
        """
        Read from the kluster_main FqprProject (provided here is the line_data from that project) and add the lines
        that are not currently in project tree.  self.tree_data contains the record of the data in the tree.

        Parameters
        ----------
        parent: PySide2.QtGui.QStandardItem, the item that represents the 'Converted' entry in the tree.  All fqpr
                projects go underneath.
        line_data: dict, a dictionary of project paths: multibeam lines.
                   ex: {'C:\\collab\\dasktest\\data_dir\\hassler_acceptance\\refsurf\\converted':
                                {'0015_20200304_070725_S250.all': [1583305645.423, 1583305889.905]}

        """
        current_fq_proj = self.tree_data['Converted'][1:]
        for fq_proj in line_data:
            if fq_proj not in current_fq_proj:
                proj_child = QtGui.QStandardItem(fq_proj)
                parent.appendRow(proj_child)
                for fq_line in line_data[fq_proj]:
                    line_child = QtGui.QStandardItem(fq_line)
                    proj_child.appendRow([line_child])
                self.tree_data['Converted'].append(fq_proj)

    def _add_new_surf_from_proj(self, parent, surf_data):
        """
        Read from the kluster_main FqprProject (provided here is the line_data from that project) and add the surfaces
        that are not currently in project tree.  self.tree_data contains the record of the data in the tree.

        Parameters
        ----------
        parent: PySide2.QtGui.QStandardItem, the item that represents the 'Surfaces' entry in the tree.  All fqpr
                projects go underneath.
        surf_data: dict, a dictionary of surface paths: surface objects.
                   ex: {'C:/collab/dasktest/data_dir/hassler_acceptance/refsurf/refsurf.npz':
                            <HSTB.kluster.fqpr_surface.BaseSurface object at 0x0000019CFFF1A520>}

        """
        current_surfs = self.tree_data['Surfaces'][1:]
        for surf in surf_data:
            if surf not in current_surfs:
                surf_child = QtGui.QStandardItem(surf)
                parent.appendRow(surf_child)
                for lyr in surf_data[surf].return_layer_names():
                    lyr_child = QtGui.QStandardItem(lyr)
                    lyr_child.setCheckable(True)
                    surf_child.appendRow([lyr_child])
                self.tree_data['Surfaces'].append(surf)

    def _remove_fqpr_not_in_proj(self, parent, line_data):
        """
        Read from the kluster_main FqprProject (provided here is the line_data from that project) and remove the lines
        that are not currently in project tree.  self.tree_data contains the record of the data in the tree.

        Parameters
        ----------
        parent: PySide2.QtGui.QStandardItem, the item that represents the 'Converted' entry in the tree.  All fqpr
                projects go underneath.
        line_data: dict, a dictionary of project paths: multibeam lines.
                   ex: {'C:\\collab\\dasktest\\data_dir\\hassler_acceptance\\refsurf\\converted':
                                {'0015_20200304_070725_S250.all': [1583305645.423, 1583305889.905]}

        """
        current_fq_proj = self.tree_data['Converted'][1:]
        needs_removal = [f for f in current_fq_proj if f not in line_data]
        for remv in needs_removal:
            try:
                idx = self.tree_data['Converted'][1:].index(remv)
            except ValueError:
                print('Unable to close {} in project tree, not found in kluster_project_tree.tree_data'.format(remv))
                continue
            if idx != -1:
                self.tree_data['Converted'].pop(idx + 1)
                parent.removeRow(idx)

    def _remove_surf_not_in_proj(self, parent, surf_data):
        """
        Read from the kluster_main FqprProject (provided here is the line_data from that project) and remove the
        surfaces that are not currently in project tree.  self.tree_data contains the record of the data in the tree.

        Parameters
        ----------
        parent: PySide2.QtGui.QStandardItem, the item that represents the 'Surfaces' entry in the tree.  All fqpr
                projects go underneath.
        surf_data: dict, a dictionary of surface paths: surface objects.
                   ex: {'C:/collab/dasktest/data_dir/hassler_acceptance/refsurf/refsurf.npz':
                            <HSTB.kluster.fqpr_surface.BaseSurface object at 0x0000019CFFF1A520>}

        """
        current_surfs = self.tree_data['Surfaces'][1:]
        needs_removal = [f for f in current_surfs if f not in surf_data]
        for remv in needs_removal:
            try:
                idx = self.tree_data['Surfaces'][1:].index(remv)
            except ValueError:
                print('Unable to close {} in project tree, not found in kluster_project_tree.tree_data'.format(remv))
                continue
            if idx != -1:
                self.tree_data['Surfaces'].pop(idx + 1)
                parent.removeRow(idx)

    def refresh_project(self, proj):
        """
        Loading from a FqprProject will update the tree, triggered on dragging in a converted data folder

        Parameters
        ----------
        proj: fqpr_project.FqprProject

        """
        for cnt, c in enumerate(self.categories):
            parent = self.tree_data[c][0]
            if c == 'Converted':
                line_data = proj.return_project_lines()
                self._add_new_fqpr_from_proj(parent, line_data)
                self._remove_fqpr_not_in_proj(parent, line_data)
            elif c == 'Surfaces':
                surf_data = proj.surface_instances
                self._add_new_surf_from_proj(parent, surf_data)
                self._remove_surf_not_in_proj(parent, surf_data)

    def item_selected(self, index):
        """
        Selecting one of the items in the tree will activate an event depending on the item type.  See comments below.

        Parameters
        ----------
        index: PySide2.QtCore.QModelIndex, index of selected item

        """
        top_lvl_name = index.parent().parent().data()
        mid_lvl_name = index.parent().data()
        selected_name = index.data()

        if top_lvl_name in self.categories:  # this is a sub sub item, something like a line name or a surface name
            if top_lvl_name == 'Converted':
                self.line_selected.emit(selected_name)
            elif top_lvl_name == 'Surfaces':
                self.surface_layer_selected.emit(mid_lvl_name, selected_name, self.model.itemFromIndex(index).checkState())
        elif mid_lvl_name in self.categories:  # this is a sub item, like a converted fqpr path
            if mid_lvl_name == 'Converted':
                self.fqpr_selected.emit(selected_name)
            elif mid_lvl_name == 'Surfaces':
                self.surface_selected.emit(selected_name)
        elif selected_name in self.categories:
            if selected_name == 'Converted':
                # self.all_lines_selected.emit(True)
                pass

    def return_selected_fqprs(self):
        """
        Return all the selected fqpr instances that are selected.  If the user selects a line (a child of the fqpr),
        return the line owner fqpr.  Only returns unique fqpr instances

        Returns
        -------
        fqprs: list, list of all str paths to fqpr instances selected, either directly or through selecting a line

        """
        fqprs = []
        new_fqpr = ''
        idxs = self.selectedIndexes()
        for idx in idxs:
            top_lvl_name = idx.parent().parent().data()
            mid_lvl_name = idx.parent().data()
            if mid_lvl_name == 'Converted':  # user has selected a fqpr instance
                new_fqpr = self.model.data(idx)
            elif top_lvl_name == 'Converted':  # user selected a line
                new_fqpr = mid_lvl_name
            if new_fqpr not in fqprs:
                fqprs.append(new_fqpr)
        return fqprs


class OutWindow(QtWidgets.QMainWindow):
    """
    Simple Window for viewing the KlusterProjectTree for testing

    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.resize(450, 800)
        self.setWindowTitle('Kluster Project Tree')
        self.top_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.top_widget)
        layout = QtWidgets.QHBoxLayout()
        self.top_widget.setLayout(layout)

        self.project = FqprProject()

        self.k_tree = KlusterProjectTree(self)
        self.k_tree.setObjectName('kluster_project_tree')
        self.k_tree.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        self.k_tree.setMinimumWidth(300)
        layout.addWidget(self.k_tree)

        self.k_tree.file_added.connect(self.update_ktree)

        layout.layout()
        self.setLayout(layout)
        self.centralWidget().setLayout(layout)
        self.show()

    def update_ktree(self, fil):
        for f in fil:
            fqpr_entry = self.project.add_fqpr(f, skip_dask=True)
            if fqpr_entry is None:
                print('update_ktree: Unable to add to Project: {}'.format(f))

        self.k_tree.refresh_project(self.project)


if __name__ == '__main__':
    app = QtWidgets.QApplication()
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())