import logging
import sys, os

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.fqpr_project import FqprProject
from HSTB.kluster.gdal_helpers import get_raster_bands


class KlusterProjectTree(QtWidgets.QTreeView):
    """
    Tree widget to view the surfaces and converted data folders/lines associated with a FqprProject.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    """
    # signals must be defined on the class, not the instance of the class
    file_added = Signal(object)
    fqpr_selected = Signal(str)
    surface_selected = Signal(str)
    lines_selected = Signal(object)
    all_lines_selected = Signal(bool)
    surface_layer_selected = Signal(str, str, bool)
    raster_layer_selected = Signal(str, str, bool)
    close_fqpr = Signal(str)
    close_surface = Signal(str)
    manage_fqpr = Signal(str)
    manage_surface = Signal(str)
    load_console_fqpr = Signal(str)
    load_console_surface = Signal(str)
    show_explorer = Signal(str)
    zoom_extents_fqpr = Signal(str)
    zoom_extents_surface = Signal(str)
    reprocess_instance = Signal(str)
    update_surface = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.model = QtGui.QStandardItemModel()
        self.setModel(self.model)
        self.setUniformRowHeights(True)

        # ExtendedSelection - allows multiselection with shift/ctrl
        self.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        self.setDragDropMode(QtWidgets.QAbstractItemView.NoDragDrop)

        # makes it so no editing is possible with the table
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        # set up the context menu per item
        self.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.right_click_menu_converted = None
        self.right_click_menu_surfaces = None
        self.setup_menu()

        self.categories = ['Project', 'Vessel File', 'Converted', 'Surfaces', 'Raster', 'Vector', 'Mesh']
        self.tree_data = {}
        self.shown_layers = []

        self.clicked.connect(self.item_selected)
        self.customContextMenuRequested.connect(self.show_context_menu)
        self.configure()

    def print(self, msg: str, loglevel: int):
        """
        convenience method for printing using kluster_main logger

        Parameters
        ----------
        msg
            print text
        loglevel
            logging level, ex: logging.INFO
        """

        if self.parent() is not None:
            if self.parent().parent() is not None:  # widget is docked, kluster_main is the parent of the dock
                self.parent().parent().print(msg, loglevel)
            else:  # widget is undocked, kluster_main is the parent
                self.parent().print(msg, loglevel)
        else:
            print(msg)

    def debug_print(self, msg: str, loglevel: int):
        """
        convenience method for printing using kluster_main logger, when debug is enabled

        Parameters
        ----------
        msg
            print text
        loglevel
            logging level, ex: logging.INFO
        """

        if self.parent() is not None:
            if self.parent().parent() is not None:  # widget is docked, kluster_main is the parent of the dock
                self.parent().parent().debug_print(msg, loglevel)
            else:  # widget is undocked, kluster_main is the parent
                self.parent().debug_print(msg, loglevel)
        else:
            print(msg)

    def setup_menu(self):
        """
        Setup the menu that is generated on right clicking in the project tree.
        """
        self.right_click_menu_converted = QtWidgets.QMenu('menu', self)
        self.right_click_menu_surfaces = QtWidgets.QMenu('menu', self)

        close_dat = QtWidgets.QAction('Close', self)
        close_dat.triggered.connect(self.close_item_event)
        reprocess = QtWidgets.QAction('Reprocess', self)
        reprocess.triggered.connect(self.reprocess_event)
        load_in_console = QtWidgets.QAction('Load in Console', self)
        load_in_console.triggered.connect(self.load_in_console_event)
        show_explorer_action = QtWidgets.QAction('Show in Explorer', self)
        show_explorer_action.triggered.connect(self.show_in_explorer_event)
        zoom_extents = QtWidgets.QAction('Zoom Extents', self)
        zoom_extents.triggered.connect(self.zoom_extents_event)
        update_surface = QtWidgets.QAction('Update Surface', self)
        update_surface.triggered.connect(self.update_surface_event)
        manage_fqpr = QtWidgets.QAction('Manage', self)
        manage_fqpr.triggered.connect(self.manage_data_event)

        self.right_click_menu_converted.addAction(manage_fqpr)
        self.right_click_menu_converted.addAction(reprocess)
        self.right_click_menu_converted.addSeparator()
        self.right_click_menu_converted.addAction(load_in_console)
        self.right_click_menu_converted.addAction(show_explorer_action)
        self.right_click_menu_converted.addAction(zoom_extents)
        self.right_click_menu_converted.addAction(close_dat)

        self.right_click_menu_surfaces.addAction(manage_fqpr)
        self.right_click_menu_surfaces.addAction(update_surface)
        self.right_click_menu_surfaces.addSeparator()
        self.right_click_menu_surfaces.addAction(load_in_console)
        self.right_click_menu_surfaces.addAction(show_explorer_action)
        self.right_click_menu_surfaces.addAction(zoom_extents)
        self.right_click_menu_surfaces.addAction(close_dat)

    def show_context_menu(self):
        """
        Generate a close option when you right click on a mid level item (an fqpr instance or a fqpr surface instance).

        Emit the appropriate signal and let kluster_main handle the rest.
        """
        index = self.currentIndex()
        sel_name = index.data()
        mid_lvl_name = index.parent().data()
        if mid_lvl_name == 'Converted':
            self.right_click_menu_converted.exec_(QtGui.QCursor.pos())
        elif mid_lvl_name == 'Surfaces':
            self.right_click_menu_surfaces.exec_(QtGui.QCursor.pos())

    def reprocess_event(self, e: QtCore.QEvent):
        """
        Trigger full reprocessing of the selected fqpr instance

        Parameters
        ----------
        e
            QEvent on menu button click
        """

        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        if mid_lvl_name == 'Converted':
            self.reprocess_instance.emit(sel_data)

    def load_in_console_event(self, e: QtCore.QEvent):
        """
        We want the ability for the user to right click an object and load it in the console.  Here we emit the correct
        signal for the main to determine how to load it in the console.

        Parameters
        ----------
        e
            QEvent on menu button click
        """

        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        if mid_lvl_name == 'Converted':
            self.load_console_fqpr.emit(sel_data)
        elif mid_lvl_name == 'Surfaces':
            self.load_console_surface.emit(sel_data)

    def show_in_explorer_event(self, e: QtCore.QEvent):
        """
        We want the ability for the user to right click an object and load it in the console.  Here we emit the correct
        signal for the main to determine how to load it in the console.

        Parameters
        ----------
        e
            QEvent on menu button click
        """

        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        self.show_explorer.emit(sel_data)

    def zoom_extents_event(self, e):
        """
        Zoom to the extents of the layer selected

        Parameters
        ----------
        e
            QEvent on menu button click

        """
        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        if mid_lvl_name == 'Converted':
            self.zoom_extents_fqpr.emit(sel_data)
        elif mid_lvl_name == 'Surfaces':
            self.zoom_extents_surface.emit(sel_data)

    def update_surface_event(self, e):
        """
        If user right clicks a surface and selects update surface, triggers this event.

        Parameters
        ----------
        e: QEvent on menu button click

        """
        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        if mid_lvl_name == 'Surfaces':
            self.update_surface.emit(sel_data)

    def manage_data_event(self, e):
        """
        If a user right clicks on the converted data instance and selects manage, triggers this event

        Parameters
        ----------
        e: QEvent on menu button click
        """
        index = self.currentIndex()
        mid_lvl_name = index.parent().data()
        sel_data = index.data()

        if mid_lvl_name == 'Converted':
            self.manage_fqpr.emit(sel_data)
        elif mid_lvl_name == 'Surfaces':
            self.manage_surface.emit(sel_data)

    def close_item_event(self, e):
        """
        If user right clicks on a project tree item and selects close, triggers this event.  Emit signals depending
        on what kind of item the user selects.

        Parameters
        ----------
        e: QEvent on menu button click

        """

        fqprs, _ = self.return_selected_fqprs()
        for fq in fqprs:
            self.close_fqpr.emit(fq)

        surfs = self.return_selected_surfaces()
        for surf in surfs:
            self.close_surface.emit(surf)

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

    def _add_new_fqpr_from_proj(self, parent: QtGui.QStandardItem, line_data):
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
                proj_child.setToolTip(fq_proj)
                parent.appendRow(proj_child)
                for fq_line in line_data[fq_proj]:
                    line_child = QtGui.QStandardItem(fq_line)
                    proj_child.appendRow([line_child])
                self.tree_data['Converted'].append(fq_proj)
            else:  # see if there are new lines to display
                child_match = [parent.child(r) for r in range(parent.rowCount()) if parent.child(r).text() == fq_proj]
                if child_match:
                    proj_child = child_match[0]
                    if proj_child.rowCount() != len(line_data[fq_proj]):  # new lines
                        tree_lines = [proj_child.child(rw).text() for rw in range(proj_child.rowCount())]
                        for fq_line in line_data[fq_proj]:
                            if fq_line not in tree_lines:
                                line_child = QtGui.QStandardItem(fq_line)
                                proj_child.appendRow([line_child])
        parent.sortChildren(0, order=QtCore.Qt.AscendingOrder)

    def _add_new_surf_from_proj(self, parent: QtGui.QStandardItem, surf_data):
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
        needs_refresh = []
        current_surfs = self.tree_data['Surfaces'][1:]
        for surf in surf_data:
            layer_names = surf_data[surf].return_layer_names()
            if surf not in current_surfs:
                surf_child = QtGui.QStandardItem(surf)
                surf_child.setToolTip(surf)
                parent.appendRow(surf_child)
                for lyr in surf_data[surf].return_layer_names():
                    lyr_child = QtGui.QStandardItem(lyr)
                    lyr_child.setCheckable(True)
                    surf_child.appendRow([lyr_child])
                    if lyr == 'depth':  # add optional hillshade layer
                        lyr_child = QtGui.QStandardItem('hillshade')
                        lyr_child.setCheckable(True)
                        surf_child.appendRow([lyr_child])
                try:  # add the ability to draw the grid outline, new in bathygrid 1.1.2
                    lyr_child = QtGui.QStandardItem('tiles')
                    lyr_child.setCheckable(True)
                    surf_child.appendRow([lyr_child])
                except AttributeError:  # bathygrid does not support this method
                    pass
                self.tree_data['Surfaces'].append(surf)
            else:  # an empty surface has no checkable layers, if the layer names change, we need to update the project tree
                surf_child = [parent.child(idx) for idx in range(parent.rowCount()) if parent.child(idx).text() == surf][0]
                existing_surf_layer_names = [surf_child.child(idx).text() for idx in range(surf_child.rowCount())]
                no_update_needed = all([lname in existing_surf_layer_names for lname in layer_names])
                if not no_update_needed:
                    needs_refresh.append(surf)
        if needs_refresh:
            force_removal = {k: None for k in surf_data.keys() if k not in needs_refresh}
            self._remove_surf_not_in_proj(parent, force_removal)
            self._add_new_surf_from_proj(parent, surf_data)
        parent.sortChildren(0, order=QtCore.Qt.AscendingOrder)

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
                self.print('_remove_fqpr_not_in_proj: Unable to close {} in project tree, not found in kluster_project_tree.tree_data'.format(remv), logging.ERROR)
                continue
            if idx != -1:
                self.tree_data['Converted'].pop(idx + 1)
                tree_idx = [idx for idx in range(parent.rowCount()) if parent.child(idx).text() == remv]
                if tree_idx and len(tree_idx) == 1:
                    parent.removeRow(tree_idx[0])
                else:
                    self.print('_remove_fqpr_not_in_proj: Unable to remove "{}"'.format(remv), logging.ERROR)

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
                self.print('_remove_surf_not_in_proj: Unable to close {} in project tree, not found in kluster_project_tree.tree_data'.format(remv), logging.ERROR)
                continue
            if idx != -1:
                self.tree_data['Surfaces'].pop(idx + 1)
                tree_idx = [idx for idx in range(parent.rowCount()) if parent.child(idx).text() == remv]
                if tree_idx and len(tree_idx) == 1:
                    parent.removeRow(tree_idx[0])
                else:
                    self.print('_remove_surf_not_in_proj: Unable to remove "{}"'.format(remv), logging.ERROR)

    def _setup_project(self, parent, proj_directory):
        if len(self.tree_data['Project']) == 1:
            proj_child = QtGui.QStandardItem(proj_directory)
            proj_child.setToolTip(proj_directory)
            parent.appendRow(proj_child)
            self.tree_data['Project'].append(proj_directory)
        else:
            parent.removeRow(0)
            proj_child = QtGui.QStandardItem(proj_directory)
            proj_child.setToolTip(proj_directory)
            parent.appendRow(proj_child)
            self.tree_data['Project'][1] = proj_directory

    def _setup_vessel_file(self, parent, vessel_path):
        if len(self.tree_data['Vessel File']) == 1:
            if vessel_path:
                proj_child = QtGui.QStandardItem(vessel_path)
                proj_child.setToolTip(vessel_path)
                parent.appendRow(proj_child)
                self.tree_data['Vessel File'].append(vessel_path)
        else:
            parent.removeRow(0)
            if vessel_path:
                proj_child = QtGui.QStandardItem(vessel_path)
                proj_child.setToolTip(vessel_path)
                parent.appendRow(proj_child)
                self.tree_data['Vessel File'][1] = vessel_path

    def _add_new_raster(self, parent: QtGui.QStandardItem, raster_path: str):
        """
        Add from a generic raster file, like a geotiff

        Parameters
        ----------
        parent: PySide2.QtGui.QStandardItem, the item that represents the 'Converted' entry in the tree.  All fqpr
                projects go underneath.
        raster_path: full filepath to the raster
        """

        current_rasterdata = self.tree_data['Raster'][1:]
        rbands = get_raster_bands(raster_path)
        if raster_path not in current_rasterdata and rbands:
            proj_child = QtGui.QStandardItem(raster_path)
            proj_child.setToolTip(raster_path)
            parent.appendRow(proj_child)
            for rband in rbands:
                band_child = QtGui.QStandardItem(rband)
                band_child.setCheckable(True)
                proj_child.appendRow([band_child])
            self.tree_data['Raster'].append(raster_path)

    def refresh_project(self, proj, add_raster=None):
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
            elif c == 'Project':
                if proj.path:
                    self._setup_project(parent, proj.path)
            elif c == 'Vessel File':
                if proj.vessel_file:
                    self._setup_vessel_file(parent, proj.vessel_file)
            elif c == 'Raster' and add_raster:
                if isinstance(add_raster, str):
                    add_raster = [add_raster]
                for arast in add_raster:
                    self._add_new_raster(parent, arast)

    def select_multibeam_lines(self, line_names: list, clear_existing_selection: bool = True):
        parent = self.tree_data['Converted'][0]
        num_containers = parent.rowCount()
        clrfirst = clear_existing_selection
        if line_names:
            for cnt in range(num_containers):
                container_item = parent.child(cnt, 0)
                numlines = container_item.rowCount()
                for lcnt in range(numlines):
                    lineitem = container_item.child(lcnt, 0)
                    if lineitem.text() in line_names:
                        sel = lineitem.index()
                        if clrfirst:  # we programmatically select it with ClearAndSelect
                            self.selectionModel().select(sel, QtCore.QItemSelectionModel.ClearAndSelect | QtCore.QItemSelectionModel.Rows)
                            clrfirst = False
                        else:
                            self.selectionModel().select(sel, QtCore.QItemSelectionModel.Select | QtCore.QItemSelectionModel.Rows)
                        self.item_selected(sel)
        else:
            self.selectionModel().select(parent.index(), QtCore.QItemSelectionModel.Clear | QtCore.QItemSelectionModel.Rows)

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
                self.lines_selected.emit(self.return_selected_lines())
            elif top_lvl_name == 'Surfaces':
                ischecked = self.model.itemFromIndex(index).checkState()
                self.surface_layer_selected.emit(mid_lvl_name, selected_name, ischecked)
            elif top_lvl_name == 'Raster':
                ischecked = self.model.itemFromIndex(index).checkState()
                self.raster_layer_selected.emit(mid_lvl_name, selected_name, ischecked)
        elif mid_lvl_name in self.categories:  # this is a sub item, like a converted fqpr path
            if mid_lvl_name == 'Converted':
                self.fqpr_selected.emit(selected_name)
            elif mid_lvl_name == 'Surfaces':
                self.surface_selected.emit(selected_name)
        elif selected_name in self.categories:
            if selected_name == 'Converted':
                # self.all_lines_selected.emit(True)
                pass

    def return_selected_fqprs(self, force_line_list: bool = False):
        """
        Return all the selected fqpr instances that are selected.  If the user selects a line (a child of the fqpr),
        return the line owner fqpr.  Only returns unique fqpr instances

        Parameters
        ----------
        force_line_list
            if you want to force the return of all the lines when a parent Fqpr converted instance is selected,
            use this option.

        Returns
        -------
        list
            list of all str paths to fqpr instances selected, either directly or through selecting a line
        dict
            dictionary of all selected lines, with the fqpr as key
        """
        fqprs = []
        line_list = {}
        idxs = self.selectedIndexes()
        for idx in idxs:
            new_fqpr = ''
            top_lvl_name = idx.parent().parent().data()
            mid_lvl_name = idx.parent().data()
            low_lvl_name = idx.data()
            if mid_lvl_name == 'Converted':  # user has selected a fqpr instance
                new_fqpr = low_lvl_name
                if force_line_list:
                    cont_index = idx.row()
                    parent = self.tree_data['Converted'][0]
                    container_item = parent.child(cont_index, 0)
                    numlines = container_item.rowCount()
                    for lcnt in range(numlines):
                        linename = container_item.child(lcnt, 0).text()
                        if new_fqpr in line_list:
                            line_list[new_fqpr].append(linename)
                        else:
                            line_list[new_fqpr] = [linename]
            elif top_lvl_name == 'Converted':  # user selected a line
                new_fqpr = mid_lvl_name
                if new_fqpr in line_list:
                    line_list[new_fqpr].append(low_lvl_name)
                else:
                    line_list[new_fqpr] = [low_lvl_name]
            if new_fqpr and (new_fqpr not in fqprs):
                fqprs.append(new_fqpr)
        return fqprs, line_list

    def return_selected_surfaces(self):
        """
        Return all the selected surface instances that are selected.  Only returns unique surface instances

        Returns
        -------
        list
            list of all str paths to surface instance folders selected

        """
        surfs = []
        new_surf = ''
        idxs = self.selectedIndexes()
        for idx in idxs:
            mid_lvl_name = idx.parent().data()
            if mid_lvl_name == 'Surfaces':  # user has selected a surface
                new_surf = self.model.data(idx)
            if new_surf and new_surf not in surfs:
                surfs.append(new_surf)
        return surfs

    def return_selected_lines(self):
        """
        Return all the selected line instances that are selected.  Only returns unique line instances

        Returns
        -------
        list
            list of all str line names selected
        """

        linenames = []
        idxs = self.selectedIndexes()
        for idx in idxs:
            new_line = ''
            top_lvl_name = idx.parent().parent().data()
            if top_lvl_name == 'Converted':  # user selected a line
                new_line = self.model.data(idx)
            if new_line and (new_line not in linenames):
                linenames.append(new_line)
        return linenames


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
            fqpr_entry, already_in = self.project.add_fqpr(f, skip_dask=True)
            if fqpr_entry is None:
                self.print('update_ktree: Unable to add to Project: {}'.format(f), logging.ERROR)

        self.k_tree.refresh_project(self.project)


if __name__ == '__main__':
    try:  # pyside2
        app = QtWidgets.QApplication()
    except TypeError:  # pyqt5
        app = QtWidgets.QApplication([])
    test_window = OutWindow()
    test_window.show()
    sys.exit(app.exec_())