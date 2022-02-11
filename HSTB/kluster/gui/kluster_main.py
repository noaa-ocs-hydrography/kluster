import os
# added this in kluster 0.8.8, 2/11/2022
# setting env variable SETUPTOOLS_USE_DISTUTILS to resolve setuptools/xarray issue with setuptools/distutils conflict, see xarray pull request #6096 and setuptools issue #2353
# can probably remove this once distutils is removed from dependencies (currently can be found in xarray at least)
os.environ['SETUPTOOLS_USE_DISTUTILS'] = 'stdlib'

# Import qt first, to resolve the backend issues youll get in matplotlib if you dont import this first, as it prefers PySide2
from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled, found_path
if qgis_enabled:
    from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui

import matplotlib
matplotlib.use('qt5agg')

import sys
import webbrowser
import numpy as np
import multiprocessing
from typing import Union
from datetime import datetime
from pyproj import CRS, Transformer
import qdarkstyle
import matplotlib.pyplot as plt

from HSTB.kluster.gui import dialog_vesselview, kluster_explorer, kluster_project_tree, kluster_3dview_v2, \
    kluster_output_window, kluster_2dview, kluster_actions, kluster_monitor, dialog_daskclient, dialog_surface, \
    dialog_export, kluster_worker, kluster_interactive_console, dialog_basicplot, dialog_advancedplot, dialog_project_settings, \
    dialog_export_grid, dialog_layer_settings, dialog_settings, dialog_importppnav, dialog_overwritenav, dialog_surface_data, \
    dialog_about, dialog_setcolors, dialog_patchtest, dialog_manualpatchtest, dialog_managedata, dialog_managesurface, \
    dialog_reprocess
from HSTB.kluster.fqpr_project import FqprProject
from HSTB.kluster.fqpr_intelligence import FqprIntel
from HSTB.kluster.fqpr_vessel import convert_from_fqpr_xyzrph, convert_from_vessel_xyzrph, compare_dict_data
from HSTB.kluster import __version__ as kluster_version
from HSTB.kluster import __file__ as kluster_init_file
from HSTB.shared import RegistryHelpers, path_to_supplementals
from HSTB.kluster import kluster_variables
from bathygrid.grid_variables import allowable_grid_root_names

# list of icons
# https://joekuan.wordpress.com/2015/09/23/list-of-qt-icons/


settings_translator = {'Kluster/dark_mode': {'newname': 'dark_mode', 'defaultvalue': False},
                       'Kluster/proj_settings_epsgradio': {'newname': 'use_epsg', 'defaultvalue': False},
                       'Kluster/proj_settings_epsgval': {'newname': 'epsg', 'defaultvalue': ''},
                       'Kluster/proj_settings_utmradio': {'newname': 'use_coord', 'defaultvalue': True},
                       'Kluster/proj_settings_utmval': {'newname': 'coord_system', 'defaultvalue': kluster_variables.default_coordinate_system},
                       'Kluster/proj_settings_vertref': {'newname': 'vert_ref', 'defaultvalue': kluster_variables.default_vertical_reference},
                       'Kluster/layer_settings_background': {'newname': 'layer_background', 'defaultvalue': 'Default'},
                       'Kluster/layer_settings_transparency': {'newname': 'layer_transparency', 'defaultvalue': '0'},
                       'Kluster/layer_settings_surfacetransparency': {'newname': 'surface_transparency', 'defaultvalue': 0},
                       'Kluster/settings_keep_waterline_changes': {'newname': 'keep_waterline_changes', 'defaultvalue': True},
                       'Kluster/settings_enable_parallel_writes': {'newname': 'write_parallel', 'defaultvalue': True},
                       'Kluster/settings_vdatum_directory': {'newname': 'vdatum_directory', 'defaultvalue': ''},
                       'Kluster/settings_auto_processing_mode': {'newname': 'autoprocessing_mode', 'defaultvalue': 'normal'},
                       'Kluster/settings_force_coordinate_match': {'newname': 'force_coordinate_match', 'defaultvalue': False},
                       }


class KlusterProxyStyle(QtWidgets.QProxyStyle):
    """
    Override the default style to make a few improvements.  Currently we only override the style hint to make tooltips
    show up immediately, so that people know they exist
    """
    def styleHint(self, *args, **kwargs):
        if args[0] == QtWidgets.QStyle.SH_ToolTip_WakeUpDelay:  # make tooltips show immediately
            return 0
        return super().styleHint(*args, **kwargs)


class KlusterMain(QtWidgets.QMainWindow):
    """
    Main window for kluster application
    """
    def __init__(self, app=None, app_library='pyqt5'):
        """
        Build out the dock widgets with the kluster widgets inside.  Will use QSettings object to retain size and
        position.
        """
        super().__init__()

        self.app = app
        self.app_library = app_library
        self.start_horiz_size = 1360
        self.start_vert_size = 768

        self.resize(self.start_horiz_size, self.start_vert_size)

        self.setWindowTitle('Kluster {}'.format(kluster_version))
        self.setDockNestingEnabled(True)

        self.widget_obj_names = []

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.project = FqprProject(is_gui=False)  # is_gui controls the progress bar text, used to disable it for gui, no longer
        self.intel = FqprIntel(self.project, self)
        # settings, like the chosen vertical reference
        # ex: {'use_epsg': True, 'epsg': 26910, ...}
        self.settings = {}
        self._load_previously_used_settings()

        self.project_tree = kluster_project_tree.KlusterProjectTree(self)
        self.tree_dock = self.dock_this_widget('Project Tree', 'project_dock', self.project_tree)

        self.two_d = kluster_2dview.Kluster2dview(self, self.settings.copy())
        self.two_d_dock = self.dock_this_widget('2d View', 'two_d_dock', self.two_d)

        self.points_view = kluster_3dview_v2.ThreeDWidget(self, self.settings_object)
        self.points_dock = self.dock_this_widget("Points View", 'points_dock', self.points_view)
        # for now we remove the ability to undock the three d window, vispy wont work if we do
        self.points_dock.setFeatures(QtWidgets.QDockWidget.DockWidgetMovable)

        self.explorer = kluster_explorer.KlusterExplorer(self)
        self.explorer_dock = self.dock_this_widget("Explorer", 'explorer_dock', self.explorer)

        self.output_window = kluster_output_window.KlusterOutput(self)
        self.output_window_dock = self.dock_this_widget('Output', 'output_window_dock', self.output_window)

        self.attribute = kluster_explorer.KlusterAttribution(self)
        self.attribute_dock = self.dock_this_widget("Attribute", 'attribute_dock', self.attribute)

        self.actions = kluster_actions.KlusterActions(self)
        self.actions_dock = self.dock_this_widget('Actions', 'actions_dock', self.actions)
        self.actions.update_actions(process_mode=self.intel.autoprocessing_mode)

        self.monitor = kluster_monitor.KlusterMonitor(self)
        self.monitor_dock = self.dock_this_widget('Monitor', 'monitor_dock', self.monitor)

        self.console = kluster_interactive_console.KlusterConsole(self)
        self.console_dock = self.dock_this_widget('Console', 'console_dock', self.console)

        self.vessel_win = None
        self.basicplots_win = None
        self.advancedplots_win = None
        self.managedata_win = None
        self.managedata_surf = None
        self._manpatchtest = None

        self.iconpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images', 'kluster_img.ico')
        self.setWindowIcon(QtGui.QIcon(self.iconpath))

        self.generic_progressbar = QtWidgets.QProgressBar(self)
        self.generic_progressbar.setMaximum(1)
        self.generic_progressbar.setMinimum(0)
        self.statusBar().addPermanentWidget(self.generic_progressbar, stretch=1)

        self.action_thread = kluster_worker.ActionWorker()
        self.import_ppnav_thread = kluster_worker.ImportNavigationWorker()
        self.overwrite_nav_thread = kluster_worker.OverwriteNavigationWorker()
        self.surface_thread = kluster_worker.SurfaceWorker()
        self.surface_update_thread = kluster_worker.SurfaceUpdateWorker()
        self.export_thread = kluster_worker.ExportWorker()
        self.export_grid_thread = kluster_worker.ExportGridWorker()
        self.open_project_thread = kluster_worker.OpenProjectWorker()
        self.draw_navigation_thread = kluster_worker.DrawNavigationWorker()
        self.draw_surface_thread = kluster_worker.DrawSurfaceWorker()
        self.load_points_thread = kluster_worker.LoadPointsWorker()
        self.patch_test_load_thread = kluster_worker.PatchTestUpdateWorker()
        self.allthreads = [self.action_thread, self.import_ppnav_thread, self.overwrite_nav_thread, self.surface_thread,
                           self.surface_update_thread, self.export_thread, self.export_grid_thread, self.open_project_thread,
                           self.draw_navigation_thread, self.draw_surface_thread, self.load_points_thread, self.patch_test_load_thread]

        # connect FqprActionContainer with actions pane, called whenever actions changes
        self.intel.bind_to_action_update(self.actions.update_actions)

        # self.project_tree.file_added.connect(self.update_on_file_added)
        self.project_tree.lines_selected.connect(self.tree_line_selected)
        self.project_tree.fqpr_selected.connect(self.tree_fqpr_selected)
        self.project_tree.surface_selected.connect(self.tree_surf_selected)
        self.project_tree.surface_layer_selected.connect(self.tree_surface_layer_selected)
        self.project_tree.set_color_ranges.connect(self.set_color_ranges)
        self.project_tree.all_lines_selected.connect(self.tree_all_lines_selected)
        self.project_tree.close_fqpr.connect(self.close_fqpr)
        self.project_tree.close_surface.connect(self.close_surface)
        self.project_tree.manage_fqpr.connect(self.manage_fqpr)
        self.project_tree.manage_surface.connect(self.manage_surface)
        self.project_tree.load_console_fqpr.connect(self.load_console_fqpr)
        self.project_tree.load_console_surface.connect(self.load_console_surface)
        self.project_tree.show_explorer.connect(self.show_in_explorer)
        self.project_tree.zoom_extents_fqpr.connect(self.zoom_extents_fqpr)
        self.project_tree.zoom_extents_surface.connect(self.zoom_extents_surface)
        self.project_tree.reprocess_instance.connect(self.reprocess_fqpr)
        self.project_tree.update_surface.connect(self.update_surface_selected)

        self.explorer.row_selected.connect(self.points_view.superselect_point)

        self.actions.execute_action.connect(self._action_process)
        self.actions.exclude_queued_file.connect(self._action_remove_file)
        self.actions.exclude_unmatched_file.connect(self._action_remove_file)
        self.actions.undo_exclude_file.connect(self._action_add_files)

        #self.two_d.box_select.connect(self.select_line_by_box)
        self.two_d.lines_select.connect(self.select_lines_by_name)
        self.two_d.box_points.connect(self.select_points_in_box)
        self.two_d.turn_off_pointsview.connect(self.clear_points)

        self.points_view.points_selected.connect(self.show_points_in_explorer)
        self.points_view.points_cleaned.connect(self.set_pointsview_points_status)
        self.points_view.patch_test_sig.connect(self.manual_patch_test)

        self.action_thread.started.connect(self._start_action_progress)
        self.action_thread.finished.connect(self._kluster_execute_action_results)
        self.overwrite_nav_thread.started.connect(self._start_action_progress)
        self.overwrite_nav_thread.finished.connect(self._kluster_overwrite_nav_results)
        self.import_ppnav_thread.started.connect(self._start_action_progress)
        self.import_ppnav_thread.finished.connect(self._kluster_import_ppnav_results)
        self.surface_thread.started.connect(self._start_action_progress)
        self.surface_thread.finished.connect(self._kluster_surface_generation_results)
        self.surface_update_thread.started.connect(self._start_action_progress)
        self.surface_update_thread.finished.connect(self._kluster_surface_update_results)
        self.export_thread.started.connect(self._start_action_progress)
        self.export_thread.finished.connect(self._kluster_export_results)
        self.export_grid_thread.started.connect(self._start_action_progress)
        self.export_grid_thread.finished.connect(self._kluster_export_grid_results)
        self.open_project_thread.started.connect(self._start_action_progress)
        self.open_project_thread.finished.connect(self._kluster_open_project_results)
        self.draw_navigation_thread.started.connect(self._start_action_progress)
        self.draw_navigation_thread.finished.connect(self._kluster_draw_navigation_results)
        self.draw_surface_thread.started.connect(self._start_action_progress)
        self.draw_surface_thread.finished.connect(self._kluster_draw_surface_results)
        self.load_points_thread.started.connect(self._start_action_progress)
        self.load_points_thread.finished.connect(self._kluster_load_points_results)
        self.patch_test_load_thread.started.connect(self._start_action_progress)
        self.patch_test_load_thread.finished.connect(self._kluster_update_manual_patch_test)

        self.monitor.monitor_file_event.connect(self.intel._handle_monitor_event)
        self.monitor.monitor_start.connect(self._create_new_project_if_not_exist)
        self._patch = None  # retain patch test dialog object while running it for updates

        self.setup_menu()
        self.setup_widgets()
        self.read_settings()

        self.setAcceptDrops(True)
        if self.settings.get('dark_mode'):
            self.set_dark_mode(self.settings['dark_mode'])
        else:
            self.set_dark_mode(False)

    @property
    def settings_object(self):
        kluster_dir = os.path.dirname(kluster_init_file)
        kluster_ini = os.path.join(kluster_dir, 'misc', 'kluster.ini')
        return QtCore.QSettings(kluster_ini, QtCore.QSettings.IniFormat)

    def _load_previously_used_settings(self):
        settings = self.settings_object
        for settname, opts in settings_translator.items():
            if settings.value(settname) is not None:
                setval = settings.value(settname)
                self.settings[opts['newname']] = setval
                if isinstance(setval, str) and setval.lower() == 'true':
                    self.settings[opts['newname']] = True
                elif isinstance(setval, str) and setval.lower() == 'false':
                    self.settings[opts['newname']] = False
            else:
                self.settings[opts['newname']] = opts['defaultvalue']
        if not self.settings.get('vdatum_directory'):
            possible_vdatum = path_to_supplementals('VDatum')
            if possible_vdatum and os.path.exists(possible_vdatum):
                self.settings['vdatum_directory'] = possible_vdatum
                self.two_d.vdatum_directory = self.settings['vdatum_directory']  # used for the 2d vdatum region display
        if self.project.path is not None:
            self.project.set_settings(self.settings.copy())
        self.intel.set_settings(self.settings.copy())

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
            self.update_on_file_added(fils)
        else:
            e.ignore()

    def setup_menu(self):
        """
        Build the menu bar for the application
        """

        add_files_action = QtWidgets.QAction('Add Files', self)
        add_files_action.triggered.connect(self._action_filemenu_add_files)
        add_converted_action = QtWidgets.QAction('Open Converted', self)
        add_converted_action.triggered.connect(self._action_filemenu_add_converted)
        add_surface_action = QtWidgets.QAction('Open Surface', self)
        add_surface_action.triggered.connect(self._action_filemenu_add_surface)
        new_proj_action = QtWidgets.QAction('New Project', self)
        new_proj_action.triggered.connect(self._action_new_project)
        open_proj_action = QtWidgets.QAction('Open Project', self)
        open_proj_action.triggered.connect(self._action_open_project)
        save_proj_action = QtWidgets.QAction('Save Project', self)
        save_proj_action.triggered.connect(self._action_save_project)
        close_proj_action = QtWidgets.QAction('Close Project', self)
        close_proj_action.triggered.connect(self.close_project)
        add_vessel_action = QtWidgets.QAction('New Vessel File', self)
        add_vessel_action.triggered.connect(self._action_new_vessel_file)
        open_vessel_action = QtWidgets.QAction('Open Vessel File', self)
        open_vessel_action.triggered.connect(self._action_open_vessel_file)
        settings_action = QtWidgets.QAction('Settings', self)
        settings_action.triggered.connect(self.set_settings)
        export_action = QtWidgets.QAction('Export Soundings', self)
        export_action.triggered.connect(self._action_export)
        export_grid_action = QtWidgets.QAction('Export Surface', self)
        export_grid_action.triggered.connect(self._action_export_grid)

        view_darkstyle = QtWidgets.QAction('Dark Mode', self)
        view_darkstyle.setCheckable(True)
        view_darkstyle.triggered.connect(self.set_dark_mode)
        view_layers = QtWidgets.QAction('Layer Settings', self)
        view_layers.triggered.connect(self.set_layer_settings)
        view_dashboard_action = QtWidgets.QAction('Dashboard', self)
        view_dashboard_action.triggered.connect(self.open_dask_dashboard)
        view_reset_action = QtWidgets.QAction('Reset Layout', self)
        view_reset_action.triggered.connect(self.reset_settings)

        set_project_settings = QtWidgets.QAction('Set Project Settings', self)
        set_project_settings.triggered.connect(self.set_project_settings)
        setup_client_action = QtWidgets.QAction('Dask Client', self)
        setup_client_action.triggered.connect(self.start_dask_client)
        vessel_view_action = QtWidgets.QAction('Vessel Offsets', self)
        vessel_view_action.triggered.connect(self._action_vessel_view)

        importppnav_action = QtWidgets.QAction('Import Processed Navigation', self)
        importppnav_action.triggered.connect(self._action_import_ppnav)
        overwritenav_action = QtWidgets.QAction('Overwrite Raw Navigation', self)
        overwritenav_action.triggered.connect(self._action_overwrite_nav)
        surface_action = QtWidgets.QAction('New Surface', self)
        surface_action.triggered.connect(self._action_surface_generation)
        patch_action = QtWidgets.QAction('Patch Test', self)
        patch_action.triggered.connect(self._action_patch_test)

        basicplots_action = QtWidgets.QAction('Basic Plots', self)
        basicplots_action.triggered.connect(self._action_basicplots)
        advancedplots_action = QtWidgets.QAction('Advanced Plots', self)
        advancedplots_action.triggered.connect(self._action_advancedplots)

        about_action = QtWidgets.QAction('About', self)
        about_action.triggered.connect(self._action_show_about)
        docs_action = QtWidgets.QAction('Documentation', self)
        docs_action.triggered.connect(self._action_show_docs)
        videos_action = QtWidgets.QAction('YouTube Videos', self)
        videos_action.triggered.connect(self.open_youtube_playlist)

        menubar = self.menuBar()
        file = menubar.addMenu("File")
        file.addAction(add_files_action)
        file.addAction(add_converted_action)
        file.addAction(add_surface_action)
        file.addSeparator()
        file.addAction(new_proj_action)
        file.addAction(open_proj_action)
        file.addAction(save_proj_action)
        file.addAction(close_proj_action)
        file.addSeparator()
        file.addAction(add_vessel_action)
        file.addAction(open_vessel_action)
        file.addSeparator()
        file.addAction(settings_action)
        file.addSeparator()
        file.addAction(export_action)
        file.addAction(export_grid_action)

        view = menubar.addMenu('View')
        view.addAction(view_darkstyle)
        view.addAction(view_layers)
        view.addAction(view_dashboard_action)
        view.addAction(view_reset_action)

        setup = menubar.addMenu('Setup')
        setup.addAction(set_project_settings)
        setup.addAction(vessel_view_action)
        setup.addAction(setup_client_action)

        process = menubar.addMenu('Process')
        process.addAction(overwritenav_action)
        process.addAction(importppnav_action)
        process.addAction(surface_action)
        # process.addAction(patch_action)

        visual = menubar.addMenu('Visualize')
        visual.addAction(basicplots_action)
        visual.addAction(advancedplots_action)

        klusterhelp = menubar.addMenu('Help')
        klusterhelp.addAction(about_action)
        klusterhelp.addAction(docs_action)
        klusterhelp.addAction(videos_action)

    def update_on_file_added(self, fil: Union[str, list] = ''):
        """
        Adding a new path to a fqpr data store will update all the child widgets.  Will also load the data and add it
        to this class' project.

        Dragging in multiple files/folders will mean fil is a list.

        fqpr = fully qualified ping record, the term for the datastore in kluster

        Parameters
        ----------
        fil: str or list, one of the following: str path to converted data folder, list of str paths to converted data
             folders, str path to multibeam file, list of str paths to multibeam files, str path to multibeam file
             directory, list of str paths to multibeam file directory
        """

        if type(fil) is str and fil != '':
            fil = [fil]

        new_fqprs = []
        for f in fil:  # first pass to weed out a potential project, want to load that first
            fnorm = os.path.normpath(f)
            if os.path.split(fnorm)[1] == 'kluster_project.json':
                self.open_project(fnorm)
                fil.remove(f)
                return  # we can't handle loading a new project and adding data at the same time, if a project is added, halt

        potential_surface_paths = []
        potential_fqpr_paths = []
        for f in fil:
            f = os.path.normpath(f)
            try:
                updated_type, new_data, new_project = self.intel.add_file(f)
            except Exception as e:
                print('Unable to load from file {}, {}'.format(f, e))
                updated_type, new_data, new_project = None, True, None

            if new_project:  # user added a data file when there was no project, so we loaded or created a new one
                new_fqprs.extend([fqpr for fqpr in self.project.fqpr_instances.keys() if fqpr not in new_fqprs])
            if new_data is None:
                if any([os.path.exists(os.path.join(f, gname)) for gname in allowable_grid_root_names]):
                    potential_surface_paths.append(f)
                elif os.path.isdir(f):
                    potential_fqpr_paths.append(f)
        self.refresh_project(new_fqprs)
        self.open_project_thread.populate(force_add_fqprs=potential_fqpr_paths, force_add_surfaces=potential_surface_paths)
        self.open_project_thread.start()

    def refresh_project(self, fqpr=None):
        if fqpr:
            self.redraw(new_fqprs=fqpr)
        else:
            self.redraw()

    def redraw(self, new_fqprs=None, add_surface=None, remove_surface=None, surface_layer_name=''):
        """
        After adding new projects or surfaces, refresh the widgets to display the new data

        Parameters
        ----------
        new_fqprs: list, list of str file paths to converted fqpr instances
        add_surface: optional, str, path to new surface to add
        remove_surface: optional, str, path to existing surface to hide
        surface_layer_name: optional, str, name of the layer of the surface to add or hide
        """

        self.project_tree.refresh_project(proj=self.project)
        if remove_surface is not None:
            surf_object = self.project.surface_instances[remove_surface]
            if surface_layer_name == 'tiles':
                if surface_layer_name:
                    self.two_d.hide_line(remove_surface)
                else:
                    self.two_d.remove_line(remove_surface)
            else:
                for resolution in surf_object.resolutions:
                    if surface_layer_name:
                        self.two_d.hide_surface(remove_surface, surface_layer_name, resolution)
                    else:
                        self.two_d.remove_surface(remove_surface, resolution)
        if add_surface is not None and surface_layer_name:
            if self.surface_update_thread.isRunning():
                print('Surface is currently updating, please wait until after that process is complete.')
                return
            surf_object = self.project.surface_instances[add_surface]
            needs_drawing = []
            if surface_layer_name == 'tiles':
                if self.settings['dark_mode']:
                    shown = self.two_d.show_line(add_surface, color='white')
                else:
                    shown = self.two_d.show_line(add_surface, color='black')
                if not shown:  # show didnt work, must need to add the surface instead, loading from disk...
                    needs_drawing.append(None)
            else:
                for resolution in surf_object.resolutions:
                    shown = self.two_d.show_surface(add_surface, surface_layer_name, resolution)
                    if not shown:  # show didnt work, must need to add the surface instead, loading from disk...
                        needs_drawing.append(resolution)
            if needs_drawing:
                print('Drawing {} - {}, resolution {}'.format(add_surface, surface_layer_name, needs_drawing))
                self.draw_surface_thread.populate(add_surface, surf_object, needs_drawing, surface_layer_name)
                self.draw_surface_thread.start()

        if new_fqprs is not None and new_fqprs:
            self.draw_navigation_thread.populate(self.project, new_fqprs)
            self.draw_navigation_thread.start()

    def manage_fqpr(self, pth):
        fq = self.project.fqpr_instances[pth]
        self.managedata_win = None
        self.managedata_win = dialog_managedata.ManageDataDialog()
        self.managedata_win.refresh_fqpr.connect(self._refresh_manage_fqpr)
        self.managedata_win.populate(fq)
        self.managedata_win.show()

    def manage_surface(self, pth):
        surf = self.project.surface_instances[pth]
        self.managedata_surf = None
        self.managedata_surf = dialog_managesurface.ManageSurfaceDialog()
        self.managedata_surf.populate(surf)
        self.managedata_surf.show()

    def _refresh_manage_fqpr(self, fq, dlog):
        self.project.add_fqpr(fq)
        self.refresh_explorer(fq)
        dlog.populate(fq)

    def close_fqpr(self, pth):
        """
        With the given path to the Fqpr instance, remove the loaded data associated with the Fqpr and remove it from
        the gui widgets / project.

        Parameters
        ----------
        pth: str, path to the Fqpr top level folder

        """
        for ln in self.project.return_project_lines(proj=pth, relative_path=True):
            self.two_d.remove_line(ln)
        self.two_d.refresh_screen()
        self.points_view.clear()
        self.project.remove_fqpr(pth, relative_path=True)
        self.project_tree.refresh_project(self.project)

    def open_fqpr(self, pth):
        """
        With the given path to the Fqpr instance, add the loaded data associated with the Fqpr and add it to
        the gui widgets / project.

        Parameters
        ----------
        pth: str, path to the Fqpr top level folder

        """
        self.update_on_file_added(pth)

    def load_console_fqpr(self, pth):
        """
        Right click in the project tree and load in console to run this code block.  Will load the fqpr_generation
        object and all the important datasets that you probably want to access.

        Parameters
        ----------
        pth: str, path to the fqpr_generation saved data

        """
        absolute_fqpath = self.project.absolute_path_from_relative(pth)
        self.console.runCmd('data = reload_data(r"{}", skip_dask=True)'.format(absolute_fqpath))
        self.console.runCmd('first_system = data.multibeam.raw_ping[0]')
        self.console.runCmd('att = data.multibeam.raw_att')
        self.console.runCmd('# try plotting surface soundspeed, "first_system.soundspeed.plot()"')

    def load_console_surface(self, pth: str):
        """
        Right click in the project tree and load in console to run this code block.  Will load the surface object and
        demonstrate how to access the tree

        Parameters
        ----------
        pth
            path to the grid folder
        """
        absolute_fqpath = self.project.absolute_path_from_relative(pth)
        self.console.runCmd('surf = reload_surface(r"{}")'.format(absolute_fqpath))

    def show_in_explorer(self, pth: str):
        """
        Right click in the project tree and show in explorer to run this code block.  Will open the folder location
        in windows explorer.

        Parameters
        ----------
        pth
            path to the grid folder
        """
        absolute_path = self.project.absolute_path_from_relative(pth)
        os.startfile(absolute_path)

    def zoom_extents_fqpr(self, pth: str):
        """
        Right click on converted data instance and zoom to the extents of that layer

        Parameters
        ----------
        pth
            path to the converted data/surface
        """

        fq = self.project.fqpr_instances[pth]
        lines = list(fq.multibeam.raw_ping[0].multibeam_files.keys())
        self.two_d.set_extents_from_lines(subset_lines=lines)

    def zoom_extents_surface(self, pth: str):
        """
        Right click on surface and zoom to the extents of that layer

        Parameters
        ----------
        pth
            path to the converted data/surface
        """
        if pth in self.project.surface_instances:
            self.two_d.set_extents_from_surfaces(subset_surf=pth,
                                                 resolution=self.project.surface_instances[pth].resolutions[0])

    def _action_process(self, is_auto):
        if is_auto:
            self.intel.execute_action(0)
        else:
            self.intel.execute_action(0)

    def _action_remove_file(self, filname):
        self.intel.remove_file(filname)

    def _action_add_files(self, list_of_files):
        for fil in list_of_files:
            if os.path.exists(fil):
                self.intel.add_file(fil)
            else:
                print('Unable to find {}'.format(fil))

    def visualize_orientation(self, pth):
        self.project.build_visualizations(pth, 'orientation')

    def visualize_beam_vectors(self, pth):
        self.project.build_visualizations(pth, 'beam_vectors')

    def visualize_corrected_beam_vectors(self, pth):
        self.project.build_visualizations(pth, 'corrected_beam_vectors')

    def close_surface(self, pth):
        """
        With the given path to the surface instance, remove the loaded data associated with the surface and remove it from
        the gui widgets / project.

        Parameters
        ----------
        pth: str, path to the Fqpr top level folder

        """

        surf_object = self.project.surface_instances[pth]
        for resolution in surf_object.resolutions:
            self.two_d.remove_surface(pth, resolution)
        self.project.remove_surface(pth, relative_path=True)
        self.project_tree.refresh_project(self.project)

    def no_threads_running(self):
        """
        Simple check to see if any of the available processes are running.  Maybe in the future we want to allow
        multiple threads, for now only allow one at a time.

        Returns
        -------
        bool, if True, none of the threads are running (surface generation, conversion, etc)

        """
        for thrd in self.allthreads:
            if thrd.isRunning():
                return False
        return True

    def kluster_vessel_offsets(self):
        """
        Runs the dialog_vesselview that allows you to visualize your sensor locations and boat

        If you have a data container selected, it will populate from it's xyzrph attribute.
        """

        vessel_file = self.project.vessel_file
        fqprs, _ = self.return_selected_fqprs()

        self.vessel_win = None
        self.vessel_win = dialog_vesselview.VesselWidget()
        self.vessel_win.vessel_file_modified.connect(self.regenerate_offsets_actions)
        self.vessel_win.converted_xyzrph_modified.connect(self.update_offsets_vesselwidget)

        if vessel_file:
            self.vessel_win.load_from_config_file(vessel_file)
        elif fqprs:
            fqpr = self.project.fqpr_instances[self.project.path_relative_to_project(fqprs[0])]
            vess_xyzrph = convert_from_fqpr_xyzrph(fqpr.multibeam.xyzrph, fqpr.multibeam.raw_ping[0].sonartype,
                                                   fqpr.multibeam.raw_ping[0].system_identifier,
                                                   os.path.split(fqpr.output_folder)[1])
            self.vessel_win.xyzrph = vess_xyzrph
            self.vessel_win.load_from_existing_xyzrph()
        self.vessel_win.show()

    def regenerate_offsets_actions(self, is_modified: bool):
        """
        Action triggered on saving a vessel file in self.vessel_win.  Automatically generates new actions based on
        changes to this file.

        Parameters
        ----------
        is_modified
            If the file was modified, this is True
        """

        vessel_file = self.project.return_vessel_file()
        if vessel_file:
            self.intel.regenerate_actions()

    def update_offsets_vesselwidget(self, vess_xyzrph: dict):
        """
        If the user brings up the vessel setup tool with a converted fqpr container selected in the main gui, it loads
        from the xyzrph in that converted container.  The user can then make changes and save it back to the converted
        data container, which is what this method does.  If the data saved back is different, we figure out where the
        difference is and generate a new corresponding action by saving to the current_processing_status and running
        regenerate_actions

        Parameters
        ----------
        vess_xyzrph
            the data from the vessel setup widget, used to overwrite the converted fqpr container xyzrph record
        """

        xyzrph, sonar_type, system_identifiers, source = convert_from_vessel_xyzrph(vess_xyzrph)
        for cnt, sysident in enumerate(system_identifiers):
            matching_fq = list(source[0].values())[0]
            for fqname, fq in self.project.fqpr_instances.items():
                if fqname == matching_fq:
                    print('Updating xyzrph record for {}'.format(fqname))
                    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(fq.multibeam.xyzrph,
                                                                                                                        xyzrph[cnt])
                    # # drop the vessel setup specific keys, like the vessel file used and the vess_center location
                    # drop_these = [ky for ky in xyzrph[cnt].keys() if ky not in fq.multibeam.xyzrph.keys()]
                    # [xyzrph[cnt].pop(ky) for ky in drop_these]
                    fq.write_attribute_to_ping_records({'xyzrph': xyzrph[cnt]})
                    fq.multibeam.xyzrph.update(xyzrph[cnt])
                    if not identical_angles:  # if the angles changed then we have to start over at converted status
                        if fq.multibeam.raw_ping[0].current_processing_status > 0:
                            fq.write_attribute_to_ping_records({'current_processing_status': 0})
                            fq.logger.info('Setting processing status to 0, starting over at computing orientation')
                    elif not identical_offsets or new_waterline is not None:  # have to re-soundvelocitycorrect
                        if fq.multibeam.raw_ping[0].current_processing_status >= 3:
                            fq.write_attribute_to_ping_records({'current_processing_status': 2})
                            fq.logger.info('Setting processing status to 2, starting over at sound velocity correction')
                    elif not identical_tpu:  # have to re-tpu
                        if fq.multibeam.raw_ping[0].current_processing_status >= 5:
                            fq.write_attribute_to_ping_records({'current_processing_status': 4})
                            fq.logger.info('Setting processing status to 4, starting over at uncertainty calculation')
                    self.project.refresh_fqpr_attribution(fqname, relative_path=True)
        self.intel.regenerate_actions()

    def reprocess_fqpr(self):
        """
        Right click an fqpr instance and trigger reprocessing, should only be necessary in case of emergency.
        """

        fqprs, linedict = self.project_tree.return_selected_fqprs()
        if fqprs:
            # start over at 0, which is conversion in our state machine
            fq = self.project.fqpr_instances[fqprs[0]]
            current_status = fq.multibeam.raw_ping[0].current_processing_status
            if current_status == 0:
                print('reprocess_fqpr: Unable to reprocess converted data, current process is already at the beginning (conversion)')
                return
            dlog = dialog_reprocess.ReprocessDialog(current_status, fq.output_folder)
            cancelled = False
            if dlog.exec_():
                if not dlog.canceled:
                    newstatus = dlog.newstatus
                    if newstatus is not None:
                        fq.write_attribute_to_ping_records({'current_processing_status': newstatus})
                        fq.logger.info(f'Setting processing status to {newstatus}, starting over at computing {kluster_variables.status_lookup[newstatus + 1]}')
                        self.project.refresh_fqpr_attribution(fqprs[0], relative_path=True)
                        fq.multibeam.reload_pingrecords(skip_dask=fq.client is None)
                        self.intel.regenerate_actions()
                    else:
                        print('reprocess_fqpr: new status is None, unable to set status')
                else:
                    print('reprocess_fqpr: cancelled')

    def update_surface_selected(self):
        """
        Right click on bathygrid instance and trigger updating the data, runs the update dialog and processes with those
        options.
        """
        self.kluster_surface_update()

    def kluster_basic_plots(self):
        """
        Runs the basic plots dialog, for plotting the variables using the xarray/matplotlib functionality
        """
        fqprspaths, fqprs = self.return_selected_fqprs(subset_by_line=True)

        self.basicplots_win = None
        self.basicplots_win = dialog_basicplot.BasicPlotDialog()

        if fqprs:
            self.basicplots_win.data_widget.new_fqpr_path(fqprspaths[0], fqprs[0])
            self.basicplots_win.data_widget.initialize_controls()
        self.basicplots_win.show()

    def kluster_advanced_plots(self):
        """
        Runs the advanced plots dialog, for plotting the sat tests and other more sophisticated stuff
        """
        fqprspaths, fqprs = self.return_selected_fqprs(subset_by_line=True)
        first_surf = None
        default_plots = None
        if self.project.surface_instances:
            first_surf = list(self.project.surface_instances.keys())[0]
            first_surf = self.project.absolute_path_from_relative(first_surf)
            default_plots = os.path.join(os.path.dirname(first_surf), 'accuracy_test')
            if os.path.exists(default_plots):
                default_plots = os.path.join(os.path.dirname(first_surf), 'accuracy_test_{}'.format(datetime.now().strftime('%Y%m%d_%H%M%S')))

        self.advancedplots_win = None
        self.advancedplots_win = dialog_advancedplot.AdvancedPlotDialog()

        if fqprspaths:
            self.advancedplots_win.data_widget.new_fqpr_path(fqprspaths[0], fqprs[0])
            self.advancedplots_win.data_widget.initialize_controls()
        if first_surf:
            self.advancedplots_win.surf_text.setText(first_surf)
            self.advancedplots_win.out_text.setText(default_plots)
        self.advancedplots_win.show()

    def kluster_execute_action(self, action_container: list, action_index: int = 0):
        """
        Run the next action in the fqpr_action ActionContainer.  The next action will always be the highest priority
        one, i.e. first in the list.  Therefore the default action_index will always be zero

        Parameters
        ----------
        action_container
            fqpr_actions.FqprActionContainer instance for the GUI
        action_index
            integer index in the action list to run
        """

        cancelled = False
        if not self.no_threads_running():
            # print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        if not cancelled:
            self.output_window.clear()
            self.action_thread.populate(action_container, action_index)
            self.action_thread.start()

    def _kluster_execute_action_results(self):
        """
        Read the results of the executed action.  Multibeam actions can generate new converted data that would need
        to be showin in the project window.
        """

        # fqpr is now the output path of the Fqpr instance
        fqpr = self.action_thread.result
        if fqpr is not None and not self.action_thread.error:
            fqpr_entry, already_in = self.project.add_fqpr(fqpr)
            self.project.save_project()
            self.intel.update_intel_for_action_results(action_type=self.action_thread.action_type)

            if already_in and self.action_thread.action_type != 'multibeam':
                self.refresh_project()
                self.refresh_explorer(self.project.fqpr_instances[fqpr_entry])
            else:  # new fqpr, or conversion actions always need a full refresh
                self.refresh_project(fqpr=[fqpr_entry])
        else:
            print('Error running action {}'.format(self.action_thread.action_type))
            print(self.action_thread.exceptiontxt)
            print('kluster_action: no data returned from action execution: {}'.format(fqpr))
        self.action_thread.populate(None, None)
        self._stop_action_progress()

    def kluster_overwrite_nav(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the overwrite navigation dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new attribution

        """
        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            fqprs, _ = self.return_selected_fqprs()
            dlog = dialog_overwritenav.OverwriteNavigationDialog()
            dlog.update_fqpr_instances(addtl_files=fqprs)
            cancelled = False
            if dlog.exec_():
                opts = dlog.return_processing_options()
                if opts is not None and not dlog.canceled:
                    nav_opts = opts
                    fqprs = nav_opts.pop('fqpr_inst')
                    fq_chunks = []
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            print('Unable to find {} in currently loaded project'.format(relfq))
                            return
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
                            fq_chunks.append([fq_inst, nav_opts])
                    if fq_chunks:
                        self.overwrite_nav_thread.populate(fq_chunks)
                        self.overwrite_nav_thread.start()
                else:
                    cancelled = True
        if cancelled:
            print('kluster_import_navigation: Processing was cancelled')

    def _kluster_overwrite_nav_results(self):
        """
        Method is run when the import navigation thread signals completion.  All we need to do here is refresh the project
        and display.
        """
        fq_inst = self.overwrite_nav_thread.fqpr_instances
        if fq_inst and not self.overwrite_nav_thread.error:
            for fq in fq_inst:
                self.project.add_fqpr(fq)
                self.refresh_explorer(fq)
        else:
            print('Error overwriting raw navigation')
            print(self.overwrite_nav_thread.exceptiontxt)
        self.overwrite_nav_thread.populate(None)
        self._stop_action_progress()

    def kluster_import_ppnav(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the import navigation dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new attribution

        """
        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            fqprs, _ = self.return_selected_fqprs()
            dlog = dialog_importppnav.ImportPostProcNavigationDialog()
            dlog.update_fqpr_instances(addtl_files=fqprs)
            cancelled = False
            if dlog.exec_():
                opts = dlog.return_processing_options()
                if opts is not None and not dlog.canceled:
                    nav_opts = opts
                    fqprs = nav_opts.pop('fqpr_inst')
                    fq_chunks = []
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            print('Unable to find {} in currently loaded project'.format(relfq))
                            return
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
                            fq_chunks.append([fq_inst, nav_opts])
                    if fq_chunks:
                        self.import_ppnav_thread.populate(fq_chunks)
                        self.import_ppnav_thread.start()
                else:
                    cancelled = True
        if cancelled:
            print('kluster_import_navigation: Processing was cancelled')

    def _kluster_import_ppnav_results(self):
        """
        Method is run when the import navigation thread signals completion.  All we need to do here is refresh the project
        and display.
        """
        fq_inst = self.import_ppnav_thread.fqpr_instances
        if fq_inst and not self.import_ppnav_thread.error:
            for fq in fq_inst:
                self.project.add_fqpr(fq)
                self.refresh_explorer(fq)
        else:
            print('Error importing post processed navigation')
            print(self.import_ppnav_thread.exceptiontxt)
        self.import_ppnav_thread.populate(None)
        self._stop_action_progress()

    def manual_patch_test(self, e):
        """
        Triggered by patch test button in Points View.  Will retrieve the relevant installation parameter records for
        the lines in the Points View subset, and display them (PrePatchDialog) for the user to then select the record they want to use in
        the patch test tool (ManualPatchTestWidget).  After that selection, we run the Patch Test tool.
        """
        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
        else:
            systems, linenames, time_segments = self.points_view.return_lines_and_times()
            if systems:
                datablock = self.project.retrieve_data_for_time_segments(systems, time_segments)
                if datablock:
                    dlog_patch = dialog_manualpatchtest.PrePatchDialog()
                    dlog_patch.add_data(datablock)
                    if dlog_patch.exec_():
                        if dlog_patch.canceled:
                            print('Patch Test: test canceled')
                        else:
                            final_datablock = dlog_patch.return_final_data()
                            if final_datablock:
                                vessel_file_name = datablock[0][-1]
                                roll, pitch, heading = final_datablock[8], final_datablock[9], final_datablock[10]
                                xlever, ylever, zlever = final_datablock[11], final_datablock[12], final_datablock[13]
                                latency = final_datablock[14]
                                fqprs, timesegments = final_datablock[0], final_datablock[6]
                                for cnt, fq in enumerate(fqprs):
                                    fq.subset_by_times(timesegments[cnt].tolist())
                                self._manpatchtest = None
                                self._manpatchtest = dialog_manualpatchtest.ManualPatchTestWidget()
                                self._manpatchtest.new_offsets_angles.connect(self._update_manual_patch_test)
                                self._manpatchtest.patchdatablock = final_datablock
                                self._manpatchtest.populate(vessel_file_name, ','.join(final_datablock[1]), final_datablock[2],
                                                            final_datablock[3], ','.join(final_datablock[4]), ','.join(final_datablock[5]),
                                                            roll, pitch, heading, xlever, ylever, zlever, latency)
                                self._manpatchtest.show()
                            else:
                                print('Patch Test: no data selected')
                else:
                    print('Patch Test: no data selected for running in Patch Test utility')
            else:
                print('Patch Test: no data found in Points View')

    def _update_manual_patch_test(self):
        """
        Triggered on hitting update in the patch test tool.  Will run the patch_test_load_thread to reprocess the data
        in the patch test subset (see load_points_thread.polygon for the boundary of this subset) and display the
        new data in points view, replacing the old points.
        """

        if self._manpatchtest.patchdatablock is not None:
            roll, pitch, heading = self._manpatchtest.roll, self._manpatchtest.pitch, self._manpatchtest.heading
            xlever, ylever, zlever = self._manpatchtest.x_lever, self._manpatchtest.y_lever, self._manpatchtest.z_lever
            fqprs, tstamps, timesegments, headindex = self._manpatchtest.patchdatablock[0], self._manpatchtest.patchdatablock[5], self._manpatchtest.patchdatablock[6], int(self._manpatchtest.patchdatablock[7])
            latency = self._manpatchtest.latency
            prefixes = self._manpatchtest.patchdatablock[15]
            serial_num = str(self._manpatchtest.patchdatablock[3])
            self.patch_test_load_thread.populate(fqprs, [roll, pitch, heading, xlever, ylever, zlever, latency],
                                                 headindex, prefixes, tstamps, serial_num, self.load_points_thread.polygon)
            self.patch_test_load_thread.start()
        else:
            print('Unable to load the data for updating the manual patch test')

    def _kluster_update_manual_patch_test(self):
        """
        Method is run when the patch test load thread completes.  We take the reprocessed soundings and replace the old
        soundings in the points view with these new values
        """

        results = self.patch_test_load_thread.result
        cur_azimuth = self.load_points_thread.azimuth
        headindex = int(self._manpatchtest.patchdatablock[7])
        self.points_view.store_view_settings()
        hl = self.points_view.three_d_window.hide_lines
        self.points_view.clear_display()
        if results and not self.patch_test_load_thread.error:
            for cnt, (fq, rslt) in enumerate(zip(self.patch_test_load_thread.fqprs, results)):
                head, x, y, z, tvu, rejected, pointtime, beam = rslt
                newid = self._manpatchtest.patchdatablock[1][cnt]
                linenames = fq.return_lines_for_times(pointtime)
                self.points_view.remove_points(system_id=newid + '_' + str(headindex))
                self.points_view.add_points(head, x, y, z, tvu, rejected, pointtime, beam, newid, linenames, cur_azimuth)
            self.points_view.three_d_window.hide_lines = hl
            self.points_view.display_points()
            self.points_view.load_view_settings()
            self.points_view.patch_test_running = True
        else:
            print('Error reprocessing patch test subset')
            print(self.patch_test_load_thread.exceptiontxt)
        self.patch_test_load_thread.populate(None)
        self._stop_action_progress()

    def kluster_auto_patch_test(self):
        """
        IN PROGRESS - run the automated patch test tool on the selected lines.  Still have not addressed the issues
        in the auto patch test procedure, so the results of this should not be used.
        """
        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            cancelled = False
            self._patch = dialog_patchtest.PatchTestDialog()
            self._patch.patch_query.connect(self._feed_auto_patch_test_dialog)
            if self._patch.exec_():
                cancelled = self._patch.canceled
                pairs = self._patch.return_pairs
                if pairs:
                    self.project.run_auto_patch_test(pairs)
        if cancelled:
            print('kluster_auto_patch_test: Processing was cancelled')
        self._patch = None

    def _feed_auto_patch_test_dialog(self, mode: str):
        """
        Populate the auto patch test dialog with the line pairs selected.  Pair them by recipricol azimuth and the closeness
        of the start and end positions

        Parameters
        ----------
        mode
            either 'pointsview' to load data from the points view window (not currently supported) or 'lines' to load
            from the currently selected lines
        """

        if self._patch is None:
            print('ERROR: Lost handle on patch test dialog')
            return
        self._patch.clear()
        if mode == 'pointsview':
            # get lineinfo from points view
            pass
        else:
            fqprs, linedict = self.project_tree.return_selected_fqprs(force_line_list=True)
            total_lines = [x for y in linedict.values() for x in y]
            pair_list, ldict = self.project.sort_lines_patch_test_pairs(total_lines)
            cur_cnt = 0
            for lpair in pair_list:
                for lline in lpair:
                    self._patch.add_line([cur_cnt, lline, ldict[lline]['azimuth']])
                cur_cnt += 1

    def kluster_surface_generation(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the generate surface dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new surface
        """

        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            cancelled = False
            fqprspaths, fqprs = self.return_selected_fqprs()
            dlog = dialog_surface.SurfaceDialog()
            dlog.update_fqpr_instances(addtl_files=fqprspaths)
            if dlog.exec_():
                cancelled = dlog.canceled
                opts = dlog.return_processing_options()
                if opts is not None and not cancelled:
                    surface_opts = opts
                    fq_chunks = []
                    fqprs = surface_opts.pop('fqpr_inst')

                    if dlog.line_surface_checkbox.isChecked():  # we now subset the fqpr instances by lines selected
                        fqprspaths, fqprs = self.return_selected_fqprs(subset_by_line=True)
                        for fq in fqprs:
                            fq_chunks.extend([fq])
                    else:
                        for fq in fqprs:
                            try:
                                relfq = self.project.path_relative_to_project(fq)
                            except:
                                print('No project loaded, you must load some data before generating a surface')
                                return
                            if relfq not in self.project.fqpr_instances:
                                print('Unable to find {} in currently loaded project'.format(relfq))
                                return
                            if relfq in self.project.fqpr_instances:
                                fq_inst = self.project.fqpr_instances[relfq]
                                # use the project client, or start a new LocalCluster if client is None
                                # fq_inst.client = self.project.get_dask_client()
                                fq_chunks.extend([fq_inst])
                    if not dlog.canceled:
                        # if the project has a client, use it here.  If None, BatchRead starts a new LocalCluster
                        self.output_window.clear()
                        self.surface_thread.populate(fq_chunks, opts)
                        self.surface_thread.start()
        if cancelled:
            print('kluster_surface_generation: Processing was cancelled')

    def _kluster_surface_generation_results(self):
        """
        Method is run when the surface_thread signals completion.  All we need to do here is add the surface to the project
        and display.
        """

        fq_surf = self.surface_thread.fqpr_surface
        if fq_surf is not None and not self.surface_thread.error:
            self.project.add_surface(fq_surf)
            self.redraw()
        else:
            print('Error building surface')
            print(self.surface_thread.exceptiontxt)
        self.surface_thread.populate(None, {})
        self._stop_action_progress()

    def kluster_surface_update(self):
        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            cancelled = False
            surfs = self.return_selected_surfaces()
            if surfs:
                existing_container_names, possible_container_names = self.project.return_surface_containers(surfs[0], relative_path=False)
                surf = self.project.surface_instances[self.project.path_relative_to_project(surfs[0])]
                dlog = dialog_surface_data.SurfaceDataDialog(title=surf.output_folder)
                dlog.setup(existing_container_names, possible_container_names)
                if dlog.exec_():
                    cancelled = dlog.canceled
                    add_fqpr_names, remove_fqpr_names, opts = dlog.return_processing_options()
                    if not cancelled:
                        add_fqpr = []
                        for fqpr_inst in self.project.fqpr_instances.values():
                            fname = os.path.split(fqpr_inst.multibeam.raw_ping[0].output_path)[1]
                            if fname in add_fqpr_names:
                                add_fqpr.append(fqpr_inst)
                                add_fqpr_names.remove(fname)
                        if add_fqpr_names:
                            print('kluster_surface_update: {} must be loaded in Kluster for it to be added to the surface.'.format(add_fqpr_names))
                            return
                        self.output_window.clear()
                        self.surface_update_thread.populate(surf, add_fqpr, remove_fqpr_names, opts)
                        self.surface_update_thread.start()
                    else:
                        print('kluster_surface_update: Processing was cancelled')

    def _kluster_surface_update_results(self):
        """
        Method is run when the surface_update_thread signals completion.  All we need to do here is add the surface to the project
        and display.
        """

        fq_surf = self.surface_update_thread.fqpr_surface
        if fq_surf is not None and not self.surface_thread.error:
            relpath_surf = self.project.path_relative_to_project(os.path.normpath(fq_surf.output_folder))
            self.close_surface(relpath_surf)
            self.project.add_surface(fq_surf)
            self.project_tree.refresh_project(proj=self.project)
        else:
            print('Error updating surface')
            print(self.surface_update_thread.exceptiontxt)
        self.surface_update_thread.populate(None, None, None, {})
        self._stop_action_progress()

    def kluster_export_grid(self):
        """
        Trigger export on a surface provided.  Currently only supports export of xyz to csv file(s), geotiff and bag.
        """

        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            surfs = self.return_selected_surfaces()
            dlog = dialog_export_grid.ExportGridDialog()
            if surfs:
                first_surf = surfs[0]  # just use the first of the selected surfaces
                dlog.update_input_path(first_surf)
                relsurf = self.project.path_relative_to_project(first_surf)
                if relsurf in self.project.surface_instances:
                    dlog.update_vert_ref(self.project.surface_instances[relsurf].vertical_reference)
            cancelled = False
            if dlog.exec_():
                if not dlog.canceled:
                    opts = dlog.return_processing_options()
                    surf = dlog.input_pth
                    output_path = opts.pop('output_path')
                    export_format = opts.pop('export_format')
                    z_pos_up = opts.pop('z_positive_up')
                    relsurf = self.project.path_relative_to_project(surf)
                    if (export_format == 'BAG' or z_pos_up) and opts['vert_crs']:
                        opts['vert_crs'] = opts['vert_crs'].replace('"depth (D)",down', '"gravity-related height (H),up')
                    if relsurf not in self.project.surface_instances:
                        print('Unable to find {} in currently loaded project'.format(relsurf))
                        return
                    if relsurf in self.project.surface_instances:
                        surf_inst = self.project.surface_instances[relsurf]
                        self.output_window.clear()
                        print('Exporting to {}, format {}..'.format(output_path, export_format))
                        self.export_grid_thread.populate(surf_inst, export_format, output_path, z_pos_up, opts)
                        self.export_grid_thread.start()
                    else:
                        print('kluster_grid_export: Unable to load from {}'.format(surf))
                else:
                    cancelled = True
        if cancelled:
            print('kluster_grid_export: Export was cancelled')

    def _kluster_export_grid_results(self):
        """
        Method is run when the surface_update_thread signals completion.  All we need to do here is add the surface to the project
        and display.
        """

        if self.export_grid_thread.error:
            print('Error exporting grid')
            print(self.export_grid_thread.exceptiontxt)
        else:
            print('Export complete.')
        self.export_grid_thread.populate(None, '', '', True, {})
        self._stop_action_progress()

    def kluster_export(self):
        """
        Trigger export on all the fqprs provided.  Currently only supports export of xyz to csv file(s), las file(s)
        and entwine point store.
        """

        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            fqprs, _ = self.return_selected_fqprs()
            dlog = dialog_export.ExportDialog()
            dlog.update_fqpr_instances(addtl_files=fqprs)
            cancelled = False
            if dlog.exec_():
                basic_export_mode = dlog.basic_export_group.isChecked()
                line_export_mode = dlog.line_export.isChecked()
                points_export_mode = dlog.points_view_export.isChecked()
                if line_export_mode:
                    linenames = self.project_tree.return_selected_lines()
                else:
                    linenames = []
                if points_export_mode:
                    datablock = self.points_view.return_points()
                else:
                    datablock = []

                export_type = dlog.export_opts.currentText()
                delimiter = dlog.csvdelimiter_dropdown.currentText()
                filterset = dlog.filter_chk.isChecked()
                separateset = dlog.byidentifier_chk.isChecked()
                z_pos_down = dlog.zdirect_check.isChecked()
                if not dlog.canceled and export_type in ['csv', 'las', 'entwine']:
                    fq_chunks = []
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            print('Unable to find {} in currently loaded project'.format(relfq))
                            return
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
                            fq_chunks.append([fq_inst])
                    if fq_chunks:
                        self.output_window.clear()
                        self.export_thread.populate(fq_chunks, linenames, datablock, export_type, z_pos_down, delimiter, filterset,
                                                    separateset, basic_export_mode, line_export_mode, points_export_mode)
                        self.export_thread.start()
                else:
                    cancelled = True
        if cancelled:
            print('kluster_export: Export was cancelled')

    def _kluster_export_results(self):
        """
        Method is run when the surface_update_thread signals completion.  All we need to do here is add the surface to the project
        and display.
        """

        if self.export_thread.error:
            print('Export complete: Unable to export')
            print(self.export_thread.exceptiontxt)
        else:
            print('Export complete.')
        self.export_thread.populate(None, None, [], '', False, 'comma', False, False, True, False, False)
        self._stop_action_progress()

    def _start_action_progress(self):
        """
        For worker threads not started through the action widget, we have to manually trigger starting the progress
        bar here.
        """

        self.generic_progressbar.setMaximum(0)

    def _stop_action_progress(self):
        """
        For worker threads not started through the action widget, we have to manually trigger stopping the progress
        here.
        """

        if self.no_threads_running():
            self.generic_progressbar.setMaximum(1)

    def _create_new_project_if_not_exist(self, pth):
        """
        Setup a new project with the provided project path, if the project has not been setup already

        Parameters
        ----------
        pth
            folder path to the directory you want to create the project in
        """

        if self.project.path is None:
            self.project._setup_new_project(pth)

    def new_project(self, directory: str):
        """
        Create a new project file in the directory provided

        Parameters
        ----------
        directory
            path to the folder containing the new project you want to create
        """

        self.close_project()
        self.project._setup_new_project(directory)
        if self.settings:  # set_settings will set the project settings and save the project
            self.project.set_settings(self.settings.copy())
        else:  # just save the project
            self.project.save_project()

        self.redraw()

    def open_project(self, pth):
        """
        Open a project from project file

        Parameters
        ----------
        pth: str, path to the parent Fqpr project folder
        """

        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            self.close_project()
            self.output_window.clear()
            self.open_project_thread.populate(new_project_path=pth)
            self.open_project_thread.start()
            cancelled = False
        if cancelled:
            print('open_project: opening project was cancelled')

    def _kluster_open_project_results(self):
        """
        After running the open_project_thread, we get here and replace the existing project with the newly opened
        project.  We then draw the new lines to the screen.
        """
        if not self.open_project_thread.error:
            for new_fq in self.open_project_thread.new_fqprs:
                fqpr_entry, already_in = self.project.add_fqpr(new_fq, skip_dask=True)
                if already_in:
                    print('{} already exists in project'.format(new_fq.output_folder))
            for new_surf in self.open_project_thread.new_surfaces:
                self.project.add_surface(new_surf)
            self.redraw(new_fqprs=[self.project.path_relative_to_project(fq.output_folder) for fq in self.open_project_thread.new_fqprs])
        else:
            print('Error on opening data')
            print(self.open_project_thread.exceptiontxt)
        self.open_project_thread.populate(None)
        self._stop_action_progress()

    def _kluster_draw_navigation_results(self):
        """
        After opening a project, we run the draw_navigation_thread to load all navigation for all lines in the project
        """
        if not self.draw_navigation_thread.error:
            self.project = self.draw_navigation_thread.project
            for ln in self.draw_navigation_thread.line_data:
                self.two_d.add_line(ln, self.draw_navigation_thread.line_data[ln][0], self.draw_navigation_thread.line_data[ln][1])
            self.two_d.set_extents_from_lines()
        else:
            print('Error drawing lines from {}'.format(self.draw_navigation_thread.new_fqprs))
            print(self.draw_navigation_thread.exceptiontxt)
        self.draw_navigation_thread.populate(None, None)
        self._stop_action_progress()
        print('draw_navigation: Drawing navigation complete.')

    def _kluster_draw_surface_results(self):
        """
        After clicking on a surface layer, we load the data in this thread and in this method we draw the loaded data
        """
        if not self.draw_surface_thread.error:
            surf_path = self.draw_surface_thread.surface_path
            surf_epsg = self.draw_surface_thread.surf_object.epsg
            if self.draw_surface_thread.surface_layer_name == 'tiles':
                x, y = self.draw_surface_thread.surface_data
                trans = Transformer.from_crs(CRS.from_epsg(self.draw_surface_thread.surf_object.epsg),
                                             CRS.from_epsg(self.two_d.epsg), always_xy=True)
                lon, lat = trans.transform(x, y)
                if self.settings['dark_mode']:
                    self.two_d.add_line(surf_path, lat, lon, color='white')
                else:
                    self.two_d.add_line(surf_path, lat, lon, color='black')
                self.two_d.set_extents_from_lines()
            else:
                drawresolution = None
                for surf_resolution in self.draw_surface_thread.surface_data:
                    for surflayername in self.draw_surface_thread.surface_data[surf_resolution]:
                        data = self.draw_surface_thread.surface_data[surf_resolution][surflayername][0]
                        geo_transform = self.draw_surface_thread.surface_data[surf_resolution][surflayername][1]
                        self.two_d.add_surface([surf_path, surflayername, data, geo_transform, surf_epsg, surf_resolution])
                        if not drawresolution:
                            drawresolution = surf_resolution
                if drawresolution:
                    self.two_d.set_extents_from_surfaces(surf_path, drawresolution)
        else:
            print('Error drawing surface {}'.format(self.draw_surface_thread.surface_path))
            print(self.draw_surface_thread.exceptiontxt)
        self.draw_surface_thread.populate(None, None, None, None)
        self._stop_action_progress()
        print('draw_surface: Drawing surface complete.')

    def close_project(self):
        """
        Close all open Fqpr instances and surfaces
        """

        # go to list so you avoid the dreaded dict changed size during iteration error
        surf_to_close = []
        for surf in self.project.surface_instances:
            surf_to_close.append(surf)
        for surf in surf_to_close:
            self.close_surface(surf)

        fq_to_close = []
        for fq in self.project.fqpr_instances:
            fq_to_close.append(fq)
        for fq in fq_to_close:
            self.close_fqpr(fq)

        self.project_tree.configure()
        self.points_view.clear()
        self.two_d.clear()
        self.explorer.clear_explorer_data()
        self.attribute.clear_attribution_data()
        self.monitor.stop_all_monitoring()
        self.output_window.clear()

        self.project.close()
        self.intel.clear()

    def open_dask_dashboard(self):
        """
        Opens the bokeh dashboard in a web browser to view progress.  Either
        start a new LocalCluster client if there is no client yet OR get the existing client you've setup.
        """

        self.project.get_dask_client()
        webbrowser.open_new(self.project.client.dashboard_link)

    def open_youtube_playlist(self):
        """
        Opens the link to the Kluster 5 minute modules video playlist
        """

        webbrowser.open_new(r'https://www.youtube.com/playlist?list=PLrjCvP_J9AA_memBs2ZyKXGHG1AMx0GWx')

    def start_dask_client(self):
        """
        Set the project up with a new Client object, either LocalCluster or a client to a remote cluster
        """

        dlog = dialog_daskclient.DaskClientStart()
        if dlog.exec_():
            client = dlog.cl
            if client is None:
                print('start_dask_client: no client started successfully')
            else:
                self.project.client = client

    def set_project_settings(self):
        """
        Triggered on hitting OK in the project settings dialog.  Takes the provided settings and saves it to the project
        and intel instance.
        """

        dlog = dialog_project_settings.ProjectSettingsDialog(settings=self.settings_object)
        if dlog.exec_() and not dlog.canceled:
            settings = dlog.return_processing_options()
            self.settings.update(settings)
            settings_obj = self.settings_object
            for settname, opts in settings_translator.items():
                settings_obj.setValue(settname, self.settings[opts['newname']])

            if self.project.path is not None:
                self.project.set_settings(settings)
            self.intel.set_settings(settings)

    def set_layer_settings(self):
        """
        Triggered on hitting OK in the layer settings dialog.  Takes the provided settings and regenerates the 2d display.
        """

        dlog = dialog_layer_settings.LayerSettingsDialog(settings=self.settings_object)
        if dlog.exec_() and not dlog.canceled:
            settings = dlog.return_layer_options()
            self.settings.update(settings)
            settings_obj = self.settings_object
            for settname, opts in settings_translator.items():
                settings_obj.setValue(settname, self.settings[opts['newname']])

            self.two_d.vdatum_directory = self.settings['vdatum_directory']
            self.two_d.set_background(self.settings['layer_background'], self.settings['layer_transparency'],
                                      self.settings['surface_transparency'])

    def set_settings(self):
        """
        Triggered on hitting OK in the settings dialog.  Takes the provided settings and saves it to the project
        and intel instance.
        """

        dlog = dialog_settings.SettingsDialog(settings=self.settings_object)
        if dlog.exec_() and not dlog.canceled:
            settings = dlog.return_options()
            self.settings.update(settings)
            settings_obj = self.settings_object
            for settname, opts in settings_translator.items():
                settings_obj.setValue(settname, self.settings[opts['newname']])

            if self.project.path is not None:
                self.project.set_settings(settings)
            self.intel.set_settings(settings)

            # now overwrite the default kluster variables and save them to the ini file as well
            newkvars = dlog.return_kvars()
            for kvarkey, kvarval in newkvars.items():
                kluster_variables.alter_variable(kvarkey, kvarval)
                settings_obj.setValue(f'Kluster/kvariables_{kvarkey}', kvarval)

    def set_dark_mode(self, check_state: bool):
        """
        Using the excellent qdarkstyle module, set the qt app style to darkmode if the user selects it under view - dark mode

        Parameters
        ----------
        check_state
            check state of the dark mode checkbox
        """

        self.settings['dark_mode'] = check_state
        settings_obj = self.settings_object
        for settname, opts in settings_translator.items():
            settings_obj.setValue(settname, self.settings[opts['newname']])
        if check_state:
            try:
                self.app.setStyleSheet(qdarkstyle.load_stylesheet(qt_api=self.app_library))
                self.two_d.canvas.setCanvasColor(QtCore.Qt.black)
                self.two_d.toolPoints.base_color = QtCore.Qt.white
                self.points_view.colorbar.fig.set_facecolor('black')
                plt.style.use('dark_background')
            except:
                print('Unable to set qdarkstyle style sheet for app library {}'.format(self.app_library))
        else:
            self.app.setStyleSheet('')
            self.two_d.canvas.setCanvasColor(QtCore.Qt.white)
            self.two_d.toolPoints.base_color = QtCore.Qt.black
            self.points_view.colorbar.fig.set_facecolor('white')
            plt.style.use('seaborn')
        # now update the control if we are doing this manually, not through the checkbox event
        view_menu = [mn for mn in self.menuBar().actions() if mn.text() == 'View']
        if view_menu:
            view_menu = view_menu[0]
            darkaction = [mn for mn in view_menu.menu().actions() if mn.text() == 'Dark Mode']
            if darkaction:
                darkaction = darkaction[0]
                darkaction.setChecked(check_state)
            else:
                print('Warning: Can not find the Dark Mode action to set dark mode control!')
        else:
            print('Warning: Can not find the view menu to set dark mode control!')

    def dockwidget_is_visible(self, widg):
        """
        Surprisingly difficult to figure out whether or not a tab is visible, with it either being floating or the active
        tab in a tabified widget container.  This will check if any part of the widget is visible.

        Parameters
        ----------
        widg: QDockWidget

        Returns
        -------
        bool, True if the widget is visible
        """
        return (not widg.visibleRegion().isEmpty()) or (widg.isFloating())

    def _line_selected(self, linename, idx=0):
        """
        Each time a line is selected, we populate the explorer widget with the line information

        Some operations (like showing attitude for a line) we only want to run once when a bunch of lines are selected.
        To make this happen, we use the idx parameter and only run certain things on the first line in a set of lines.

        Parameters
        ----------
        linename: str, line name
        idx: int, optional, the index of the provided line in the list of lines that are to be selected

        Returns
        -------
        bool
            True if this line was successfully selected, if a grid outline was selected for example, that would not
            be a valid multibeam line and would return False
        """
        try:
            convert_pth = self.project.convert_path_lookup[linename]
            raw_attribution = self.project.fqpr_attrs[convert_pth]
            self.explorer.populate_explorer_with_lines(linename, raw_attribution)
            return True
        except KeyError:  # surface outline is added to 2d view as a 'line' but it would not be used here
            return False

    def refresh_explorer(self, fq_inst):
        """
        After reloading the fqpr instance (generally done after all processing), you need to also refresh the explorer
        widget, so that the attribution view accurately reflects the new attribution.  We only want to do this in a
        targeted way, so that we don't have to re-translate attribution for all the fqpr instances in the project.

        Parameters
        ----------
        fq_inst: fqpr_generation.Fqpr object

        """
        lines = list(fq_inst.return_line_dict().keys())
        for line in lines:
            if line in self.explorer.row_translated_attribution:
                self.explorer.row_translated_attribution.pop(line)

    def tree_line_selected(self, linenames):
        """
        method is run on selecting a multibeam line in the KlusterProjectTree

        Parameters
        ----------
        linenames: list, line names

        """
        self.two_d.reset_line_colors()
        self.explorer.clear_explorer_data()
        for linename in linenames:
            self._line_selected(linename)
        self.two_d.change_line_colors(linenames, 'red')

    def tree_fqpr_selected(self, converted_pth):
        """
        method is run on selecting a Fqpr object in the KlusterProjectTree

        Parameters
        ----------
        converted_pth: str, path to converted Fqpr object

        """

        self.two_d.reset_line_colors()
        self.explorer.clear_explorer_data()
        linenames = self.project.return_project_lines(proj=os.path.normpath(converted_pth))
        self.attribute.display_file_attribution(self.project.fqpr_attrs[converted_pth])
        for cnt, ln in enumerate(linenames):
            self._line_selected(ln, idx=cnt)
        self.two_d.change_line_colors(linenames, 'red')

    def tree_surf_selected(self, converted_pth):
        """
        On selecting a surface in the project tree, display the surface attribution in the attribute window

        Parameters
        ----------
        converted_pth: str, surface path, used as key in project structure

        """

        self.attribute.display_file_attribution(self.project.surface_instances[converted_pth].return_attribution())

    def tree_surface_layer_selected(self, surfpath, layername, checked):
        """
        Click on a surface layer in the project tree will get you here.  Surface layers will show if the checkbox
        next to them is checked.  Otherwise we hide it from view.

        Parameters
        ----------
        surfpath: str, path to the surface, used as key in the project
        layername: str, layer name (depth, density, etc)
        checked: bool, True if checked
        """

        if checked:
            self.redraw(add_surface=surfpath, surface_layer_name=layername)
        else:
            self.redraw(remove_surface=surfpath, surface_layer_name=layername)

    def set_color_ranges(self, set_ranges: bool):
        """
        Run when user right clicks Surfaces label and sets color ranges

        Parameters
        ----------
        set_ranges: bool, if True user selected set color ranges
        """

        dlog = dialog_setcolors.ColorRanges()

        if 'depth' in self.two_d.force_band_minmax:
            dlog.mindepth.setText(str(self.two_d.force_band_minmax['depth'][0]))
            dlog.maxdepth.setText(str(self.two_d.force_band_minmax['depth'][1]))
            dlog.depth_box.setChecked(True)
        elif 'depth' in self.two_d.band_minmax:
            dlog.mindepth.setText(str(self.two_d.band_minmax['depth'][0]))
            dlog.maxdepth.setText(str(self.two_d.band_minmax['depth'][1]))
            dlog.depth_box.setChecked(False)
        else:
            dlog.mindepth.setText(str(0.0))
            dlog.maxdepth.setText(str(0.0))
            dlog.depth_box.setChecked(False)

        if 'density' in self.two_d.force_band_minmax:
            dlog.mindensity.setText(str(int(self.two_d.force_band_minmax['density'][0])))
            dlog.maxdensity.setText(str(int(self.two_d.force_band_minmax['density'][1])))
            dlog.density_box.setChecked(True)
        elif 'density' in self.two_d.band_minmax:
            dlog.mindensity.setText(str(int(self.two_d.band_minmax['density'][0])))
            dlog.maxdensity.setText(str(int(self.two_d.band_minmax['density'][1])))
            dlog.density_box.setChecked(False)
        else:
            dlog.mindensity.setText(str(0))
            dlog.maxdensity.setText(str(0))
            dlog.density_box.setChecked(False)

        if 'vertical_uncertainty' in self.two_d.force_band_minmax:
            dlog.minvunc.setText(str(self.two_d.force_band_minmax['vertical_uncertainty'][0]))
            dlog.maxvunc.setText(str(self.two_d.force_band_minmax['vertical_uncertainty'][1]))
            dlog.vunc_box.setChecked(True)
        elif 'vertical_uncertainty' in self.two_d.band_minmax:
            dlog.minvunc.setText(str(self.two_d.band_minmax['vertical_uncertainty'][0]))
            dlog.maxvunc.setText(str(self.two_d.band_minmax['vertical_uncertainty'][1]))
            dlog.vunc_box.setChecked(False)
        else:
            dlog.minvunc.setText(str(0.0))
            dlog.maxvunc.setText(str(0.0))
            dlog.vunc_box.setChecked(False)

        if 'horizontal_uncertainty' in self.two_d.force_band_minmax:
            dlog.minhunc.setText(str(self.two_d.force_band_minmax['horizontal_uncertainty'][0]))
            dlog.maxhunc.setText(str(self.two_d.force_band_minmax['horizontal_uncertainty'][1]))
            dlog.hunc_box.setChecked(True)
        elif 'horizontal_uncertainty' in self.two_d.band_minmax:
            dlog.minhunc.setText(str(self.two_d.band_minmax['horizontal_uncertainty'][0]))
            dlog.maxhunc.setText(str(self.two_d.band_minmax['horizontal_uncertainty'][1]))
            dlog.hunc_box.setChecked(False)
        else:
            dlog.minhunc.setText(str(0.0))
            dlog.maxhunc.setText(str(0.0))
            dlog.hunc_box.setChecked(False)

        if dlog.exec_():
            if not dlog.cancelled:
                if dlog.depth_box.isChecked():
                    self.two_d.force_band_minmax['depth'] = [float(dlog.mindepth.text()), float(dlog.maxdepth.text())]
                else:
                    if 'depth' in self.two_d.force_band_minmax:
                        self.two_d.force_band_minmax.pop('depth')
                if dlog.density_box.isChecked():
                    self.two_d.force_band_minmax['density'] = [int(dlog.mindensity.text()), int(dlog.mindensity.text())]
                else:
                    if 'density' in self.two_d.force_band_minmax:
                        self.two_d.force_band_minmax.pop('density')
                if dlog.vunc_box.isChecked():
                    self.two_d.force_band_minmax['vertical_uncertainty'] = [float(dlog.minvunc.text()), float(dlog.maxvunc.text())]
                else:
                    if 'vertical_uncertainty' in self.two_d.force_band_minmax:
                        self.two_d.force_band_minmax.pop('vertical_uncertainty')
                if dlog.hunc_box.isChecked():
                    self.two_d.force_band_minmax['horizontal_uncertainty'] = [float(dlog.minhunc.text()), float(dlog.maxhunc.text())]
                else:
                    if 'horizontal_uncertainty' in self.two_d.force_band_minmax:
                        self.two_d.force_band_minmax.pop('horizontal_uncertainty')
                self.two_d.update_layer_minmax('depth')
                self.two_d.update_layer_minmax('density')
                self.two_d.update_layer_minmax('vertical_uncertainty')
                self.two_d.update_layer_minmax('horizontal_uncertainty')
                self.two_d.canvas.redrawAllLayers()

    def tree_all_lines_selected(self, is_selected):
        """
        method is run on selecting a the top level 'Converted' heading in KlusterProjectTree

        Parameters
        ----------
        is_selected: bool, if True, 'Converted' was selected

        """

        self.two_d.reset_line_colors()
        self.explorer.clear_explorer_data()
        if is_selected:
            all_lines = self.project.return_sorted_line_list()
            for cnt, ln in enumerate(all_lines):
                self._line_selected(ln, idx=cnt)
            self.two_d.change_line_colors(all_lines, 'red')

    def select_lines_by_name(self, linenames: list):
        """
        method run on using the 2dview box select tool, selects all lines that intersect the drawn box using the
        QGIS intersect ability

        Parameters
        ----------
        linenames
            list of line names that are found to intersect the drawn box
        """

        skip_these = []
        for cnt, ln in enumerate(linenames):
            valid = self._line_selected(ln, idx=cnt)
            if not valid:
                skip_these.append(ln)
        linenames = [ln for ln in linenames if ln not in skip_these]
        self.project_tree.select_multibeam_lines(linenames, clear_existing_selection=True)
        if not linenames:
            self.two_d.reset_line_colors()
            self.explorer.clear_explorer_data()
            self.two_d.change_line_colors(linenames, 'red')

    def select_line_by_box(self, min_lat, max_lat, min_lon, max_lon):
        """
        Deprecated, select tool now uses select_lines_by_name

        method run on using the 2dview box select tool.  Selects all lines that are within the box boundaries

        Parameters
        ----------
        min_lat: float, minimum latitude of the box
        max_lat: float, maximum latitude of the box
        min_lon: float, minimum longitude of the box
        max_lon: float, minimum longitude of the box

        """

        self.two_d.reset_line_colors()
        self.explorer.clear_explorer_data()
        lines = self.project.return_lines_in_box(min_lat, max_lat, min_lon, max_lon)
        for cnt, ln in enumerate(lines):
            self._line_selected(ln, idx=cnt)
        self.two_d.change_line_colors(lines, 'red')

    def select_points_in_box(self, polygon: np.ndarray, azimuth: float):
        """
        method run on using the 2dview points select tool.  Gathers all points in the box and shows in 3d.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon,  (latitude, longitude) in degrees
        azimuth
            azimuth of the selection polygon in radians
        """

        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            cancelled = False
            self.load_points_thread.populate(polygon, azimuth, self.project)
            self.load_points_thread.start()
        if cancelled:
            print('select_points_in_box: Processing was cancelled')

    def _kluster_load_points_results(self):
        """
        After running the load_points_thread to get the soundings in the polygon for every fqpr instance in the project,
        we load the points into the Points View here.
        """

        pointcount = 0
        if not self.load_points_thread.error:
            self._manpatchtest = None
            points_data = self.load_points_thread.points_data
            azimuth = self.load_points_thread.azimuth
            for fqpr_name, pointdata in points_data.items():
                self.points_view.add_points(pointdata[0], pointdata[1], pointdata[2], pointdata[3], pointdata[4], pointdata[5],
                                            pointdata[6], pointdata[7], fqpr_name, pointdata[8], azimuth=azimuth)
                pointcount += pointdata[0].size
            self.points_view.display_points()
        else:
            print('Error loading points from project')
            print(self.load_points_thread.exceptiontxt)
        self.two_d.finalize_points_tool()
        print('Selected {} Points for display'.format(pointcount))
        # we retain the polygon/azimuth in case you are using the patch test tool
        self.load_points_thread.populate(polygon=self.load_points_thread.polygon, azimuth=self.load_points_thread.azimuth)
        self._stop_action_progress()

    def clear_points(self, clrsig: bool):
        """
        Trigger clearing all currently loaded data in the points view widget
        """

        self.points_view.clear()

    def show_points_in_explorer(self, point_index: np.array, linenames: np.array, point_times: np.array, beam: np.array,
                                x: np.array, y: np.array, z: np.array, tvu: np.array, status: np.array, id: np.array):
        """
        Take in the selected points from the 3d view and send the point attributes to the explorer widget for a
        spreadsheet like display of the data.

        Parameters
        ----------
        point_index
            point index for the points, corresponds to the index of the point in the 3dview selected points
        linenames
            multibeam file name that the points come from
        point_times
            time of the soundings/points
        beam
            beam number of the points
        x
            easting of the points
        y
            northing of the points
        z
            depth of the points
        tvu
            total vertical uncertainty of the points
        status
            rejected/amplitude/phase return qualifier of the points
        id
            data container that the points come from
        """

        self.explorer.populate_explorer_with_points(point_index, linenames, point_times, beam, x, y, z, tvu, status, id)

    def set_pointsview_points_status(self, new_status: Union[np.array, int, str, float] = 2):
        """
        Take selected points in pointsview and set them to this new status (see detectioninfo).  Saved to memory and disk

        Parameters
        ----------
        new_status
            new integer flag for detection info status, 2 = Rejected
        """

        if not self.points_view.patch_test_running:
            selected_points = self.points_view.return_select_index()
            if isinstance(new_status, np.ndarray):
                new_status = self.points_view.split_by_selected(new_status)
            for fqpr_name in selected_points:
                fqpr = self.project.fqpr_instances[fqpr_name]
                sel_points_idx = selected_points[fqpr_name]
                if isinstance(new_status, dict):
                    fqpr.set_variable_by_filter('detectioninfo', new_status[fqpr_name], sel_points_idx)
                else:
                    fqpr.set_variable_by_filter('detectioninfo', new_status, sel_points_idx)
                fqpr.write_attribute_to_ping_records({'_soundings_last_cleaned': datetime.utcnow().strftime('%c')})
                self.project.refresh_fqpr_attribution(fqpr_name, relative_path=True)
            self.points_view.clear_selection()
        else:
            print('Cleaning disabled while patch test is running')

    def dock_this_widget(self, title, objname, widget):
        """
        All the kluster widgets go into dock widgets so we can undock and move them around.  This will wrap the
        widget in a new dock widget and return that dock widget

        Parameters
        ----------
        title: str, title shown in widget on the screen
        objname: str, internal object name for widget
        widget: QWidget, the widget we want in the dock widget

        Returns
        -------
        QDockWidget, the dock widget created that contains the provided widget

        """

        dock = QtWidgets.QDockWidget(title, self)
        # currently the Points View will crash if you try to undock it, I believe due to the Vispy app
        if title == 'Points View':
            dock.setFeatures(QtWidgets.QDockWidget.NoDockWidgetFeatures)
            dock.setTitleBarWidget(QtWidgets.QWidget(widget))
        else:
            dock.setFeatures(QtWidgets.QDockWidget.DockWidgetMovable | QtWidgets.QDockWidget.DockWidgetFloatable)
        dock.setObjectName(objname)
        self.widget_obj_names.append(objname)
        dock.setWidget(widget)

        # id like a maximize/minimize button on the undocked widget, haven't gotten this working just yet
        # dock.topLevelChanged.connect(self.dockwidget_setup_undocked_flags)
        return dock

    def setup_widgets(self):
        """
        Build out the initial positioning of the widgets.  read_settings will override some of this if the user has
        settings saved.

        """

        # hide the central widget so that we can have an application with all dockable widgets
        hiddenwidg = QtWidgets.QTextEdit()
        hiddenwidg.hide()
        self.setCentralWidget(hiddenwidg)

        self.addDockWidget(QtCore.Qt.TopDockWidgetArea, self.tree_dock)
        self.addDockWidget(QtCore.Qt.TopDockWidgetArea, self.two_d_dock)
        self.addDockWidget(QtCore.Qt.TopDockWidgetArea, self.points_dock)
        self.addDockWidget(QtCore.Qt.TopDockWidgetArea, self.actions_dock)
        self.addDockWidget(QtCore.Qt.TopDockWidgetArea, self.monitor_dock)
        self.splitDockWidget(self.tree_dock, self.two_d_dock, QtCore.Qt.Horizontal)
        self.splitDockWidget(self.two_d_dock, self.actions_dock, QtCore.Qt.Horizontal)
        self.tabifyDockWidget(self.actions_dock, self.monitor_dock)
        self.tabifyDockWidget(self.actions_dock, self.points_dock)

        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.explorer_dock)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.output_window_dock)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.attribute_dock)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.console_dock)
        self.splitDockWidget(self.explorer_dock, self.attribute_dock, QtCore.Qt.Horizontal)
        self.tabifyDockWidget(self.explorer_dock, self.console_dock)
        self.tabifyDockWidget(self.explorer_dock, self.output_window_dock)

        window_width = self.width()
        horiz_docks = [self.tree_dock, self.two_d_dock, self.actions_dock]
        self.resizeDocks(horiz_docks, [int(window_width * .2), int(window_width * .7), int(window_width * .2)],
                         QtCore.Qt.Horizontal)

        # cant seem to get this to work, size percentage remains at 50% regardless, horizontal resizing works though
        #
        # window_height = self.height()
        # vert_docks = [self.tree_dock, self.two_d_dock, self.actions_dock, self.explorer_dock, self.attribute_dock]
        # docksizes = [window_height * .7, window_height * .7, window_height * .7, window_height * .3, window_height * .3]
        # self.resizeDocks(vert_docks, docksizes, QtCore.Qt.Vertical)

        # have these on top of the tab list
        self.two_d_dock.raise_()
        self.actions_dock.raise_()

    def dockwidget_setup_undocked_flags(self, isfloating):
        """
        Currently not working

        I'd like this to set min/max buttons when the widget is undocked.  Needs more work.

        Parameters
        ----------
        isfloating: bool, if the window is undocked this is True

        """
        widget = self.sender()
        if isfloating:
            widget.setWindowFlags(self.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
        else:
            widget.setWindowFlags(self.windowFlags() & ~QtCore.Qt.WindowStaysOnTopHint)
        widget.show()

    def _action_vessel_view(self):
        """
        Connect menu action 'Vessel Offsets' with vessel view dialog
        """
        self.kluster_vessel_offsets()

    def _action_basicplots(self):
        """
        Connect menu action 'Basic Plots' with basicplots dialog
        """
        self.kluster_basic_plots()

    def _action_advancedplots(self):
        """
        Connect menu action 'Advanced Plots' with basicplots dialog
        """
        self.kluster_advanced_plots()

    def _action_import_ppnav(self):
        """
        Connect menu action 'Import Processed Navigation' with ppnav dialog
        """
        self.kluster_import_ppnav()

    def _action_overwrite_nav(self):
        """
        Connect menu action 'Overwrite Navigation' with overwrite nav dialog
        """
        self.kluster_overwrite_nav()

    def _action_surface_generation(self):
        """
        Connect menu action 'New Surface' with surface dialog
        """
        self.kluster_surface_generation()

    def _action_patch_test(self):
        """
        Connect menu action 'Patch Test' with patch test dialog
        """
        self.kluster_auto_patch_test()

    def _action_filemenu_add_files(self):
        """
        Connect menu action 'Add Files' with file dialog and update_on_file_added
        """
        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster', Title='Add any data file (multibeam, sbet, svp, etc.)',
                                                         AppName='klusterproj', bMulti=True, bSave=False, fFilter='all files (*.*)')
        if msg:
            self.update_on_file_added(fil)

    def _action_filemenu_add_converted(self):
        """
        Connect menu action 'Open Converted' with folder dialog and update_on_file_added
        """
        msg, folder = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='kluster', Title='Add any Kluster generated converted data folder',
                                                       AppName='klusterproj')
        if msg:
            self.update_on_file_added(folder)

    def _action_filemenu_add_surface(self):
        """
        Connect menu action 'Open Surface' with folder dialog and update_on_file_added
        """
        msg, folder = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='kluster', Title='Add any Kluster generated surface data folder',
                                                       AppName='klusterproj')
        if msg:
            self.update_on_file_added(folder)

    def _action_new_project(self):
        """
        Connect menu action 'Open Project' with file dialog and open_project
        """
        msg, folder = RegistryHelpers.GetDirFromUserQT(self, RegistryKey='kluster', Title='Select folder to create a new project from',
                                                       AppName='klusterproj')
        if msg:
            self.new_project(folder)

    def _action_open_project(self):
        """
        Connect menu action 'Open Project' with file dialog and open_project
        """
        msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster', Title='Open Project File',
                                                         AppName='klusterproj', bMulti=False, bSave=False,
                                                         fFilter='kluster project file (*.json)')
        if msg:
            self.open_project(fil)

    def _action_save_project(self):
        """
        Connect menu action 'Save Project' with file dialog and save_project
        """
        self.project.save_project()

    def _action_new_vessel_file(self):
        if self.project.path is not None:
            default_vessel_file = os.path.join(os.path.dirname(self.project.path), 'vessel_file.kfc')
            msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster', Title='New Vessel File',
                                                             AppName='klusterproj', bMulti=False, bSave=True,
                                                             DefaultFile=default_vessel_file,
                                                             fFilter='kluster vessel file (*.kfc)')
            if msg:
                self.project.add_vessel_file(fil)
                self.refresh_project()
        else:
            print('Build a new project or open an existing project before creating a vessel file')

    def _action_open_vessel_file(self):
        if self.project.path is not None:
            msg, fil = RegistryHelpers.GetFilenameFromUserQT(self, RegistryKey='kluster', Title='Open Vessel File',
                                                             AppName='klusterproj', bMulti=False, bSave=False,
                                                             fFilter='kluster vessel file (*.kfc)')
            if msg:
                self.project.add_vessel_file(fil)
                self.refresh_project()
            self.regenerate_offsets_actions(True)
        else:
            print('Build a new project or open an existing project before opening a vessel file')

    def _action_export(self):
        """
        Connect menu action 'Export Soundings' with kluster_export
        """
        self.kluster_export()

    def _action_export_grid(self):
        """
        Connect menu action 'Export Surface' with kluster_export_grid
        """
        self.kluster_export_grid()

    def _action_show_about(self):
        """
        Show the about screen when selecting 'Help - About'
        """
        dlog = dialog_about.AboutDialog()
        if dlog.exec_():
            pass

    def _action_show_docs(self):
        """
        Show the offline docs that come with Kluster in a browser window
        """
        doc_html = os.path.join(os.path.dirname(kluster_init_file), 'docbuild', 'index.html')
        if os.path.exists(doc_html):
            webbrowser.open_new(doc_html)
        else:
            print('Unable to find documentation at {}'.format(doc_html))

    def _action_show_odocs(self):
        """
        Show the online docs for kluster
        """
        doc_path = 'https://kluster.readthedocs.io/en/latest/'
        webbrowser.open_new(doc_path)

    def read_settings(self):
        """
        Read the settings saved in the registry
        """
        # from currentuser\software\noaa\kluster in registry
        settings = self.settings_object
        self.monitor.read_settings(settings)
        if settings.value("Kluster/geometry"):
            self.restoreGeometry(settings.value("Kluster/geometry"))
        if settings.value("Kluster/windowState"):
            self.restoreState(settings.value("Kluster/windowState"), version=0)

    def reset_settings(self):
        """
        Restore the default settings
        """
        # setUpdatesEnabled should be the freeze/thaw wx equivalent i think, but does not appear to do anything here
        # self.setUpdatesEnabled(False)
        # settings = self.settings_object
        # settings.clear()
        # set all docked widgets to 'docked' so that they reset properly
        for widg in self.findChildren(QtWidgets.QDockWidget):
            widg.setFloating(False)
        self.setup_widgets()
        # self.setUpdatesEnabled(True)
        print('Reset interface settings to default')

    def return_selected_fqprs(self, subset_by_line: bool = False):
        """
        Return absolute paths to fqprs selected and the loaded fqpr instances.  In most instances, this is pretty simple,
        you

        Parameters
        ----------
        subset_by_line
            if True, will subset each Fqpr object to just the data associated with the lines selected

        Returns
        -------
        list
            absolute path to the fqprs selected in the GUI
        list
            list of loaded fqpr instances
        """
        fqprs, linedict = self.project_tree.return_selected_fqprs()
        if subset_by_line and linedict:
            fqpr_paths, fqpr_loaded = self.project.get_fqprs_by_paths(fqprs, linedict)
        else:
            fqpr_paths, fqpr_loaded = self.project.get_fqprs_by_paths(fqprs)
        return fqpr_paths, fqpr_loaded

    def return_selected_surfaces(self):
        """
        Return absolute paths to the surface instance folders selected

        Returns
        -------
        list
            absolute path to the surfaces selected in the GUI
        """

        surfs = self.project_tree.return_selected_surfaces()
        surfs = [self.project.absolute_path_from_relative(f) for f in surfs]
        return surfs

    def closeEvent(self, event):
        """
        override the close event for the mainwindow, attach saving settings
        """

        settings = self.settings_object
        self.monitor.save_settings(settings)
        for settname, opts in settings_translator.items():
            settings.setValue(settname, self.settings[opts['newname']])

        self.close_project()
        settings.setValue('Kluster/geometry', self.saveGeometry())
        settings.setValue('Kluster/windowState', self.saveState(version=0))
        self.points_view.save_settings()

        if qgis_enabled:
            self.app.exitQgis()

        super(KlusterMain, self).closeEvent(event)


def main():
    ispyinstaller = False
    if sys.argv[0][-4:] == '.exe':
        ispyinstaller = True
        setattr(sys, 'frozen', True)
    # add support in windows for when you build this as a frozen executable (pyinstaller)
    multiprocessing.freeze_support()
    kluster_dir = os.path.dirname(kluster_init_file)
    kluster_icon = os.path.join(kluster_dir, 'images', 'kluster_img.ico')

    if qgis_enabled:
        app_library = 'pyqt5'
        app = qgis_core.QgsApplication([], True)
        if ispyinstaller:
            kluster_main_exe = sys.argv[0]
            curdir = os.path.dirname(kluster_main_exe)
            plugin_dir = os.path.join(curdir, 'qgis_plugins')
            prefix_dir = curdir
        else:
            plugin_dir = os.path.join(os.path.dirname(found_path), 'plugins')
            prefix_dir = os.path.join(found_path, 'qgis')

        app.setPrefixPath(prefix_dir, True)
        app.setPluginPath(plugin_dir)

        app.initQgis()
        # print(app.showSettings())
    else:
        try:  # pyside2
            app_library = 'pyside2'
            app = QtWidgets.QApplication()
        except TypeError:  # pyqt5
            app_library = 'pyqt5'
            app = QtWidgets.QApplication([])
    try:
        app.setStyle(KlusterProxyStyle())
    except:
        print('Unable to set custom Kluster style')
    try:
        app.setWindowIcon(QtGui.QIcon(kluster_icon))
    except:
        print('Unable to set icon to {}'.format(kluster_icon))
    window = KlusterMain(app, app_library=app_library)
    window.show()
    exitcode = app.exec_()
    sys.exit(exitcode)


if __name__ == '__main__':
    main()
