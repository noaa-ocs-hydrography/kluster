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
import logging
import subprocess

from HSTB.kluster.gui import dialog_vesselview, kluster_explorer, kluster_project_tree, kluster_3dview_v2, \
    kluster_output_window, kluster_2dview, kluster_actions, kluster_monitor, dialog_daskclient, dialog_surface, \
    dialog_export, kluster_worker, kluster_interactive_console, dialog_basicplot, dialog_advancedplot, dialog_project_settings, \
    dialog_export_grid, dialog_layer_settings, dialog_settings, dialog_importppnav, dialog_overwritenav, dialog_surface_data, \
    dialog_about, dialog_patchtest, dialog_manualpatchtest, dialog_managedata, dialog_managesurface, \
    dialog_reprocess, dialog_fileanalyzer, dialog_export_tracklines, dialog_filter, dialog_surfacefrompoints, dialog_mosaic
from HSTB.kluster.fqpr_project import FqprProject
from HSTB.kluster.fqpr_intelligence import FqprIntel
from HSTB.kluster.fqpr_vessel import convert_from_fqpr_xyzrph, convert_from_vessel_xyzrph, compare_dict_data
from HSTB.kluster.dask_helpers import dask_close_localcluster
from HSTB.kluster.gdal_helpers import ogr_output_file_exists, gdal_output_file_exists, get_raster_attribution, get_vector_attribution
from HSTB.kluster.logging_conf import return_logger, add_file_handler, logfile_matches, logger_remove_file_handlers
from HSTB.kluster import __version__ as kluster_version
from HSTB.kluster import __file__ as kluster_init_file
from HSTB.shared import RegistryHelpers, path_to_supplementals
from HSTB.kluster import kluster_variables
from bathygrid.grid_variables import allowable_grid_root_names

# list of icons
# https://joekuan.wordpress.com/2015/09/23/list-of-qt-icons/


settings_translator = {'Kluster/debug': {'newname': 'debug', 'defaultvalue': False},
                       'Kluster/dark_mode': {'newname': 'dark_mode', 'defaultvalue': False},
                       'Kluster/proj_settings_epsgradio': {'newname': 'use_epsg', 'defaultvalue': False},
                       'Kluster/proj_settings_epsgval': {'newname': 'epsg', 'defaultvalue': ''},
                       'Kluster/proj_settings_utmradio': {'newname': 'use_coord', 'defaultvalue': True},
                       'Kluster/proj_settings_utmval': {'newname': 'coord_system', 'defaultvalue': kluster_variables.default_coordinate_system},
                       'Kluster/proj_settings_vertref': {'newname': 'vert_ref', 'defaultvalue': kluster_variables.default_vertical_reference},
                       'Kluster/proj_settings_svmode': {'newname': 'cast_selection_method', 'defaultvalue': kluster_variables.default_cast_selection_method},
                       'Kluster/proj_settings_designated_surf_path': {'newname': 'designated_surface', 'defaultvalue': ''},
                       'Kluster/layer_settings_background': {'newname': 'layer_background', 'defaultvalue': 'Default'},
                       'Kluster/layer_settings_transparency': {'newname': 'layer_transparency', 'defaultvalue': '0'},
                       'Kluster/layer_settings_surfacetransparency': {'newname': 'surface_transparency', 'defaultvalue': 0},
                       'Kluster/settings_keep_waterline_changes': {'newname': 'keep_waterline_changes', 'defaultvalue': True},
                       'Kluster/settings_draw_navigation': {'newname': 'draw_navigation', 'defaultvalue': 'raw'},
                       'Kluster/settings_enable_parallel_writes': {'newname': 'write_parallel', 'defaultvalue': True},
                       'Kluster/settings_vdatum_directory': {'newname': 'vdatum_directory', 'defaultvalue': ''},
                       'Kluster/settings_filter_directory': {'newname': 'filter_directory', 'defaultvalue': ''},
                       'Kluster/settings_main_log_file': {'newname': 'main_log_file', 'defaultvalue': ''},
                       'Kluster/settings_auto_processing_mode': {'newname': 'autoprocessing_mode', 'defaultvalue': 'normal'},
                       'Kluster/settings_force_coordinate_match': {'newname': 'force_coordinate_match', 'defaultvalue': False},
                       }

config_text = ''


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

        # initialize the output window first, to get stdout/stderr configured
        self.output_window = kluster_output_window.KlusterOutput(self)
        # and then initialize the logger
        self.logger = return_logger('kluster_main')

        self.resize(self.start_horiz_size, self.start_vert_size)

        self.setWindowTitle('Kluster {}'.format(kluster_version))
        self.setDockNestingEnabled(True)

        self.widget_obj_names = []

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.project = FqprProject(logger=self.logger)
        self.intel = FqprIntel(self.project, self, logger=self.logger)
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

        self.output_window_dock = self.dock_this_widget('Output', 'output_window_dock', self.output_window)

        self.attribute = kluster_explorer.KlusterAttribution(self)
        self.attribute_dock = self.dock_this_widget("Attribute", 'attribute_dock', self.attribute)

        self.actions = kluster_actions.KlusterActions(self)
        self.actions_dock = self.dock_this_widget('Actions', 'actions_dock', self.actions)
        self.actions.update_actions(process_mode=self.intel.autoprocessing_mode)

        self._monitor = kluster_monitor.KlusterMonitor(self)
        self.monitor_dock = self.dock_this_widget('Monitor', 'monitor_dock', self._monitor)
        self.monitor = self._monitor.widget

        self.console = kluster_interactive_console.KlusterConsole(self)
        self.console_dock = self.dock_this_widget('Console', 'console_dock', self.console)

        self.vessel_win = None
        self.basicplots_win = None
        self.advancedplots_win = None
        self.managedata_win = None
        self.managedata_surf = None
        self._manpatchtest = None
        self._fileanalyzer = None

        self.iconpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images', 'kluster_img.ico')
        self.setWindowIcon(QtGui.QIcon(self.iconpath))

        self.generic_progressbar = QtWidgets.QProgressBar(self)
        self.generic_progressbar.setMaximum(1)
        self.generic_progressbar.setMinimum(0)
        self.statusBar().addPermanentWidget(self.generic_progressbar, stretch=1)

        self.action_thread = kluster_worker.ActionWorker(self)
        self.import_ppnav_thread = kluster_worker.ImportNavigationWorker(self)
        self.overwrite_nav_thread = kluster_worker.OverwriteNavigationWorker(self)
        self.surface_thread = kluster_worker.SurfaceWorker(self)
        self.mosaic_thread = kluster_worker.MosaicWorker(self)
        self.surface_update_thread = kluster_worker.SurfaceUpdateWorker(self)
        self.export_thread = kluster_worker.ExportWorker(self)
        self.export_tracklines_thread = kluster_worker.ExportTracklinesWorker(self)
        self.export_grid_thread = kluster_worker.ExportGridWorker(self)
        self.filter_thread = kluster_worker.FilterWorker(self)
        self.open_project_thread = kluster_worker.OpenProjectWorker(self)
        self.draw_navigation_thread = kluster_worker.DrawNavigationWorker(self)
        self.draw_surface_thread = kluster_worker.DrawSurfaceWorker(self)
        self.load_points_thread = kluster_worker.LoadPointsWorker(self)
        self.patch_test_load_thread = kluster_worker.PatchTestUpdateWorker(self)
        self.allthreads = [self.action_thread, self.import_ppnav_thread, self.overwrite_nav_thread, self.surface_thread,
                           self.surface_update_thread, self.export_thread, self.export_grid_thread, self.open_project_thread,
                           self.draw_navigation_thread, self.draw_surface_thread, self.load_points_thread, self.patch_test_load_thread,
                           self.export_tracklines_thread, self.filter_thread]

        # connect FqprActionContainer with actions pane, called whenever actions changes
        self.intel.bind_to_action_update(self.actions.update_actions)

        # self.project_tree.file_added.connect(self.update_on_file_added)
        self.project_tree.lines_selected.connect(self.tree_line_selected)
        self.project_tree.fqpr_selected.connect(self.tree_fqpr_selected)
        self.project_tree.surface_selected.connect(self.tree_surf_selected)
        self.project_tree.raster_selected.connect(self.tree_raster_selected)
        self.project_tree.vector_selected.connect(self.tree_vector_selected)
        self.project_tree.surface_layer_selected.connect(self.tree_surface_layer_selected)
        self.project_tree.raster_layer_selected.connect(self.tree_raster_layer_selected)
        self.project_tree.vector_layer_selected.connect(self.tree_vector_layer_selected)
        self.project_tree.mesh_layer_selected.connect(self.tree_mesh_layer_selected)
        self.project_tree.all_lines_selected.connect(self.tree_all_lines_selected)
        self.project_tree.close_fqpr.connect(self.close_fqpr)
        self.project_tree.close_surface.connect(self.close_surface)
        self.project_tree.close_raster.connect(self.close_raster)
        self.project_tree.close_vector.connect(self.close_vector)
        self.project_tree.close_mesh.connect(self.close_mesh)
        self.project_tree.manage_fqpr.connect(self.manage_fqpr)
        self.project_tree.manage_surface.connect(self.manage_surface)
        self.project_tree.load_console_fqpr.connect(self.load_console_fqpr)
        self.project_tree.load_console_surface.connect(self.load_console_surface)
        self.project_tree.show_explorer.connect(self.show_in_explorer)
        self.project_tree.show_log.connect(self.show_process_log)
        self.project_tree.show_properties.connect(self.show_layer_properties)
        self.project_tree.zoom_extents_fqpr.connect(self.zoom_extents_fqpr)
        self.project_tree.zoom_extents_surface.connect(self.zoom_extents_surface)
        self.project_tree.zoom_extents_raster.connect(self.zoom_extents_raster)
        self.project_tree.zoom_extents_vector.connect(self.zoom_extents_vector)
        self.project_tree.zoom_extents_mesh.connect(self.zoom_extents_mesh)
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

        self.action_thread.tstarted.connect(self._start_action_progress)
        self.action_thread.tfinished.connect(self._kluster_execute_action_results)
        self.overwrite_nav_thread.tstarted.connect(self._start_action_progress)
        self.overwrite_nav_thread.tfinished.connect(self._kluster_overwrite_nav_results)
        self.import_ppnav_thread.tstarted.connect(self._start_action_progress)
        self.import_ppnav_thread.tfinished.connect(self._kluster_import_ppnav_results)
        self.surface_thread.tstarted.connect(self._start_action_progress)
        self.surface_thread.tfinished.connect(self._kluster_surface_generation_results)
        self.mosaic_thread.tstarted.connect(self._start_action_progress)
        self.mosaic_thread.tfinished.connect(self._kluster_mosaic_generation_results)
        self.surface_update_thread.tstarted.connect(self._start_action_progress)
        self.surface_update_thread.tfinished.connect(self._kluster_surface_update_results)
        self.export_thread.tstarted.connect(self._start_action_progress)
        self.export_thread.tfinished.connect(self._kluster_export_results)
        self.export_tracklines_thread.tstarted.connect(self._start_action_progress)
        self.export_tracklines_thread.tfinished.connect(self._kluster_export_tracklines_results)
        self.export_grid_thread.tstarted.connect(self._start_action_progress)
        self.export_grid_thread.tfinished.connect(self._kluster_export_grid_results)
        self.filter_thread.tstarted.connect(self._start_action_progress)
        self.filter_thread.tfinished.connect(self._kluster_filter_results)
        self.open_project_thread.tstarted.connect(self._start_action_progress)
        self.open_project_thread.tfinished.connect(self._kluster_open_project_results)
        self.draw_navigation_thread.tstarted.connect(self._start_action_progress)
        self.draw_navigation_thread.tfinished.connect(self._kluster_draw_navigation_results)
        self.draw_surface_thread.tstarted.connect(self._start_action_progress)
        self.draw_surface_thread.tfinished.connect(self._kluster_draw_surface_results)
        self.load_points_thread.tstarted.connect(self._start_action_progress)
        self.load_points_thread.tfinished.connect(self._kluster_load_points_results)
        self.patch_test_load_thread.tstarted.connect(self._start_action_progress)
        self.patch_test_load_thread.tfinished.connect(self._kluster_update_manual_patch_test)

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
        if self.settings.get('debug'):
            self.set_debug(self.settings['debug'])
        else:
            self.set_debug(False)
        self.debug_print(config_text)

    @property
    def settings_object(self):
        kluster_dir = os.path.dirname(kluster_init_file)
        kluster_ini = os.path.join(kluster_dir, 'misc', 'kluster.ini')
        return QtCore.QSettings(kluster_ini, QtCore.QSettings.IniFormat)

    @property
    def debug(self):
        if self.settings.get('debug'):
            return self.settings['debug']
        else:
            return False

    def print(self, msg: str, loglevel: int = logging.INFO):
        # all gui objects are going to use this method in printing
        if self.logger is not None:
            self.logger.log(loglevel, msg)
        else:
            print(msg)

    def debug_print(self, msg: str, loglevel: int = logging.INFO):
        # all gui objects are going to use this method in debug printing
        if self.debug:
            if self.logger is not None:
                self.logger.log(loglevel, msg)
            else:
                print(msg)

    def _configure_logfile(self):
        newlogfile = self.settings.get('main_log_file')
        if newlogfile:
            if not logfile_matches(self.logger, newlogfile):
                add_file_handler(self.logger, newlogfile)
                self.logger.info('******************************************************************************')
                self.logger.info('Logfile initialized: {}'.format(newlogfile))
        else:  # a blank newlogfile was found, so we remove all file handlers
            logger_remove_file_handlers(self.logger)

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
        self.project.set_settings(self.settings.copy())
        self.intel.set_settings(self.settings.copy())
        self._configure_logfile()

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
        export_action = QtWidgets.QAction('Soundings', self)
        export_action.triggered.connect(self._action_export)
        export_tracklines_action = QtWidgets.QAction('Tracklines', self)
        export_tracklines_action.triggered.connect(self._action_export_tracklines)
        export_grid_action = QtWidgets.QAction('Surface', self)
        export_grid_action.triggered.connect(self._action_export_grid)
        import_action = QtWidgets.QAction('Soundings', self)
        import_action.triggered.connect(self._action_surfacefrompoints_generation)

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

        qgis_action = QtWidgets.QAction('Start QGIS', self)
        qgis_action.triggered.connect(self._action_qgis)
        file_analyzer = QtWidgets.QAction('File Analyzer', self)
        file_analyzer.triggered.connect(self._action_file_analyzer)

        importppnav_action = QtWidgets.QAction('Import Processed Navigation', self)
        importppnav_action.triggered.connect(self._action_import_ppnav)
        overwritenav_action = QtWidgets.QAction('Overwrite Raw Navigation', self)
        overwritenav_action.triggered.connect(self._action_overwrite_nav)
        surface_action = QtWidgets.QAction('New Surface', self)
        surface_action.triggered.connect(self._action_surface_generation)
        mosaic_action = QtWidgets.QAction('New Mosaic', self)
        mosaic_action.triggered.connect(self._action_mosaic_generation)
        # patch_action = QtWidgets.QAction('Patch Test', self)
        # patch_action.triggered.connect(self._action_patch_test)
        filter_action = QtWidgets.QAction('Filter', self)
        filter_action.triggered.connect(self._action_filter)

        basicplots_action = QtWidgets.QAction('Basic Plots', self)
        basicplots_action.triggered.connect(self._action_basicplots)
        advancedplots_action = QtWidgets.QAction('Advanced Plots', self)
        advancedplots_action.triggered.connect(self._action_advancedplots)

        set_debug = QtWidgets.QAction('Debug', self)
        set_debug.setCheckable(True)
        set_debug.triggered.connect(self.set_debug)
        about_action = QtWidgets.QAction('About', self)
        about_action.triggered.connect(self._action_show_about)
        docs_action = QtWidgets.QAction('Offline Documentation', self)
        docs_action.triggered.connect(self._action_show_docs)
        odocs_action = QtWidgets.QAction('Online Documentation', self)
        odocs_action.triggered.connect(self._action_show_odocs)
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
        exportmenu = file.addMenu('Export')
        exportmenu.addAction(export_action)
        exportmenu.addAction(export_tracklines_action)
        exportmenu.addAction(export_grid_action)
        importmenu = file.addMenu('Import')
        importmenu.addAction(import_action)

        view = menubar.addMenu('View')
        view.addAction(view_darkstyle)
        view.addAction(view_layers)
        view.addAction(view_dashboard_action)
        view.addAction(view_reset_action)

        setup = menubar.addMenu('Setup')
        setup.addAction(set_project_settings)
        setup.addAction(vessel_view_action)
        setup.addAction(setup_client_action)

        tools = menubar.addMenu('Tools')
        tools.addAction(qgis_action)
        tools.addAction(file_analyzer)

        process = menubar.addMenu('Process')
        process.addAction(overwritenav_action)
        process.addAction(importppnav_action)
        process.addAction(surface_action)
        process.addAction(mosaic_action)
        process.addAction(filter_action)
        # process.addAction(patch_action)

        visual = menubar.addMenu('Visualize')
        visual.addAction(basicplots_action)
        visual.addAction(advancedplots_action)

        klusterhelp = menubar.addMenu('Help')
        klusterhelp.addAction(set_debug)
        klusterhelp.addAction(about_action)
        klusterhelp.addAction(docs_action)
        klusterhelp.addAction(odocs_action)
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
                self.debug_print("project file detected, we can't handle loading a new project and adding data at the same time, if a project is added, halt", logging.WARNING)
                return

        potential_surface_paths = []
        potential_fqpr_paths = []
        potential_raster_paths = []
        potential_vector_paths = []
        potential_mesh_paths = []

        for f in fil:
            f = os.path.normpath(f)
            try:
                updated_type, new_data, new_project = self.intel.add_file(f)
            except Exception as e:
                self.print('Unable to load from file {}, {}'.format(f, e), logging.ERROR)
                updated_type, new_data, new_project = None, True, None

            if new_project:
                self.debug_print("user added a data file when there was no project, so we loaded or created a new one", logging.INFO)
                new_fqprs.extend([fqpr for fqpr in self.project.fqpr_instances.keys() if fqpr not in new_fqprs])
            if new_data is None:
                fextension = os.path.splitext(f)[1]
                if any([os.path.exists(os.path.join(f, gname)) for gname in allowable_grid_root_names]):
                    self.debug_print("Got surfaces that match allowed grid root names: {}".format(allowable_grid_root_names), logging.INFO)
                    potential_surface_paths.append(f)
                elif os.path.isdir(f):
                    potential_fqpr_paths.append(f)
                else:
                    if gdal_output_file_exists(f):
                        potential_raster_paths.append(f)
                    elif ogr_output_file_exists(f):
                        potential_vector_paths.append(f)
                    elif fextension in kluster_variables.supported_mesh:
                        potential_mesh_paths.append(f)
        self.refresh_project(new_fqprs, new_raster=potential_raster_paths, new_vector=potential_vector_paths, new_mesh=potential_mesh_paths)
        self.open_project_thread.populate(force_add_fqprs=potential_fqpr_paths, force_add_surfaces=potential_surface_paths)
        self.open_project_thread.start()

    def refresh_project(self, fqpr=None, new_raster=None, new_vector=None, new_mesh=None):
        self.redraw(new_fqprs=fqpr, add_raster=new_raster, add_vector=new_vector, add_mesh=new_mesh)

    def redraw_all_lines(self):
        for linelyr in self.project.buffered_fqpr_navigation:
            self.two_d.remove_line(linelyr)
        self.project.buffered_fqpr_navigation = {}
        fqprs = [fqpr for fqpr in self.project.fqpr_instances.keys()]
        self.refresh_project(fqprs)

    def _redraw_remove_surface(self, remove_surface, surface_layer_name):
        if remove_surface is not None:
            surf_object = self.project.surface_instances[remove_surface]
            if surface_layer_name == 'tiles':
                self.debug_print("Hiding {} tiles layer".format(remove_surface), logging.INFO)
                self.two_d.hide_line(remove_surface)
            else:
                for resolution in surf_object.resolutions:
                    if surface_layer_name:
                        self.debug_print("Hiding {} {} {} layer".format(remove_surface, surface_layer_name, resolution), logging.INFO)
                        self.two_d.hide_surface(remove_surface, surface_layer_name, resolution)
                    else:
                        self.debug_print("Removing all {} {} layers".format(remove_surface, resolution), logging.INFO)
                        self.two_d.remove_surface(remove_surface, resolution)

    def _redraw_add_surface(self, add_surface, surface_layer_name):
        if add_surface is not None and surface_layer_name:
            if self.surface_update_thread.isRunning():
                self.print('Surface is currently updating, please wait until after that process is complete.', logging.WARNING)
                return
            surf_object = self.project.surface_instances[add_surface]
            needs_drawing = []
            if surface_layer_name == 'tiles':
                self.debug_print("Trying to show {} {} layer".format(add_surface, surface_layer_name), logging.INFO)
                if self.settings['dark_mode']:
                    shown = self.two_d.show_line(add_surface, color='white')
                else:
                    shown = self.two_d.show_line(add_surface, color='black')
                if not shown:
                    self.debug_print("show didnt work, must need to add the surface instead, loading from disk...", logging.INFO)
                    needs_drawing.append(None)
            else:
                for resolution in surf_object.resolutions:
                    self.debug_print("Trying to show {} {} {} layer".format(add_surface, surface_layer_name, resolution), logging.INFO)
                    shown = self.two_d.show_surface(add_surface, surface_layer_name, resolution)
                    if not shown:
                        self.debug_print("show didnt work, must need to add the surface instead, loading from disk...", logging.INFO)
                        needs_drawing.append(resolution)
            if needs_drawing:
                self.print('Drawing {} - {}, resolution {}'.format(add_surface, surface_layer_name, needs_drawing), logging.INFO)
                self.draw_surface_thread.populate(add_surface, surf_object, needs_drawing, surface_layer_name)
                self.draw_surface_thread.start()

    def _redraw_add_raster(self, add_raster, layer_name):
        if add_raster is not None and layer_name:
            self.debug_print("Trying to show {} {} layer".format(add_raster, layer_name), logging.INFO)
            shown = self.two_d.show_raster(add_raster, layer_name)
            if not shown:
                self.debug_print("show didnt work, must need to add the raster instead, loading from disk...", logging.INFO)
                self.two_d.add_raster(add_raster, layer_name)
            self.two_d.set_extents_from_rasters(add_raster, layer_name)

    def _redraw_remove_raster(self, remove_raster, layer_name):
        if remove_raster is not None:
            if layer_name:
                self.debug_print("Hiding {} {} layer".format(remove_raster, layer_name), logging.INFO)
                self.two_d.hide_raster(remove_raster, layer_name)
            else:
                self.debug_print("Removing all {} layers".format(remove_raster), logging.INFO)
                self.two_d.remove_raster(remove_raster, layer_name)

    def _redraw_add_vector(self, add_vector, layer_name):
        if add_vector is not None and layer_name:
            self.debug_print("Trying to show {} {} layer".format(add_vector, layer_name), logging.INFO)
            shown = self.two_d.show_vector(add_vector, layer_name)
            if not shown:
                self.debug_print("show didnt work, must need to add the vector instead, loading from disk...", logging.INFO)
                self.two_d.add_vector(add_vector, layer_name)
            self.two_d.set_extents_from_vectors(add_vector, layer_name)

    def _redraw_remove_vector(self, remove_vector, layer_name):
        if remove_vector is not None:
            if layer_name:
                self.debug_print("Hiding {} {} layer".format(remove_vector, layer_name), logging.INFO)
                self.two_d.hide_vector(remove_vector, layer_name)
            else:
                self.debug_print("Removing all {} layers".format(remove_vector), logging.INFO)
                self.two_d.remove_vector(remove_vector, layer_name)

    def _redraw_add_mesh(self, add_mesh, layer_name):
        if add_mesh is not None and layer_name:
            self.debug_print("Trying to show {} {} layer".format(add_mesh, layer_name), logging.INFO)
            shown = self.two_d.show_mesh(add_mesh, layer_name)
            if not shown:
                self.debug_print("show didnt work, must need to add the mesh instead, loading from disk...", logging.INFO)
                self.two_d.add_mesh(add_mesh, layer_name)
            self.two_d.set_extents_from_meshes(add_mesh, layer_name)

    def _redraw_remove_mesh(self, remove_mesh, layer_name):
        if remove_mesh is not None:
            if layer_name:
                self.debug_print("Hiding {} {} layer".format(remove_mesh, layer_name), logging.INFO)
                self.two_d.hide_mesh(remove_mesh, layer_name)
            else:
                self.debug_print("Removing all {} layers".format(remove_mesh), logging.INFO)
                self.two_d.remove_mesh(remove_mesh, layer_name)

    def redraw(self, new_fqprs=None, add_surface=None, remove_surface=None, surface_layer_name='',
               add_raster=None, remove_raster=None, add_vector=None, remove_vector=None, add_mesh=None, remove_mesh=None):
        """
        After adding new projects or surfaces, refresh the widgets to display the new data

        Parameters
        ----------
        new_fqprs: list, list of str file paths to converted fqpr instances
        add_surface: optional, str, path to new surface to add
        remove_surface: optional, str, path to existing surface to hide
        surface_layer_name: optional, str, name of the layer of the surface to add or hide
        add_raster: optional, list of raster paths to add
        remove_raster: optional, str, path to existing raster to hide
        add_vector: optional, list of vector paths to add
        remove_vector: optional, str, path to existing vector to hide
        add_mesh: optional, list of mesh paths to add
        remove_mesh: optional, str, path to existing mesh to hide
        """

        self.project_tree.refresh_project(proj=self.project, add_raster=add_raster, add_vector=add_vector, add_mesh=add_mesh)
        self._redraw_add_surface(add_surface, surface_layer_name)
        self._redraw_remove_surface(remove_surface, surface_layer_name)
        self._redraw_add_raster(add_raster, surface_layer_name)
        self._redraw_remove_raster(remove_raster, surface_layer_name)
        self._redraw_add_vector(add_vector, surface_layer_name)
        self._redraw_remove_vector(remove_vector, surface_layer_name)
        self._redraw_add_mesh(add_mesh, surface_layer_name)
        self._redraw_remove_mesh(remove_mesh, surface_layer_name)

        if new_fqprs is not None and new_fqprs:
            self.draw_navigation_thread.populate(self.project, new_fqprs)
            self.draw_navigation_thread.start()

    def manage_fqpr(self, pth):
        fq = self.project.fqpr_instances[pth]
        self.managedata_win = None
        self.managedata_win = dialog_managedata.ManageDataDialog(parent=self)
        self.managedata_win.refresh_fqpr.connect(self._refresh_manage_fqpr)
        self.managedata_win.populate(fq)
        self.managedata_win.set_on_top()
        self.managedata_win.show()

    def manage_surface(self, pth):
        surf = self.project.surface_instances[pth]
        self.managedata_surf = None
        self.managedata_surf = dialog_managesurface.ManageSurfaceDialog(parent=self)
        self.managedata_surf.populate(surf)
        self.managedata_surf.set_on_top()
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
        if not os.path.exists(pth):
            absolute_path = self.project.absolute_path_from_relative(pth)
        else:
            # this must be an auxiliary file, like a geotiff or a s57.  Show the containing folder
            absolute_path = os.path.dirname(pth)
        self.print(f'Opening {absolute_path}', logging.INFO)
        os.startfile(absolute_path)

    def show_process_log(self, pth: str):
        """
        Show the process log associated with the given

        Parameters
        ----------
        pth
            path to the grid folder
        """
        if not os.path.exists(pth):
            absolute_path = self.project.absolute_path_from_relative(pth)
        else:
            # this must be an auxiliary file, like a geotiff or a s57.  Show the containing folder
            absolute_path = os.path.dirname(pth)
        logpath = os.path.join(absolute_path, 'logfile.txt')
        self.print(f'Opening log file {logpath}', logging.INFO)
        os.startfile(logpath)

    def show_layer_properties(self, layertype: str, pth: str):
        """
        Right click in the project tree and select properties to run this code block.  Will open the layer properties
        for editing qgis display attributes.

        Parameters
        ----------
        layertype:
            the layer category for the file
        pth
            path to the layer source file
        """

        translator_layer_type = {'Converted': 'line', 'Surfaces': 'surface', 'Raster': 'raster', 'Vector': 'vector', 'Mesh': 'mesh'}
        if layertype not in translator_layer_type:
            self.print(f'show_layer_properties: got unrecognized layer category: {layertype}', logging.ERROR)
            return
        self.two_d.show_properties(translator_layer_type[layertype], pth)

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

    def zoom_extents_raster(self, pth: str):
        """
        Right click on raster and zoom to the extents of that layer

        Parameters
        ----------
        pth
            path to the converted data/surface
        """
        self.two_d.set_extents_from_rasters(pth)

    def zoom_extents_vector(self, pth: str):
        """
        Right click on vector and zoom to the extents of that layer

        Parameters
        ----------
        pth
            path to the converted data/surface
        """
        self.two_d.set_extents_from_vectors(pth)

    def zoom_extents_mesh(self, pth: str):
        """
        Right click on mesh and zoom to the extents of that layer

        Parameters
        ----------
        pth
            path to the converted data/surface
        """
        self.two_d.set_extents_from_meshes(pth)

    def set_auto_processing(self, enable: bool):
        if enable != self.actions.auto_checkbox.isChecked():
            self.actions.auto_checkbox.setChecked(enable)
            self.actions.auto_process()

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
                self.print('Unable to find {}'.format(fil), logging.ERROR)

    def visualize_orientation(self, pth):
        self.project.build_visualizations(pth, 'orientation')

    def visualize_beam_vectors(self, pth):
        self.project.build_visualizations(pth, 'beam_vectors')

    def visualize_corrected_beam_vectors(self, pth):
        self.project.build_visualizations(pth, 'corrected_beam_vectors')

    def update_surface(self, pth: str, new_surface, only_resolutions: list = None):
        """
        Update the attached bathygrid instance with the provided one, refresh the project tree and display

        Parameters
        ----------
        pth
            relative path to the bathygrid instance
        new_surface
            new bathygrid instance
        only_resolutions
            list of resolutions to close, default is all the resolutions in the grid
        """

        if not only_resolutions:
            only_resolutions = new_surface.resolutions
        for resolution in only_resolutions:
            self.two_d.remove_surface(pth, resolution)
        self.two_d.remove_line(pth)  # also remove the tiles layer if that was loaded
        self.project.update_surface(pth, new_surface, relative_path=True)
        self.project_tree.refresh_project(self.project)

    def close_surface(self, pth: str, only_resolutions: list = None):
        """
        With the given path to the surface instance, remove the loaded data associated with the surface and remove it from
        the gui widgets / project.

        Parameters
        ----------
        pth
            path to the bathygrid top level folder
        only_resolutions
            list of resolutions to close, default is all the resolutions in the grid
        """

        surf_object = self.project.surface_instances[pth]
        if not only_resolutions:
            only_resolutions = surf_object.resolutions
        for resolution in only_resolutions:
            self.two_d.remove_surface(pth, resolution)
        self.two_d.remove_line(pth)  # also remove the tiles layer if that was loaded
        self.project.remove_surface(pth, relative_path=True)
        self.project_tree.refresh_project(self.project)

    def close_raster(self, pth: str):
        self.two_d.remove_raster(pth)
        self.project_tree.refresh_project(self.project, remove_raster=pth)

    def close_vector(self, pth: str):
        self.two_d.remove_vector(pth)
        self.project_tree.refresh_project(self.project, remove_vector=pth)

    def close_mesh(self, pth: str):
        self.two_d.remove_mesh(pth)
        self.project_tree.refresh_project(self.project, remove_mesh=pth)

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
        self.vessel_win = dialog_vesselview.VesselWidget(parent=self)
        self.vessel_win.vessel_file_modified.connect(self.regenerate_offsets_actions)
        self.vessel_win.converted_xyzrph_modified.connect(self.update_offsets_vesselwidget)

        if vessel_file:
            self.debug_print("Load offsets/angles from designated vessel file {}".format(vessel_file), logging.INFO)
            self.vessel_win.load_from_config_file(vessel_file)
        elif fqprs:
            fqpr = self.project.fqpr_instances[self.project.path_relative_to_project(fqprs[0])]
            self.debug_print("Load offsets/angles from selected container {}".format(fqprs[0]), logging.INFO)
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
            self.debug_print("Regenerating processing actions on new vessel file {}".format(self.project.vessel_file), logging.INFO)
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
            self.debug_print("Searching for multibeam containers that match serial number {}".format(sysident), logging.INFO)
            matching_fq = list(source[0].values())[0]
            for fqname, fq in self.project.fqpr_instances.items():
                if fqname == matching_fq:
                    self.print('Updating xyzrph record for {}'.format(fqname), logging.INFO)
                    identical_offsets, identical_angles, identical_tpu, data_matches, new_waterline = compare_dict_data(fq.multibeam.xyzrph,
                                                                                                                        xyzrph[cnt])
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

        fqprs, _ = self.project_tree.return_selected_fqprs()
        if fqprs:
            # start over at 0, which is conversion in our state machine
            fq = self.project.fqpr_instances[fqprs[0]]
            current_status = fq.multibeam.raw_ping[0].current_processing_status
            if current_status == 0:
                self.print('reprocess_fqpr: Unable to reprocess converted data, current process is already at the beginning (conversion)', logging.ERROR)
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
                        self.print('reprocess_fqpr: new status is None, unable to set status', logging.ERROR)
                else:
                    self.print('reprocess_fqpr: cancelled', logging.INFO)

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
        self.basicplots_win = dialog_basicplot.BasicPlotDialog(self)

        if fqprs:
            self.basicplots_win.data_widget.new_fqpr_path(fqprspaths[0], fqprs[0])
            self.basicplots_win.data_widget.initialize_controls()
        self.basicplots_win.setWindowFlags(self.basicplots_win.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
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
        self.advancedplots_win = dialog_advancedplot.AdvancedPlotDialog(self)

        if fqprspaths:
            self.advancedplots_win.data_widget.new_fqpr_path(fqprspaths[0], fqprs[0])
            self.advancedplots_win.data_widget.initialize_controls()
        if first_surf:
            self.advancedplots_win.surf_text.setText(first_surf)
            self.advancedplots_win.out_text.setText(default_plots)
        self.advancedplots_win.setWindowFlags(self.advancedplots_win.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
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
            cancelled = True
        if not cancelled:
            self.output_window.clear()
            self.action_thread.populate(action_container, action_index)
            self.action_thread.start()

    def _kluster_execute_action_results(self):
        """
        Read the results of the executed action.  Multibeam actions can generate new converted data that would need
        to be shown in the project window.
        """

        # fqpr is now the output path of the Fqpr instance
        if not self.action_thread.error:
            if self.action_thread.action_type != 'gridding':
                fqpr = self.action_thread.result
                if fqpr is not None:
                    fqpr_entry, already_in = self.project.add_fqpr(fqpr)
                    self.project.save_project()
                    self.intel.update_intel_for_action_results(action_type=self.action_thread.action_type)

                    if already_in and self.action_thread.action_type != 'multibeam':
                        self.refresh_project()
                        self.refresh_explorer(self.project.fqpr_instances[fqpr_entry])
                    else:  # new fqpr, or conversion actions always need a full refresh
                        self.refresh_project(fqpr=[fqpr_entry])
                else:
                    self.print('Error running action {}'.format(self.action_thread.action_type), logging.ERROR)
            else:
                if self.action_thread.result:
                    fq_surf, oldrez, newrez = self.action_thread.result
                    if fq_surf is not None:
                        relpath_surf = self.project.path_relative_to_project(os.path.normpath(fq_surf.output_folder))
                        self.update_surface(relpath_surf, fq_surf, only_resolutions=oldrez)
                else:
                    self.print('Error running action {}'.format(self.action_thread.action_type), logging.ERROR)
        else:
            self.print('Error running action: {}'.format(self.action_thread.action_type), logging.ERROR)
            self.print(self.action_thread.exceptiontxt, logging.INFO)
            self.print('kluster_action: no data returned from action execution', logging.INFO)
            self.intel.update_intel_for_action_results(action_type=self.action_thread.action_type)
            self.set_auto_processing(False)  # turn off auto processing if an action fails
        self._stop_action_progress()
        self.action_thread.show_error()
        self.action_thread.populate(None, None)

    def kluster_overwrite_nav(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the overwrite navigation dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new attribution

        """
        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            fqprs, _ = self.return_selected_fqprs()
            dlog = dialog_overwritenav.OverwriteNavigationDialog(parent=self)
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
                            self.print('Unable to find {} in currently loaded project'.format(relfq), logging.ERROR)
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
            self.print('kluster_import_navigation: Processing was cancelled', logging.INFO)

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
            self.print('Error overwriting raw navigation', logging.ERROR)
            self.print(self.overwrite_nav_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.overwrite_nav_thread.show_error()
        self.overwrite_nav_thread.populate(None)

    def kluster_import_ppnav(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the import navigation dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new attribution

        """
        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
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
                            self.print('Unable to find {} in currently loaded project'.format(relfq), logging.ERROR)
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
            self.print('kluster_import_navigation: Processing was cancelled', logging.INFO)

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
            self.print('Error importing post processed navigation', logging.ERROR)
            self.print(self.import_ppnav_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.import_ppnav_thread.show_error()
        self.import_ppnav_thread.populate(None)

    def manual_patch_test(self, e):
        """
        Triggered by patch test button in Points View.  Will retrieve the relevant installation parameter records for
        the lines in the Points View subset, and display them (PrePatchDialog) for the user to then select the record they want to use in
        the patch test tool (ManualPatchTestWidget).  After that selection, we run the Patch Test tool.
        """
        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
        else:
            systems, linenames, time_segments = self.points_view.return_lines_and_times()
            self.debug_print("Using data from points view by system and time\nSystems:{}\nTime Segments{}".format(systems, time_segments), logging.INFO)
            if systems:
                datablock = self.project.retrieve_data_for_time_segments(systems, time_segments)
                if datablock:
                    dlog_patch = dialog_manualpatchtest.PrePatchDialog(parent=self)
                    dlog_patch.add_data(datablock)
                    if dlog_patch.exec_():
                        if dlog_patch.canceled:
                            self.print('Patch Test: test canceled', logging.INFO)
                        else:
                            final_datablock = dlog_patch.return_final_data()
                            if final_datablock:
                                vessel_file_name = datablock[0][-1]
                                roll, pitch, heading = final_datablock[8], final_datablock[9], final_datablock[10]
                                xlever, ylever, zlever = final_datablock[11], final_datablock[12], final_datablock[13]
                                latency, prefixes = final_datablock[14], final_datablock[15]
                                fqprs, timesegments = final_datablock[0], final_datablock[6]
                                for cnt, fq in enumerate(fqprs):
                                    fq.subset_by_times(timesegments[cnt].tolist())
                                self._manpatchtest = None
                                self._manpatchtest = dialog_manualpatchtest.ManualPatchTestWidget(prefixes=prefixes)
                                self._manpatchtest.new_offsets_angles.connect(self._update_manual_patch_test)
                                self._manpatchtest.patchdatablock = final_datablock
                                self._manpatchtest.populate(vessel_file_name, ','.join(final_datablock[1]), final_datablock[2],
                                                            final_datablock[3], ','.join(final_datablock[4]), ','.join(final_datablock[5]),
                                                            roll, pitch, heading, xlever, ylever, zlever, latency)
                                self._manpatchtest.setWindowFlags(self._manpatchtest.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
                                self._manpatchtest.show()
                            else:
                                self.print('Patch Test: no data selected', logging.ERROR)
                else:
                    self.print('Patch Test: no data selected for running in Patch Test utility', logging.ERROR)
            else:
                self.print('Patch Test: no data found in Points View', logging.ERROR)

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
            try:
                vdatum_directory = self.settings['vdatum_directory']
                if not vdatum_directory:
                    vdatum_directory = None
            except:
                self.print('Unable to find vdatum_directory attribute for patch test processing', logging.WARNING)
                vdatum_directory = None

            self.patch_test_load_thread.populate(fqprs, [roll, pitch, heading, xlever, ylever, zlever, latency],
                                                 headindex, prefixes, tstamps, serial_num, self.load_points_thread.polygon,
                                                 vdatum_directory)
            self.patch_test_load_thread.start()
        else:
            self.print('Unable to load the data for updating the manual patch test', logging.ERROR)

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
                self.debug_print("Loading points from {} : {}".format(newid, linenames), logging.INFO)
                self.points_view.remove_points(system_id=newid + '_' + str(headindex))
                self.points_view.add_points(head, x, y, z, tvu, rejected, pointtime, beam, newid, linenames, cur_azimuth)
            self.points_view.three_d_window.hide_lines = hl
            self.points_view.display_points()
            self.points_view.load_view_settings()
            self.points_view.patch_test_running = True
        else:
            self.print('Error reprocessing patch test subset', logging.ERROR)
            self.print(self.patch_test_load_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.patch_test_load_thread.show_error()
        self.patch_test_load_thread.populate(None)

    def kluster_auto_patch_test(self):
        """
        IN PROGRESS - run the automated patch test tool on the selected lines.  Still have not addressed the issues
        in the auto patch test procedure, so the results of this should not be used.
        """
        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            cancelled = False
            self._patch = dialog_patchtest.PatchTestDialog(parent=self)
            self._patch.patch_query.connect(self._feed_auto_patch_test_dialog)
            if self._patch.exec_():
                cancelled = self._patch.canceled
                pairs = self._patch.return_pairs
                if pairs:
                    self.project.run_auto_patch_test(pairs)
        if cancelled:
            self.print('kluster_auto_patch_test: Processing was cancelled', logging.INFO)
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
            self.print('ERROR: Lost handle on patch test dialog', logging.ERROR)
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

    def _kluster_filter_dialog(self, filter_list, filter_module, fqprs, filter_descrip):
        dlog = dialog_filter.FilterDialog(filter_list, filter_descrip, parent=self)
        dlog.update_fqpr_instances(addtl_files=fqprs)
        cancelled = False
        basic_filter_mode = False
        line_filter_mode = False
        points_filter_mode = False
        linenames = None
        pointtime = None
        pointbeam = None
        filter_controls = None
        filter_name = ''
        savetodisk = True

        if dlog.exec_():
            if not dlog.canceled:
                basic_filter_mode = dlog.basic_filter_group.isChecked()
                line_filter_mode = dlog.line_filter.isChecked()
                points_filter_mode = dlog.points_view_filter.isChecked()
                if line_filter_mode:  # we only operate on the selected lines
                    linenames = self.project_tree.return_selected_lines()
                else:
                    linenames = []
                if points_filter_mode:  # we need to get the time/beam of all points in Points View to subset the Fqpr
                    savetodisk = dlog.save_to_disk_checkbox.isChecked()  # can skip saving to disk for Poinst View filter
                    pointtime, pointbeam = self.points_view.return_array('pointtime'), self.points_view.return_array('beam')
                else:
                    savetodisk = True
                    pointtime, pointbeam = None, None

                filter_name = dlog.filter_opts.currentText()
                if filter_name and not dlog.canceled:
                    # these controls are specified in the custom filter (see controls attribute of the Filter class)
                    filter_controls = filter_module.return_optional_filter_controls(filter_name)
                else:
                    cancelled = True
            else:
                cancelled = True
        else:
            cancelled = True
        return basic_filter_mode, line_filter_mode, points_filter_mode, savetodisk, linenames, pointtime, pointbeam, filter_name, filter_controls, cancelled

    def _kluster_additional_filter_dialog(self, filter_controls, filter_name):
        cancelled = False
        if filter_controls:
            add_dlog = dialog_filter.AdditionalFilterOptionsDialog(title=filter_name, controls=filter_controls, parent=self)
            if add_dlog.exec_():
                if not add_dlog.canceled:
                    kwargs = add_dlog.return_kwargs()
                else:
                    kwargs = None
                    cancelled = True
            else:
                kwargs = None
                cancelled = True
        else:
            kwargs = None
        return kwargs, cancelled

    def kluster_filter(self):
        """
        Trigger filter on all the fqprs provided.  Can be selected fqpr, line, or points view selection.
        """

        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            fqprs, _ = self.return_selected_fqprs()
            # list of filters that we can pull from any fqpr, just use the first one loaded in the project
            try:
                filter_module = list(self.project.fqpr_instances.values())[0].filter
                filter_list = filter_module.list_filters()
                filter_descrip = filter_module.list_descriptions()
            except:
                self.print('Error: kluster_filter no loaded converted data found to filter, unable to initialize filter list.', logging.ERROR)
                return

            result = self._kluster_filter_dialog(filter_list, filter_module, fqprs, filter_descrip)
            basic_filter_mode, line_filter_mode, points_filter_mode, savetodisk, linenames, pointtime, pointbeam, filter_name, filter_controls, cancelled = result
            if not cancelled:
                kwargs, cancelled = self._kluster_additional_filter_dialog(filter_controls, filter_name)
                if not cancelled:
                    fq_chunks = []
                    if points_filter_mode:  # still need these even if you are filtering the points view only, we can just grab them.
                        fqprs = [self.project.absolute_path_from_relative(pth) for pth in self.points_view.return_fqpr_paths()]
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            self.print('kluster_filter: Unable to find {} in currently loaded project'.format(relfq), logging.ERROR)
                            return
                        fq_inst = self.project.fqpr_instances[relfq]
                        # use the project client, or start a new LocalCluster if client is None
                        fq_inst.client = self.project.get_dask_client()
                        if points_filter_mode:
                            if relfq not in pointtime or relfq not in pointbeam:
                                self.print(f'kluster_filter: {relfq} is not currently being used in Points View, skipping filter', logging.WARNING)
                                continue
                            fq_chunks.append([fq_inst, pointtime[relfq], pointbeam[relfq], relfq])
                        else:
                            fq_chunks.append([fq_inst, relfq])
                        self.filter_thread.populate(fq_chunks, linenames, filter_name, basic_filter_mode, line_filter_mode,
                                                    points_filter_mode, savetodisk, kwargs)
                        self.filter_thread.start()
                else:
                    self.print('kluster_filter: Filter was cancelled', logging.WARNING)
            else:
                self.print('kluster_filter: Filter was cancelled', logging.WARNING)

    def _kluster_filter_results(self):
        if self.filter_thread.error:
            self.print('Filter complete: Unable to filter', logging.ERROR)
            self.print(self.filter_thread.exceptiontxt, logging.ERROR)
        else:
            self.print('Filter complete.', logging.INFO)
        if self.filter_thread.mode == 'points':
            self.debug_print("Updating points view data with filter results", logging.INFO)
            newinfo = self.filter_thread.new_status
            selindex = self.filter_thread.selected_index
            base_points_view_status = self.points_view.three_d_window.rejected.copy()
            base_points_time = self.points_view.three_d_window.pointtime
            base_points_beam = self.points_view.three_d_window.beam
            for cnt, (fq, subset_time, subset_beam, fqname) in enumerate(self.filter_thread.fq_chunks):
                fqinfo, fqsel = newinfo[cnt], selindex[cnt]
                for fcnt, ninfo in enumerate(fqinfo):
                    sonarid = f'{fqname}_{fcnt}'
                    fqheadsel = fqsel[fcnt].reshape(ninfo.shape)
                    matches_sonar = self.points_view.three_d_window.id == sonarid
                    # align the new sounding status values with the values in points view by querying by system/time/beam
                    pointsview_timebeam = np.column_stack([base_points_time[matches_sonar], base_points_beam[matches_sonar]])
                    results_timebeam = np.column_stack([subset_time, subset_beam])
                    chk = np.intersect1d(pointsview_timebeam.view(dtype=np.complex128), results_timebeam.view(dtype=np.complex128), return_indices=True, assume_unique=True)
                    results_indices = chk[2]
                    # account for possibility of the (time, beam) in the source not being sorted, the intersect1d method
                    #  shown above works if results_timebeam is not sorted, but not if pointsview_timebeam is not sorted
                    time_sort = np.argsort(np.argsort(pointsview_timebeam.view(dtype=np.complex128).ravel()))
                    results_indices = results_indices[time_sort]
                    # now replace with the new values in the correct order
                    base_points_view_status[matches_sonar] = ninfo[fqheadsel][results_indices]
            self.points_view.override_sounding_status(base_points_view_status)
        self._stop_action_progress()
        self.filter_thread.show_error()
        self.filter_thread.populate(None, None, '', True, False, False, True, None)

    def kluster_surface_generation(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the generate surface dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new surface
        """

        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            cancelled = False
            fqprspaths, fqprs = self.return_selected_fqprs()
            dlog = dialog_surface.SurfaceDialog(parent=self)
            dlog.update_fqpr_instances(addtl_files=fqprspaths)
            if dlog.exec_():
                cancelled = dlog.canceled
                opts = dlog.return_processing_options()
                if opts is not None and not cancelled:
                    surface_opts = opts
                    fq_chunks = []
                    fqprs = surface_opts.pop('fqpr_inst')

                    if dlog.line_surface_checkbox.isChecked():  # we now subset the fqpr instances by lines selected
                        fqprspaths, fqprs = self.return_selected_fqprs(subset_by_line=True, concatenate=False)
                        for fq in fqprs:
                            fq_chunks.extend([fq])
                    else:
                        for fq in fqprs:
                            try:
                                relfq = self.project.path_relative_to_project(fq)
                            except:
                                self.print('No project loaded, you must load some data before generating a surface', logging.ERROR)
                                return
                            if relfq not in self.project.fqpr_instances:
                                self.print('Unable to find {} in currently loaded project'.format(relfq), logging.ERROR)
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
            self.print('kluster_surface_generation: Processing was cancelled', logging.INFO)

    def _kluster_surface_generation_results(self):
        """
        Method is run when the surface_thread signals completion.  All we need to do here is add the surface to the project
        and display.
        """

        fq_surf = self.surface_thread.fqpr_surface
        if fq_surf is not None and not self.surface_thread.error:
            relpath_surf = self.project.path_relative_to_project(os.path.normpath(fq_surf.output_folder))
            if relpath_surf in self.project.surface_instances:
                self.close_surface(relpath_surf)
            self.project.add_surface(fq_surf)
            self.project_tree.refresh_project(proj=self.project)
            self.redraw()
        else:
            self.print('Error building surface', logging.ERROR)
            self.print(self.surface_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.surface_thread.show_error()
        self.surface_thread.populate(None, {})

    def kluster_mosaic_generation(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the generate mosaic dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new surface
        """

        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            cancelled = False
            fqprspaths, fqprs = self.return_selected_fqprs()
            dlog = dialog_mosaic.MosaicDialog(parent=self)
            dlog.update_fqpr_instances(addtl_files=fqprspaths)
            if dlog.exec_():
                cancelled = dlog.canceled
                opts = dlog.return_processing_options()
                if opts is not None and not cancelled:
                    surface_opts = opts
                    fq_chunks = []
                    fqprs = surface_opts.pop('fqpr_inst')

                    if dlog.line_surface_checkbox.isChecked():  # we now subset the fqpr instances by lines selected
                        fqprspaths, fqprs = self.return_selected_fqprs(subset_by_line=True, concatenate=False)
                        for fq in fqprs:
                            fq_chunks.extend([fq])
                    else:
                        for fq in fqprs:
                            try:
                                relfq = self.project.path_relative_to_project(fq)
                            except:
                                self.print('No project loaded, you must load some data before generating a surface', logging.ERROR)
                                return
                            if relfq not in self.project.fqpr_instances:
                                self.print('Unable to find {} in currently loaded project'.format(relfq), logging.ERROR)
                                return
                            if relfq in self.project.fqpr_instances:
                                fq_inst = self.project.fqpr_instances[relfq]
                                # use the project client, or start a new LocalCluster if client is None
                                # fq_inst.client = self.project.get_dask_client()
                                fq_chunks.extend([fq_inst])
                    if not dlog.canceled:
                        # if the project has a client, use it here.  If None, BatchRead starts a new LocalCluster
                        self.output_window.clear()
                        self.mosaic_thread.populate(fq_chunks, opts)
                        self.mosaic_thread.start()
        if cancelled:
            self.print('kluster_mosaic_generation: Processing was cancelled', logging.INFO)

    def _kluster_mosaic_generation_results(self):
        """
        Method is run when the mosaic_thread signals completion.  All we need to do here is add the mosaic to the project
        and display.
        """

        pbscatter = self.mosaic_thread.opts['process_backscatter']
        avgon = self.mosaic_thread.opts['angle_varying_gain']
        if pbscatter or avgon:
            # processing backscatter will add the backscatter_settings and avg_table attributes, so we need to refresh attribution
            for fqpr in self.mosaic_thread.fqpr_instances:
                self.project.refresh_fqpr_attribution(fqpr.output_folder, relative_path=False)

        fq_surf = self.mosaic_thread.fqpr_surface
        if not self.mosaic_thread.error:
            if self.mosaic_thread.opts['create_mosaic'] and fq_surf:
                relpath_surf = self.project.path_relative_to_project(os.path.normpath(fq_surf.output_folder))
                if relpath_surf in self.project.surface_instances:
                    self.close_surface(relpath_surf)
                self.project.add_surface(fq_surf)
                self.project_tree.refresh_project(proj=self.project)
                self.redraw()
        else:
            self.print('Error building mosaic', logging.ERROR)
            self.print(self.mosaic_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.mosaic_thread.show_error()
        self.mosaic_thread.populate(None, {})

    def kluster_surfacefrompoints_generation(self):
        """
        Ask for input files to grid, supporting las and csv.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new surface.
        """

        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            cancelled = False
            dlog = dialog_surfacefrompoints.SurfaceFromPointsDialog(parent=self)
            if dlog.exec_():
                cancelled = dlog.canceled
                opts = dlog.return_processing_options()
                if opts is not None and not cancelled:
                    surface_opts = opts
                    infiles = surface_opts.pop('fqpr_inst')
                    self.output_window.clear()
                    self.surface_thread.populate(infiles, opts)
                    self.surface_thread.mode = 'from_points'
                    self.surface_thread.start()
        if cancelled:
            self.print('kluster_surface_generation: Processing was cancelled', logging.INFO)

    def kluster_surface_update(self):
        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            cancelled = False
            surfs = self.return_selected_surfaces()
            if surfs:
                surf = self.project.surface_instances[self.project.path_relative_to_project(surfs[0])]
                surf_version = [int(vnumber) for vnumber in surf.version.split('.')]
                if surf_version[0] < 1 or (surf_version[0] == 1 and surf_version[1] < 3) or (surf_version[0] == 1 and surf_version[1] == 3 and surf_version[2] < 5):
                    self.print('kluster_surface_update: surface update received a rework in bathygrid 1.3.5, grid created prior to that cannot be updated in Kluster.', logging.ERROR)
                    return
                # we need to grab all the resolutions in the grid so that when we close it later, we close the correct layers
                #  the resolutions can change during regridding, so we need the original ones
                all_resolutions = surf.resolutions
                existing_container_names, possible_container_names = self.project.return_surface_containers(surfs[0], relative_path=False)
                dlog = dialog_surface_data.SurfaceDataDialog(parent=self, title=surf.output_folder)
                dlog.setup(existing_container_names, possible_container_names)
                if dlog.exec_():
                    cancelled = dlog.canceled
                    add_container, add_lines, remove_container, remove_lines, opts = dlog.return_processing_options()
                    add_fqpr = []
                    if not cancelled:
                        if add_container:
                            for fqpr_inst in self.project.fqpr_instances.values():
                                fname = os.path.split(fqpr_inst.multibeam.raw_ping[0].output_path)[1]
                                if fname in add_container:
                                    add_fqpr.append(fqpr_inst)
                                    add_container.remove(fname)
                            if add_container:
                                self.print('kluster_surface_update: {} must be loaded in Kluster for it to be added to the surface.'.format(add_container), logging.ERROR)
                                return
                        self.output_window.clear()
                        self.surface_update_thread.populate(surf, add_fqpr, add_lines, remove_container, remove_lines, opts, all_resolutions)
                        self.surface_update_thread.start()
                    else:
                        self.print('kluster_surface_update: Processing was cancelled', logging.INFO)

    def _kluster_surface_update_results(self):
        """
        Method is run when the surface_update_thread signals completion.  All we need to do here is add the surface to the project
        and display.
        """

        fq_surf = self.surface_update_thread.fqpr_surface
        if fq_surf is not None and not self.surface_update_thread.error:
            relpath_surf = self.project.path_relative_to_project(os.path.normpath(fq_surf.output_folder))
            self.update_surface(relpath_surf, fq_surf, only_resolutions=self.surface_update_thread.all_resolutions)
            self.print('Updating surface complete', logging.INFO)
        else:
            self.print('Error updating surface', logging.ERROR)
            self.print(self.surface_update_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.surface_update_thread.show_error()
        self.surface_update_thread.populate(None, None, None, None, None, {}, None)

    def kluster_export_grid(self):
        """
        Trigger export on a surface provided.  Currently only supports export of xyz to csv file(s), geotiff and bag.
        """

        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
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
                    dlog.update_isbackscatter(self.project.surface_instances[relsurf].is_backscatter)
            cancelled = False
            if dlog.exec_():
                if not dlog.canceled:
                    opts = dlog.return_processing_options()
                    surf = dlog.input_pth
                    output_path = opts.pop('output_path')
                    export_format = opts.pop('export_format')
                    z_pos_up = opts.pop('z_positive_up')
                    relsurf = self.project.path_relative_to_project(surf)

                    if relsurf not in self.project.surface_instances:
                        self.print('Unable to find {} in currently loaded project'.format(relsurf), logging.ERROR)
                        return
                    if relsurf in self.project.surface_instances:
                        surf_inst = self.project.surface_instances[relsurf]
                        self.output_window.clear()
                        self.print('Exporting to {}, format {}..'.format(output_path, export_format), logging.INFO)
                        self.export_grid_thread.populate(surf_inst, export_format, output_path, z_pos_up, opts)
                        self.export_grid_thread.start()
                    else:
                        self.print('kluster_grid_export: Unable to load from {}'.format(surf), logging.ERROR)
                else:
                    cancelled = True
        if cancelled:
            self.print('kluster_grid_export: Export was cancelled', logging.INFO)

    def _kluster_export_grid_results(self):
        """
        Method is run when the surface_update_thread signals completion.  All we need to do here is add the surface to the project
        and display.
        """

        if self.export_grid_thread.error:
            self.print('Error exporting grid', logging.ERROR)
            self.print(self.export_grid_thread.exceptiontxt, logging.ERROR)
        else:
            self.print('Export complete.', logging.INFO)
        self._stop_action_progress()
        self.export_grid_thread.show_error()
        self.export_grid_thread.populate(None, '', '', True, {})

    def kluster_export(self):
        """
        Trigger export on all the fqprs provided.  Currently only supports export of xyz to csv file(s), las file(s)
        and entwine point store.
        """

        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
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
                formattype = dlog.format_dropdown.currentText()
                filterset = dlog.filter_chk.isChecked()
                separateset = dlog.byidentifier_chk.isChecked()
                z_pos_down = dlog.zdirect_check.isChecked()
                if not dlog.canceled and export_type in ['csv', 'las', 'entwine']:
                    fq_chunks = []
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            self.print('Unable to find {} in currently loaded project'.format(relfq), logging.ERROR)
                            return
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
                            fq_chunks.append([fq_inst])
                    if fq_chunks:
                        self.output_window.clear()
                        self.export_thread.populate(fq_chunks, linenames, datablock, export_type, z_pos_down, delimiter,
                                                    formattype, filterset, separateset, basic_export_mode,
                                                    line_export_mode, points_export_mode)
                        self.export_thread.start()
                else:
                    cancelled = True
        if cancelled:
            self.print('kluster_export: Export was cancelled', logging.INFO)

    def _kluster_export_results(self):
        """
        Method is run when the export_thread signals completion.  All we need to do here is check for errors
        """

        if self.export_thread.error:
            self.print('Export complete: Unable to export', logging.ERROR)
            self.print(self.export_thread.exceptiontxt, logging.ERROR)
        else:
            self.print('Export complete.', logging.INFO)
        self._stop_action_progress()
        self.export_thread.show_error()
        self.export_thread.populate(None, None, [], '', False, 'comma', 'xyz', False, False, True, False, False)

    def kluster_export_tracklines(self):
        """
        Trigger export on all the fqprs provided.  Currently only supports export of xyz to csv file(s), las file(s)
        and entwine point store.
        """

        if not self.no_threads_running():
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            fqprs, _ = self.return_selected_fqprs()
            dlog = dialog_export_tracklines.ExportTracklinesDialog()
            dlog.update_fqpr_instances(addtl_files=fqprs)
            cancelled = False
            if dlog.exec_():
                basic_export_mode = dlog.basic_export_group.isChecked()
                line_export_mode = dlog.line_export.isChecked()
                if line_export_mode:
                    linenames = self.project_tree.return_selected_lines()
                else:
                    linenames = []

                export_type = dlog.export_format
                output_pth = dlog.output_text.text()
                basic_export_mode = dlog.basic_export_group.isChecked()
                line_export_mode = dlog.line_export.isChecked()
                if not dlog.canceled:
                    fq_chunks = []
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            self.print('Unable to find {} in currently loaded project'.format(relfq), logging.ERROR)
                            return
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
                            fq_chunks.append([fq_inst])
                    if fq_chunks:
                        self.output_window.clear()
                        self.export_tracklines_thread.populate(fq_chunks, linenames, export_type, basic_export_mode,
                                                               line_export_mode, output_pth)
                        self.export_tracklines_thread.start()
                else:
                    cancelled = True
        if cancelled:
            self.print('kluster_export_tracklines: Export was cancelled', logging.INFO)

    def _kluster_export_tracklines_results(self):
        """
        Method is run when the export_tracklines_thread signals completion.  All we need to do here is check for errors
        """

        if self.export_tracklines_thread.error:
            self.print('Export complete: Unable to export', logging.ERROR)
            self.print(self.export_tracklines_thread.exceptiontxt, logging.ERROR)
        else:
            self.print('Export complete.', logging.INFO)
        self._stop_action_progress()
        self.export_tracklines_thread.show_error()
        self.export_tracklines_thread.populate(None, None, '', False, True, '')

    def _start_action_progress(self, start: bool):
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
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            self.close_project()
            self.output_window.clear()
            self.open_project_thread.populate(new_project_path=pth)
            self.open_project_thread.start()
            cancelled = False
        if cancelled:
            self.print('open_project: opening project was cancelled', logging.INFO)

    def _kluster_open_project_results(self):
        """
        After running the open_project_thread, we get here and replace the existing project with the newly opened
        project.  We then draw the new lines to the screen.
        """
        if not self.open_project_thread.error:
            for new_fq in self.open_project_thread.new_fqprs:
                fqpr_entry, already_in = self.project.add_fqpr(new_fq, skip_dask=True)
                if already_in:
                    self.print('{} already exists in project'.format(new_fq.output_folder), logging.WARNING)
            for new_surf in self.open_project_thread.new_surfaces:
                self.project.add_surface(new_surf)
            self.redraw(new_fqprs=[self.project.path_relative_to_project(fq.output_folder) for fq in self.open_project_thread.new_fqprs])
        else:
            self.print('Error on opening data', logging.ERROR)
            self.print(self.open_project_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.open_project_thread.show_error()
        self.open_project_thread.populate(None)

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
            self.print('Error drawing lines from {}'.format(self.draw_navigation_thread.new_fqprs), logging.ERROR)
            self.print(self.draw_navigation_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.draw_navigation_thread.show_error()
        self.draw_navigation_thread.populate(None, None)
        self.print('draw_navigation: Drawing navigation complete.', logging.INFO)

    def _kluster_draw_surface_results(self):
        """
        After clicking on a surface layer, we load the data in this thread and in this method we draw the loaded data
        """
        if not self.draw_surface_thread.error:
            surf_path = self.draw_surface_thread.surface_path
            surf_epsg = self.draw_surface_thread.surf_object.epsg
            if self.draw_surface_thread.surface_layer_name == 'tiles':
                if self.draw_surface_thread.surface_data:
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
            self.print('Error drawing surface {}'.format(self.draw_surface_thread.surface_path), logging.ERROR)
            self.print(self.draw_surface_thread.exceptiontxt, logging.ERROR)
        self._stop_action_progress()
        self.draw_surface_thread.show_error()
        self.draw_surface_thread.populate(None, None, None, None)
        self.print('draw_surface: Drawing surface complete.', logging.INFO)

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

        if not self.project.skip_dask:
            self.project.get_dask_client()
            webbrowser.open_new(self.project.client.dashboard_link)
        else:
            self.print('Unable to open Dask dashboard when in "No Client" mode', logging.ERROR)

    def open_youtube_playlist(self):
        """
        Opens the link to the Kluster 5 minute modules video playlist
        """

        webbrowser.open_new(r'https://www.youtube.com/playlist?list=PLrjCvP_J9AA_memBs2ZyKXGHG1AMx0GWx')

    def start_dask_client(self):
        """
        Set the project up with a new Client object, either LocalCluster or a client to a remote cluster
        """

        dlog = dialog_daskclient.DaskClientStart(parent=self)
        if dlog.exec_():
            client = dlog.cl
            if dlog.canceled:
                self.print('start_dask_client: canceled', logging.INFO)
            elif client is None and not dlog.noclient_box.isChecked():
                self.print('start_dask_client: no client started successfully', logging.ERROR)
            elif dlog.noclient_box.isChecked():
                self.project.client = client
                self.project.skip_dask = True
            else:
                self.project.client = client
                self.project.skip_dask = False

    def set_project_settings(self):
        """
        Triggered on hitting OK in the project settings dialog.  Takes the provided settings and saves it to the project
        and intel instance.
        """

        dlog = dialog_project_settings.ProjectSettingsDialog(parent=self, settings=self.settings_object)
        if dlog.exec_() and not dlog.canceled:
            settings = dlog.return_processing_options()
            new_surface_options = None
            if 'new_surface_options' in settings:
                new_surface_options = settings.pop('new_surface_options')

            # now handle the designated surface setting, create a new one if the user asked for it, and make sure that
            #   we load the surface to the project afterwards, if it isn't already
            if 'designated_surface' in settings and settings["designated_surface"]:
                try:
                    if new_surface_options:
                        bg = kluster_worker.generate_new_surface(None, **new_surface_options)
                    elif self.project.path and self.project.path_relative_to_project(settings['designated_surface']) in self.project.surface_instances:
                        bg = None  # no need to create or reload, it is already in the project
                    else:
                        bg = kluster_worker.reload_surface(settings['designated_surface'])
                    if bg:
                        self.project.add_surface(bg)
                    self.redraw()
                except:
                    self.print(f'set_project_settings: Unable to designate surface {settings["designated_surface"]}', logging.ERROR)
                    settings["designated_surface"] = ''

            self.settings.update(settings)
            settings_obj = self.settings_object
            for settname, opts in settings_translator.items():
                settings_obj.setValue(settname, self.settings[opts['newname']])
            self.project.set_settings(settings)
            self.intel.set_settings(settings)

    def set_layer_settings(self):
        """
        Triggered on hitting OK in the layer settings dialog.  Takes the provided settings and regenerates the 2d display.
        """

        dlog = dialog_layer_settings.LayerSettingsDialog(parent=self, settings=self.settings_object)
        if dlog.exec_() and not dlog.canceled:
            settings = dlog.return_layer_options()
            self.settings.update(settings)
            settings_obj = self.settings_object
            for settname, opts in settings_translator.items():
                settings_obj.setValue(settname, self.settings[opts['newname']])

            self.two_d.vdatum_directory = self.settings['vdatum_directory']
            self.two_d.set_background(self.settings['layer_background'], self.settings['layer_transparency'])
            self.two_d.canvas.redrawAllLayers()

    def set_settings(self):
        """
        Triggered on hitting OK in the settings dialog.  Takes the provided settings and saves it to the project
        and intel instance.
        """

        dlog = dialog_settings.SettingsDialog(parent=self, settings=self.settings_object)
        if dlog.exec_() and not dlog.canceled:
            settings = dlog.return_options()
            redraw = False
            if 'draw_navigation' in self.settings and settings['draw_navigation'] != self.settings['draw_navigation']:
                self.print(f'regenerating all tracklines with draw_navigation={settings["draw_navigation"]}', logging.INFO)
                redraw = True
            self.settings.update(settings)
            settings_obj = self.settings_object
            for settname, opts in settings_translator.items():
                settings_obj.setValue(settname, self.settings[opts['newname']])

            self.project.set_settings(settings)
            self.intel.set_settings(settings)

            # now overwrite the default kluster variables and save them to the ini file as well
            newkvars = dlog.return_kvars()
            for kvarkey, kvarval in newkvars.items():
                kluster_variables.alter_variable(kvarkey, kvarval)
                settings_obj.setValue(f'Kluster/kvariables_{kvarkey}', kvarval)
            if redraw:
                # since we changed the nav source, we need to clear and redraw all the tracklines
                self.redraw_all_lines()
            self._configure_logfile()

    def set_debug(self, check_state: bool):
        """
        Set a new debug status
        """
        self.settings['debug'] = check_state
        settings_obj = self.settings_object
        for settname, opts in settings_translator.items():
            settings_obj.setValue(settname, self.settings[opts['newname']])
        if check_state:
            self.print('Debug messaging enabled', logging.INFO)
        # now update the control if we are doing this manually, not through the checkbox event
        help_menu = [mn for mn in self.menuBar().actions() if mn.text() == 'Help']
        if help_menu:
            help_menu = help_menu[0]
            debugaction = [mn for mn in help_menu.menu().actions() if mn.text() == 'Debug']
            if debugaction:
                debugaction = debugaction[0]
                debugaction.setChecked(check_state)
            else:
                self.print('Warning: Can not find the Debug action to set debug control!', logging.WARNING)
        else:
            self.print('Warning: Can not find the help menu to set debug control!', logging.WARNING)

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
                self.print('Unable to set qdarkstyle style sheet for app library {}'.format(self.app_library), logging.ERROR)
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
                self.print('Warning: Can not find the Dark Mode action to set dark mode control!', logging.WARNING)
        else:
            self.print('Warning: Can not find the view menu to set dark mode control!', logging.WARNING)

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
        attrs = self.project.fqpr_attrs[converted_pth]
        filtered_attrs = {a: attrs[a] for a in attrs.keys() if a not in kluster_variables.hidden_fqpr_attributes}
        self.attribute.display_file_attribution(filtered_attrs)
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

        attrs = self.project.surface_instances[converted_pth].return_attribution()
        filtered_attrs = {a: attrs[a] for a in attrs.keys() if a not in kluster_variables.hidden_grid_attributes}
        combined_source = {}
        remove_keys = []
        for ky, val in filtered_attrs.items():
            if ky[:6] == 'source':
                remove_keys += [ky]
                splitky = ky.split('__')
                if len(splitky) != 2:  # this must be pre bathygrid 1.3.5 where we started combining container name and line name as the key
                    combined_source[ky] = val
                else:  # we combine the containers by just appending the multibeam line to the total lines
                    contname, linename = splitky
                    if contname not in combined_source:
                        combined_source[contname] = val
                    else:
                        for mline in val['multibeam_lines']:
                            if mline not in combined_source[contname]['multibeam_lines']:
                                combined_source[contname]['multibeam_lines'] += [mline]
        for ky in remove_keys:
            filtered_attrs.pop(ky)
        for ky in combined_source.keys():
            combined_source[ky]['multibeam_lines'] = sorted(combined_source[ky]['multibeam_lines'])
        filtered_attrs.update(combined_source)
        self.attribute.display_file_attribution(filtered_attrs)

    def tree_raster_selected(self, rasterpath: str):
        """
        Click on a raster layer and get raster attribution in the Kluster Attribute window.

        Parameters
        ----------
        rasterpath
            path to the raster file
        """

        attrs = get_raster_attribution(rasterpath)
        self.attribute.display_file_attribution(attrs)

    def tree_vector_selected(self, vectorpath: str):
        """
        Click on a vector layer and get vector attribution in the Kluster Attribute window.

        Parameters
        ----------
        vectorpath
            path to the vector file
        """
        i = 0
        attrs = get_vector_attribution(vectorpath)
        self.attribute.display_file_attribution(attrs)

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

    def tree_raster_layer_selected(self, rasterpath, layername, checked):
        if checked:
            self.redraw(add_raster=rasterpath, surface_layer_name=layername)
        else:
            self.redraw(remove_raster=rasterpath, surface_layer_name=layername)

    def tree_vector_layer_selected(self, vectorpath, layername, checked):
        if checked:
            self.redraw(add_vector=vectorpath, surface_layer_name=layername)
        else:
            self.redraw(remove_vector=vectorpath, surface_layer_name=layername)

    def tree_mesh_layer_selected(self, meshpath, layername, checked):
        if checked:
            self.redraw(add_mesh=meshpath, surface_layer_name=layername)
        else:
            self.redraw(remove_mesh=meshpath, surface_layer_name=layername)

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
            self.print('Processing is already occurring.  Please wait for the process to finish', logging.WARNING)
            cancelled = True
        else:
            cancelled = False
            self.load_points_thread.populate(polygon, azimuth, self.project)
            self.load_points_thread.start()
        if cancelled:
            self.print('select_points_in_box: Processing was cancelled', logging.INFO)

    def _kluster_load_points_results(self):
        """
        After running the load_points_thread to get the soundings in the polygon for every fqpr instance in the project,
        we load the points into the Points View here.
        """

        pointcount = 0
        if not self.load_points_thread.error:
            self._manpatchtest = None  # clear the patch test tool if it is loaded
            self.clear_points(True)
            points_data = self.load_points_thread.points_data
            azimuth = self.load_points_thread.azimuth
            vert_ref = ''
            for fqpr_name, pointdata in points_data.items():
                new_vert_ref = self.project.fqpr_instances[fqpr_name].vert_ref
                if vert_ref and new_vert_ref != vert_ref:
                    self.print(f"New vertical reference {new_vert_ref} doesn't match current vertical reference {vert_ref}, using {vert_ref} to determine z sign convention", loglevel=logging.WARNING)
                if not vert_ref:
                    vert_ref = new_vert_ref
                    if vert_ref in kluster_variables.positive_up_vertical_references:
                        self.points_view.z_flipped = False
                    else:
                        self.points_view.z_flipped = True
            for fqpr_name, pointdata in points_data.items():
                self.points_view.add_points(pointdata[0], pointdata[1], pointdata[2], pointdata[3], pointdata[4], pointdata[5],
                                            pointdata[6], pointdata[7], fqpr_name, pointdata[8], azimuth=azimuth)
                pointcount += pointdata[0].size
            self.points_view.display_points()
        else:
            self.print('Error loading points from project', logging.ERROR)
            self.print(self.load_points_thread.exceptiontxt, logging.ERROR)
        self.two_d.finalize_points_tool()
        self.print('Selected {} Points for display'.format(pointcount), logging.INFO)
        # we retain the polygon/azimuth in case you are using the patch test tool
        self._stop_action_progress()
        self.load_points_thread.show_error()
        self.load_points_thread.populate(polygon=self.load_points_thread.polygon, azimuth=self.load_points_thread.azimuth)

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
            self.print('Cleaning disabled while patch test is running', logging.WARNING)

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

    def _action_qgis(self):
        if sys.platform == "linux":
            print(f'Starting QGIS:  Path={kluster_variables.linux_qgis_executable}')
            subprocess.Popen(kluster_variables.linux_qgis_executable,
                             shell=True, 
                             stdout=subprocess.PIPE, 
                             stderr=subprocess.PIPE)
        else:
            try:
                rtn = subprocess.check_output(['where', 'qgis.exe'])
                pth = rtn.rstrip().decode()
            except:
                pth = 'unknown'
            print(f'Starting QGIS:  Path={pth}')
            os.startfile('qgis.exe')

    def _action_file_analyzer(self):
        self._fileanalyzer = dialog_fileanalyzer.FileAnalyzerDialog(parent=self)
        self._fileanalyzer.setWindowFlags(self._fileanalyzer.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
        self._fileanalyzer.show()

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

    def _action_mosaic_generation(self):
        """
        Connect menu action 'New Mosaic' with surface dialog
        """
        self.kluster_mosaic_generation()

    def _action_surfacefrompoints_generation(self):
        self.kluster_surfacefrompoints_generation()

    def _action_filter(self):
        """
        Connect menu action 'New Surface' with surface dialog
        """
        self.kluster_filter()

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
            self.print('Build a new project or open an existing project before creating a vessel file', logging.ERROR)

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
            self.print('Build a new project or open an existing project before opening a vessel file', logging.ERROR)

    def _action_export(self):
        """
        Connect menu action 'Export Soundings' with kluster_export
        """
        self.kluster_export()

    def _action_export_tracklines(self):
        """
        Connect menu action 'Export Tracklines' with kluster_export_tracklines
        """
        self.kluster_export_tracklines()

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
            self.print('Unable to find documentation at {}'.format(doc_html), logging.ERROR)

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
        self.print('Reset interface settings to default', logging.INFO)

    def return_selected_fqprs(self, subset_by_line: bool = False, concatenate: bool = True):
        """
        Return absolute paths to fqprs selected and the loaded fqpr instances.  Subset by line if you want to only
        return the data for the lines selected.

        Parameters
        ----------
        subset_by_line
            if True, will subset each Fqpr object to just the data associated with the lines selected
        concatenate
            if True, will concatenate the fqprs into one object, only possible if the sonar serial numbers match

        Returns
        -------
        list
            absolute path to the fqprs selected in the GUI
        list
            list of loaded fqpr instances
        """
        fqprs, linedict = self.project_tree.return_selected_fqprs()
        if subset_by_line and linedict:
            fqpr_paths, fqpr_loaded = self.project.get_fqprs_by_paths(fqprs, linedict, concatenate=concatenate)
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
        dask_close_localcluster()

        super(KlusterMain, self).closeEvent(event)


def main():
    ispyinstaller = False
    if sys.argv[0][-4:] == '.exe' or sys.argv[0][-3:] == '.so':
        ispyinstaller = True
        setattr(sys, 'frozen', True)
    # add support in windows for when you build this as a frozen executable (pyinstaller)
    multiprocessing.freeze_support()
    if ispyinstaller:
        kluster_main_exe = sys.argv[0]
        curdir = os.path.dirname(kluster_main_exe)
        kluster_icon = os.path.join(curdir, 'HSTB', 'kluster', 'images', 'kluster_img.ico')
    else:
        kluster_dir = os.path.dirname(kluster_init_file)
        kluster_icon = os.path.join(kluster_dir, 'images', 'kluster_img.ico')

    if qgis_enabled:
        app_library = 'pyqt5'
        app = qgis_core.QgsApplication([], False)
        if ispyinstaller:
            kluster_main_exe = sys.argv[0]
            curdir = os.path.dirname(kluster_main_exe)
            plugin_dir = os.path.join(curdir, 'qgis_plugins')
            prefix_dir = curdir
            processing_dir = os.path.join(curdir, 'qgis_plugins', 'processing')
        else:
            if sys.platform == 'linux':
                env_dir = os.path.dirname(os.path.dirname(os.path.dirname(found_path)))
                plugin_dir = os.path.join(env_dir, 'lib', 'qgis', 'plugins')
                prefix_dir = os.path.join(os.path.dirname(found_path))
                processing_dir = os.path.join(os.path.dirname(found_path), 'python', 'plugins', 'processing')
            else:
                plugin_dir = os.path.join(os.path.dirname(found_path), 'plugins')
                prefix_dir = os.path.join(os.path.dirname(found_path))
                processing_dir = os.path.join(os.path.dirname(found_path), 'python', 'plugins', 'processing')

        try:
            assert os.path.exists(plugin_dir)
        except:
            print(f"WARNING: QGIS - Can't find plugin directory at {plugin_dir}, is pyinstaller={ispyinstaller}")
        try:
            assert os.path.exists(prefix_dir)
        except:
            print(f"WARNING: QGIS - Can't find prefix directory at {prefix_dir}, is pyinstaller={ispyinstaller}")
        try:
            assert os.path.exists(processing_dir)
        except:
            print(f"WARNING: QGIS - Can't find processing directory at {processing_dir}, is pyinstaller={ispyinstaller}")

        app.setPrefixPath(prefix_dir, True)
        app.setPluginPath(plugin_dir)

        app.initQgis()

        # set a configuration text block, in case you want to display it for the user
        global config_text
        config_text = f'**************************************************\nQGIS Version: {qgis_core.Qgis.QGIS_VERSION}\n\n{app.showSettings()}**************************************************'

        # setup the processing algorithms in no gui mode, this is pretty hacky but the only documented way to get it
        #   to work that I have found.

        # disabling this for now as it causes some other weird possibly namespace related issues
        # sys.path.append(os.path.dirname(processing_dir))
        # from processing.core.Processing import Processing
        # Processing.initialize()

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
