import matplotlib
matplotlib.use('qt5agg')

import os
import sys
import webbrowser
import numpy as np
import multiprocessing

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal, qgis_enabled
if qgis_enabled:
    from HSTB.kluster.gui.backends._qt import qgis_core, qgis_gui

from HSTB.kluster.gui import dialog_vesselview, kluster_explorer, kluster_project_tree, kluster_3dview_v2, \
    kluster_output_window, kluster_2dview, kluster_actions, kluster_monitor, dialog_daskclient, dialog_surface, \
    dialog_export, kluster_worker, kluster_interactive_console, dialog_basicplot, dialog_advancedplot, dialog_project_settings, \
    dialog_export_grid, dialog_layer_settings, dialog_settings, dialog_importppnav, dialog_overwritenav
from HSTB.kluster.fqpr_project import FqprProject
from HSTB.kluster.fqpr_intelligence import FqprIntel
from HSTB.kluster import __version__ as kluster_version
from HSTB.kluster import __file__ as kluster_init_file
from HSTB.shared import RegistryHelpers, path_to_supplementals

# list of icons
# https://joekuan.wordpress.com/2015/09/23/list-of-qt-icons/


settings_translator = {'Kluster/proj_settings_epsgradio': {'newname': 'use_epsg', 'defaultvalue': False},
                       'Kluster/proj_settings_epsgval': {'newname': 'epsg', 'defaultvalue': ''},
                       'Kluster/proj_settings_utmradio': {'newname': 'use_coord', 'defaultvalue': True},
                       'Kluster/proj_settings_utmval': {'newname': 'coord_system', 'defaultvalue': 'NAD83'},
                       'Kluster/proj_settings_vertref': {'newname': 'vert_ref', 'defaultvalue': 'waterline'},
                       'Kluster/layer_settings_background': {'newname': 'layer_background', 'defaultvalue': 'Default'},
                       'Kluster/layer_settings_transparency': {'newname': 'layer_transparency', 'defaultvalue': '0'},
                       'Kluster/layer_settings_surfacetransparency': {'newname': 'surface_transparency', 'defaultvalue': 0},
                       'Kluster/settings_enable_parallel_writes': {'newname': 'write_parallel', 'defaultvalue': True},
                       'Kluster/settings_vdatum_directory': {'newname': 'vdatum_directory', 'defaultvalue': ''}
                       }


class KlusterMain(QtWidgets.QMainWindow):
    """
    Main window for kluster application

    """
    def __init__(self, app=None):
        """
        Build out the dock widgets with the kluster widgets inside.  Will use QSettings object to retain size and
        position.

        """
        super().__init__()

        self.app = app
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

        self.points_view = kluster_3dview_v2.ThreeDWidget(self)
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

        self.monitor = kluster_monitor.KlusterMonitor(self)
        self.monitor_dock = self.dock_this_widget('Monitor', 'monitor_dock', self.monitor)

        self.console = kluster_interactive_console.KlusterConsole(self)
        self.console_dock = self.dock_this_widget('Console', 'console_dock', self.console)

        self.vessel_win = None
        self.basicplots_win = None
        self.advancedplots_win = None

        self.iconpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images', 'kluster_img.ico')
        self.setWindowIcon(QtGui.QIcon(self.iconpath))

        self.action_thread = kluster_worker.ActionWorker()
        self.import_ppnav_thread = kluster_worker.ImportNavigationWorker()
        self.overwrite_nav_thread = kluster_worker.OverwriteNavigationWorker()
        self.surface_thread = kluster_worker.SurfaceWorker()
        self.export_thread = kluster_worker.ExportWorker()
        self.export_grid_thread = kluster_worker.ExportGridWorker()
        self.allthreads = [self.action_thread, self.import_ppnav_thread, self.overwrite_nav_thread, self.surface_thread,
                           self.export_thread, self.export_grid_thread]

        # connect FqprActionContainer with actions pane, called whenever actions changes
        self.intel.bind_to_action_update(self.actions.update_actions)

        self.project_tree.file_added.connect(self.update_on_file_added)
        self.project_tree.line_selected.connect(self.tree_line_selected)
        self.project_tree.fqpr_selected.connect(self.tree_fqpr_selected)
        self.project_tree.surface_selected.connect(self.tree_surf_selected)
        self.project_tree.surface_layer_selected.connect(self.tree_surface_layer_selected)
        self.project_tree.all_lines_selected.connect(self.tree_all_lines_selected)
        self.project_tree.close_fqpr.connect(self.close_fqpr)
        self.project_tree.close_surface.connect(self.close_surface)
        self.project_tree.load_console_fqpr.connect(self.load_console_fqpr)
        self.project_tree.load_console_surface.connect(self.load_console_surface)
        self.project_tree.zoom_extents_fqpr.connect(self.zoom_extents_fqpr)
        self.project_tree.zoom_extents_surface.connect(self.zoom_extents_surface)

        self.explorer.row_selected.connect(self.points_view.superselect_point)

        self.actions.execute_action.connect(self.intel.execute_action)
        self.actions.exclude_queued_file.connect(self._action_remove_file)
        self.actions.exclude_unmatched_file.connect(self._action_remove_file)
        self.actions.undo_exclude_file.connect(self._action_add_files)

        self.two_d.box_select.connect(self.select_line_by_box)
        self.two_d.box_3dpoints.connect(self.select_points_in_box)
        self.two_d.box_swath.connect(self.select_slice_in_box)

        self.points_view.points_selected.connect(self.show_points_in_explorer)

        self.action_thread.finished.connect(self._kluster_execute_action_results)
        self.overwrite_nav_thread.started.connect(self._start_action_progress)
        self.overwrite_nav_thread.finished.connect(self._kluster_overwrite_nav_results)
        self.import_ppnav_thread.started.connect(self._start_action_progress)
        self.import_ppnav_thread.finished.connect(self._kluster_import_ppnav_results)
        self.surface_thread.started.connect(self._start_action_progress)
        self.surface_thread.finished.connect(self._kluster_surface_generation_results)
        self.export_thread.started.connect(self._start_action_progress)
        self.export_thread.finished.connect(self._stop_action_progress)
        self.export_grid_thread.started.connect(self._start_action_progress)
        self.export_grid_thread.finished.connect(self._stop_action_progress)

        self.monitor.monitor_file_event.connect(self.intel._handle_monitor_event)
        self.monitor.monitor_start.connect(self._create_new_project_if_not_exist)

        self.setup_menu()
        self.setup_widgets()
        self.read_settings()

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
                self.two_d.vdatum_directory = self.settings['vdatum_directory']
        if self.project.path is not None:
            self.project.set_settings(self.settings.copy())
        self.intel.set_settings(self.settings.copy())

    def setup_menu(self):
        """
        Build the menu bar for the application
        """

        new_proj_action = QtWidgets.QAction('New Project', self)
        new_proj_action.triggered.connect(self._action_new_project)
        open_proj_action = QtWidgets.QAction('Open Project', self)
        open_proj_action.triggered.connect(self._action_open_project)
        save_proj_action = QtWidgets.QAction('Save Project', self)
        save_proj_action.triggered.connect(self._action_save_project)
        close_proj_action = QtWidgets.QAction('Close Project', self)
        close_proj_action.triggered.connect(self.close_project)
        settings_action = QtWidgets.QAction('Settings', self)
        settings_action.triggered.connect(self.set_settings)
        export_action = QtWidgets.QAction('Export Soundings', self)
        export_action.triggered.connect(self._action_export)
        export_grid_action = QtWidgets.QAction('Export Surface', self)
        export_grid_action.triggered.connect(self._action_export_grid)

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

        basicplots_action = QtWidgets.QAction('Basic Plots', self)
        basicplots_action.triggered.connect(self._action_basicplots)
        advancedplots_action = QtWidgets.QAction('Advanced Plots', self)
        advancedplots_action.triggered.connect(self._action_advancedplots)

        menubar = self.menuBar()
        file = menubar.addMenu("File")
        file.addAction(new_proj_action)
        file.addAction(open_proj_action)
        file.addAction(save_proj_action)
        file.addAction(close_proj_action)
        file.addSeparator()
        file.addAction(settings_action)
        file.addSeparator()
        file.addAction(export_action)
        file.addAction(export_grid_action)

        view = menubar.addMenu('View')
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

        visual = menubar.addMenu('Visualize')
        visual.addAction(basicplots_action)
        visual.addAction(advancedplots_action)

    def update_on_file_added(self, fil=''):
        """
        Adding a new path to a fqpr data store will update all the child widgets.  Will also load the data and add it
        to this class' project.

        Menubar Convert Multibeam will just run this method with empty fil to get to the convert dialog

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

        for f in fil:
            f = os.path.normpath(f)
            updated_type, new_data, new_project = self.intel.add_file(f)
            if new_project:  # user added a data file when there was no project, so we loaded or created a new one
                new_fqprs.extend([fqpr for fqpr in self.project.fqpr_instances.keys() if fqpr not in new_fqprs])
            if new_data is None:
                if os.path.split(f)[1] == 'grid.pickle':
                    self.project.add_surface(os.path.dirname(f))
                elif os.path.exists(os.path.join(f, 'grid.pickle')):
                    self.project.add_surface(f)
                else:
                    fqpr_entry, already_in = self.project.add_fqpr(f, skip_dask=True)
                    if fqpr_entry is None:  # no fqpr instance successfully loaded
                        print('update_on_file_added: Unable to add to Project from existing: {}'.format(f))
                    if already_in:
                        print('{} already exists in {}'.format(f, self.project.path))
                    elif fqpr_entry:
                        new_fqprs.append(fqpr_entry)
        self.refresh_project(new_fqprs)

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
        if new_fqprs is not None:
            for fq in new_fqprs:
                for ln in self.project.return_project_lines(proj=fq, relative_path=True):
                    lats, lons = self.project.return_line_navigation(ln, samplerate=5)
                    self.two_d.add_line(ln, lats, lons)
            self.two_d.set_extents_from_lines()
        if add_surface is not None and surface_layer_name:
            surf_object = self.project.surface_instances[add_surface]
            shown = self.two_d.show_surface(add_surface, surface_layer_name)
            if not shown:  # show didnt work, must need to add the surface instead
                if qgis_enabled:
                    data, geo_transform, bandnames = surf_object._gdal_preprocessing(nodatavalue=np.nan,
                                                                                     z_positive_up=False,
                                                                                     layer_names=(surface_layer_name,))
                    self.two_d.add_surface([add_surface, surface_layer_name, data, geo_transform, surf_object.crs])
                else:
                    x, y, z, valid, newmins, newmaxs = surf_object.return_surf_xyz(surface_layer_name, pcolormesh=True)
                    self.two_d.add_surface([add_surface, surface_layer_name, x, y, z, surf_object.crs])
                self.two_d.set_extents_from_surfaces()
        if remove_surface is not None and surface_layer_name:
            self.two_d.hide_surface(remove_surface, surface_layer_name)

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
        self.console.runCmd('nav = data.multibeam.raw_nav')
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
        self.console.runCmd('# try plotting the tree, "surf.tree.draw_tree()')

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

        self.two_d.set_extents_from_surfaces(subset_surf=pth)

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

        self.project.remove_surface(pth, relative_path=True)
        self.project_tree.refresh_project(self.project)
        self.two_d.remove_surface(pth)

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
        fqprs = self.return_selected_fqprs()

        self.vessel_win = None
        self.vessel_win = dialog_vesselview.VesselWidget()

        if fqprs:
            fqpr = self.project.fqpr_instances[self.project.path_relative_to_project(fqprs[0])]
            self.vessel_win.xyzrph = fqpr.multibeam.xyzrph
            self.vessel_win.load_from_existing_xyzrph()
        self.vessel_win.show()

    def kluster_basic_plots(self):
        """
        Runs the basic plots dialog, for plotting the variables using the xarray/matplotlib functionality
        """
        fqprs = self.return_selected_fqprs()

        self.basicplots_win = None
        self.basicplots_win = dialog_basicplot.BasicPlotDialog()

        if fqprs:
            self.basicplots_win.data_widget.new_fqpr_path(fqprs[0])
            self.basicplots_win.data_widget.initialize_controls()
        self.basicplots_win.show()

    def kluster_advanced_plots(self):
        """
        Runs the advanced plots dialog, for plotting the sat tests and other more sophisticated stuff
        """
        fqprs = self.return_selected_fqprs()

        self.advancedplots_win = None
        self.advancedplots_win = dialog_advancedplot.AdvancedPlotDialog()

        if fqprs:
            self.advancedplots_win.data_widget.new_fqpr_path(fqprs[0])
            self.advancedplots_win.data_widget.initialize_controls()
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
            print('Processing is already occurring.  Please wait for the process to finish')
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
            print('kluster_action: no data returned from action execution: {}'.format(fqpr))

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
            fqprs = self.return_selected_fqprs()
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
                            self.update_on_file_added(fq)
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
        if fq_inst:
            for fq in fq_inst:
                self.project.add_fqpr(fq)
                self.refresh_explorer(fq)
        else:
            print('kluster_import_navigation: Unable to complete process')

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
            fqprs = self.return_selected_fqprs()
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
                            self.update_on_file_added(fq)
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
        if fq_inst:
            for fq in fq_inst:
                self.project.add_fqpr(fq)
                self.refresh_explorer(fq)
        else:
            print('kluster_import_navigation: Unable to complete process')

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
            fqprs = self.return_selected_fqprs()
            dlog = dialog_surface.SurfaceDialog()
            dlog.update_fqpr_instances(addtl_files=fqprs)
            if dlog.exec_():
                cancelled = dlog.canceled
                opts = dlog.return_processing_options()
                if opts is not None and not cancelled:
                    surface_opts = opts
                    fqprs = surface_opts.pop('fqpr_inst')
                    fq_chunks = []
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            self.update_on_file_added(fq)
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
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
        if fq_surf is not None:
            self.project.add_surface(fq_surf)
            self.redraw()
        else:
            print('kluster_surface_generation: Unable to complete process')
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
                        self.update_on_file_added(surf)
                    if relsurf in self.project.surface_instances:
                        surf_inst = self.project.surface_instances[relsurf]
                        self.output_window.clear()
                        self.export_grid_thread.populate(surf_inst, export_format, output_path, z_pos_up, opts)
                        self.export_grid_thread.start()
                    else:
                        print('kluster_grid_export: Unable to load from {}'.format(surf))
                else:
                    cancelled = True
        if cancelled:
            print('kluster_grid_export: Export was cancelled')

    def kluster_export(self):
        """
        Trigger export on all the fqprs provided.  Currently only supports export of xyz to csv file(s), las file(s)
        and entwine point store.
        """

        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            fqprs = self.return_selected_fqprs()
            dlog = dialog_export.ExportDialog()
            dlog.update_fqpr_instances(addtl_files=fqprs)
            cancelled = False
            if dlog.exec_():
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
                            self.update_on_file_added(fq)
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
                            fq_chunks.append([fq_inst])
                    if fq_chunks:
                        self.output_window.clear()
                        self.export_thread.populate(fq_chunks, export_type, z_pos_down, delimiter, filterset,
                                                    separateset)
                        self.export_thread.start()
                else:
                    cancelled = True
        if cancelled:
            print('kluster_export: Export was cancelled')

    def _start_action_progress(self):
        """
        For worker threads not started through the action widget, we have to manually trigger starting the progress
        bar here.
        """

        self.actions.progress.setMaximum(0)

    def _stop_action_progress(self):
        """
        For worker threads not started through the action widget, we have to manually trigger stopping the progress
        here.
        """

        self.actions.progress.setMaximum(1)

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

        self.close_project()
        data = self.project._load_project_file(pth)
        for pth in data['fqpr_paths']:
            self.open_fqpr(pth)
        for pth in data['surface_paths']:
            self.project.add_surface(pth)

        self.redraw()

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

    def start_dask_client(self):
        """
        Set the project up with a new Client object, either LocalCluster or a client to a remote cluster
        """

        dlog = dialog_daskclient.DaskClientStart()
        if dlog.exec_():
            client = dlog.cl
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

        """
        convert_pth = self.project.convert_path_lookup[linename]
        raw_attribution = self.project.fqpr_attrs[convert_pth]
        self.explorer.populate_explorer_with_lines(linename, raw_attribution)

        # if self.dockwidget_is_visible(self.attitude_dock) and idx == 0:
        #     att = self.project.build_raw_attitude_for_line(linename, subset=True)
        #     if att is not None:
        #         self.attitude.initialize_datastore()
        #         self.attitude.initialize_data(att)
        #         self.attitude.start_plotting()
        # elif not self.dockwidget_is_visible(self.attitude_dock):
        #     self.attitude.stop_plotting()

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

    def tree_line_selected(self, linename):
        """
        method is run on selecting a multibeam line in the KlusterProjectTree

        Parameters
        ----------
        linename: str, line name

        """
        self.two_d.reset_line_colors()
        self.explorer.clear_explorer_data()
        self._line_selected(linename)
        self.two_d.change_line_colors([linename], 'red')

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
        On selecting a surface in the project tree, show the surface in 3D if the depth layer exists

        Parameters
        ----------
        converted_pth: str, surface path, used as key in project structure

        """
        pass
        # if self.dockwidget_is_visible(self.points_dock):
        #     self.points_view.clear_plot_area()
        #     surf_object = self.project.surface_instances[converted_pth]
        #     lyr = surf_object.get_layer_by_name('depth')
        #     self.points_view.add_surface_dataset(surf_object.node_x_loc, surf_object.node_y_loc, lyr)

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

    def select_line_by_box(self, min_lat, max_lat, min_lon, max_lon):
        """
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

    def select_points_in_box(self, min_lat, max_lat, min_lon, max_lon):
        """
        method run on using the 2dview points select tool.  Gathers all points in the box.

        Parameters
        ----------
        min_lat: float, minimum latitude of the box
        max_lat: float, maximum latitude of the box
        min_lon: float, minimum longitude of the box
        max_lon: float, minimum longitude of the box

        """
        self.points_view.clear()
        pts_data = self.project.return_soundings_in_box(min_lat, max_lat, min_lon, max_lon)
        for fqpr_name, pointdata in pts_data.items():
            self.points_view.add_points(pointdata[0], pointdata[1], pointdata[2], pointdata[3], pointdata[4], pointdata[5],
                                    pointdata[6], fqpr_name, pointdata[7], is_3d=True)
        self.points_view.display_points()

    def select_slice_in_box(self, min_lat, max_lat, min_lon, max_lon):
        """
        method run on using the 2dview swath select tool.  Gathers all swaths in the box.

        Parameters
        ----------
        min_lat: float, minimum latitude of the box
        max_lat: float, maximum latitude of the box
        min_lon: float, minimum longitude of the box
        max_lon: float, minimum longitude of the box

        """
        self.points_view.clear()
        pts_data = self.project.return_soundings_in_box(min_lat, max_lat, min_lon, max_lon)
        for fqpr_name, pointdata in pts_data.items():
            self.points_view.add_points(pointdata[0], pointdata[1], pointdata[2], pointdata[3], pointdata[4], pointdata[5],
                                    pointdata[6], fqpr_name, pointdata[7], is_3d=False)
        self.points_view.display_points()

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
        settings = self.settings_object
        settings.clear()
        # set all docked widgets to 'docked' so that they reset properly
        for widg in self.findChildren(QtWidgets.QDockWidget):
            widg.setFloating(False)
        self.setup_widgets()
        # self.setUpdatesEnabled(True)
        print('Reset interface settings to default')

    def return_selected_fqprs(self):
        """
        Return absolute paths to fqprs selected

        Returns
        -------
        list
            absolute path to the fqprs selected in the GUI
        """
        fqprs = self.project_tree.return_selected_fqprs()
        fqprs = [self.project.absolute_path_from_relative(f) for f in fqprs]
        return fqprs

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

    if qgis_enabled:
        app = qgis_core.QgsApplication([], True)
        if ispyinstaller:
            plugin_dir = os.path.join(os.getcwd(), 'qgis_plugins')
            app.setPrefixPath(os.getcwd(), True)
            app.setPluginPath(plugin_dir)
        app.initQgis()
    else:
        try:  # pyside2
            app = QtWidgets.QApplication()
        except TypeError:  # pyqt5
            app = QtWidgets.QApplication([])
    window = KlusterMain(app)
    window.show()
    exitcode = app.exec_()
    sys.exit(exitcode)


if __name__ == '__main__':
    main()
