import os
import sys
import webbrowser

from PySide2 import QtGui, QtCore, QtWidgets

from HSTB.kluster.gui import dialog_vesselview, kluster_explorer, kluster_project_tree, kluster_3dview, kluster_attitudeview, \
    kluster_output_window, kluster_2dview, dialog_conversion, dialog_all_processing, dialog_daskclient, dialog_surface, \
    dialog_export, kluster_worker, kluster_interactive_console, dialog_importnav, dialog_basicplot, dialog_advancedplot
from HSTB.kluster.fqpr_project import FqprProject
from HSTB.kluster.fqpr_helpers import return_files_from_path
from HSTB.kluster import __version__ as kluster_version
from HSTB.shared import RegistryHelpers

# list of icons
# https://joekuan.wordpress.com/2015/09/23/list-of-qt-icons/


class KlusterMain(QtWidgets.QMainWindow):
    """
    Main window for kluster application

    """
    def __init__(self):
        """
        Build out the dock widgets with the kluster widgets inside.  Will use QSettings object to retain size and
        position.

        """
        super().__init__()

        self.start_horiz_size = 800
        self.start_vert_size = 600

        # self.resize(self.start_horiz_size, self.start_vert_size)

        self.setWindowTitle('Kluster {}'.format(kluster_version))
        self.setDockNestingEnabled(True)

        self.widget_obj_names = []

        # fqpr = fully qualified ping record, the term for the datastore in kluster
        self.project = FqprProject(is_gui=False)

        self.project_tree = kluster_project_tree.KlusterProjectTree(self)
        self.tree_dock = self.dock_this_widget('Project Tree', 'project_dock', self.project_tree)

        self.two_d = kluster_2dview.Kluster2dview(self)
        self.two_d_dock = self.dock_this_widget('2d view', 'two_d_dock', self.two_d)

        self.three_d = kluster_3dview.Kluster3dview(self)
        self.three_d_dock = self.dock_this_widget("3d view", 'three_d_dock', self.three_d)

        self.explorer = kluster_explorer.KlusterExplorer(self)
        self.explorer_dock = self.dock_this_widget("Explorer", 'explorer_dock', self.explorer)

        self.output_window = kluster_output_window.KlusterOutput(self)
        self.output_window_dock = self.dock_this_widget('Output', 'output_window_dock', self.output_window)

        self.attribute = kluster_explorer.KlusterAttribution(self)
        self.attribute_dock = self.dock_this_widget("Attribute", 'attribute_dock', self.attribute)

        self.attitude = kluster_attitudeview.KlusterAttitudeView(self)
        self.attitude_dock = self.dock_this_widget('Attitude', 'attitude_dock', self.attitude)

        self.console = kluster_interactive_console.KlusterConsole(self)
        self.console_dock = self.dock_this_widget('Console', 'console_dock', self.console)

        self.vessel_win = None
        self.basicplots_win = None
        self.advancedplots_win = None

        self.iconpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images', 'kluster_img.ico')
        self.setWindowIcon(QtGui.QIcon(self.iconpath))

        self.convert_thread = kluster_worker.ConversionWorker()
        self.allprocessing_thread = kluster_worker.AllProcessingWorker()
        self.importnav_thread = kluster_worker.ImportNavigationWorker()
        self.surface_thread = kluster_worker.SurfaceWorker()
        self.export_thread = kluster_worker.ExportWorker()
        self.allthreads = [self.convert_thread, self.allprocessing_thread, self.importnav_thread, self.surface_thread,
                           self.export_thread]

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
        self.explorer.row_selected.connect(self.attribute.display_file_attribution)
        self.two_d.box_select.connect(self.select_line_by_box)
        self.convert_thread.finished.connect(self._kluster_convert_multibeam_results)
        self.allprocessing_thread.finished.connect(self._kluster_all_processing_results)
        self.importnav_thread.finished.connect(self._kluster_import_nav_results)
        self.surface_thread.finished.connect(self._kluster_surface_genertaion_results)

        self.setup_menu()
        self.setup_widgets()
        self.read_settings()

        self.show()

    def setup_menu(self):
        """
        Build the menu bar for the application

        """
        new_proj_action = QtWidgets.QAction('New Project', self)
        new_proj_action.triggered.connect(self.new_project)
        open_proj_action = QtWidgets.QAction('Open Project', self)
        open_proj_action.triggered.connect(self._action_open_project)
        save_proj_action = QtWidgets.QAction('Save Project', self)
        save_proj_action.triggered.connect(self._action_save_project)
        export_action = QtWidgets.QAction('Export Soundings', self)
        export_action.triggered.connect(self._action_export)

        view_dashboard_action = QtWidgets.QAction('Dashboard', self)
        view_dashboard_action.triggered.connect(self.open_dask_dashboard)

        setup_client_action = QtWidgets.QAction('Dask Client', self)
        setup_client_action.triggered.connect(self.start_dask_client)
        vessel_view_action = QtWidgets.QAction('Vessel Offsets', self)
        vessel_view_action.triggered.connect(self._action_vessel_view)

        conv_multi_action = QtWidgets.QAction('Convert Multibeam', self)
        conv_multi_action.triggered.connect(self._action_conversion)
        import_nav_action = QtWidgets.QAction('Import Navigation', self)
        import_nav_action.triggered.connect(self._action_import_navigation)
        all_process_action = QtWidgets.QAction('All Processing', self)
        all_process_action.triggered.connect(self._action_all_processing)
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
        file.addSeparator()
        file.addAction(export_action)

        view = menubar.addMenu('View')
        view.addAction(view_dashboard_action)

        setup = menubar.addMenu('Setup')
        # setup.addAction('Coordinate Reference System')
        setup.addAction(vessel_view_action)
        setup.addAction(setup_client_action)

        process = menubar.addMenu('Process')
        process.addAction(conv_multi_action)
        process.addAction(import_nav_action)
        process.addAction(all_process_action)
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

        multibeamfiles = []
        surfaces = []
        new_fqprs = []

        for f in fil:
            possible_multibeam_files = return_files_from_path(f, file_ext=('.kmall', '.all'))
            possible_surface_files = return_files_from_path(f, file_ext=('.npz',))
            if possible_multibeam_files or possible_surface_files:
                multibeamfiles.extend(possible_multibeam_files)
                surfaces.extend(possible_surface_files)
            if os.path.split(f)[1] == 'kluster_project.json':
                print('Please open this project file {} using Open Project'.format(f))
                continue

            f = os.path.normpath(f)
            fqpr_entry = self.project.add_fqpr(f, skip_dask=True)
            if fqpr_entry is None:  # no fqpr instance successfully loaded
                if not possible_multibeam_files and not possible_surface_files:
                    print('update_on_file_added: Unable to add to Project from existing: {}'.format(f))
            else:
                new_fqprs.append(fqpr_entry)
        if new_fqprs:
            self.redraw(new_fqprs=new_fqprs)

        if multibeamfiles:  # go ahead and convert if the user dragged in some multibeam files
            self.kluster_convert_multibeam(multibeamfiles)
        if surfaces:
            for surf in surfaces:
                self.project.add_surface(surf)
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
            lyr = surf_object.get_layer_by_name(surface_layer_name)
            self.two_d.add_surface(add_surface, surface_layer_name, surf_object.node_x_loc, surf_object.node_y_loc, lyr,
                                   surf_object.crs)
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
        absolute_fqpath = self.project._absolute_path_from_relative(pth)
        self.console.runCmd('data = reload_data(r"{}", skip_dask=True)'.format(absolute_fqpath))
        self.console.runCmd('first_system = data.multibeam.raw_ping[0]')
        self.console.runCmd('nav = data.multibeam.raw_nav')
        self.console.runCmd('att = data.multibeam.raw_att')

    def load_console_surface(self, pth):
        pass

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

        self.project.remove_surface(pth)
        self.project_tree.refresh_project(self.project)
        if pth in self.two_d.active_layers:
            for lyr in self.two_d.active_layers[pth]:
                self.redraw(remove_surface=pth, surface_layer_name=lyr)

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

    def kluster_convert_multibeam(self, fil):
        """
        Runs the kluster conversion driven by the conversion dialog on the given files/directory path.  Expects fil to
        either be a path to a multibeam file, a list of files or a path to a directory of multibeam files.

        Parameters
        ----------
        fil: str or list, multibeam file(s)/directory of multibeam files.  if fil is an empty string, will open up a
              blank conversion dialog.

        Returns
        -------
        fil: str or list, same as input fil, but if the conversion succeeds, this will be overriden with the path to
             the converted data

        """
        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            dlog = dialog_conversion.ConversionDialog()
            dlog.show()
            dlog.update_multibeam_files(return_files_from_path(fil, file_ext=('.kmall', '.all')))
            cancelled = False
            if dlog.exec_():
                output_folder = dlog.output_pth
                mbes_files = dlog.multibeam_files
                if output_folder is not None and not dlog.cancelled:
                    # if the project has a client, use it here.  If None, BatchRead starts a new LocalCluster
                    self.convert_thread.populate(mbes_files, output_folder, self.project.get_dask_client())
                    self.convert_thread.start()
                else:
                    # dialog was cancelled
                    cancelled = True

        if cancelled:
            print('kluster_convert_multibeam: Conversion was cancelled')

    def _kluster_convert_multibeam_results(self):
        """
        This method is run on the conclusion of the convert_thread operation.  We want to make sure the converted data
        converted, and exists in the project.  If it does, go ahead and draw it on the 2d widget.

        """
        # fil is now the output path of the Fqpr instance
        fq = self.convert_thread.fq
        if fq is not None:
            fqpr_entry = self.project.add_fqpr(fq)
            if fqpr_entry is None:
                print('kluster_convert_multibeam: Unable to add to Project from conversion: {}'.format(fq))
            else:
                self.redraw(new_fqprs=[fqpr_entry])
        else:
            print('kluster_convert_multibeam: Unable to convert {}'.format(self.convert_thread.mbes_files))

    def kluster_all_processing(self):
        """
        Takes all the selected fqpr instances in the project tree and runs the all processing dialog to process those
        instances.  Dialog allows for adding/removing instances.

        If a dask client hasn't been setup in this Kluster run, we auto setup a dask LocalCluster for processing

        Refreshes the project at the end to load in the new attribution

        """
        if not self.no_threads_running():
            print('Processing is already occurring.  Please wait for the process to finish')
            cancelled = True
        else:
            fqprs = self.return_selected_fqprs()
            dlog = dialog_all_processing.AllProcessingDialog()
            dlog.update_fqpr_instances(addtl_files=fqprs)
            cancelled = False
            if dlog.exec_():
                opts = dlog.return_processing_options()
                if opts is not None and not dlog.canceled:
                    mbes_opts = opts
                    fqprs = mbes_opts.pop('fqpr_inst')
                    fq_chunks = []
                    for fq in fqprs:
                        relfq = self.project.path_relative_to_project(fq)
                        if relfq not in self.project.fqpr_instances:
                            self.update_on_file_added(fq)
                        if relfq in self.project.fqpr_instances:
                            fq_inst = self.project.fqpr_instances[relfq]
                            # use the project client, or start a new LocalCluster if client is None
                            fq_inst.client = self.project.get_dask_client()
                            fq_chunks.append([fq_inst, mbes_opts])
                    if fq_chunks:
                        self.allprocessing_thread.populate(fq_chunks)
                        self.allprocessing_thread.start()
                else:
                    cancelled = True
        if cancelled:
            print('kluster_all_processing: Processing was cancelled')

    def _kluster_all_processing_results(self):
        """
        Method is run when the allprocessing thread signals completion.  All we need to do here is refresh the project
        and display.
        """
        fq_inst = self.allprocessing_thread.fqpr_instances
        if fq_inst:
            for fq in fq_inst:
                self.project.add_fqpr(fq)
                self.refresh_explorer(fq)
        else:
            print('kluster_all_processing: Unable to complete process')

    def kluster_import_nav(self):
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
            dlog = dialog_importnav.ImportNavigationDialog()
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
                        self.importnav_thread.populate(fq_chunks)
                        self.importnav_thread.start()
                else:
                    cancelled = True
        if cancelled:
            print('kluster_import_navigation: Processing was cancelled')

    def _kluster_import_nav_results(self):
        """
        Method is run when the import navigation thread signals completion.  All we need to do here is refresh the project
        and display.
        """
        fq_inst = self.importnav_thread.fqpr_instances
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

                    opts['client'] = self.project.get_dask_client()
                    if not dlog.canceled:
                        # if the project has a client, use it here.  If None, BatchRead starts a new LocalCluster
                        self.surface_thread.populate(fq_chunks, opts)
                        self.surface_thread.start()
        if cancelled:
            print('kluster_surface_generation: Processing was cancelled')

    def _kluster_surface_genertaion_results(self):
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

    def kluster_export(self):
        """
        Trigger export on all the fqprs provided.  Currently only supports export of xyz to csv file(s)
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
                        self.export_thread.populate(fq_chunks, export_type, z_pos_down, delimiter, filterset, separateset)
                        self.export_thread.start()
                else:
                    cancelled = True
        if cancelled:
            print('kluster_export: Export was cancelled')

    def new_project(self):
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
        self.project.path = None

        self.project_tree.configure()
        self.two_d.set_extents_from_lines()
        self.explorer.clear_explorer_data()
        self.attribute.clear_attribution_data()

    def open_project(self, pth):
        """
        Open a project from project file

        Parameters
        ----------
        pth: str, path to the parent Fqpr project folder

        """
        data = self.project._load_project_file(pth)
        for pth in data['fqpr_paths']:
            self.open_fqpr(pth)
        for pth in data['surface_paths']:
            self.project.add_surface(pth)
        self.redraw()

    def open_dask_dashboard(self):
        """
        Opens the bokeh dashboard in a web browser to view progress.  Either
        start a new LocalCluster client if there is no client yet OR get the existing client you've setup.
        """
        if self.project.client is None:
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
        self.explorer.populate_explorer(linename, raw_attribution)
        if self.dockwidget_is_visible(self.three_d_dock):
            xyz = self.project.build_point_cloud_for_line(linename)
            if xyz is not None:
                self.three_d.add_point_dataset(xyz[0], xyz[1], xyz[2])

        if self.dockwidget_is_visible(self.attitude_dock) and idx == 0:
            att = self.project.build_raw_attitude_for_line(linename, subset=True)
            if att is not None:
                self.attitude.initialize_datastore()
                self.attitude.initialize_data(att)
                self.attitude.start_plotting()
        elif not self.dockwidget_is_visible(self.attitude_dock):
            self.attitude.stop_plotting()

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
        self.two_d.reset_colors()
        self.three_d.clear_plot_area()
        self.explorer.clear_explorer_data()
        self._line_selected(linename)
        self.two_d.change_line_colors([linename], 'r')

    def tree_fqpr_selected(self, converted_pth):
        """
        method is run on selecting a Fqpr object in the KlusterProjectTree

        Parameters
        ----------
        converted_pth: str, path to converted Fqpr object

        """
        self.two_d.reset_colors()
        self.three_d.clear_plot_area()
        self.explorer.clear_explorer_data()
        linenames = self.project.return_project_lines(proj=os.path.normpath(converted_pth))
        for cnt, ln in enumerate(linenames):
            self._line_selected(ln, idx=cnt)
        self.two_d.change_line_colors(linenames, 'r')

    def tree_surf_selected(self, converted_pth):
        """
        On selecting a surface in the project tree, show the surface in 3D if the depth layer exists

        Parameters
        ----------
        converted_pth: str, surface path, used as key in project structure

        """
        if self.dockwidget_is_visible(self.three_d_dock):
            self.three_d.clear_plot_area()
            surf_object = self.project.surface_instances[converted_pth]
            lyr = surf_object.get_layer_by_name('depth')
            self.three_d.add_surface_dataset(surf_object.node_x_loc, surf_object.node_y_loc, lyr)

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
        self.two_d.reset_colors()
        self.three_d.clear_plot_area()
        self.explorer.clear_explorer_data()
        if is_selected:
            all_lines = self.project.return_sorted_line_list()
            for cnt, ln in enumerate(all_lines):
                self._line_selected(ln, idx=cnt)
            self.two_d.change_line_colors(all_lines, 'r')

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
        self.two_d.reset_colors()
        self.explorer.clear_explorer_data()
        lines = self.project.return_lines_in_box(min_lat, max_lat, min_lon, max_lon)
        for cnt, ln in enumerate(lines):
            self._line_selected(ln, idx=cnt)
        self.two_d.change_line_colors(lines, 'r')

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
        self.addDockWidget(QtCore.Qt.TopDockWidgetArea, self.three_d_dock)
        self.addDockWidget(QtCore.Qt.TopDockWidgetArea, self.attitude_dock)
        self.splitDockWidget(self.tree_dock, self.two_d_dock, QtCore.Qt.Horizontal)
        self.tabifyDockWidget(self.two_d_dock, self.three_d_dock)
        self.tabifyDockWidget(self.two_d_dock, self.attitude_dock)

        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.explorer_dock)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.output_window_dock)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.attribute_dock)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self.console_dock)
        self.splitDockWidget(self.explorer_dock, self.attribute_dock, QtCore.Qt.Horizontal)
        self.tabifyDockWidget(self.explorer_dock, self.output_window_dock)
        self.tabifyDockWidget(self.explorer_dock, self.console_dock)

        self.two_d_dock.raise_()

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

    def _action_conversion(self):
        """
        Connect menu action 'convert multibeam' with conversion dialog
        """
        self.kluster_convert_multibeam('')

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

    def _action_all_processing(self):
        """
        Connect menu action 'All processing' with all processing dialog
        """
        self.kluster_all_processing()

    def _action_import_navigation(self):
        """
        Connect menu action 'Import Navigation' with import navigation dialog
        """
        self.kluster_import_nav()

    def _action_surface_generation(self):
        """
        Connect menu action 'New Surface' with surface dialog
        """
        self.kluster_surface_generation()

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
        Connect menu action 'Export' with kluster_export
        """
        self.kluster_export()

    def read_settings(self):
        """
        Read the settings saved in the registry

        """
        # from currentuser\software\noaa\kluster in registry
        settings = QtCore.QSettings("NOAA", "Kluster")
        self.restoreGeometry(settings.value("Kluster/geometry"))
        self.restoreState(settings.value("Kluster/windowState"), version=0)

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

    def closeEvent(self, event):
        """
        override the close event for the mainwindow, attach saving settings

        """
        settings = QtCore.QSettings("NOAA", "Kluster")
        settings.setValue('Kluster/geometry', self.saveGeometry())
        settings.setValue('Kluster/windowState', self.saveState(version=0))
        super(KlusterMain, self).closeEvent(event)


def main():
    app = QtWidgets.QApplication()
    window = KlusterMain()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
