import os.path

import numpy as np
import traceback
import logging

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal
from HSTB.kluster.fqpr_project import return_project_data, reprocess_fqprs
from HSTB.kluster import kluster_variables
from HSTB.kluster.fqpr_convenience import generate_new_surface, import_processed_navigation, overwrite_raw_navigation, \
    update_surface, reload_data, reload_surface, points_to_surface, generate_new_mosaic


class MyWorker(QtCore.QThread):

    tstarted = Signal(bool)
    tfinished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.error = False
        self.action_type = None
        self.errortxt = ''
        self.exceptiontxt = None

    def reset(self):
        self.error = False
        self.errortxt = ''
        self.exceptiontxt = None

    def log_exception(self, e: Exception):
        self.error = True
        self.errortxt = str(e)
        self.exceptiontxt = traceback.format_exc()

    def show_error(self):
        if self.errortxt:
            msgbox = QtWidgets.QMessageBox()
            msgbox.setText(f'ERROR: {self.action_type}')
            msgbox.setInformativeText(self.errortxt)
            msgbox.setDetailedText(self.exceptiontxt)
            msgbox.exec_()


class ActionWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.action_container = None
        self.action_index = None
        self.result = None

    def populate(self, action_container, action_index):
        super().reset()
        self.action_container = action_container
        self.action_index = action_index
        self.result = None

    def run(self):
        self.tstarted.emit(True)
        try:
            action = self.action_container.actions[self.action_index]
            self.parent().debug_print(f'current action container')
            self.parent().debug_print(f'running {action}: {action.function}, kwargs={action.kwargs}', logging.INFO)
            self.action_type = action.action_type
            self.result = self.action_container.execute_action(self.action_index)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class OpenProjectWorker(MyWorker):
    """
    Thread that runs when the user drags in a new project file or opens a project using the menu
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.new_project_path = None
        self.force_add_fqprs = None
        self.force_add_surfaces = None
        self.new_fqprs = []
        self.new_surfaces = []

    def populate(self, new_project_path=None, force_add_fqprs=None, force_add_surfaces=None):
        super().reset()
        self.new_project_path = new_project_path
        self.force_add_fqprs = force_add_fqprs
        self.force_add_surfaces = force_add_surfaces
        self.new_fqprs = []
        self.new_surfaces = []

    def run(self):
        self.tstarted.emit(True)
        try:
            self.action_type = 'Open Project'
            self.new_fqprs = []
            if self.new_project_path:
                data = return_project_data(self.new_project_path)
            else:
                data = {'fqpr_paths': [], 'surface_paths': []}
                if self.force_add_fqprs:
                    data['fqpr_paths'] = self.force_add_fqprs
                if self.force_add_surfaces:
                    data['surface_paths'] = self.force_add_surfaces
            self.parent().debug_print(f'loading {data}', logging.INFO)
            for pth in data['fqpr_paths']:
                fqpr_entry = reload_data(pth, skip_dask=True, silent=True, show_progress=True)
                if fqpr_entry is not None:  # no fqpr instance successfully loaded
                    self.new_fqprs.append(fqpr_entry)
                else:
                    self.parent().print('Unable to load converted data from {}'.format(pth), logging.WARNING)
            for pth in data['surface_paths']:
                surf_entry = reload_surface(pth)
                if surf_entry is not None:  # no grid instance successfully loaded
                    self.new_surfaces.append(surf_entry)
                else:
                    self.parent().print('Unable to load surface from {}'.format(pth), logging.WARNING)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class DrawNavigationWorker(MyWorker):
    """
    On opening a project, you have to get the navigation for each line and draw it in the 2d view
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.project = None
        self.new_fqprs = None
        self.line_data = {}

    def populate(self, project, new_fqprs):
        super().reset()
        self.project = project
        self.new_fqprs = new_fqprs
        self.line_data = {}

    def run(self):
        self.tstarted.emit(True)
        try:
            self.action_type = 'Draw Lines'
            for fq in self.new_fqprs:
                self.parent().print('building tracklines for {}...'.format(fq), logging.INFO)
                for ln in self.project.return_project_lines(proj=fq, relative_path=True):
                    lats, lons = self.project.return_line_navigation(ln)
                    if lats is not None:
                        self.line_data[ln] = [lats, lons]
                        self.parent().debug_print(f'project.return_line_navigation: drawing {ln}: {len(lats)} points, {lats[0]},{lons[0]} to {lats[-1]},{lons[-1]}', logging.INFO)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class DrawSurfaceWorker(MyWorker):
    """
    On opening a new surface, you have to get the surface tiles to display as in memory geotiffs in kluster_main
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.surface_path = None
        self.surf_object = None
        self.resolution = None
        self.surface_layer_name = None
        self.surface_data = {}

    def populate(self, surface_path, surf_object, resolution, surface_layer_name):
        super().reset()
        self.surface_path = surface_path
        self.surf_object = surf_object
        self.resolution = resolution
        # handle optional hillshade layer
        self.surface_layer_name = surface_layer_name
        self.surface_data = {}

    def run(self):
        self.tstarted.emit(True)
        try:
            self.action_type = 'Draw Surface'
            if self.surface_layer_name == 'tiles':
                try:
                    x, y = self.surf_object.get_tile_boundaries()
                    self.parent().debug_print(f'surf_object.get_tile_boundaries: getting bathygrid tile boundaries, {len(x)} points from {x[0]},{y[0]} to {x[-1]},{y[-1]}', logging.INFO)
                    self.surface_data = [x, y]
                except:
                    self.parent().print('Unable to load tile layer from {}, no surface data found'.format(self.surface_path), logging.WARNING)
                    self.surface_data = {}
            else:
                if self.surface_layer_name == 'hillshade':
                    surface_layer_name = 'depth'
                else:
                    surface_layer_name = self.surface_layer_name
                for resolution in self.resolution:
                    self.surface_data[resolution] = {}
                    chunk_count = 1
                    for geo_transform, maxdim, data in self.surf_object.get_chunks_of_tiles(resolution=resolution, layer=surface_layer_name,
                                                                                            override_maximum_chunk_dimension=kluster_variables.chunk_size_display,
                                                                                            nodatavalue=np.float32(np.nan), z_positive_up=self.surf_object.positive_up,
                                                                                            for_gdal=True):
                        data = list(data.values())
                        tilename = self.surface_layer_name + '_{}'.format(chunk_count)
                        self.surface_data[resolution][tilename] = [data, geo_transform]
                        chunk_count += 1
                        self.parent().debug_print(f'surf_object.get_chunks_of_tiles: {self.surface_path} : {tilename} : {resolution}m geotransform {geo_transform} maxdimension {maxdim}', logging.INFO)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class LoadPointsWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.polygon = None
        self.azimuth = None
        self.project = None
        self.points_data = None

    def populate(self, polygon=None, azimuth=None, project=None):
        super().reset()
        self.polygon = polygon
        self.azimuth = azimuth
        self.project = project
        self.points_data = None

    def run(self):
        self.tstarted.emit(True)
        self.action_type = 'Load Points'
        try:
            self.parent().debug_print(f'project.return_soundings_in_polygon: Returning soundings within polygon {self.polygon}', logging.INFO)
            self.points_data = self.project.return_soundings_in_polygon(self.polygon)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class ImportNavigationWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []

    def populate(self, fq_chunks):
        super().reset()
        self.fq_chunks = fq_chunks
        self.fqpr_instances = []

    def run(self):
        self.tstarted.emit(True)
        self.action_type = 'Import Navigation'
        try:
            for chnk in self.fq_chunks:
                self.parent().debug_print(f'fqpr_convenience.import_processed_navigation {chnk[1]}', logging.INFO)
                self.fqpr_instances.append(import_processed_navigation(chnk[0], **chnk[1]))
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class OverwriteNavigationWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []

    def populate(self, fq_chunks):
        super().reset()
        self.fq_chunks = fq_chunks
        self.fqpr_instances = []

    def run(self):
        self.tstarted.emit(True)
        self.action_type = 'Overwrite Navigation'
        try:
            for chnk in self.fq_chunks:
                self.parent().debug_print(f'fqpr_convenience.overwrite_raw_navigation {chnk[1]}', logging.INFO)
                self.fqpr_instances.append(overwrite_raw_navigation(chnk[0], **chnk[1]))
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class ExportWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.line_names = None
        self.datablock = []
        self.fqpr_instances = []
        self.export_type = ''
        self.mode = ''
        self.z_pos_down = False
        self.delimiter = ' '
        self.formattype = 'xyz'
        self.filterset = False
        self.separateset = False

    def populate(self, fq_chunks, line_names, datablock, export_type, z_pos_down, delimiter, formattype, filterset,
                 separateset, basic_mode, line_mode, points_mode):
        super().reset()
        if basic_mode:
            self.mode = 'basic'
        elif line_mode:
            self.mode = 'line'
        elif points_mode:
            self.mode = 'points'

        self.fqpr_instances = []
        self.line_names = line_names
        self.datablock = datablock
        self.fq_chunks = fq_chunks
        self.export_type = export_type
        self.z_pos_down = z_pos_down
        if delimiter == 'comma':
            self.delimiter = ','
        elif delimiter == 'space':
            self.delimiter = ' '
        else:
            raise ValueError('ExportWorker: Expected either "comma" or "space", received {}'.format(delimiter))
        self.formattype = formattype
        self.filterset = filterset
        self.separateset = separateset

    def export_process(self, fq, datablock=None):
        if self.mode == 'basic':
            self.parent().debug_print(f'export_pings_to_file file_format={self.export_type}, csv_delimiter={self.delimiter}, filter_by_detection={self.filterset}, format_type={self.formattype}, z_pos_down={self.z_pos_down}, export_by_identifiers={self.separateset}', logging.INFO)
            fq.export_pings_to_file(file_format=self.export_type, csv_delimiter=self.delimiter, filter_by_detection=self.filterset,
                                    format_type=self.formattype, z_pos_down=self.z_pos_down, export_by_identifiers=self.separateset)
        elif self.mode == 'line':
            self.parent().debug_print(f'export_lines_to_file linenames={self.line_names}, file_format={self.export_type}, csv_delimiter={self.delimiter}, filter_by_detection={self.filterset}, format_type={self.formattype}, z_pos_down={self.z_pos_down}, export_by_identifiers={self.separateset}', logging.INFO)
            fq.export_lines_to_file(linenames=self.line_names, file_format=self.export_type, csv_delimiter=self.delimiter,
                                    filter_by_detection=self.filterset, format_type=self.formattype, z_pos_down=self.z_pos_down, export_by_identifiers=self.separateset)
        else:
            self.parent().debug_print(f'export_soundings_to_file file_format={self.export_type}, csv_delimiter={self.delimiter}, filter_by_detection={self.filterset}, format_type={self.formattype}, z_pos_down={self.z_pos_down}', logging.INFO)
            fq.export_soundings_to_file(datablock=datablock, file_format=self.export_type, csv_delimiter=self.delimiter,
                                        filter_by_detection=self.filterset, format_type=self.formattype, z_pos_down=self.z_pos_down)
        return fq

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'Export Dataset ({self.mode})'
        try:
            if self.mode in ['basic', 'line']:
                for chnk in self.fq_chunks:
                    self.fqpr_instances.append(self.export_process(chnk[0]))
            else:
                fq = self.fq_chunks[0][0]
                self.fqpr_instances.append(self.export_process(fq, datablock=self.datablock))
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class FilterWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.line_names = None
        self.fqpr_instances = []
        self.new_status = []
        self.mode = ''
        self.selected_index = None
        self.filter_name = ''
        self.save_to_disk = True

        self.kwargs = None
        self.selected_index = []

    def populate(self, fq_chunks, line_names, filter_name, basic_mode, line_mode, points_mode, save_to_disk, kwargs):
        super().reset()
        if basic_mode:
            self.mode = 'basic'
        elif line_mode:
            self.mode = 'line'
        elif points_mode:
            self.mode = 'points'

        self.fqpr_instances = []
        self.new_status = []
        self.line_names = line_names
        self.fq_chunks = fq_chunks
        self.filter_name = filter_name
        self.save_to_disk = save_to_disk

        self.kwargs = kwargs
        if self.kwargs is None:
            self.kwargs = {}
        self.selected_index = []

    def filter_process(self, fq, subset_time=None, subset_beam=None):
        if self.mode == 'basic':
            self.parent().debug_print(f'run_filter {self.filter_name}, {self.kwargs}', logging.INFO)
            new_status = fq.run_filter(self.filter_name, **self.kwargs)
            fq.multibeam.reload_pingrecords()
        elif self.mode == 'line':
            self.parent().debug_print(f'run_filter {self.filter_name}, {self.kwargs}', logging.INFO)
            fq.subset_by_lines(self.line_names)
            new_status = fq.run_filter(self.filter_name, **self.kwargs)
            fq.restore_subset()
            fq.multibeam.reload_pingrecords()
        else:
            self.parent().debug_print(f'take the provided Points View time and subset the provided fqpr to just those times,beams', logging.INFO)
            selected_index = fq.subset_by_time_and_beam(subset_time, subset_beam)
            self.parent().debug_print(f'run_filter {self.filter_name}, {self.kwargs}', logging.INFO)
            new_status = fq.run_filter(self.filter_name, selected_index=selected_index, save_to_disk=self.save_to_disk, **self.kwargs)
            fq.restore_subset()
            if self.save_to_disk:
                fq.multibeam.reload_pingrecords()
            self.selected_index.append(selected_index)
        return fq, new_status

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'Filter {self.filter_name} ({self.mode})'
        try:
            if self.mode in ['basic', 'line']:
                for chnk in self.fq_chunks:
                    fq, new_status = self.filter_process(chnk[0])
                    self.fqpr_instances.append(fq)
                    self.new_status.append(new_status)
            else:
                for chnk in self.fq_chunks:
                    fq, subset_time, subset_beam = chnk[0], chnk[1], chnk[2]
                    fq, new_status = self.filter_process(fq, subset_time, subset_beam)
                    self.fqpr_instances.append(fq)
                    self.new_status.append(new_status)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class ExportTracklinesWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.line_names = None
        self.fqpr_instances = []
        self.export_type = ''
        self.mode = ''
        self.output_path = ''

    def populate(self, fq_chunks, line_names, export_type, basic_mode, line_mode, output_path):
        super().reset()
        if basic_mode:
            self.mode = 'basic'
        elif line_mode:
            self.mode = 'line'

        self.fqpr_instances = []
        self.line_names = line_names
        self.fq_chunks = fq_chunks
        self.export_type = export_type
        self.output_path = output_path

    def export_process(self, fq):
        if self.mode == 'basic':
            self.parent().debug_print(f'export_tracklines_to_file output_file={self.output_path}, file_format={self.export_type}', logging.INFO)
            fq.export_tracklines_to_file(linenames=None, output_file=self.output_path, file_format=self.export_type)
        elif self.mode == 'line':
            self.parent().debug_print(f'export_tracklines_to_file linenames={self.line_names} output_file={self.output_path}, file_format={self.export_type}', logging.INFO)
            fq.export_tracklines_to_file(linenames=self.line_names, output_file=self.output_path, file_format=self.export_type)
        return fq

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'Export Tracklines ({self.mode})'
        try:
            for chnk in self.fq_chunks:
                self.fqpr_instances.append(self.export_process(chnk[0]))
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class ExportGridWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.surf_instance = None
        self.export_type = ''
        self.output_path = ''
        self.z_pos_up = True
        self.bag_kwargs = {}

    def populate(self, surf_instance, export_type, output_path, z_pos_up, bag_kwargs):
        super().reset()
        self.surf_instance = surf_instance
        self.export_type = export_type
        self.output_path = output_path
        self.bag_kwargs = bag_kwargs
        self.z_pos_up = z_pos_up

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'Export Grid'
        try:
            self.parent().debug_print(f'surf_instance.export {self.output_path} export_type={self.export_type}, z_pos_up={self.z_pos_up}', logging.INFO)
            # None in the 4th arg to indicate you want to export all resolutions
            self.surf_instance.export(self.output_path, self.export_type, self.z_pos_up, None,
                                      override_maximum_chunk_dimension=kluster_variables.chunk_size_export,
                                      **self.bag_kwargs)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class SurfaceWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fqpr_instances = None
        self.fqpr_surface = None
        self.opts = {}
        self.mode = 'from_fqpr'

    def populate(self, fqpr_instances, opts):
        super().reset()
        self.fqpr_instances = fqpr_instances
        self.fqpr_surface = None
        self.opts = opts
        self.mode = 'from_fqpr'

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'New Surface'
        try:
            if self.mode == 'from_fqpr':
                self.parent().debug_print(f'generate_new_surface {self.opts}', logging.INFO)
                self.fqpr_surface = generate_new_surface(self.fqpr_instances, **self.opts)
            elif self.mode == 'from_points':
                self.parent().debug_print(f'points_to_surface {self.opts}', logging.INFO)
                self.fqpr_surface = points_to_surface(self.fqpr_instances, **self.opts)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class MosaicWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fqpr_instances = None
        self.fqpr_surface = None
        self.opts = {}

    def populate(self, fqpr_instances, opts):
        super().reset()
        self.fqpr_instances = fqpr_instances
        self.fqpr_surface = None
        self.opts = opts

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'New Mosaic'
        try:
            self.parent().debug_print(f'generate_new_mosaic {self.opts}', logging.INFO)
            self.fqpr_surface = generate_new_mosaic(self.fqpr_instances, **self.opts)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class SurfaceUpdateWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fqpr_surface = None
        self.add_fqpr_instances = None
        self.add_lines = None
        self.remove_fqpr_names = None
        self.remove_lines = None
        self.opts = {}
        self.all_resolutions = None

    def populate(self, fqpr_surface, add_fqpr_instances, add_lines, remove_fqpr_names, remove_lines, opts, all_resolutions):
        super().reset()
        self.fqpr_surface = fqpr_surface
        self.add_fqpr_instances = add_fqpr_instances
        self.add_lines = add_lines
        self.remove_fqpr_names = remove_fqpr_names
        self.remove_lines = remove_lines
        self.all_resolutions = all_resolutions
        self.opts = opts

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'Update Surface'
        try:
            self.parent().debug_print(f'update_surface add_fqpr={self.add_fqpr_instances}, add_lines={self.add_lines}, remove_fqpr={self.remove_fqpr_names}, remove_lines={self.remove_lines}, {self.opts}', logging.INFO)
            self.fqpr_surface, oldrez, newrez = update_surface(self.fqpr_surface, self.add_fqpr_instances, self.add_lines,
                                                               self.remove_fqpr_names, self.remove_lines, **self.opts)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)


class PatchTestUpdateWorker(MyWorker):
    """
    Executes code in a seperate thread.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fqprs = None
        self.newvalues = []
        self.headindex = None
        self.prefixes = None
        self.timestamps = None
        self.serial_number = None
        self.polygon = None
        self.vdatum_directory = None

        self.result = []

    def populate(self, fqprs=None, newvalues=None, headindex=None, prefixes=None, timestamps=None, serial_number=None,
                 polygon=None, vdatum_directory=None):
        super().reset()
        self.fqprs = fqprs
        self.newvalues = newvalues
        self.headindex = headindex
        self.prefixes = prefixes
        self.timestamps = timestamps
        self.serial_number = serial_number
        self.polygon = polygon
        self.vdatum_directory = vdatum_directory

        self.result = []

    def run(self):
        self.tstarted.emit(True)
        self.action_type = f'Patch Test'
        try:
            self.parent().debug_print(f'reprocess_fqprs fqprs={self.fqprs}, newvalues={self.newvalues}, headindex={self.headindex}, prefixes={self.prefixes}, timestamps={self.timestamps}, serial_number={self.serial_number}, polygon={self.polygon}, vdatum_directory={self.vdatum_directory}', logging.INFO)
            self.fqprs, self.result = reprocess_fqprs(self.fqprs, self.newvalues, self.headindex, self.prefixes, self.timestamps,
                                                      self.serial_number, self.polygon, self.vdatum_directory)
        except Exception as e:
            super().log_exception(e)
        self.tfinished.emit(True)
