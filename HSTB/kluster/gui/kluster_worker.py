import numpy as np
import os
from datetime import datetime

from HSTB.kluster.gui.backends._qt import QtGui, QtCore, QtWidgets, Signal

from HSTB.kluster.fqpr_convenience import generate_new_surface, import_processed_navigation, overwrite_raw_navigation, \
    update_surface

import time


class ActionWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.action_container = None
        self.action_index = None
        self.result = None
        self.action_type = None
        self.error = False

    def populate(self, action_container, action_index):
        self.action_container = action_container
        self.action_index = action_index

    def run(self):
        self.started.emit(True)
        try:
            self.action_type = self.action_container.actions[self.action_index].action_type
            self.result = self.action_container.execute_action(self.action_index)
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class OpenProjectWorker(QtCore.QThread):
    """
    Thread that runs when the user drags in a new project file or opens a project using the menu
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.new_project_path = None
        self.project = None
        self.force_add_fqprs = None
        self.force_add_surfaces = None
        self.new_fqprs = []
        self.error = False

    def populate(self, project, new_project_path=None, force_add_fqprs=None, force_add_surfaces=None):
        self.new_project_path = new_project_path
        self.project = project
        self.force_add_fqprs = force_add_fqprs
        self.force_add_surfaces = force_add_surfaces
        self.error = False

    def run(self):
        self.started.emit(True)
        try:
            self.new_fqprs = []
            if self.new_project_path:
                data = self.project._load_project_file(self.new_project_path)
            else:
                data = {'fqpr_paths': [], 'surface_paths': []}
                if self.force_add_fqprs:
                    data['fqpr_paths'] = self.force_add_fqprs
                if self.force_add_surfaces:
                    data['surface_paths'] = self.force_add_surfaces

            for pth in data['fqpr_paths']:
                print('Loading from {}'.format(pth))
                fqpr_entry, already_in = self.project.add_fqpr(pth, skip_dask=True)
                if fqpr_entry is None:  # no fqpr instance successfully loaded
                    print('update_on_file_added: Unable to add to Project from existing: {}'.format(pth))
                if already_in:
                    print('{} already exists in {}'.format(pth, self.project.path))
                elif fqpr_entry:
                    self.new_fqprs.append(fqpr_entry)
            for pth in data['surface_paths']:
                self.project.add_surface(pth)
            time.sleep(0.1)
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class DrawNavigationWorker(QtCore.QThread):
    """
    On opening a project, you have to get the navigation for each line and draw it in the 2d view
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.project = None
        self.new_fqprs = None
        self.line_data = {}
        self.error = False

    def populate(self, project, new_fqprs):
        self.project = project
        self.new_fqprs = new_fqprs
        self.error = False
        self.line_data = {}

    def run(self):
        self.started.emit(True)
        try:
            for fq in self.new_fqprs:
                print('building navigation for {}...'.format(fq))
                for ln in self.project.return_project_lines(proj=fq, relative_path=True):
                    lats, lons = self.project.return_line_navigation(ln)
                    self.line_data[ln] = [lats, lons]
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class DrawSurfaceWorker(QtCore.QThread):
    """
    On opening a new surface, you have to get the surface tiles to display as in memory geotiffs in kluster_main
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.surface_path = None
        self.surf_object = None
        self.resolution = None
        self.surface_layer_name = None
        self.surface_data = {}
        self.error = False

    def populate(self, surface_path, surf_object, resolution, surface_layer_name):
        self.surface_path = surface_path
        self.surf_object = surf_object
        self.resolution = resolution
        self.surface_layer_name = surface_layer_name
        self.error = False
        self.surface_data = {}

    def run(self):
        self.started.emit(True)
        try:
            for resolution in self.resolution:
                self.surface_data[resolution] = {}
                chunk_count = 1
                for geo_transform, maxdim, data in self.surf_object.get_chunks_of_tiles(resolution=resolution, layer=self.surface_layer_name,
                                                                                        nodatavalue=np.float32(np.nan), z_positive_up=False,
                                                                                        for_gdal=True):
                    data = list(data.values())
                    self.surface_data[resolution][self.surface_layer_name + '_{}'.format(chunk_count)] = [data, geo_transform]
                    chunk_count += 1
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class ImportNavigationWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []
        self.error = False

    def populate(self, fq_chunks):
        self.fq_chunks = fq_chunks
        self.error = False

    def run(self):
        self.started.emit(True)
        try:
            for chnk in self.fq_chunks:
                self.fqpr_instances.append(import_processed_navigation(chnk[0], **chnk[1]))
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class OverwriteNavigationWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []
        self.error = False

    def populate(self, fq_chunks):
        self.fq_chunks = fq_chunks
        self.error = False

    def run(self):
        self.started.emit(True)
        try:
            for chnk in self.fq_chunks:
                self.fqpr_instances.append(overwrite_raw_navigation(chnk[0], **chnk[1]))
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class ExportWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fq_chunks = None
        self.fqpr_instances = []
        self.export_type = ''
        self.z_pos_down = False
        self.delimiter = ' '
        self.filterset = False
        self.separateset = False
        self.error = False

    def populate(self, fq_chunks, export_type, z_pos_down, delimiter, filterset, separateset):
        self.fq_chunks = fq_chunks
        self.export_type = export_type
        self.z_pos_down = z_pos_down
        if delimiter == 'comma':
            self.delimiter = ','
        elif delimiter == 'space':
            self.delimiter = ' '
        else:
            raise ValueError('ExportWorker: Expected either "comma" or "space", received {}'.format(delimiter))
        self.filterset = filterset
        self.separateset = separateset
        self.error = False

    def export_process(self, fq):
        fq.export_pings_to_file(file_format=self.export_type, csv_delimiter=self.delimiter, filter_by_detection=self.filterset,
                                z_pos_down=self.z_pos_down, export_by_identifiers=self.separateset)
        return fq

    def run(self):
        self.started.emit(True)
        try:
            for chnk in self.fq_chunks:
                self.fqpr_instances.append(self.export_process(chnk[0]))
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class ExportGridWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.surf_instance = None
        self.export_type = ''
        self.output_path = ''
        self.z_pos_up = True
        self.bag_kwargs = {}
        self.error = False

    def populate(self, surf_instance, export_type, output_path, z_pos_up, bag_kwargs):
        self.surf_instance = surf_instance
        self.export_type = export_type
        self.output_path = output_path
        self.bag_kwargs = bag_kwargs
        self.z_pos_up = z_pos_up
        self.error = False

    def run(self):
        self.started.emit(True)
        try:
            # None in the 4th arg to indicate you want to export all resolutions
            self.surf_instance.export(self.output_path, self.export_type, self.z_pos_up, None, **self.bag_kwargs)
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class SurfaceWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fqpr_instances = None
        self.fqpr_surface = None
        self.opts = {}
        self.error = False

    def populate(self, fqpr_instances, opts):
        self.fqpr_instances = fqpr_instances
        self.opts = opts
        self.error = False

    def run(self):
        self.started.emit(True)
        try:
            self.fqpr_surface = generate_new_surface(self.fqpr_instances, **self.opts)
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)


class SurfaceUpdateWorker(QtCore.QThread):
    """
    Executes code in a seperate thread.
    """

    started = Signal(bool)
    finished = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.fqpr_surface = None
        self.add_fqpr_instances = None
        self.remove_fqpr_instances = None
        self.opts = {}
        self.error = False

    def populate(self, fqpr_surface, add_fqpr_instances, remove_fqpr_instances, opts):
        self.fqpr_surface = fqpr_surface
        self.add_fqpr_instances = add_fqpr_instances
        self.remove_fqpr_instances = remove_fqpr_instances
        self.opts = opts
        self.error = False

    def run(self):
        self.started.emit(True)
        try:
            self.fqpr_surface = update_surface(self.fqpr_surface, self.add_fqpr_instances, self.remove_fqpr_instances,
                                               **self.opts)
        except Exception as e:
            print(e)
            self.error = True
        self.finished.emit(True)
