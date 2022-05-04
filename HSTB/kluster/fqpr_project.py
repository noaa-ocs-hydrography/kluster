import os
import numpy as np
import xarray as xr
import json
from typing import Union
from datetime import datetime, timezone
from types import FunctionType
import logging

from HSTB.kluster.fqpr_generation import Fqpr
from HSTB.kluster.dask_helpers import dask_find_or_start_client, client_needs_restart
from HSTB.kluster.fqpr_convenience import reload_data, reload_surface, get_attributes_from_fqpr, reprocess_sounding_selection
from HSTB.kluster.xarray_helpers import slice_xarray_by_dim
from HSTB.kluster.fqpr_helpers import haversine
from HSTB.kluster.fqpr_vessel import VesselFile, create_new_vessel_file, convert_from_fqpr_xyzrph, compare_dict_data, \
    split_by_timestamp, trim_xyzrprh_to_times
from HSTB.kluster.modules.autopatch import PatchTest
from bathygrid.bgrid import BathyGrid
from HSTB.kluster.logging_conf import LoggerClass


class FqprProject(LoggerClass):
    """
    The FqprProject class contains all the fqpr_generated.Fqpr objects and has methods for interacting with multiple
    of these objects as one big project.

    Each fqpr_generated.Fqpr object is like a container of lines, it can either be a single processed line, a bunch of
    lines for one day, a week's worth of lines, etc.  If you want to find which container has which line, or find all
    lines within a specific area, the FqprProject should be able to do this.

    | proj_data contains the paths to the top level folders for two Fqpr generated objects
    | C:/data_dir/EM2040/convert1
    | C:/data_dir/EM2040/convert1/attitude.zarr
    | C:/data_dir/EM2040/convert1/soundings.zarr
    | C:/data_dir/EM2040/convert1/navigation.zarr
    | C:/data_dir/EM2040/convert1/ping_40107_0_260000.zarr
    | C:/data_dir/EM2040/convert1/ping_40107_1_320000.zarr
    | C:/data_dir/EM2040/convert1/ping_40107_2_290000.zarr
    | C:/data_dir/EM2040/convert1/logfile_094418.txt

    | proj_data = [r"C:/data_dir/convert1", r"C:/data_dir/convert2"]
    | fqp = FqprProject()
    | for pd in proj_data:
    |    fqp.add_fqpr(pd, skip_dask=True)
    """

    def __init__(self, is_gui: bool = False, **kwargs):
        """

        Parameters
        ----------
        is_gui
             if True, this project is attached to a gui, so we disable progress so that we aren't filling up the output window
        """
        super().__init__(**kwargs)

        self.client = None
        self.path = None
        self.is_gui = is_gui
        self.file_format = 1.0

        self.vessel_file = None

        # all paths are relative to the project file location...

        # bathygrid.bgrid.BathyGrid instances per grid folder path, see add_surface
        # ex: {'vrtilegrid_mean': <bathygrid.maingrid.VRGridTile object at 0x0000015013FD5760>}
        self.surface_instances = {}

        # fqpr_generation.FQPR instances per converted folder path, see add_fqpr
        # ex: {'EM2040\\convert1': <HSTB.kluster.fqpr_generation.Fqpr at 0x25f910e8eb0>}
        self.fqpr_instances = {}

        # line names and start/stop times per line per converted folder path, see regenerate_fqpr_lines
        # ex: {'EM2040\\convert1': {'0001_20170822_144548_S5401_X.all': [1503413148.045, 1503413720.475]}
        self.fqpr_lines = {}

        # fqpr attribution per converted folder path, see add_fqpr
        # ex: {'EM2040\\convert1': {'frequency_identifier': [260000, 320000, 290000], ...}
        self.fqpr_attrs = {}

        # converted folder path per line name, see regenerate_fqpr_lines
        # ex: {'0001_20170822_144548_S5401_X.all': 'EM2040\\convert1'}
        self.convert_path_lookup = {}

        # project settings, like the chosen vertical reference
        # ex: {'use_epsg': True, 'epsg': 26910, ...}
        self.settings = {}

        self.buffered_fqpr_navigation = {}
        self.point_cloud_for_line = {}
        self.node_vals_for_surf = {}

        self._project_observers = []

    def path_relative_to_project(self, pth: str):
        """
        Return the relative path for the provided pth from the project file

        Parameters
        ----------
        pth
            absolute file path

        Returns
        -------
        str
            relative file path from the project file
        """
        if self.path is None:
            self.print_msg('path_relative_to_project: path to project file not setup, is currently undefined.', logging.ERROR)
            raise ValueError('path_relative_to_project: path to project file not setup, is currently undefined.')
        return os.path.relpath(pth, os.path.dirname(self.path))

    def absolute_path_from_relative(self, pth: str):
        """
        see path_relative_to_project, will convert the returned relative path from that method to an absolute file
        path

        Parameters
        ----------
        pth
            relative file path from the directory containing the project file

        Returns
        -------
        str
            absolute file path
        """

        if self.path is None:
            self.print_msg('absolute_path_from_relative: path to project file not setup, is currently undefined.', logging.ERROR)
            raise ValueError('absolute_path_from_relative: path to project file not setup, is currently undefined.')
        return os.path.abspath(os.path.join(os.path.dirname(self.path), pth))

    def _setup_new_project(self, pth: str):
        """
        Automatically run on adding new fqpr instances.  Will save the project file in the same directory as the data

        Give the path to the project folder, all stored paths to fqpr instances will be relative to this path

        Parameters
        ----------
        pth
            path to the folder where you want a new project or existing project file

        """
        if self.path is None:
            if os.path.isdir(pth):  # user provided a directory
                self.path = os.path.join(pth, 'kluster_project.json')
            else:
                self.path = pth

    def _load_project_file(self, projfile: str):
        """
        Load from saved json project file, return the data in the file.  Used in open project.

        Parameters
        ----------
        projfile
            path to the project file

        Returns
        -------
        dict
            loaded project file data
        """

        if os.path.split(projfile)[1] != 'kluster_project.json':
            self.print_msg('_load_project_file: Expected a file named kluster_project.json, found {}'.format(projfile), logging.ERROR)
            raise IOError('_load_project_file: Expected a file named kluster_project.json, found {}'.format(projfile))
        with open(projfile, 'r') as pf:
            data = json.load(pf)
        # now translate the relative paths to absolute
        self.path = projfile
        if 'vessel_file' in data:
            if data['vessel_file']:
                self.vessel_file = self.absolute_path_from_relative(data['vessel_file'])
                if not os.path.exists(self.vessel_file):
                    self.print_msg('_load_project_file: Unable to find vessel file: {}'.format(self.vessel_file), logging.ERROR)
                    self.vessel_file = None
                    data['vessel_file'] = None
        else:
            data['vessel_file'] = None
        data['fqpr_paths'] = [self.absolute_path_from_relative(f) for f in data['fqpr_paths']]
        data['surface_paths'] = [self.absolute_path_from_relative(f) for f in data['surface_paths']]
        for ky in ['fqpr_paths', 'surface_paths']:
            for fil in data[ky]:
                if not os.path.exists(fil):
                    self.print_msg('_load_project_file: Unable to find {}'.format(fil), logging.ERROR)
                    data[ky].remove(fil)
        return data

    def _bind_to_project_updated(self, callback: FunctionType):
        """
        Connect the provided callback function to the observers list.  callback is called when add_fqpr/remove_fqpr is
        used.

        Parameters
        ----------
        callback
            function to be called when the add/remove is called
        """

        self._project_observers.append(callback)

    def get_dask_client(self):
        """
        Project is the holder of the Dask client object.  Use this method to return the current Client.  Client is
        currently setup with kluster_main.start_dask_client or kluster_main.open_dask_dashboard

        If the client does not exist, we set it here and then set the client to the Fqpr and BatchRead instance
        """

        if self.client is None or (self.client.status != 'running'):
            self.client = dask_find_or_start_client()
        needs_restart = client_needs_restart(self.client)  # handle memory leaks by restarting if memory utilization on fresh client is > 50%
        if needs_restart:
            self.client.restart()
        for fqname, fqinstance in self.fqpr_instances.items():
            fqinstance.client = self.client
            fqinstance.multibeam.client = self.client
        return self.client

    def new_project_from_directory(self, directory_path: str):
        """
        Take in a path to a directory where we want to build a new project.  This can be an empty project (if the
        directory provided is empty) or a populated project with the converted data in the provided directory.

        Parameters
        ----------
        directory_path
            Path to a directory that is either empty or has converted data in it
        """

        for fil in os.listdir(directory_path):
            full_path = os.path.join(directory_path, fil)
            if os.path.isdir(full_path):
                self.add_fqpr(full_path, skip_dask=True)
            # elif os.path.isfile(full_path):  # skip trying to load surfaces, we don't have a good way to tell, could just try except i guess
            #     self.add_surface(full_path)
        self.path = os.path.join(directory_path, 'kluster_project.json')
        self.save_project()

    def save_project(self):
        """
        Save the current FqprProject instance to file.  Use open_project to reload this instance.
        """

        if self.path is None:
            self.print_msg('kluster_project save_project - no data found, you must add data before saving a project', logging.ERROR)
            raise EnvironmentError('kluster_project save_project - no data found, you must add data before saving a project')
        if os.path.exists(self.path):
            try:
                data = self._load_project_file(self.path)
                data['fqpr_paths'] = [self.path_relative_to_project(pth) for pth in data['fqpr_paths']]
                data['surface_paths'] = [self.path_relative_to_project(pth) for pth in data['surface_paths']]
            except:
                self.print_msg('save_project: Unable to read from project file: {}'.format(self.path), logging.WARNING)
                data = {'fqpr_paths': [], 'surface_paths': [], 'vessel_file': None}
        else:
            data = {'fqpr_paths': [], 'surface_paths': [], 'vessel_file': None}
        with open(self.path, 'w') as pf:
            data['fqpr_paths'] = list(set(self.return_fqpr_paths() + data['fqpr_paths']))
            data['surface_paths'] = list(set(self.return_surface_paths() + data['surface_paths']))
            data['file_format'] = self.file_format
            if self.vessel_file:
                data['vessel_file'] = self.path_relative_to_project(self.vessel_file)
            data.update(self.settings)
            json.dump(data, pf, sort_keys=True, indent=4)
        self.print_msg('Project saved to {}'.format(self.path), logging.INFO)

    def open_project(self, projfile: str, skip_dask: bool = False):
        """
        Open a project from file.  See save_project for how to generate this file.

        Parameters
        ----------
        projfile
            path to the project file
        skip_dask
            if True, will not autostart a dask client. client is necessary for conversion/processing
        """

        data = self._load_project_file(projfile)
        self.path = projfile
        self.file_format = data['file_format']

        for pth in data['fqpr_paths']:
            if os.path.exists(pth):
                self.add_fqpr(pth, skip_dask=skip_dask)
            else:  # invalid path
                self.print_msg('open_project: Unable to find converted data: {}'.format(pth), logging.ERROR)

        for pth in data['surface_paths']:
            if os.path.exists(pth):
                self.add_surface(pth)
            else:  # invalid path
                self.print_msg('open_project: Unable to find surface: {}'.format(pth), logging.ERROR)

        data.pop('vessel_file')
        data.pop('fqpr_paths')
        data.pop('surface_paths')
        data.pop('file_format')
        # rest of the data belongs in settings
        self.settings = data

    def add_vessel_file(self, vessel_file_path: str = None, update_with_project: bool = True):
        """
        Attach a new or existing vessel file to this project.  Optionally populate it with the found offsets and angles
        in the existing fqpr instances in the project

        Parameters
        ----------
        vessel_file_path
            path to the new or existing vessel file
        update_with_project
            if True, will update the vessel file with the offsets and angles of all the fqpr instances in the project
        """

        if vessel_file_path:
            vessel_file = vessel_file_path
        elif self.path:
            vessel_file = os.path.join(os.path.dirname(self.path), 'vessel_file.kfc')
        else:
            self.print_msg('add_vessel_file: Unable to setup new vessel file, save the project or add data first.', logging.ERROR)
            return
        if not os.path.exists(vessel_file):
            create_new_vessel_file(vessel_file)
        self.vessel_file = vessel_file
        if update_with_project:
            vess_file = self.return_vessel_file()
            for fq, fqpr in self.fqpr_instances.items():
                serial_number = fqpr.multibeam.raw_ping[0].system_identifier
                sonar_type = fqpr.multibeam.raw_ping[0].sonartype
                output_identifier = os.path.split(fqpr.output_folder)[1]
                vess_xyzrph = convert_from_fqpr_xyzrph(fqpr.multibeam.xyzrph, sonar_type, serial_number, output_identifier)
                vess_file.update(serial_number, vess_xyzrph[serial_number])
            vess_file.save()

    def close(self):
        """
        close project and clear all data.  have to close the fqpr instances with the fqpr close method.
        """
        for fq, fqinst in self.fqpr_instances.items():
            fqinst.close()

        self.path = None
        self.vessel_file = None
        self.surface_instances = {}
        self.fqpr_instances = {}
        self.fqpr_lines = {}
        self.fqpr_attrs = {}
        self.convert_path_lookup = {}
        self.buffered_fqpr_navigation = {}
        self.point_cloud_for_line = {}
        self.node_vals_for_surf = {}

    def set_settings(self, settings: dict):
        """
        Set the project settings with the provided dictionary.  Pull out fqpr specific settings like whether or not
        to enable parallel write as well

        Parameters
        ----------
        settings
            dictionary from the Qsettings store, see kluster_main._load_previously_used_settings
        """

        self.settings.update(settings)
        for relpath, fqpr_instance in self.fqpr_instances.items():
            self._update_fqpr_settings(fqpr_instance)
        self.save_project()

    def _update_fqpr_settings(self, fq: Fqpr):
        """
        Update an FQPR instance with the latest settings
        """

        if 'parallel_write' in self.settings:
            fq.parallel_write = self.settings['parallel_write']
        if 'filter_directory' in self.settings:
            fq.filter.external_filter_directory = self.settings['filter_directory']

    def add_fqpr(self, pth: Union[str, Fqpr], skip_dask: bool = False):
        """
        Add a new Fqpr object to this project.  If skip_dask is True, will auto start a new dask LocalCluster

        Parameters
        ----------
        pth
            path to the top level folder for the Fqpr project or the already loaded Fqpr instance itself
        skip_dask
            if True will skip auto starting a dask LocalCluster

        Returns
        -------
        str
            project entry in the dictionary, will be the relative path to the kluster data store from the project file
        bool
            False if the fqpr was already in the project, True if added
        """

        if type(pth) == str:
            fq = reload_data(pth, skip_dask=skip_dask, silent=True, show_progress=not self.is_gui)
        else:  # pth is the new Fqpr instance, pull the actual path from the Fqpr attribution
            fq = pth
            pth = os.path.normpath(fq.multibeam.raw_ping[0].output_path)

        if fq is not None:
            if self.path is None:
                self._setup_new_project(os.path.dirname(pth))
            relpath = self.path_relative_to_project(pth)
            if relpath in self.fqpr_instances:
                already_in = True
            else:
                already_in = False
            self._update_fqpr_settings(fq)
            self.fqpr_instances[relpath] = fq
            self.fqpr_attrs[relpath] = get_attributes_from_fqpr(fq, include_mode=False)
            self.regenerate_fqpr_lines(relpath)
            for callback in self._project_observers:
                callback(True)
            self.print_msg('Successfully added {}'.format(pth), logging.INFO)
            return relpath, already_in
        return None, False

    def remove_fqpr(self, pth: str, relative_path: bool = False):
        """
        Remove an attached Fqpr instance from the project by path to Fqpr converted folder

        Parameters
        ----------
        pth
            path to the top level folder for the Fqpr project
        relative_path
            if True, pth is a relative path (relative to self.path)
        """

        if relative_path:
            relpath = pth
        else:
            relpath = self.path_relative_to_project(pth)

        if relpath in self.fqpr_instances:
            self.fqpr_instances[relpath].close(close_dask=False)
            self.fqpr_instances.pop(relpath)
            if relpath in self.fqpr_attrs:
                self.fqpr_attrs.pop(relpath)
            else:
                self.print_msg('remove_fqpr: On removing from project, unable to find attributes for {}'.format(relpath), logging.WARNING)
            for linename in self.fqpr_lines[relpath]:
                if linename in self.convert_path_lookup:
                    self.convert_path_lookup.pop(linename)
                else:
                    self.print_msg('remove_fqpr: On removing from project, unable to find loaded line attributes for {} in {}'.format(linename, relpath), logging.WARNING)
            if relpath in self.fqpr_lines:
                self.fqpr_lines.pop(relpath)
            else:
                self.print_msg('remove_fqpr: On removing from project, unable to find loaded lines for {}'.format(relpath), logging.WARNING)
            for callback in self._project_observers:
                callback(True)
        else:
            self.print_msg('remove_fqpr: Unable to remove instance {}'.format(relpath), logging.ERROR)

    def refresh_fqpr_attribution(self, pth: str, relative_path: bool = False):
        if relative_path:
            relpath = pth
        else:
            relpath = self.path_relative_to_project(pth)
        if relpath in self.fqpr_instances:
            fq = self.fqpr_instances[relpath]
            self.fqpr_attrs[relpath] = get_attributes_from_fqpr(fq, include_mode=False)
        else:
            self.print_msg('refresh_fqpr_attribution: {} not found in project, unable to refresh attribution'.format(relpath), logging.WARNING)

    def add_surface(self, pth: Union[str, BathyGrid]):
        """
        Add a new Bathygrid object to the project, either by loading from file or by directly adding a Bathygrid
        object provided

        Parameters
        ----------
        pth
            path to surface file or existing Bathygrid object
        """

        if type(pth) == str:
            bg = reload_surface(pth)
            pth = os.path.normpath(pth)
        else:  # fq is the new Fqpr instance, pth is the output path that is saved as an attribute
            bg = pth
            pth = os.path.normpath(bg.output_folder)
        if bg is not None:
            if self.path is None:
                self._setup_new_project(os.path.dirname(pth))
            relpath = self.path_relative_to_project(pth)
            self.surface_instances[relpath] = bg
            self.print_msg('Successfully added {}'.format(pth), logging.INFO)

    def remove_surface(self, pth: str, relative_path: bool = False):
        """
        Remove an attached Bathygrid instance from the project by path to Fqpr converted folder

        Parameters
        ----------
        pth
            path to the surface file
        relative_path
            if True, pth is a relative path (relative to self.path)
        """

        if relative_path:
            relpath = pth
        else:
            relpath = self.path_relative_to_project(pth)

        if relpath in self.surface_instances:
            self.surface_instances.pop(relpath)

    def build_raw_attitude_for_line(self, line: str, subset: bool = True):
        """
        With the given linename, return the raw_attitude dataset from the fqpr_generation.FQPR instance that contains
        the line.  If subset is true, the returned attitude will only be the raw attitude that covers the line.

        Parameters
        ----------
        line
            line name
        subset
            if True will only return the dataset cut to the min max time of the multibeam line

        Returns
        -------
        xr.Dataset
            the raw attitude either for the whole Fqpr instance that contains the line, or subset to the min/max time of the line
        """

        line_att = None
        fq_inst = self.return_line_owner(line)
        if fq_inst is not None:
            line_att = fq_inst.multibeam.raw_att
            if subset:
                # attributes are all the same across raw_ping datasets, just use the first
                line_start_time, line_end_time = fq_inst.multibeam.raw_ping[0].multibeam_files[line][0], fq_inst.multibeam.raw_ping[0].multibeam_files[line][1]
                line_att = slice_xarray_by_dim(line_att, dimname='time', start_time=line_start_time, end_time=line_end_time)
        return line_att

    def regenerate_fqpr_lines(self, pth: str):
        """
        After adding a new Fqpr object, we want to get the line information from the attributes so that we can quickly
        access how many lines are in a project, and the time boundaries of these lines.

        Parameters
        ----------
        pth
            path to the Fqpr object
        """
        for fq_name, fq_inst in self.fqpr_instances.items():
            if fq_name == pth:
                self.fqpr_lines[fq_name] = fq_inst.return_line_dict()
                for linename in self.fqpr_lines[fq_name]:
                    self.convert_path_lookup[linename] = pth

    def build_visualizations(self, pth: str, visualization_type: str):
        """
        Take the provided project path and create visualizations of that project

        Parameters
        ----------
        pth
            path to the Fqpr object
        visualization_type
            one of 'orientation', 'beam_vectors', 'corrected_beam_vectors'
        """

        for fq_name, fq_inst in self.fqpr_instances.items():
            if fq_name == pth:
                if visualization_type == 'orientation':
                    fq_inst.plot.visualize_orientation_vector()
                elif visualization_type == 'beam_vectors':
                    fq_inst.plot.visualize_beam_pointing_vectors(corrected=False)
                elif visualization_type == 'corrected_beam_vectors':
                    fq_inst.plot.visualize_beam_pointing_vectors(corrected=True)
                else:
                    self.print_msg("build_visualizations: Expected one of 'orientation', 'beam_vectors', 'corrected_beam_vectors', got {}".format(visualization_type), logging.ERROR)
                    raise ValueError("build_visualizations: Expected one of 'orientation', 'beam_vectors', 'corrected_beam_vectors', got {}".format(visualization_type))

    def return_line_owner(self, line: str):
        """
        Return the Fqpr instance that contains the provided multibeam line

        Parameters
        ----------
        line
            line name

        Returns
        -------
        Fqpr
            None if you can't find a line owner, else the fqpr_generation.Fqpr object associated with the line
        """

        if line in self.convert_path_lookup:
            convert_pth = self.convert_path_lookup[line]
            return self.fqpr_instances[convert_pth]
        else:
            self.print_msg('return_line_owner: Unable to find project for line {}'.format(line), logging.ERROR)
            return None

    def return_surface_paths(self):
        """
        Get the absolute paths to all loaded surface instances

        Returns
        -------
        list
            list of str paths to all surface instances
        """
        pths = list(self.surface_instances.keys())
        return pths

    def return_fqpr_paths(self):
        """
        Get the absolute paths to all loaded fqpr instances

        Returns
        -------
        list
            list of str paths to all fqpr instances
        """
        pths = list(self.fqpr_instances.keys())
        return pths

    def return_fqpr_instances(self):
        """
        Get all loaded fqpr instances

        Returns
        -------
        list
            list of fqpr_generation.Fqpr objects
        """

        return list(self.fqpr_instances.values())

    def return_project_lines(self, proj: str = None, relative_path: bool = True):
        """
        Return the lines associated with the provided Fqpr path (proj) or all projects/lines

        Parameters
        ----------
        proj
            optional, str, Fqpr path if you only want lines associated with that project
        relative_path
            if True, proj is a relative path (relative to self.path)

        Returns
        -------
        dict
            all line names in the project or just the line names associated with proj
        """

        if proj is not None:
            if type(proj) is str:
                if relative_path:
                    return self.fqpr_lines[proj]
                else:
                    return self.fqpr_lines[self.path_relative_to_project(proj)]
            else:
                self.print_msg('return_project_lines: expected a string path to be provided to the kluster fqpr datastore', logging.ERROR)
                return None
        return self.fqpr_lines

    def return_sorted_line_list(self):
        """
        Return all lines in the project sorted by name

        Returns
        -------
        dict
            sorted list of line names
        """

        total_lines = []
        for fq_proj in self.fqpr_lines:
            for fq_line in self.fqpr_lines[fq_proj]:
                total_lines.append(fq_line)
        return sorted(total_lines)

    def return_line_navigation(self, line: str):
        """
        For given line name, return the latitude/longitude from the ping record

        Parameters
        ----------
        line
            line name

        Returns
        -------
        np.array
            latitude values (geographic) downsampled in degrees
        np.array
            longitude values (geographic) downsampled in degrees
        """

        if line not in self.buffered_fqpr_navigation:
            fq_inst = self.return_line_owner(line)
            if fq_inst is not None:
                line_start_time, line_end_time = fq_inst.multibeam.raw_ping[0].multibeam_files[line][0], fq_inst.multibeam.raw_ping[0].multibeam_files[line][1]
                nav = fq_inst.return_navigation(line_start_time, line_end_time)
                if nav is not None:
                    lat, lon = nav.latitude.values, nav.longitude.values
                    # save nav so we don't have to redo this routine if asked for the same line
                    self.buffered_fqpr_navigation[line] = [lat, lon]
                else:
                    self.print_msg('No navigation found for line {}'.format(line), logging.ERROR)
                    return None, None
            else:
                self.print_msg('{} not found in project'.format(line), logging.ERROR)
                return None, None
        else:
            lat, lon = self.buffered_fqpr_navigation[line]
        return lat, lon

    def return_lines_in_box(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float):
        """
        With the given latitude/longitude boundaries, return the lines that are completely within these boundaries

        Parameters
        ----------
        min_lat
            float, minimum latitude in degrees
        max_lat
            float, maximum latitude in degrees
        min_lon
            float, minimum longitude in degrees
        max_lon
            float, maximum longitude in degrees

        Returns
        -------
        list
            line names that fall within the box
        """

        lines_in_box = []

        for fq_proj in self.fqpr_lines:
            for fq_line in self.fqpr_lines[fq_proj]:
                lats, lons = self.return_line_navigation(fq_line)

                line_min_lat = np.min(lats)
                line_max_lat = np.max(lats)
                line_min_lon = np.min(lons)
                line_max_lon = np.max(lons)

                if (line_max_lat < max_lat) and (line_min_lat > min_lat) and (line_max_lon < max_lon) and \
                        (line_min_lon > min_lon):
                    lines_in_box.append(fq_line)
        return lines_in_box

    def return_soundings_in_polygon(self, polygon: np.ndarray):
        """
        With the given latitude/longitude polygon, return the soundings that are within the boundaries.  Use the
        Fqpr horizontal_crs recorded EPSG to do the transformation to northing/easting, and then query all the x, y to get
        the soundings.

        If full swath is used return the whole swaths that are within the bounds.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon,  (longitude, latitude) in degrees

        Returns
        -------
        dict
            dict where keys are the fqpr instance name, values are the sounding values as 1d arrays
        """
        data = {}
        for fq_name, fq_inst in self.fqpr_instances.items():
            fq_inst.ping_filter = []  # reset ping filter for all instances when you try and make a new selection
            # if fq_inst.intersects(polygon[:, 1].min(), polygon[:, 1].max(), polygon[:, 0].min(), polygon[:, 0].max(), geographic=True):  # rely on geohash intersect instead
            head, x, y, z, tvu, rejected, pointtime, beam = fq_inst.return_soundings_in_polygon(polygon, geographic=True)
            if x is not None:
                linenames = fq_inst.return_lines_for_times(pointtime)
                data[fq_name] = [head, x, y, z, tvu, rejected, pointtime, beam, linenames]
        return data

    def return_project_folder(self):
        """
        Return the project folder, the folder that contains the project file

        Returns
        -------
        str
            either None (if the project hasn't been set up yet) or the folder containing the kluster_project.json file
        """
        if self.path:
            return os.path.dirname(self.path)
        else:
            return None

    def get_fqpr_by_serial_number(self, primary_serial_number: int, secondary_serial_number: int, same_day_as: datetime = None):
        """
        Find the fqpr instance that matches the provided serial number.  Should just be one instance in a project with
        the same serial number, if there are more, that is going to be a problem.

        Parameters
        ----------
        primary_serial_number
            primary serial number for the system you want to find, primary serial number will just be the serial number
            of the port system for dual head kongsberg
        secondary_serial_number
            secondary serial number for the system you want to find, this will be zero if not dual head, otherwise it
            is the serial number of the starboard head
        same_day_as
            optional, if provided wil only return an Fqpr instance if it is on the same day as the provided datetime object

        Returns
        -------
        str
            folder path to the fqpr instance
        Fqpr
            fqpr instance that matches the serial numbers provided
        """

        out_path = None
        out_instance = None
        matches = 0
        for fqpr_path, fqpr_instance in self.fqpr_instances.items():
            if primary_serial_number in fqpr_instance.multibeam.raw_ping[0].system_serial_number:
                if secondary_serial_number in fqpr_instance.multibeam.raw_ping[0].secondary_system_serial_number:
                    if same_day_as:
                        fq_day = datetime.fromtimestamp(fqpr_instance.multibeam.raw_ping[0].time.values[0], tz=timezone.utc)
                        if fq_day.timetuple().tm_yday != same_day_as.timetuple().tm_yday:
                            continue
                    out_path = self.absolute_path_from_relative(fqpr_path)
                    out_instance = fqpr_instance
                    matches += 1
        if matches > 1:
            raise ValueError("Found {} matches by serial number, project should not have multiple fqpr instances with the same serial number".format(matches))
        return out_path, out_instance

    def get_fqprs_by_paths(self, fqpr_paths: list, line_dict: dict = None, relative_path: bool = True,
                           allow_different_offsets: bool = True, concatenate: bool = True, raise_exception: bool = False):
        """
        Return a list of the paths and Fqpr instances associated with each provided path.  Path can be relative to the project
        or an absolute path.

        If a line_dict is provided, we will subset each Fqpr object to just the data associated with the lines in the line_dict.
        If you provide multiple fqpr_paths, it will take the data for the lines across all fqpr_paths provided, and merge
        them into one Fqpr object.  So if you provide 2 fqpr_paths, and a line_dict that includes 2 lines from each fqpr
        object, the end result is a single Fqpr object with the data for all four lines across the two original Fqpr instances.

        multibeam_files attribute the in the returned fqpr object is adjusted for the actual lines in the Fqpr returned.

        Parameters
        ----------
        fqpr_paths
            list of relative/absolute paths to the Fqpr objects we want
        line_dict
            dict of {fqpr_path: [line1, line2, ...]} for the desired lines
        relative_path
            if True, provided paths are relative to the project, otherwise they are absolute paths
        allow_different_offsets
            if True, and concatenate is True, will concatenate Fqpr instances that have different offsets/angles.
            Otherwise will reject.
        concatenate
            if True, will concatenate the fqprs into one object, only possible if the sonar serial numbers match
        raise_exception
            if True, will raise an exception instead of handling the error

        Returns
        -------
        list
            absolute paths to the fqprs queried
        list
            list of loaded fqpr instances
        """

        fqpr_loaded = []
        fqpr_abs_paths = []
        for fq in fqpr_paths:
            if relative_path:
                fq_path = self.absolute_path_from_relative(fq)
            else:
                fq_path = fq
                fq = self.path_relative_to_project(fq)
            fqpr_abs_paths.append(fq_path)  # the full file path to the Fqpr data
            if fq not in self.fqpr_instances:
                self.print_msg('get_fqprs_by_paths: Unable to find {} in project'.format(fq), logging.WARNING)
                if not raise_exception:
                    fqpr_loaded.append(None)
                    continue
                else:
                    raise ValueError('get_fqprs_by_paths: Unable to find {} in project'.format(fq))
            if line_dict:  # only get the data for the desired lines
                if fq in line_dict:
                    fqlines = line_dict[fq]
                    basefq = self.fqpr_instances[fq].copy()
                    basefq.subset_by_lines(fqlines)  # trim the data to the desired lines that happen to be in this Fqpr object
                    basefq.subset.backup_fqpr = {}  # don't retain the backup, we are making a whole new fqpr object here
                    fqpr_loaded.append(basefq)
                else:  # this Fqpr instance does not contain any selected lines
                    fqpr_loaded.append(None)
            else:
                fqpr_loaded.append(self.fqpr_instances[fq])
        if line_dict and concatenate:
            sysids = [fq.multibeam.raw_ping[0].attrs['system_serial_number'][0] for fq in fqpr_loaded]
            if not all([sysids[0] == sid for sid in sysids]):
                self.print_msg('get_fqprs_by_paths: Data from multiple different sonars found, returning only the data for the first selected sonar', logging.WARNING)
                if not raise_exception:
                    return [fqpr_abs_paths[0]], [fqpr_loaded[0]]
                else:
                    raise ValueError('get_fqprs_by_paths: Data from multiple different sonars found')
            first_xyzrph = fqpr_loaded[0].multibeam.xyzrph
            for fq in fqpr_loaded:
                offsets, angles, _, _, _ = compare_dict_data(first_xyzrph, fq.multibeam.xyzrph)
                if not offsets or not angles:
                    if allow_different_offsets:
                        self.print_msg('get_fqprs_by_paths: loading data for selected lines when installation offsets/angles do not match between converted instances', logging.WARNING)
                    else:
                        self.print_msg('get_fqprs_by_paths: loading data for selected lines when installation offsets/angles do not match between converted instances is not allowed, returning only the data for the first selected sonar', logging.ERROR)
                        if not raise_exception:
                            return [fqpr_abs_paths[0]], [fqpr_loaded[0]]
                        else:
                            raise ValueError('get_fqprs_by_paths: loading data for selected lines when installation offsets/angles do not match between converted instances is not allowed')

            # ensure they are sorted in time before concatenating
            fqpr_loaded = sorted(fqpr_loaded, key=lambda tst: tst.multibeam.raw_ping[0].time.values[0])
            final_fqpr = fqpr_loaded[0].copy()
            try:
                final_fqpr.multibeam.raw_ping = [xr.concat([fq.multibeam.raw_ping[cnt] for fq in fqpr_loaded], dim='time') for cnt in range(len(fqpr_loaded[0].multibeam.raw_ping))]
            except ValueError:
                # must have sbet or some other variable that is in one dataset but not in another, you must have the same variables
                #  across all datasets that you are merging
                for cnt in range(len(fqpr_loaded[0].multibeam.raw_ping)):  # for each sonar head
                    fkeys = [set(list(fq.multibeam.raw_ping[cnt].variables.keys())) for fq in fqpr_loaded]
                    commonkeys = fkeys[0].intersection(*fkeys)
                    for fq in fqpr_loaded:  # for each dataset
                        dropthese = [ky for ky in fq.multibeam.raw_ping[cnt].variables.keys() if ky not in commonkeys]
                        if dropthese:
                            self.print_msg('get_fqprs_by_paths: forced to drop {} when merging these datasets, variables found in one dataset but not the other'.format(dropthese), logging.WARNING)
                            fq.multibeam.raw_ping[cnt] = fq.multibeam.raw_ping[cnt].drop(dropthese)
                final_fqpr.multibeam.raw_ping = [xr.concat([fq.multibeam.raw_ping[cnt] for fq in fqpr_loaded], dim='time') for cnt in range(len(fqpr_loaded[0].multibeam.raw_ping))]
            [final_fqpr.multibeam.raw_ping[0].multibeam_files.update(fq.multibeam.raw_ping[0].multibeam_files) for fq in fqpr_loaded]
            final_fqpr.multibeam.raw_att = xr.concat([fq.multibeam.raw_att for fq in fqpr_loaded], dim='time')
            fqpr_loaded = [final_fqpr]
            fqpr_abs_paths = [';'.join(fqpr_abs_paths)]
        return fqpr_abs_paths, fqpr_loaded

    def _return_patch_test_line_data(self, line_list: list):
        """
        Gather the line specific attribution for the patch test lines.  In kluster 0.8.3, this was added as a saved
        attribute, so we just have to gather the attributes.  Prior to this version, we have to compute them.

        Parameters
        ----------
        line_list
            list of multibeam file names

        Returns
        -------
        dict
            dictionary of line name: attributes
        list
            list of relative paths to the fqpr instance for each line
        """

        line_dict = {}
        fqpaths = []
        for multibeam_line in line_list:  # first pass to get the azimuth and positions of the lines
            if multibeam_line not in self.convert_path_lookup:
                self.print_msg('_return_patch_test_line_data: Unable to find {} in project'.format(multibeam_line), logging.WARNING)
            fqpr_rel_pth = self.convert_path_lookup[multibeam_line]
            fq = self.fqpr_instances[fqpr_rel_pth]
            try:
                start_time, end_time, start_latitude, start_longitude, end_latitude, end_longitude, line_az = fq.line_attributes(multibeam_line)
                start_position = [start_latitude, start_longitude]
                end_position = [end_latitude, end_longitude]
            except:
                self.print_msg('_return_patch_test_line_data: unable to pull line attributes added in Kluster 0.8.3, is this an older version of Kluster?', logging.ERROR)
                line_start, line_end = fq.multibeam.raw_ping[0].multibeam_files[multibeam_line][0], fq.multibeam.raw_ping[0].multibeam_files[multibeam_line][1]
                dstart = fq.multibeam.raw_ping[0].interp(time=np.array([max(line_start, fq.multibeam.raw_ping[0].time.values[0])]), method='nearest', assume_sorted=True)
                start_position = [dstart.latitude.values, dstart.longitude.values]
                dend = fq.multibeam.raw_ping[0].interp(time=np.array([min(line_end, fq.multibeam.raw_ping[0].time.values[-1])]), method='nearest', assume_sorted=True)
                end_position = [dend.latitude.values, dend.longitude.values]
                line_az = fq.multibeam.raw_att.interp(time=np.array([line_start + (line_end - line_start) / 2]), method='nearest', assume_sorted=True).heading.values
            line_dict[multibeam_line] = {'start_position': start_position, 'end_position': end_position, 'azimuth': line_az, 'fqpath': fqpr_rel_pth}
            if fqpr_rel_pth not in fqpaths:
                fqpaths.append(fqpr_rel_pth)
        return line_dict, fqpaths

    def sort_lines_patch_test_pairs(self, line_list: list):
        """
        Take the provided list of linenames and sort them into pairs for the patch test tool.  Each pair consists of two lines
        that are reciprocal and start/end in the same place.

        Parameters
        ----------
        line_list
            list of line names that we want to sort

        Returns
        -------
        list
            list of lists of line names in pairs
        dict
            line dict containing the start position, end position and azimuth of each line
        """

        final_grouping = []
        az_grouping = [[], []]
        xyzrph = None
        line_dict, fqpaths = self._return_patch_test_line_data(line_list)
        first_az = None
        for line_name, line_data in line_dict.items():
            if first_az is None:
                first_az = line_data['azimuth']
            az_diff = abs(first_az - line_data['azimuth'])
            if (150 <= az_diff <= 210) or ((330 <= az_diff) or (az_diff <= 30)):  # parallel/recipricol to first line, within 30 degrees
                az_grouping[0].append(line_name)
            elif (210 < az_diff < 330) or (30 < az_diff < 150):
                az_grouping[1].append(line_name)
        paired_lines = []
        for az_group in az_grouping:
            for az_line in az_group:
                if az_line in paired_lines:
                    continue
                line_pair = [az_line]
                paired_lines.append(az_line)
                min_dist = None
                min_line = None
                az_start, az_end = line_dict[az_line]['start_position'], line_dict[az_line]['end_position']
                for az_line_new in az_group:
                    az_diff = abs(line_dict[az_line_new]['azimuth'] - line_dict[az_line]['azimuth'])
                    if az_line_new in paired_lines or az_diff < 45 or az_diff > 315:
                        continue
                    strt_dist = haversine(line_dict[az_line_new]['start_position'][0], line_dict[az_line_new]['start_position'][1], az_start[0], az_start[1])
                    end_dist = haversine(line_dict[az_line_new]['end_position'][0], line_dict[az_line_new]['end_position'][1], az_end[0], az_end[1])
                    dist = min(strt_dist, end_dist)
                    if (min_dist is None) or (dist < min_dist):
                        min_dist = dist
                        min_line = az_line_new
                if min_line:
                    line_pair.append(min_line)
                    paired_lines.append(min_line)
                final_grouping.append(line_pair)
        return final_grouping, line_dict

    def retrieve_data_for_time_segments(self, systems: list, time_segments: list):
        """
        For the manual patch test, we retrieve the Fqpr object and time segments that are currently displayed in the
        points view and use that data to populate the patch test widget.  Here we take the returned system name
        and time segment data from the points view to get the corresponding data from the project.

        Parameters
        ----------
        systems
            list of the system name for each segment
        time_segments
            list of lists for the start time/end time for the line in utc seconds

        Returns
        -------
        list
            list of lists for each segment containing the Fqpr object, the serial number, the time segments, the xyzrph
            dict for that Fqpr, the system identifier, the head index, and the vesselfile name.

        """
        systems = np.array(systems)
        time_segments = np.array(time_segments)
        unique_systems = np.unique(systems)

        vessel_file = self.vessel_file
        if vessel_file:
            vessel_file = VesselFile(vessel_file)
            vfname = os.path.split(vessel_file)[1]
        else:
            vfname = 'None'

        datablock = []
        for system in unique_systems:
            sysid, head = system[:-2], int(system[-1])
            fq = self.fqpr_instances[sysid]
            if head == 0:
                serialnum = fq.multibeam.raw_ping[0].attrs['system_serial_number'][0]
            else:
                serialnum = fq.multibeam.raw_ping[0].attrs['secondary_system_serial_number'][0]
            fq_time_segs = time_segments[np.where(systems == system)[0]]
            if vessel_file:
                xyzrph = vessel_file.return_data(serialnum, fq_time_segs.min(), fq_time_segs.max())
            else:
                xyzrph = fq.multibeam.xyzrph
            xyzrph = trim_xyzrprh_to_times(xyzrph, fq_time_segs.min(), fq_time_segs.max())
            xyzrph = split_by_timestamp(xyzrph)
            for xyzrec in xyzrph:
                datablock.append([fq, serialnum, fq_time_segs, xyzrec, sysid, head, vfname])
        return datablock

    def return_vessel_file(self):
        """
        Return the VesselFile instance for this project's vessel_file path

        Returns
        -------
        VesselFile
            Instance of VesselFile for the vessel_file attribute path.  If self.vessel_file is not set, this returns
            None
        """

        if self.vessel_file:
            if os.path.exists(self.vessel_file):
                vf = VesselFile(self.vessel_file)
            else:
                vf = None
        else:
            vf = None
        return vf

    def return_surface_containers(self, surface_name: str, relative_path: bool = True):
        """
        Project has loaded surface and fqpr instances.  This method will return the names of the existing fqpr instances
        in the surface and a list of the fqpr instances in the project that are not in the surface yet.

        Fqpr instances marked with an asterisk are those that need to be updated in the surface.  The surface soundings
        for that instance are out of date relative to the last operation performed on the fqpr instance.

        Parameters
        ----------
        surface_name
            path to the surface, either relative to the project or absolute path
        relative_path
            if True, surface_name is a relative path

        Returns
        -------
        list
            list of the fqpr instance names that are in the surface, with an asterisk at the end if the surface version
            of the fqpr instance soundings is out of date
        list
            list of fqpr instances that are in the project and not in the surface
        """

        try:
            if relative_path:
                surf = self.surface_instances[surface_name]
            else:
                surf = self.surface_instances[self.path_relative_to_project(surface_name)]
        except:
            self.print_msg('return_surface_containers: Surface {} not found in project'.format(surface_name), logging.ERROR)
            return [], []
        existing_container_names = surf.return_unique_containers()
        existing_needs_update = []
        for existname in existing_container_names:
            if existname in self.fqpr_instances:
                existtime = None
                for ename, etime in surf.container_timestamp.items():
                    if ename.find(existname) != -1:
                        existtime = datetime.strptime(etime, '%Y%m%d_%H%M%S')
                        break
                if existtime:
                    last_time = self.fqpr_instances[existname].last_operation_date
                    if last_time > existtime:
                        existing_needs_update.append(existname)
        existing_container_names = [exist if exist not in existing_needs_update else exist + '*' for exist in existing_container_names]
        possible_container_names = [os.path.split(fqpr_inst.multibeam.raw_ping[0].output_path)[1] for fqpr_inst in self.fqpr_instances.values()]
        possible_container_names = [pname for pname in possible_container_names if (pname not in existing_container_names) and (pname + '*' not in existing_container_names)]
        return existing_container_names, possible_container_names

    def _validate_xyzrph_for_lines(self, line_list: list):
        """
        Ensure that the offsets/angles portion of the kluster installation parameters match across all lines.  This is
        mandatory for the patch test.

        Parameters
        ----------
        line_list
            list of multibeam file names for the patch test lines

        Returns
        -------
        dict
            single timestamp entry for the xyzrph record that we will use for all lines in the patch test
        """

        xyzrph = None
        for line in line_list:
            fq = self.convert_path_lookup[line]
            line_xyzrph = self.fqpr_instances[fq].return_line_xyzrph(line)
            if xyzrph is None:
                # only retain the first time stamp entry, there really should only be one timestamp that applies to the line anyway
                line_xyzrph = split_by_timestamp(line_xyzrph)[0]
                xyzrph = line_xyzrph
            else:
                offsets, angles, _, _, _ = compare_dict_data(xyzrph, line_xyzrph)
                if not offsets or not angles:
                    msg = '_validate_xyzrph_for_lines: line {} was found to have different offsets/angles relative to the other lines.'.format(line)
                    msg += '  All lines must have the same offsets/angles for the patch test to be valid.'
                    self.print_msg(msg, logging.ERROR)
                    raise NotImplementedError(msg)
        return xyzrph

    def _return_xyz_for_lines(self, line_list: list):
        """
        Return the soundings for the provided line pair

        Parameters
        ----------
        line_list
            multibeam file names for the pair of lines

        Returns
        -------
        list
            list of numpy arrays for the xyz data
        """

        lineone, linetwo = line_list
        fqone, fqtwo = self.convert_path_lookup[lineone], self.convert_path_lookup[linetwo]
        if fqone != fqtwo:
            dsetone = self.fqpr_instances[fqone].subset_variables_by_line(['x', 'y', 'z'], lineone)
            dsettwo = self.fqpr_instances[fqtwo].subset_variables_by_line(['x', 'y', 'z'], linetwo)
            xyz = [np.concatenate([dsetone[lineone].x.values, dsettwo[linetwo].x.values]),
                   np.concatenate([dsetone[lineone].y.values, dsettwo[linetwo].y.values]),
                   np.concatenate([dsetone[lineone].z.values, dsettwo[linetwo].z.values])]
        else:
            dsetone = self.fqpr_instances[fqone].subset_variables_by_line(['x', 'y', 'z'], [lineone, linetwo])
            xyz = [dsetone[lineone].x.values, dsetone[lineone].y.values, dsetone[lineone].z.values]
        return xyz

    def run_auto_patch_test(self, line_pairs: dict):
        total_lines = [x for y in line_pairs.values() for x in y[0:2]]
        xyzrph = self._validate_xyzrph_for_lines(total_lines)
        for pair_index, pair_data in line_pairs.items():
            lineone, linetwo, azimuth = pair_data[0], pair_data[1], pair_data[2]
            fqone, fqtwo = self.convert_path_lookup[lineone], self.convert_path_lookup[linetwo]
            if fqone != fqtwo:
                fqprs = [fqone, fqtwo]
                line_dict = {fqone: [lineone], fqtwo: [linetwo]}
            else:
                fqprs = [fqone]
                line_dict = {fqone: [lineone, linetwo]}
            fqpr_paths, fqpr_loaded = self.get_fqprs_by_paths(fqprs, line_dict, raise_exception=True)
            fqpr_loaded[0].multibeam.xyzrph = xyzrph
            patch = PatchTest(fqpr_loaded[0], azimuth=azimuth)
            patch.run_patch()
            patch.display_results()


def create_new_project(output_folder: str = None):
    """
    Create a new FqprProject by taking in multibeam files, converting them, making a new Fqpr instance and loading that
    Fqpr into a new FqprProject.

    No longer used in general, instead use _setup_new_project

    Parameters
    ----------
    output_folder
        optional, a path to an output folder, otherwise will convert right next to mbes_files

    Returns
    -------
    FqprProject
        project instance, with one new Fqpr instance loaded in
    """
    expected_project_file = os.path.join(output_folder, 'kluster_project.json')
    if os.path.exists(expected_project_file):
        print('create_new_project: Found existing project in this directory, please remove and re-create')
        print('{}'.format(expected_project_file))
        return None
    fqp = FqprProject()
    fqp.new_project_from_directory(output_folder)
    return fqp


def open_project(project_path: str):
    """
    Load from a saved fqpr_project file

    Parameters
    ----------
    project_path
        path to a saved FqprProject json file

    Returns
    -------
    FqprProject
        FqprProject instance intialized from the loaded json file
    """

    fqpr_proj = FqprProject()
    fqpr_proj.open_project(project_path)
    return fqpr_proj


def return_project_data(project_path: str):
    """
    Return the data contained in the provided project file

    Parameters
    ----------
    project_path
        path to a saved FqprProject json file

    Returns
    -------
    dict
        dict of the provided project data, ex: {'file_format': 1.0, 'fqpr_paths': ['C:\\collab\\dasktest\\data_dir\\outputtest\\tj_patch_test_710'], 'surface_paths': []}
    """

    fqp = FqprProject()
    data = fqp._load_project_file(project_path)
    return data


def reprocess_fqprs(fqprs: list, newvalues: list, headindex: int, prefixes: list, timestamps: list, serial_number: str,
                    polygon: np.ndarray):
    """
    Convenience function for reprocessing a list of Fqpr objects according to the new arguments given here.  Used in
    the manual patch test tool in Kluster Points View.

    Parameters
    ----------
    fqprs
        list of each fqpr object to reprocess
    newvalues
        list of new values as floats for the reprocessing [roll, pitch, heading, xlever, ylever, zlever, latency]
    headindex
        head index as integer, 0 for non-dual-head or port head, 1 for starboard head
    prefixes
        list of prefixes for looking up the newvalues in the xyzrph, ex: ['rx_r', 'rx_p', 'rx_h', 'tx_x', 'tx_y', 'tx_z', 'latency']
    timestamps
        timestamp for looking up the values in the xyzrph record, one for each fqpr object
    serial_number
        serial number of each fqpr instance, used in the lookup
    polygon
        polygon in geographic coordinates encompassing the patch test region

    Returns
    -------
    list
        list of lists for each fqpr containing the reprocessed xyz data
    """

    roll, pitch, heading, xlever, ylever, zlever, latency = newvalues
    results = []
    for cnt, fq in enumerate(fqprs):
        fq.multibeam.xyzrph[prefixes[0]][timestamps[cnt]] = roll
        fq.multibeam.xyzrph[prefixes[1]][timestamps[cnt]] = pitch
        fq.multibeam.xyzrph[prefixes[2]][timestamps[cnt]] = heading
        fq.multibeam.xyzrph[prefixes[3]][timestamps[cnt]] = xlever
        fq.multibeam.xyzrph[prefixes[4]][timestamps[cnt]] = ylever
        fq.multibeam.xyzrph[prefixes[5]][timestamps[cnt]] = zlever
        fq.multibeam.xyzrph[prefixes[6]][timestamps[cnt]] = latency
        fq.intermediate_dat = {}  # clear out the reprocessed cached data
        fq, soundings = reprocess_sounding_selection(fq, georeference=True)
        newx = np.concatenate([d[0][0].values for d in fq.intermediate_dat[serial_number]['georef'][timestamps[cnt]]], axis=0)
        newy = np.concatenate([d[0][1].values for d in fq.intermediate_dat[serial_number]['georef'][timestamps[cnt]]], axis=0)
        newz = np.concatenate([d[0][2].values for d in fq.intermediate_dat[serial_number]['georef'][timestamps[cnt]]], axis=0)
        fq.multibeam.raw_ping[headindex]['x'] = xr.DataArray(newx, dims=('time', 'beam'))
        fq.multibeam.raw_ping[headindex]['y'] = xr.DataArray(newy, dims=('time', 'beam'))
        fq.multibeam.raw_ping[headindex]['z'] = xr.DataArray(newz, dims=('time', 'beam'))
        fq.intermediate_dat = {}  # clear out the reprocessed cached data
        head, x, y, z, tvu, rejected, pointtime, beam = fq.return_soundings_in_polygon(polygon, geographic=True, isolate_head=headindex)
        results.append([head, x, y, z, tvu, rejected, pointtime, beam])
    return results
