import os
import numpy as np
import xarray as xr
import json
from typing import Union
from datetime import datetime, timezone
from types import FunctionType

from HSTB.kluster.fqpr_generation import Fqpr
from HSTB.kluster.dask_helpers import dask_find_or_start_client
from HSTB.kluster.fqpr_convenience import reload_data, reload_surface, get_attributes_from_fqpr
from HSTB.kluster.fqpr_surface import BaseSurface
from HSTB.kluster.xarray_helpers import slice_xarray_by_dim


class FqprProject:
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

    def __init__(self, is_gui: bool = False):
        """

        Parameters
        ----------
        is_gui
             if True, this project is attached to a gui, so we disable progress so that we aren't filling up the output window
        """

        self.client = None
        self.path = None
        self.is_gui = is_gui
        self.file_format = 1.0

        self.surface_instances = {}
        self.surface_layers = {}

        # all paths are relative to the project file location...

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
            raise ValueError('FqprProject: path to project file not setup, is currently undefined.')
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
            raise ValueError('FqprProject: path to project file not setup, is currently undefined.')
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
            # if os.path.exists(self.path):  # an existing project
            #     self.open_project(self.path, skip_dask=True)

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
            raise IOError('Expected a file named kluster_project.json, found {}'.format(projfile))
        with open(projfile, 'r') as pf:
            data = json.load(pf)
        # now translate the relative paths to absolute
        self.path = projfile
        data['fqpr_paths'] = [self.absolute_path_from_relative(f) for f in data['fqpr_paths']]
        data['surface_paths'] = [self.absolute_path_from_relative(f) for f in data['surface_paths']]
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

        if self.client is None:
            self.client = dask_find_or_start_client()
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
            elif os.path.isfile(full_path):
                self.add_surface(full_path)
        self.path = os.path.join(directory_path, 'kluster_project.json')
        self.save_project()

    def save_project(self):
        """
        Save the current FqprProject instance to file.  Use open_project to reload this instance.
        """

        if self.path is None:
            raise EnvironmentError('kluster_project save_project - no data found, you must add data before saving a project')
        with open(self.path, 'w') as pf:
            data = {}
            data['fqpr_paths'] = self.return_fqpr_paths()
            data['surface_paths'] = self.return_surface_paths()
            data['file_format'] = self.file_format
            data.update(self.settings)
            json.dump(data, pf, sort_keys=True, indent=4)
        print('Project saved to {}'.format(self.path))

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
                print('Unable to find converted data: {}'.format(pth))

        for pth in data['surface_paths']:
            if os.path.exists(pth):
                self.add_surface(pth)
            else:  # invalid path
                print('Unable to find surface: {}'.format(pth))

        data.pop('fqpr_paths')
        data.pop('surface_paths')
        data.pop('file_format')
        # rest of the data belongs in settings
        self.settings = data

    def close(self):
        """
        close project and clear all data.  have to close the fqpr instances with the fqpr close method.
        """
        for fq, fqinst in self.fqpr_instances.items():
            fqinst.close()

        self.path = None
        self.surface_instances = {}
        self.surface_layers = {}
        self.fqpr_instances = {}
        self.fqpr_lines = {}
        self.fqpr_attrs = {}
        self.convert_path_lookup = {}
        self.buffered_fqpr_navigation = {}
        self.point_cloud_for_line = {}
        self.node_vals_for_surf = {}

    def set_settings(self, settings: dict):
        self.settings = settings
        if 'use_epsg' in settings:
            for relpath, fqpr_instance in self.fqpr_instances.items():
                if settings['use_epsg']:
                    changed = fqpr_instance.construct_crs(str(settings['epsg']), settings['coord_system'], True, settings['vert_ref'])
                else:
                    changed = fqpr_instance.construct_crs(None, settings['coord_system'], True, settings['vert_ref'])
        self.save_project()

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
            self.fqpr_instances[relpath] = fq
            self.fqpr_attrs[relpath] = get_attributes_from_fqpr(fq, include_mode=False)
            self.regenerate_fqpr_lines(relpath)

            for callback in self._project_observers:
                callback(True)
            print('Successfully added {}'.format(pth))
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
            self.fqpr_instances[relpath].close()
            self.fqpr_instances.pop(relpath)
            self.fqpr_attrs.pop(relpath)
            for linename in self.fqpr_lines[relpath]:
                self.convert_path_lookup.pop(linename)
            self.fqpr_lines.pop(relpath)
            for callback in self._project_observers:
                callback(True)

    def add_surface(self, pth: Union[str, BaseSurface]):
        """
        Add a new BaseSurface object to the project, either by loading from file or by directly adding a BaseSurface
        object provided

        Parameters
        ----------
        pth
            path to surface file or existing BaseSurface object
        """

        if type(pth) == str:
            if os.path.splitext(pth)[1] == '.npz':
                basesurf = reload_surface(pth)
                pth = os.path.normpath(pth)
            else:
                basesurf = None
        else:  # fq is the new Fqpr instance, pth is the output path that is saved as an attribute
            basesurf = pth
            pth = os.path.normpath(basesurf.output_path)
        if basesurf is not None:
            if self.path is None:
                self._setup_new_project(os.path.dirname(pth))
            relpath = self.path_relative_to_project(pth)
            self.surface_instances[relpath] = basesurf
            print('Successfully added {}'.format(pth))

    def remove_surface(self, pth: str, relative_path: bool = False):
        """
        Remove an attached BaseSurface instance from the project by path to Fqpr converted folder

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

    def build_point_cloud_for_line(self, line: str):
        """
        Given line name, build out the point cloud for the line and store it in self.point_cloud_for_line as well as
        returning it in a list of numpy arrays for x y z

        Parameters
        ----------
        line
            line name

        Returns
        -------
        list
            list of numpy arrays for x y z
        """

        xyz = None
        if line not in self.point_cloud_for_line:
            fq_inst = self.return_line_owner(line)
            if fq_inst is not None:
                line_start_time, line_end_time = fq_inst.multibeam.raw_ping[0].multibeam_files[line]
                xyz = fq_inst.return_xyz(start_time=line_start_time, end_time=line_end_time, include_unc=False)
                if xyz is not None:
                    self.point_cloud_for_line[line] = xyz
        else:
            xyz = self.point_cloud_for_line[line]
        return xyz

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
                line_start_time, line_end_time = fq_inst.multibeam.raw_ping[0].multibeam_files[line]
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
                    raise ValueError("Expected one of 'orientation', 'beam_vectors', 'corrected_beam_vectors', got {}".format(visualization_type))

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
            print('return_line_owner: Unable to find project for line {}'.format(line))
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
                    return self.fqpr_lines[self.absolute_path_from_relative(proj)]
            else:
                print('return_project_lines: expected a string path to be provided to the kluster fqpr datastore')
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

    def return_line_navigation(self, line: str, samplerate: float = 1.0):
        """
        For given line name, return the latitude/longitude downsampled to the given samplerate

        Parameters
        ----------
        line
            line name
        samplerate
            new rate at which to downsample the line navigation in seconds

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
                line_start_time, line_end_time = fq_inst.multibeam.raw_ping[0].multibeam_files[line]
                lat, lon = fq_inst.return_downsampled_navigation(sample=samplerate, start_time=line_start_time,
                                                                 end_time=line_end_time)
                # convert to numpy
                lat = lat.values
                lon = lon.values
                # save nav so we don't have to redo this routine if asked for the same line
                self.buffered_fqpr_navigation[line] = [lat, lon]
            else:
                print('{} not found in project'.format(line))
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
                if fq_line not in self.buffered_fqpr_navigation:
                    lats, lons = self.return_line_navigation(fq_line, samplerate=1)
                    self.buffered_fqpr_navigation[fq_line] = [lats, lons]
                else:
                    lats, lons = self.buffered_fqpr_navigation[fq_line]

                line_min_lat = np.min(lats)
                line_max_lat = np.max(lats)
                line_min_lon = np.min(lons)
                line_max_lon = np.max(lons)

                if (line_max_lat < max_lat) and (line_min_lat > min_lat) and (line_max_lon < max_lon) and \
                        (line_min_lon > min_lon):
                    lines_in_box.append(fq_line)
        return lines_in_box

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
                    out_path = fqpr_path
                    out_instance = fqpr_instance
                    matches += 1
        if matches > 1:
            raise ValueError("Found {} matches by serial number, project should not have multiple fqpr instances with the same serial number".format(matches))
        return out_path, out_instance


def create_new_project(output_folder: str = None):
    """
    Create a new FqprProject by taking in multibeam files, converting them, making a new Fqpr instance and loading that
    Fqpr into a new FqprProject.

    Parameters
    ----------
    mbes_files
        either a list of files, a string path to a directory or a string path to a file
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
