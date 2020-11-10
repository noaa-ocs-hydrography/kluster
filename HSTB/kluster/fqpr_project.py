import os
import numpy as np
import xarray as xr
import json
from typing import Union

from HSTB.drivers import par3, kmall
from HSTB.kluster.fqpr_generation import Fqpr
from HSTB.kluster.dask_helpers import dask_find_or_start_client
from HSTB.kluster.fqpr_convenience import reload_data, convert_multibeam, reload_surface
from HSTB.kluster.fqpr_helpers import get_attributes_from_fqpr
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

    def __init__(self):
        self.client = None
        self.path = None
        self.file_format = 1.0

        self.surface_instances = {}
        self.surface_layers = {}

        # fqpr_generation.FQPR instances per converted folder path, see add_fqpr
        # ex: {'C:\\collab\\dasktest\\data_dir\\EM2040\\convert1': <HSTB.kluster.fqpr_generation.Fqpr at 0x25f910e8eb0>}
        self.fqpr_instances = {}

        # line names and start/stop times per line per converted folder path, see regenerate_fqpr_lines
        # ex: {'C:\\collab\\dasktest\\data_dir\\EM2040\\convert1': {'0001_20170822_144548_S5401_X.all': [1503413148.045, 1503413720.475]}
        self.fqpr_lines = {}

        # fqpr attribution per converted folder path, see add_fqpr
        # ex: {'C:\\collab\\dasktest\\data_dir\\EM2040\\convert1': {'frequency_identifier': [260000, 320000, 290000], ...}
        self.fqpr_attrs = {}

        # converted folder path per line name, see regenerate_fqpr_lines
        # ex: {'0001_20170822_144548_S5401_X.all': 'C:\\collab\\dasktest\\data_dir\\EM2040\\convert1'}
        self.convert_path_lookup = {}

        self.buffered_fqpr_navigation = {}
        self.point_cloud_for_line = {}
        self.node_vals_for_surf = {}

    def _path_relative_to_project(self, pth: str):
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

    def _absolute_path_from_relative(self, pth: str):
        """
        see _path_relative_to_project, will convert the returned relative path from that method to an absolute file
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

    def get_dask_client(self):
        """
        Project is the holder of the Dask client object.  Use this method to return the current Client.  Client is
        currently setup with kluster_main.start_dask_client or kluster_main.open_dask_dashboard
        """

        if self.client is None:
            self.client = dask_find_or_start_client()
        return self.client

    def setup_new_project(self, pth: str):
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
            if os.path.exists(self.path):  # an existing project
                self.open_project(self.path, skip_dask=True)

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
            json.dump(data, pf, sort_keys=True, indent=4)
        print('Project saved to {}'.format(self.path))

    def load_project_file(self, projfile: str):
        """
        Load from saved json project file, return the data in the file.

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
        data['fqpr_paths'] = [self._absolute_path_from_relative(f) for f in data['fqpr_paths']]
        data['surface_paths'] = [self._absolute_path_from_relative(f) for f in data['surface_paths']]
        return data

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

        data = self.load_project_file(projfile)
        self.path = projfile
        self.file_format = data['file_format']
        for pth in data['fqpr_paths']:
            fqpr_entry = self.add_fqpr(pth, skip_dask=skip_dask)
        for pth in data['surface_paths']:
            self.add_surface(pth)

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
        """

        if type(pth) == str:
            fq = reload_data(pth, skip_dask=skip_dask, silent=True)
        else:  # pth is the new Fqpr instance, pull the actual path from the Fqpr attribution
            fq = pth
            pth = os.path.normpath(fq.source_dat.raw_ping[0].output_path)
        if fq is not None:
            if self.path is None:
                self.setup_new_project(os.path.dirname(pth))
            relpath = self._path_relative_to_project(pth)
            self.fqpr_instances[relpath] = fq
            self.fqpr_attrs[relpath] = get_attributes_from_fqpr(fq, include_mode=False)
            self.regenerate_fqpr_lines(relpath)
            print('Successfully added {}'.format(pth))
            return relpath
        return None

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
            relpath = self._path_relative_to_project(pth)

        if relpath in self.fqpr_instances:
            self.fqpr_instances.pop(relpath)
            self.fqpr_attrs.pop(relpath)
            for linename in self.fqpr_lines[relpath]:
                self.convert_path_lookup.pop(linename)
            self.fqpr_lines.pop(relpath)

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
            basesurf = reload_surface(pth)
            pth = os.path.normpath(pth)
        else:  # fq is the new Fqpr instance, pth is the output path that is saved as an attribute
            basesurf = pth
            pth = os.path.normpath(basesurf.output_path)
        if basesurf is not None:
            if self.path is None:
                self.setup_new_project(os.path.dirname(pth))
            relpath = self._path_relative_to_project(pth)
            self.surface_instances[relpath] = basesurf
            print('Successfully added {}'.format(pth))

    def remove_surface(self, pth: Union[str, BaseSurface]):
        """
        Remove an attached BaseSurface instance from the project by path to Fqpr converted folder

        Parameters
        ----------
        pth: str, path to the surface file
        """
        relpath = self._path_relative_to_project(pth)
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
                line_start_time, line_end_time = fq_inst.source_dat.raw_ping[0].multibeam_files[line]
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
            line_att = fq_inst.source_dat.raw_att
            if subset:
                # attributes are all the same across raw_ping datasets, just use the first
                line_start_time, line_end_time = fq_inst.source_dat.raw_ping[0].multibeam_files[line]
                line_att = slice_xarray_by_dim(line_att, dimname='time', start_time=line_start_time, end_time=line_end_time)
        return line_att

    def regenerate_fqpr_lines(self, converted_pth: str):
        """
        After adding a new Fqpr object, we want to get the line information from the attributes so that we can quickly
        access how many lines are in a project, and the time boundaries of these lines.

        Parameters
        ----------
        converted_pth
            path to the Fqpr object
        """
        for fq_name, fq_inst in self.fqpr_instances.items():
            if fq_name == converted_pth:
                self.fqpr_lines[fq_name] = fq_inst.return_line_dict()
                for linename in self.fqpr_lines[fq_name]:
                    self.convert_path_lookup[linename] = converted_pth

    def return_line_owner(self, line: str):
        """
        Return the Fqpr instance that contains the provided line

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
                    return self.fqpr_lines[self._absolute_path_from_relative(proj)]
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
                line_start_time, line_end_time = fq_inst.source_dat.raw_ping[0].multibeam_files[line]
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


def create_new_project(mbes_files: Union[str, list], output_folder: str = None):
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

    fq = convert_multibeam(mbes_files, output_folder)
    fqp = FqprProject()
    fqpr_entry = fqp.add_fqpr(fq, skip_dask=False)
    return fqp


def gather_multibeam_info(multibeam_file: str):
    """
    fast method to read info from a multibeam file without reading the whole file.  Supports .all and .kmall files

    the secondary serial number will be zero for all systems except dual head.  Dual head records the secondary head
    serial number (starboard head) as the secondary serial number.  For non dual head systems, the primary serial
    number is all that is needed.

    Parameters
    ----------
    multibeam_file
        file path to a multibeam file

    Returns
    -------
    list
        [start time (utc seconds), end time (utc seconds), primary serial number, secondary serial number, sonar model number]
    """

    fileext = os.path.splitext(multibeam_file)[1]
    if fileext == '.all':
        aread = par3.AllRead(multibeam_file)
        start_end = aread.fast_read_start_end_time()
        serialnums = aread.fast_read_serial_number()
    elif fileext == '.kmall':
        km = kmall.kmall(multibeam_file)
        start_end = km.fast_read_start_end_time()
        serialnums = km.fast_read_serial_number()
    else:
        raise IOError('File ({}) is not a valid multibeam file'.format(multibeam_file))
    return start_end + serialnums
