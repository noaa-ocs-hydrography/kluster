import os
from typing import Union
from datetime import datetime
from time import perf_counter
import xarray as xr
import numpy as np
import json
from copy import deepcopy
from dask.distributed import wait, progress
from pyproj import CRS, Transformer
import traceback

from HSTB.kluster.modules.orientation import distrib_run_build_orientation_vectors
from HSTB.kluster.modules.beampointingvector import distrib_run_build_beam_pointing_vector
from HSTB.kluster.modules.svcorrect import get_sv_files_from_directory, return_supported_casts_from_list, \
    distributed_run_sv_correct, cast_data_from_file
from HSTB.kluster.modules.georeference import distrib_run_georeference, vertical_datum_to_wkt, vyperdatum_found, distance_between_coordinates, \
    aviso_tide_correct, determine_aviso_grid, aviso_clear_model
from HSTB.kluster.modules.tpu import distrib_run_calculate_tpu
from HSTB.kluster.modules.filter import FilterManager
from HSTB.kluster.xarray_conversion import BatchRead
from HSTB.kluster.fqpr_vessel import trim_xyzrprh_to_times
from HSTB.kluster.modules.visualizations import FqprVisualizations
from HSTB.kluster.modules.export import FqprExport
from HSTB.kluster.modules.subset import FqprSubset
from HSTB.kluster.xarray_helpers import combine_arrays_to_dataset, compare_and_find_gaps, \
    interp_across_chunks, slice_xarray_by_dim, get_beamwise_interpolation
from HSTB.kluster.backends._zarr import ZarrBackend
from HSTB.kluster.dask_helpers import dask_find_or_start_client, get_number_of_workers
from HSTB.kluster.fqpr_helpers import build_crs, seconds_to_formatted_string
from HSTB.kluster.rotations import return_attitude_rotation_matrix
from HSTB.kluster.logging_conf import return_logger
from HSTB.kluster.fqpr_drivers import return_xarray_from_sbet, fast_read_sbet_metadata, return_xarray_from_posfiles
from HSTB.kluster import kluster_variables


class Fqpr(ZarrBackend):
    """
    Fully qualified ping record: contains all records built from the raw MBES file and supporting data files.  Built
    around the BatchRead engine which supplies the multibeam data conversion.

    Fqpr processing is built using the method detailed in "Application of Surface Sound Speed Measurements in
    Post-processing for Multi-Sector Multibeam Echosounders" by J.D. Beaudoin and John Hughes Clarke

    | Processing consists of five main steps:
    | Fqpr.read_from_source - run xarray_conversion to get xarray Datasets for ping/attitude/navigation records
    | Fqpr.get_orientation_vectors - Build transmit/receive unit vectors rotated by attitude and mounting angle
    | Fqpr.get_beam_pointing_vectors - Correct sonar relative beam angles by orientation to get corrected
    |     beam pointing vectors and azimuths
    | Fqpr.sv_correct - Use the corrected beam vectors, travel time and sound velocity profile to ray trace the beams
    | Fqpr.georef_xyz - Using pyproj, transform the vessel relative offsets to georeferenced xyz

    See fqpr_convenience.convert_multibeam, process_multibeam and perform_all_processing for example use.

    Parameters
    ----------
    multibeam
        instance of xarray_conversion BatchRead class
    motion_latency
        optional motion latency adjustment
    address
        passed to dask_find_or_start_client to setup dask cluster
    show_progress
        If true, uses dask.distributed.progress.  Disabled for GUI, as it generates too much text
    parallel_write
        if True, will write in parallel to disk
    """

    def __init__(self, multibeam: BatchRead = None, motion_latency: float = 0.0, address: str = None, show_progress: bool = True,
                 parallel_write: bool = True):
        self.multibeam = multibeam
        if self.multibeam is not None:
            super().__init__(self.multibeam.converted_pth)
        else:
            super().__init__()

        self.intermediate_dat = None
        self.horizontal_crs = None
        self.vert_ref = None
        self.motion_latency = motion_latency

        self.parallel_write = parallel_write

        self.client = None
        self.address = address
        self.show_progress = show_progress
        self.soundspeedprofiles = None

        self.tx_vecs = None
        self.rx_vecs = None
        self.tx_reversed = False
        self.rx_reversed = False
        self.ideal_tx_vec = None
        self.ideal_rx_vec = None
        self._using_sbet = False

        self.orientation_time_complete = ''
        self.bpv_time_complete = ''
        self.sv_time_complete = ''
        self.svmethod = ''
        self.georef_time_complete = ''
        self.tpu_time_complete = ''

        # plotting module
        self.plot = FqprVisualizations(self)
        # export module
        self.export = FqprExport(self)
        # subset module
        self.subset = FqprSubset(self)
        # filter module
        self.filter = FilterManager(self)

        self.logfile = None
        self.logger = None
        self.initialize_log()

    def __repr__(self):
        try:
            try:
                kvers = self.multibeam.raw_ping[0].attrs['kluster_version']
            except:
                kvers = 'Unknown'
            heads = self.number_of_heads
            output = 'FQPR: Fully Qualified Ping Record built by Kluster Processing\n'
            output += '-------------------------------------------------------------\n'
            output += 'Contains:\n'
            if heads == 1:
                output += '1 sonar head, '
            else:
                output += '{} sonar heads, '.format(heads)
            output += '{} pings, version {}\n'.format(self.number_of_pings, kvers)
            output += 'Start: {}\n'.format(self.min_time)
            output += 'End: {}\n'.format(self.max_time)
            try:
                output += 'Minimum Latitude: {} '.format(self.multibeam.raw_ping[0].attrs['min_lat'])
                output += 'Maximum Latitude: {}\n'.format(self.multibeam.raw_ping[0].attrs['max_lat'])
            except:
                output += 'Minimum Latitude: Unknown '
                output += 'Maximum Latitude: Unknown\n'
            try:
                output += 'Minimum Longitude: {} '.format(self.multibeam.raw_ping[0].attrs['min_lon'])
                output += 'Maximum Longitude: {}\n'.format(self.multibeam.raw_ping[0].attrs['max_lon'])
            except:
                output += 'Minimum Longitude: Unknown '
                output += 'Maximum Longitude: Unknown\n'
            try:
                output += 'Minimum Northing: {} '.format(self.multibeam.raw_ping[0].attrs['min_y'])
                output += 'Maximum Northing: {}\n'.format(self.multibeam.raw_ping[0].attrs['max_y'])
            except:
                output += 'Minimum Northing: Unknown '
                output += 'Maximum Northing: Unknown\n'
            try:
                output += 'Minimum Easting: {} '.format(self.multibeam.raw_ping[0].attrs['min_x'])
                output += 'Maximum Easting: {}\n'.format(self.multibeam.raw_ping[0].attrs['max_x'])
            except:
                output += 'Minimum Easting: Unknown '
                output += 'Maximum Easting: Unknown\n'
            try:
                output += 'Minimum Depth: {} '.format(self.multibeam.raw_ping[0].attrs['min_z'])
                output += 'Maximum Depth: {}\n'.format(self.multibeam.raw_ping[0].attrs['max_z'])
            except:
                output += 'Minimum Depth: Unknown '
                output += 'Maximum Depth: Unknown\n'
            output += 'Current Status: {}\n'.format(self.status + ' complete')
            output += 'Sonar Model Number: {}\n'.format(self.sonar_model)
            try:
                output += 'Primary/Secondary System Serial Number: {}/{}\n'.format(self.multibeam.raw_ping[0].attrs['system_serial_number'][0],
                                                                                   self.multibeam.raw_ping[0].attrs['secondary_system_serial_number'][0])
            except:
                output += 'Primary/Secondary System Serial Number: Unknown\n'
            if self.horizontal_crs:
                try:
                    epsg_name = CRS.from_epsg(int(self.horizontal_crs.to_epsg())).name
                except:
                    epsg_name = 'Unknown'
                output += 'Horizontal Datum: {} ({})\n'.format(self.horizontal_crs.to_epsg(), epsg_name)
            else:
                output += 'Horizontal Datum: Unknown\n'
            try:
                output += 'Vertical Datum: {}\n'.format(self.vert_ref)
            except:
                output += 'Vertical Datum: Unknown\n'
            try:
                output += 'Navigation Source: {}\n'.format(self.multibeam.raw_ping[0].attrs['navigation_source'])
            except:
                output += 'Navigation Source: Unknown\n'
            try:
                output += 'Contains SBETs: {}\n'.format(self.has_sbet)
            except:
                output += 'Contains SBETs: Unknown\n'
            output += 'Sound Velocity Profiles: {}\n'.format(len([ky for ky in self.multibeam.raw_ping[0].attrs.keys() if ky[:7] == 'profile']))
        except:
            output = 'Unable to build string representation: {}'.format(traceback.format_exc())
        return output

    def __copy__(self):
        return self.copy()

    @property
    def sonar_model(self):
        """
        Get the sonar type from the ping record

        Returns
        -------
        str
            the sonar model string
        """

        try:
            sonarmodel = self.multibeam.raw_ping[0].attrs['sonartype']
        except:
            sonarmodel = None
        return sonarmodel

    @property
    def status(self):
        """
        Get the processing status of the Fqpr

        Returns
        -------
        str
            the processing status of the Fqpr object
        """

        try:
            cur_status = [rp.current_processing_status for rp in self.multibeam.raw_ping]
            does_match = all([cur_status[0] == curst for curst in cur_status])
            if not does_match:
                print('Warning: found the processing status of the datasets across the heads do not match')
                cur_status = [min(cur_status)]
            cur_status_descrp = self.multibeam.raw_ping[0].attrs['status_lookup'][str(cur_status[0])]
        except:
            cur_status_descrp = None
        return cur_status_descrp

    @property
    def min_time(self):
        """
        Get the nicely formatted time in UTC for the start time of this fqpr object

        Returns
        -------
        str
            the formatted string representation of the minimum time of this dataset in UTC

        """
        try:
            min_time = np.min([rp.time.values[0] for rp in self.multibeam.raw_ping])
            min_time = datetime.utcfromtimestamp(min_time).strftime('%c')
            min_time += ' UTC'
        except:
            min_time = None
        return min_time

    @property
    def max_time(self):
        """
        Get the nicely formatted time in UTC for the end time of this fqpr object

        Returns
        -------
        str
            the formatted string representation of the maximum time of this dataset in UTC

        """
        try:
            max_time = np.max([rp.time.values[-1] for rp in self.multibeam.raw_ping])
            max_time = datetime.utcfromtimestamp(max_time).strftime('%c')
            max_time += ' UTC'
        except:
            max_time = None
        return max_time

    @property
    def number_of_pings(self):
        """
        Get the number of pings for the sonar in this FQPR instance

        Returns
        -------
        int
            number of sonar heads
        """
        try:
            numpings = [rp.time.size for rp in self.multibeam.raw_ping]
            numpings = sum(numpings)
        except:
            numpings = 0
        return numpings

    @property
    def number_of_heads(self):
        """
        Get the number of sonar heads for the sonar in this FQPR instance

        Returns
        -------
        int
            number of sonar heads
        """
        try:
            numheads = len(self.multibeam.raw_ping)
        except:
            numheads = 0
        return numheads

    @property
    def has_sbet(self):
        """
        True if an SBET has been imported into this FQPR instance

        Returns
        -------
        bool
            If SBET has been imported, return True
        """
        try:
            hassbet = 'sbet_altitude' in self.multibeam.raw_ping[0]
        except:
            hassbet = False
        return hassbet

    @property
    def last_operation_date(self):
        """
        Get the datetime of the last operation performed on this fqpr instance

        Returns
        -------
        datetime
            datetime object of the last operation performed on this fqpr instance
        """

        time_attrs = kluster_variables.processing_log_names
        last_time = None
        for rp in self.multibeam.raw_ping:
            for ky, val in rp.attrs.items():
                if ky in time_attrs:
                    new_time = datetime.strptime(val, '%c')
                    if not last_time or new_time > last_time:
                        last_time = new_time
        return last_time

    @property
    def sbet_navigation(self):
        """
        Return the sbet navigation for the first sonar head.  Can assume that all sonar heads have basically the same navigation
        """

        desired_vars = ['sbet_latitude', 'sbet_longitude', 'sbet_altitude', 'sbet_north_position_error',
                        'sbet_east_position_error', 'sbet_down_position_error', 'sbet_roll_error', 'sbet_pitch_error',
                        'sbet_heading_error']
        mandatory_vars = ['sbet_latitude', 'sbet_longitude', 'sbet_altitude']
        keep_these_attributes = ['sbet_mission_date', 'sbet_datum', 'sbet_ellipsoid', 'sbet_logging_rate_hz', 'reference',
                                 'units', 'nav_files', 'nav_error_files']
        try:
            if self.multibeam.raw_ping[0]:
                chk = [x for x in mandatory_vars if x not in self.multibeam.raw_ping[0]]
                if chk:
                    return None

                drop_these = [dvar for dvar in list(self.multibeam.raw_ping[0].keys()) if dvar not in desired_vars]
                subset_nav = self.multibeam.raw_ping[0].drop(drop_these)
                subset_nav.attrs = {ky: self.multibeam.raw_ping[0].attrs[ky] for ky in keep_these_attributes if
                                    ky in self.multibeam.raw_ping[0].attrs}
                return subset_nav
        except:
            return None

    @property
    def input_datum(self):
        """
        The basic input datum of the converted multibeam data.  Will be ignored in processing if an sbet_datum exists,
        as sbet navigation and altitude are used by default if they exist unless you explicitly request non-sbet processing.
        """
        return self.multibeam.raw_ping[0].input_datum

    @input_datum.setter
    def input_datum(self, new_datum: Union[str, int]):
        isvalid, newcrs = validate_kluster_input_datum(new_datum)
        if isvalid:
            self.write_attribute_to_ping_records({'input_datum': str(new_datum)})
        else:
            self.logger.error(f'input_datum: Unable to set input datum with new datum {new_datum}')
            raise ValueError(f'input_datum: Unable to set input datum with new datum {new_datum}')

    def return_navigation(self, start_time: float = None, end_time: float = None, nav_source: str = 'raw'):
        """
        Return the navigation from the multibeam data for the first sonar head. Can assume that all sonar heads have
        basically the same navigation.  If sbet navigation exists, return that instead, renaming the sbet variables
        so that existing methods work.

        Parameters
        ----------
        start_time
            if provided will allow you to only return navigation after this time.  Selects the nearest time value to
            the one provided.
        end_time
            if provided will allow you to only return navigation before this time.  Selects the nearest time value to
            the one provided.
        nav_source
            one of ['raw', 'processed'] if you want to specify the navigation source to be the raw
            multibeam data or the processed sbet

        Returns
        -------
        xr.Dataset
            latitude/longitude/altitude pulled from the navigation part of the ping record
        """

        nav = self.sbet_navigation
        if nav is None or nav_source == 'raw':
            if nav_source == 'processed':
                self.logger.warning(f'return_navigation: processed navigation not found, defaulting to raw navigation. (starttime: {start_time}, endtime: {end_time}')
            return self.multibeam.return_raw_navigation(start_time=start_time, end_time=end_time)
        else:
            nav = nav.rename({'sbet_latitude': 'latitude', 'sbet_longitude': 'longitude', 'sbet_altitude': 'altitude'})
            if start_time or end_time:
                nav = slice_xarray_by_dim(nav, 'time', start_time=start_time, end_time=end_time)
            return nav

    def copy(self):
        """
        Return a copy of this Fqpr instance.  The xarray datasets will be distinct, so you can subset them without
        affecting this instance.

        Returns
        -------
        Fqpr
            copy of the current Fqpr object
        """
        # cannnot deepcopy the dask client, must remove reference first
        self.client = None
        self.multibeam.client = None
        copyfq = deepcopy(self)
        for cnt, rp in enumerate(self.multibeam.raw_ping):
            copyfq.multibeam.raw_ping[cnt].attrs = deepcopy(rp.attrs)
        copyfq.multibeam.raw_att.attrs = deepcopy(self.multibeam.raw_att.attrs)
        return copyfq

    def is_processed(self, in_depth: bool = False):
        """
        Kluster maintains two records for processing status.  current_processing_status is a scalar attribute used by
        the intelligence engine to max processing decisions.  processing_status is a sounding variable that records the
        integer processing status for each sounding.

        The is_processed check will see if this fqpr instance has achieved max_processing_status.  in_depth will use
        the processing_status variable, checking each sounding attribute to compare against the max_processing_status.
        Otherwise, we just check the current_processing_status number, which is much faster

        Parameters
        ----------
        in_depth
            if True, will use the more expensive check to ensure each sounding is fully processed

        Returns
        -------
        bool
            if True, this fqpr is fully processed
        """
        if not in_depth:
            for rp in self.multibeam.raw_ping:
                if rp.current_processing_status < kluster_variables.max_processing_status:
                    return False
            return True
        else:
            for rp in self.multibeam.raw_ping:
                if bool((rp.processing_status < kluster_variables.max_processing_status).any()):
                    return False
            return True

    def line_is_processed(self, line_name: str):
        """
        If line is processed, the TVU will not be all NaN in the middle of the line.  This method will check that and
        return whether or the given line is processed.  We use TVU because

        Parameters
        ----------
        line_name
            name of the line you want to check, ex: '0648_20180711_151142.all'

        Returns
        -------
        bool
            None if line is not found, False if line is not processed, True if line is processed.

        """
        for rp in self.multibeam.raw_ping:
            mlinesdict = rp.attrs['multibeam_files']
            if line_name in mlinesdict:
                starttime, endtime = mlinesdict[line_name][0], mlinesdict[line_name][1]
                # nearest to start/end time could be the next line, so just use the midpoint
                middle_time = starttime + ((endtime - starttime) / 2)
                # if it is processed, you should have all max processing status for each beam
                isprocessed = bool((rp.processing_status.sel(time=middle_time, method='nearest') >= kluster_variables.max_processing_status).all())
                return isprocessed
        print('Warning: unable to find line {} in converted dataset'.format(line_name))
        return None

    def line_attributes(self, line_name: str):
        """
        Attributes by line are added after conversion to the ping attribution.  This is a shortcut for returning the attribution
        for a line

        Parameters
        ----------
        line_name
            name of the line file, ex: 0634_20180711_142125.all

        Returns
        -------
        list
            list of line attributes, [start time, end time, start latitude, start longitude, end latitude, end longitude,
            line azimuth]
        """

        if line_name in self.multibeam.raw_ping[0].multibeam_files:
            return self.multibeam.raw_ping[0].multibeam_files[line_name]
        else:
            return None

    def return_next_unprocessed_line(self):
        """
        Return the next unprocessed line in this container, see line_is_processed

        Returns
        -------
        str
            line name for the next unprocessed line
        """
        final_linename = ''
        unprocessedlines = []
        mlinesdict = self.multibeam.raw_ping[0].attrs['multibeam_files']
        for linename in mlinesdict.keys():
            lineisprocessed = self.line_is_processed(linename)
            if lineisprocessed is not None and not lineisprocessed:
                unprocessedlines.append(linename)
        if unprocessedlines:
            starttimes = [mlinesdict[x][0] for x in unprocessedlines]
            final_linename = unprocessedlines[np.argmin(starttimes)]
        return final_linename

    def close(self, close_dask: bool = True):
        """
        Must forcibly close the logging handlers to allow the data written to disk to be moved or deleted.
        """
        if self.client is not None and close_dask:
            if self.client.status in ("running", "connecting"):
                self.client.close()
        if self.logger is not None:
            handlers = self.logger.handlers[:]
            for handler in handlers:
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)
            self.logger.handlers.clear()
            self.logger = None
        if self.multibeam is not None:
            if self.multibeam.logger is not None:
                handlers = self.multibeam.logger.handlers[:]
                for handler in handlers:
                    handler.flush()
                    handler.close()
                    self.multibeam.logger.removeHandler(handler)
                self.multibeam.logger.handlers.clear()
                self.multibeam.logger = None

    def initialize_log(self):
        """
        Initialize the fqpr logger using the multibeam logfile attribute.

        | self.logfile is the path to the text log that the logging module uses
        | self.logger is the logging.Logger object
        """

        if self.logger is None and self.multibeam is not None:
            if self.multibeam.logfile is not None:
                self.logfile = self.multibeam.logfile
                self.logger = return_logger(__name__, self.logfile)

    def set_vertical_reference(self, vert_ref: str):
        """
        Set the Fqpr instance vertical reference.  This will feed into the georef and calculate tpu processes.

        If the new vert_ref conflicts with an existing written vert_ref, issue a warning.

        Parameters
        ----------
        vert_ref
            vertical reference for the survey, one of ['ellipse', 'waterline', 'NOAA MLLW', 'NOAA MHW']
        """

        if 'vertical_reference' in self.multibeam.raw_ping[0].attrs:
            if vert_ref != self.multibeam.raw_ping[0].vertical_reference:
                self.logger.warning('Setting vertical reference to {} when existing vertical reference is {}'.format(vert_ref, self.multibeam.raw_ping[0].vertical_reference))
                self.logger.warning('You will need to georeference and calculate total uncertainty again')
        if vert_ref not in kluster_variables.vertical_references:
            self.logger.error("Unable to set vertical reference to {}: expected one of {}".format(vert_ref, kluster_variables.vertical_references))
            raise ValueError("Unable to set vertical reference to {}: expected one of {}".format(vert_ref, kluster_variables.vertical_references))
        self.vert_ref = vert_ref

    def read_from_source(self, build_offsets: bool = True):
        """
        Activate rawdat object's appropriate read class

        Parameters
        ----------
        build_offsets
            if this is set, also build the xyzrph attribute, which is mandatory for processing later in Kluster.  Make
            it optional so that when processing chunks of files, we can just run it once at the end after read()
        """

        if self.multibeam is not None:
            self.client = self.multibeam.client  # mbes read is first, pull dask distributed client from it
            if self.multibeam.raw_ping is None:
                self.multibeam.read(build_offsets=build_offsets)
            self.output_folder = self.multibeam.converted_pth
        else:
            self.client = dask_find_or_start_client(address=self.address)
        self.initialize_log()

    def construct_crs(self, epsg: str = None, datum: str = 'WGS84', projected: bool = True, vert_ref: str = None):
        """
        Build pyproj crs from several different options, used with georef_across_along_depth.

        Optionally set the vertical reference as well, using set_vertical_reference.  This isn't tied to the pyproj
        instance, so it can be done separately.

        Options include:
        - epsg mode: set epsg to string identifier
        - geographic mode: set ellips to string identifier and projected to False
        - projected mode: set ellips to sting identifier and projected to True.  Will autodetermine zone

        Parameters
        ----------
        epsg
            optional, epsg code
        datum
            datum identifier i.e. 'WGS84' or 'NAD83'
        projected
            if True uses utm zone projected coordinates
        vert_ref
            vertical reference for the survey, one of ['ellipse', 'waterline', 'NOAA MLLW', 'NOAA MHW']

        Returns
        -------
        bool
            If true, the CRS was successfully constructed and was different from the original
        """

        orig_crs = self.horizontal_crs
        orig_vert_ref = self.vert_ref
        if epsg:
            self.horizontal_crs, err = build_crs(None, datum=datum, epsg=epsg, projected=projected)
        else:
            self.horizontal_crs, err = build_crs(self.multibeam.return_utm_zone_number(), datum=datum, epsg=epsg,
                                                 projected=projected)
        if err:
            self.logger.error(err)
            raise ValueError(err)

        if vert_ref is not None:
            self.set_vertical_reference(vert_ref)

        # successfully changed the CRS, orig would be none when this is run on reloading data
        if ((orig_crs != self.horizontal_crs) or (orig_vert_ref != self.vert_ref)) and orig_crs is not None:
            return True
        else:
            return False

    def generate_starter_orientation_vectors(self, txrx: list = None, tstmp: float = None):
        """
        Take in identifiers to find the correct xyzrph entry, and use the heading value to figure out if the
        transmitter/receiver (tx/rx) is oriented backwards.  Otherwise return ideal vectors for representation of
        the tx/rx.

        Parameters
        ----------
        txrx
            transmit/receive identifiers for xyzrph dict (['tx', 'rx'])
        tstmp
            timestamp for the appropriate xyzrph entry
        """

        # we call this when we reload data to get the reversed state.  Just use the first sector as a convenience.
        if txrx is None and tstmp is None:
            sectors = self.multibeam.return_system_time_indexed_array()
            for s_cnt, sector in enumerate(sectors):
                if sector is None:  # get here if one of the heads is disabled (set to None)
                    continue
                for applicable_index, timestmp, prefixes in sector:
                    txrx = prefixes
                    tstmp = timestmp
                    break
                break

        # start with ideal vectors, flip sign if tx/rx is installed backwards
        # ideal tx vector is aligned with the x axis (forward)
        tx_heading = abs(float(self.multibeam.xyzrph[txrx[0] + '_h'][tstmp]))
        if (tx_heading > 90) and (tx_heading < 270):
            self.ideal_tx_vec = np.array([-1, 0, 0])
            self.tx_reversed = True
        else:
            self.ideal_tx_vec = np.array([1, 0, 0])

        # ideal rx vector is aligned with the y axis (stbd)
        rx_heading = abs(float(self.multibeam.xyzrph[txrx[1] + '_h'][tstmp]))
        if (rx_heading > 90) and (rx_heading < 270):
            self.ideal_rx_vec = np.array([0, -1, 0])
            self.rx_reversed = True
        else:
            self.ideal_rx_vec = np.array([0, 1, 0])

    def write_attribute_to_ping_records(self, attr_dict: dict):
        """
        Convenience method that allows you to write the provided attribute dictionary to each ping dataset and change
        the currently loaded instance as well

        Parameters
        ----------
        attr_dict
            dictionary of attributes that you want stored in the ping datasets
        """

        copy_dict = deepcopy(attr_dict)  # handle any conflicts with altering source dict
        for rp in self.multibeam.raw_ping:
            self.write_attributes('ping', copy_dict, sys_id=rp.system_identifier)
            for ky in copy_dict:  # now set the in memory version to match the written one
                try:  # first try to update with a new dict, don't always want to replace existing keys in case the new dict is just a part of the original
                    rp.attrs[ky].update(copy_dict[ky])
                except:  # all other data ends up replacing which is fine
                    rp.attrs[ky] = copy_dict[ky]

    def import_sound_velocity_files(self, src: Union[str, list], cast_selection_method: str = 'nearest_in_time'):
        """
        Load to self.cast_files the file paths to the sv casts of interest.

        Parameters
        ----------
        src
            either a list of files to include or the path to a directory containing sv files (only supporting .svp currently)
        cast_selection_method
            method used to determine the cast appropriate for each data chunk.  Used here to determine whether or not this new cast(s)
            will require reprocessing, i.e. they are selected by one or more chunks of this dataset.
        """

        if type(src) is str:
            if os.path.isdir(src):
                svfils = get_sv_files_from_directory(src)
            else:
                svfils = [src]
        elif type(src) is list:
            svfils = return_supported_casts_from_list(src)
        else:
            self.logger.error('Provided source is neither a path or a list of files.  Please provide one of those.')
            raise TypeError('Provided source is neither a path or a list of files.  Please provide one of those.')

        # include additional casts from sv files as SoundSpeedProfile objects
        if svfils is not None:
            attr_dict = {}
            cast_dict = {}
            profnames, casts, cast_times, castlocations = self.return_all_profiles()
            for f in svfils:
                data, locs, times, name = cast_data_from_file(f)
                for cnt, dat in enumerate(data):
                    cst_name = 'profile_{}'.format(int(times[cnt]))
                    attrs_name = 'attributes_{}'.format(int(times[cnt]))
                    new_dpth = [d[0] for d in dat.items()]
                    new_sv = [d[1] for d in dat.items()]
                    cast_does_not_exist = cst_name not in self.multibeam.raw_ping[0].attrs
                    cast_needs_updating = False
                    # for each cast that we currently have in the dataset, check to see if the data matches this new cast
                    if casts:
                        for cast_cnt, prev_cast in enumerate(casts):
                            prev_dpth, prev_sv = prev_cast
                            # compare casts, if we find that they match completely, we replace with the new cast which probably has a more accurate time/position
                            #   than the send-to-SIS version (which is probably what is currently in there)
                            for idx in range(len(new_dpth)):
                                if (round(new_dpth[idx], 1) != round(prev_dpth[idx], 1)) or (round(new_sv[idx], 1) != round(prev_sv[idx], 1)):
                                    cast_needs_updating = False
                                    break
                                else:
                                    cast_needs_updating = True
                            if cast_needs_updating:
                                old_profile = 'profile_{}'.format(int(cast_times[cast_cnt]))
                                self.logger.info(f'Replacing sound velocity profile {old_profile} with {cst_name}')
                                self.remove_profile(old_profile)
                                break
                    if cast_does_not_exist or cast_needs_updating:
                        attr_dict[attrs_name] = json.dumps({'location': locs[cnt], 'source': name})
                        cast_dict[cst_name] = json.dumps([list(d) for d in dat.items()])

            self.write_attribute_to_ping_records(cast_dict)
            self.write_attribute_to_ping_records(attr_dict)

            new_cast_names = list(cast_dict.keys())
            applicable_casts = self.return_applicable_casts(method=cast_selection_method)
            new_applicable_casts = [nc for nc in new_cast_names if nc in applicable_casts]
            if new_applicable_casts:
                if self.multibeam.raw_ping[0].current_processing_status >= 3:  # have to start over at sound velocity now
                    self.write_attribute_to_ping_records({'current_processing_status': 2})
                    self.logger.info('Setting processing status to 2, starting over at sound velocity correction')
            self.multibeam.reload_pingrecords(skip_dask=self.client is None)
            self.logger.info('Successfully imported {} new casts'.format(len(cast_dict)))
        else:
            self.logger.warning('Unable to import casts from {}'.format(src))

    def return_all_profiles(self):
        """
        convenience for xarray_conversion.BatchRead.return_all_profiles
        """
        return self.multibeam.return_all_profiles()

    def remove_profile(self, profile_name: str):
        """
        Sound velocity profiles are stored in the Fqpr datastore as attributes with the 'profile_timestamp' format, ex:
        'profile_1503411780'.  Here we take a profile name that is of that format, and remove the matching profile from the
        Fqpr attribution, both the loaded data and the data written to disk.

        Parameters
        ----------
        profile_name
            name of the profile with the 'profile_timestamp' format, ex: 'profile_1503411780'
        """

        if profile_name in self.multibeam.raw_ping[0].attrs:
            profile_removed = True
            for rpindex in range(len(self.multibeam.raw_ping)):  # for each sonar head (raw_ping)...
                try:
                    prof_id, prof_time = profile_name.split('_')
                    matching_attributes = 'attributes_' + prof_time
                    self.multibeam.raw_ping[rpindex].attrs.pop(profile_name)
                    self.multibeam.raw_ping[rpindex].attrs.pop(matching_attributes)
                except:
                    self.logger.warning('WARNING: Unable to find loaded profile data matching attribute "{}"'.format(profile_name))
                    profile_removed = False
                try:
                    prof_id, prof_time = profile_name.split('_')
                    matching_attributes = 'attributes_' + prof_time
                    self.remove_attribute('ping', profile_name, self.multibeam.raw_ping[rpindex].system_identifier)
                    self.remove_attribute('ping', matching_attributes, self.multibeam.raw_ping[rpindex].system_identifier)
                except:
                    self.logger.warning('WARNING: Unable to find data on disk matching attribute "{}" for sonar {}'.format(profile_name, self.multibeam.raw_ping[rpindex].system_identifier))
                    profile_removed = False
            if self.multibeam.raw_ping[0].current_processing_status >= 3:
                if profile_removed:
                    # have to start over at sound velocity now, if you removed sbet data
                    self.write_attribute_to_ping_records({'current_processing_status': 2})
                    self.logger.info('Setting processing status to 2, starting over at sound velocity correction')
                else:
                    self.logger.warning('WARNING: Profile "{}" unsuccessfully removed')
        else:
            self.logger.warning('Unable to find sound velocity profile "{}" in converted data'.format(profile_name))

    def return_chunk_indices(self, idx_mask: xr.DataArray, pings_per_chunk: int):
        """
        Use self.get_cluster_params to figure out how big the chunks should be according to the cluster memory capacity
        and pass that number in here as pings_per_chunk.  Use pings_per_chunk to divide the idx (boolean mask of
        applicable pings with time of ping as a coordinate)

        Idx_of_chunk values are dependent on the mask.  The total lengths will be equivalent, with the values counting
        from zero to length of mask.  This lets us use it to index the data later on.  Idx_of_chunk time is the ping
        time associated with the data we are going to be pulling later.

        Parameters
        ----------
        idx_mask
            the applicable_index generated from return_system_time_indexed_array
        pings_per_chunk
            number of pings in each worker chunk

        Returns
        -------
        list
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping
        """

        msk = np.where(idx_mask)[0]
        idx = xr.DataArray(msk, dims=('time',), coords={'time': idx_mask.time[msk]})

        if len(idx) < pings_per_chunk:
            # not enough data to warrant multiple chunks
            idx_by_chunk = [idx]
        else:
            split_indices = [pings_per_chunk * (i + 1) for i in
                             range(int(np.floor(idx.shape[0] / pings_per_chunk)))]
            idx_by_chunk = np.array_split(idx, split_indices, axis=0)
        return idx_by_chunk

    def return_cast_idx_nearestintime(self, idx_by_chunk: list, silent: bool = False):
        """
        Need to find the cast associated with each chunk of data.  Currently we just take the average chunk time and
        find the closest cast time, and assign that cast.  We also need the index of the chunk in the original size
        dataset, as we built the casts based on the original size soundvelocity dataarray.

        Parameters
        ----------
        idx_by_chunk
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping
        silent
            if True, will not print out messages

        Returns
        -------
        data
            list of lists, each sub-list is [xarray Datarray with times/indices for the chunk, integer index of the cast that
            applies to that chunk]
        """

        profnames, casts, cast_times, castlocations = self.return_all_profiles()
        data = []
        casts_used = []
        for chnk in idx_by_chunk:
            if not cast_times:  # no casts
                self.logger.error(f'return_cast_idx_nearestintime: Unable to find any casts!')
                data.append([chnk, None])
                continue
            # get average chunk time and find the nearest cast to that time.  Retain the index of that cast object.
            avgtme = float(chnk.time.mean())
            cst = np.argmin([np.abs(c - avgtme) for c in cast_times])
            data.append([chnk, cst])
            if cast_times[cst] not in casts_used:
                casts_used.append(cast_times[cst])

        if not silent:
            self.logger.info('nearest-in-time: selecting nearest cast for each {} pings...'.format(kluster_variables.ping_chunk_size))
        return data

    def return_cast_idx_nearestintime_fourhours(self, idx_by_chunk: list, silent: bool = False):
        """
        Need to find the cast associated with each chunk of data.  Currently we just take the average chunk time and
        find the closest cast time, and assign that cast.  We also need the index of the chunk in the original size
        dataset, as we built the casts based on the original size soundvelocity dataarray.

        This method will only retain the cast if it is within four hours, otherwise, you will get a None for that chunk

        Parameters
        ----------
        idx_by_chunk
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping
        silent
            if True, will not print out messages

        Returns
        -------
        data
            list of lists, each sub-list is [xarray Datarray with times/indices for the chunk, integer index of the cast that
            applies to that chunk]
        """

        profnames, casts, cast_times, castlocations = self.return_all_profiles()
        data = []
        casts_used = []
        for chnk in idx_by_chunk:
            if not cast_times:  # no casts
                self.logger.error(f'return_cast_idx_nearestintime_fourhours: Unable to find any casts!')
                data.append([chnk, None])
                continue
            # get average chunk time and find the nearest cast to that time.  Retain the index of that cast object.
            avgtme = float(chnk.time.mean())
            mintimes = [np.abs(c - avgtme) for c in cast_times]
            cst = np.argmin(mintimes)
            finalmintime = mintimes[cst]
            if finalmintime <= 4 * 60 * 60:
                data.append([chnk, cst])
            else:
                self.logger.error(f'return_cast_idx_nearestintime_fourhours: Unable to find a good cast within four hours for time {avgtme}')
                data.append([chnk, None])
                continue
            if cast_times[cst] not in casts_used:
                casts_used.append(cast_times[cst])

        if not silent:
            self.logger.info('nearest-in-time-four-hours: selecting nearest cast for each {} pings...'.format(kluster_variables.ping_chunk_size))
        return data

    def return_cast_idx_nearestindistance(self, idx_by_chunk: list, silent: bool = False):
        """
        Need to find the cast associated with each chunk of data.  Currently we just take the average chunk time and
        find the closest cast in terms of distance.  We also need the index of the chunk in the original size
        dataset, as we built the casts based on the original size soundvelocity dataarray.

        Parameters
        ----------
        idx_by_chunk
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping
        silent
            if True, will not print out messages

        Returns
        -------
        data
            list of lists, each sub-list is [xarray Datarray with times/indices for the chunk, integer index of the cast that
            applies to that chunk]
        """

        profnames, casts, cast_times, castlocations = self.return_all_profiles()
        data = []
        casts_used = []
        for chnk in idx_by_chunk:
            if not cast_times:  # no casts
                self.logger.error(f'return_cast_idx_nearestindistance: Unable to find any casts!')
                data.append([chnk, None])
                continue
            # get average chunk time and find the nearest cast to that time.  Retain the index of that cast object.
            avgtme = float(chnk.time.mean())
            avg_ping_dset = self.multibeam.raw_ping[0].sel(time=avgtme, method='nearest')
            ping_lat, ping_lon = float(avg_ping_dset.latitude), float(avg_ping_dset.longitude)
            cast_dists = [distance_between_coordinates(ping_lat, ping_lon, lat, lon) for lat, lon in castlocations]
            cst = np.argmin(cast_dists)
            data.append([chnk, cst])
            if cast_times[cst] not in casts_used:
                casts_used.append(cast_times[cst])
        if not silent:
            self.logger.info('nearest-in-distance: selecting nearest cast for each {} pings...'.format(kluster_variables.ping_chunk_size))
        return data

    def return_cast_idx_nearestindistance_fourhours(self, idx_by_chunk: list, silent: bool = False):
        """
        Need to find the cast associated with each chunk of data.  Currently we just take the average chunk time and
        find the closest cast in terms of distance.  We also need the index of the chunk in the original size
        dataset, as we built the casts based on the original size soundvelocity dataarray.

        Only retain the cast if it is within four hours.

        Parameters
        ----------
        idx_by_chunk
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping
        silent
            if True, will not print out messages

        Returns
        -------
        data
            list of lists, each sub-list is [xarray Datarray with times/indices for the chunk, integer index of the cast that
            applies to that chunk]
        """

        profnames, casts, cast_times, castlocations = self.return_all_profiles()
        data = []
        casts_used = []
        for chnk in idx_by_chunk:
            if not cast_times:  # no casts
                self.logger.error(f'return_cast_idx_nearestindistance_fourhours: Unable to find any casts!')
                data.append([chnk, None])
                continue
            # get average chunk time and find the nearest cast to that time.  Retain the index of that cast object.
            avgtme = float(chnk.time.mean())
            avg_ping_dset = self.multibeam.raw_ping[0].sel(time=avgtme, method='nearest')
            ping_lat, ping_lon = float(avg_ping_dset.latitude), float(avg_ping_dset.longitude)
            mintimes = [np.abs(c - avgtme) for c in cast_times]
            filtered_mintimes = [mt if mt <= 4 * 60 * 60 else None for mt in mintimes]
            filtered_cast_locations = [ct if filtered_mintimes[castlocations.index(ct)] is not None else [None, None] for ct in castlocations]
            filtered_cast_dists = [distance_between_coordinates(ping_lat, ping_lon, lat, lon) if lat is not None else np.nan for lat, lon in filtered_cast_locations]
            try:
                cst = np.nanargmin(filtered_cast_dists)
                data.append([chnk, cst])
            except ValueError:
                self.logger.error(f'return_cast_idx_nearestindistance_fourhours: Unable to find a good cast within four hours for time {avgtme}')
                data.append([chnk, None])
                continue
            if cast_times[cst] not in casts_used:
                casts_used.append(cast_times[cst])
        if not silent:
            self.logger.info('nearest-in-distance-four-hours: selecting nearest cast for each {} pings...'.format(kluster_variables.ping_chunk_size))
        return data

    def return_applicable_casts(self, method='nearest_in_time'):
        """
        When we check for sound velocity correct actions, we look to see if any new sv profiles imported into the
        fqpr instance are applicable, by running the chosen method (default is cast nearest in time to the ping chunk).
        If new profiles are applicable, we need to re-sv correct.  Use this method to find the applicable sound velocity
        casts.

        Parameters
        ----------
        method
            string identifier for the cast selection method, default is nearest in time to the ping chunk

        Returns
        -------
        list
            list of profile names for all casts that would be used if we sound velocity correct using the provided
            method
        """

        final_idxs = []
        systems = self.multibeam.return_system_time_indexed_array()
        profnames, casts, cast_times, castlocations = self.return_all_profiles()
        for s_cnt, system in enumerate(systems):
            if system is None:  # get here if one of the heads is disabled (set to None)
                continue
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()
            for applicable_index, timestmp, prefixes in system:
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if method == 'nearest_in_time':
                    cast_chunks = self.return_cast_idx_nearestintime(idx_by_chunk, silent=True)
                elif method == 'nearest_in_time_four_hours':
                    cast_chunks = self.return_cast_idx_nearestintime_fourhours(idx_by_chunk, silent=True)
                elif method == 'nearest_in_distance':
                    cast_chunks = self.return_cast_idx_nearestindistance(idx_by_chunk, silent=True)
                elif method == 'nearest_in_distance_four_hours':
                    cast_chunks = self.return_cast_idx_nearestindistance_fourhours(idx_by_chunk, silent=True)
                else:
                    msg = f'return_applicable_casts - unexpected cast selection method {method}, must be one of {kluster_variables.cast_selection_methods}'
                    self.logger.error(msg)
                    raise NotImplementedError(msg)
                final_idxs += [c[1] for c in cast_chunks]
        final_idxs = np.unique(final_idxs).tolist()
        return [profnames[idx] for idx in final_idxs if idx is not None]

    def determine_induced_heave(self, ra: xr.Dataset, hve: xr.DataArray, raw_att: xr.Dataset,
                                tx_tstmp_idx: xr.DataArray, prefixes: str, timestmp: str):
        """
        From Kongsberg datagram doc:
        Note that heave is displayed and logged as positive downwards (the sign is changed) including roll and pitch
        induced lever arm translation to the system’s transmit transducer.

        Here we use the primary to secondary lever arm to build induced heave seen at the secondary system.  As heave
        is reported at the tx of the primary system.  This will return all zeros for induced heave in instances where:

        - system is not a dual head system
        - system is the primary system of a dual head system

        Parameters
        ----------
        ra
            raw_ping dataset for this sector/freq/serial number
        hve
            heave record at ping time
        raw_att
            raw attitude Dataset including roll, pitch, yaw
        tx_tstmp_idx
            ping time index
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp for the appropriate xyzrph record

        Returns
        -------
        xr.DataArray
            induced heave (z) value for each ping time
        """

        is_primary_system = int(ra.system_identifier) == int(ra.system_serial_number[0])
        if self.multibeam.is_dual_head():  # dual dual systems
            if not is_primary_system:  # secondary head
                # self.logger.info('Building induced heave for secondary system in dual head arrangement')
                # lever arms for secondary head to ref pt
                refpt = self.multibeam.return_prefix_for_rp()
                secondary_x_lever = float(self.multibeam.xyzrph[prefixes[refpt[0]] + '_x'][timestmp])
                secondary_y_lever = float(self.multibeam.xyzrph[prefixes[refpt[1]] + '_y'][timestmp])
                secondary_z_lever = float(self.multibeam.xyzrph[prefixes[refpt[2]] + '_z'][timestmp])

                # lever arms for primary head to ref pt
                if prefixes[0].find('port') != -1:
                    prefixes = [pfix.replace('port', 'stbd') for pfix in prefixes]
                elif prefixes[0].find('stbd') != -1:
                    prefixes = [pfix.replace('stbd', 'port') for pfix in prefixes]

                primary_x_lever = float(self.multibeam.xyzrph[prefixes[refpt[0]] + '_x'][timestmp])
                primary_y_lever = float(self.multibeam.xyzrph[prefixes[refpt[1]] + '_y'][timestmp])
                primary_z_lever = float(self.multibeam.xyzrph[prefixes[refpt[2]] + '_z'][timestmp])

                final_lever = np.array([primary_x_lever - secondary_x_lever, primary_y_lever - secondary_y_lever,
                                        primary_z_lever - secondary_z_lever])
            else:
                # self.logger.info('No induced heave in primary system in dual head arrangement')
                return hve
        else:
            # self.logger.info('No induced heave in primary system in single head arrangement')
            return hve

        # self.logger.info('Rotating: {}'.format(final_lever))

        # build rotation matrix for attitude at each ping time
        att = interp_across_chunks(raw_att, tx_tstmp_idx)
        tx_att_times, tx_attitude_rotation = return_attitude_rotation_matrix(att)

        # compute rotated vector
        rot_lever = xr.DataArray(tx_attitude_rotation.data @ final_lever,
                                 coords={'time': tx_att_times, 'xyz': ['x', 'y', 'z']},
                                 dims=['time', 'xyz']).compute()

        # z offset and waterline are applied in sv correct.  The only additional z offset that should be included is
        #   the induced heave seen when rotating lever arms.  Keep that diff by subtracting the original lever arm.
        rot_lever[:, 2] = rot_lever[:, 2] - final_lever[2]
        hve = hve - rot_lever[:, 2]
        return hve

    def determine_altitude_corr(self, alt: xr.DataArray, raw_att: xr.Dataset, tx_tstmp_idx: xr.DataArray,
                                prefixes: str, timestmp: str):
        """
        We use the nav as provided by the POSMV.  This will be at the reference point designated by the POSMV.  As we
        assume that your RP is either the TX or the IMU, if there is a lever arm between TX and RP, there is induced
        heave in the altitude equal to the attitude-rotated TX lever arm.

        Generate that time series attitude adjustment and add it to the altitude record.

        Parameters
        ----------
        alt
            altitude at ping time
        raw_att
            raw attitude Dataset including roll, pitch, yaw
        tx_tstmp_idx
            ping time index
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp for the appropriate xyzrph record

        Returns
        -------
        xr.Dataset
            navigation at ping time (latitude, longitude, altitude) with altitude correction
        """

        refpt = self.multibeam.return_prefix_for_rp()
        x_lever = float(self.multibeam.xyzrph[prefixes[refpt[0]] + '_x'][timestmp])
        y_lever = float(self.multibeam.xyzrph[prefixes[refpt[1]] + '_y'][timestmp])
        z_lever = float(self.multibeam.xyzrph[prefixes[refpt[2]] + '_z'][timestmp])
        if x_lever or y_lever or z_lever:
            # There exists a lever arm between tx and rp, and the altitude is at the rp
            #  - svcorrected offsets are at tx/rx so there will be a correction necessary to use altitude
            rp_to_tx_leverarm = np.array([-x_lever, -y_lever, -z_lever])
            # self.logger.info('Applying altitude correction for RP to TX offset: {}'.format(rp_to_tx_leverarm))

            # build rotation matrix for attitude at each ping time
            att = interp_across_chunks(raw_att, tx_tstmp_idx)
            tx_att_times, tx_attitude_rotation = return_attitude_rotation_matrix(att)

            # compute rotated vector
            rot_lever = xr.DataArray(tx_attitude_rotation.data @ rp_to_tx_leverarm,
                                     coords={'time': tx_att_times, 'xyz': ['x', 'y', 'z']},
                                     dims=['time', 'xyz']).compute()

            # The only additional z offset that should be included is the induced heave seen when rotating lever arms.
            #      Keep that diff by subtracting the original lever arm.
            rot_lever[:, 2] = rot_lever[:, 2] - rp_to_tx_leverarm[2]
            alt = alt + rot_lever[:, 2].values
        # else:
            # self.logger.info('no altitude correction for RP at TX')

        return alt

    def return_additional_xyz_offsets(self, ra: xr.Dataset, prefixes: str, timestmp: str, idx_by_chunk: list):
        """
        Apply tx to reference point offset to beams.

        All the kongsberg sonars have additional offsets in the installation parameters document listed as the difference
        between the measured center of the transducer and the phase center of the transducer.  Here we get those values
        for the provided system (we've previously stored them in the xyzrph data)

        Parameters
        ----------
        ra
            xarray dataset for the rawping dataset we are working with
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp for the appropriate xyzrph record
        idx_by_chunk
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping

        Returns
        -------
        list
            [float, additional x offset, float, additional y offset, float, additional z offset]
        """

        refpt = self.multibeam.return_prefix_for_rp()
        x_base_offset = prefixes[refpt[0]] + '_x'
        y_base_offset = prefixes[refpt[1]] + '_y'

        # z included at cast creation, we will apply this in georeference bathy
        # z_base_offset = prefixes[refpt[2]] + '_z'
        addtl_offsets = []
        for cnt, chnk in enumerate(idx_by_chunk):
            sector_by_beam = ra.txsector_beam[chnk].values
            x_offsets_by_beam = np.zeros(sector_by_beam.shape, dtype=np.float32)
            y_offsets_by_beam = np.zeros(sector_by_beam.shape, dtype=np.float32)
            z_offsets_by_beam = np.zeros(sector_by_beam.shape, dtype=np.float32)

            sector_possibilities = np.unique(sector_by_beam)
            for sector in sector_possibilities:
                x_off_ky = prefixes[refpt[0]] + '_x_' + str(sector)
                y_off_ky = prefixes[refpt[1]] + '_y_' + str(sector)
                z_off_ky = prefixes[refpt[2]] + '_z_' + str(sector)

                if x_off_ky in self.multibeam.xyzrph:
                    addtl_x = float(self.multibeam.xyzrph[x_base_offset][timestmp]) + float(self.multibeam.xyzrph[x_off_ky][timestmp])
                else:
                    addtl_x = float(self.multibeam.xyzrph[x_base_offset][timestmp])
                x_offsets_by_beam[sector_by_beam == sector] = addtl_x

                if y_off_ky in self.multibeam.xyzrph:
                    addtl_y = float(self.multibeam.xyzrph[y_base_offset][timestmp]) + float(self.multibeam.xyzrph[y_off_ky][timestmp])
                else:
                    addtl_y = float(self.multibeam.xyzrph[y_base_offset][timestmp])
                y_offsets_by_beam[sector_by_beam == sector] = addtl_y

                if z_off_ky in self.multibeam.xyzrph:
                    addtl_z = float(self.multibeam.xyzrph[z_off_ky][timestmp])
                else:
                    addtl_z = 0
                z_offsets_by_beam[sector_by_beam == sector] = addtl_z
            addtl_offsets.append([x_offsets_by_beam, y_offsets_by_beam, z_offsets_by_beam])
        return addtl_offsets

    def get_cluster_params(self):
        """
        Attempt to figure out what the chunk size and number of chunks at a time parameters should be given the dims
        of the dataset.  It's pretty rough, definitely needs something more sophisticated, but this serves as a place
        holder.

        Basically uses the avg number of beams per ping and the worker memory size to get the chunk sizes (in time)

        Returns
        -------
        int
            number of pings in each chunk
        int
            number of chunks to run at once
        """

        if self.multibeam is None:
            self.logger.info('Read from data first, multibeam is None')
            return
        # This is the old way.  We would scale the chunksize on each run depending on the capabilities of the cluster.
        # Unfortunately, it turns out you always want the written chunks to be of the same size, because you can't
        # really change the zarr chunk size.  So instead, just return a constant that we made up that gets you near the
        # desired 1MB per chunk that Zarr recommends.
        #
        #  We need to get an idea of how many beams there are in an average ping in this sector
        # bpa = self.multibeam.select_array_from_rangeangle('beampointingangle', sector_identifier)
        # try:
        #     chk_indexes = int(len(bpa) / 10) * np.arange(10)
        #     actual_beams_per_ping = int(np.count_nonzero(~np.isnan(bpa[chk_indexes]).values) / 10)
        # except IndexError:
        #     chk_indexes = np.array([0])
        #     actual_beams_per_ping = int(np.count_nonzero(~np.isnan(bpa[chk_indexes]).values))
        # pings_per_chunk, total_chunks = determine_optimal_chunks(self.client, actual_beams_per_ping)
        try:
            totchunks = get_number_of_workers(self.client)
        except (AttributeError, RuntimeError):
            # client is closed or not setup, assume 4 chunks at a time for local processing
            # AttributeError, client is None, RuntimeError, client is closed
            totchunks = kluster_variables.default_number_of_chunks
        return kluster_variables.ping_chunk_size, totchunks

    def _generate_chunks_orientation(self, ra: xr.Dataset, idx_by_chunk: list, timestmp: str, prefixes: str, silent: bool = False):
        """
        Take a single system, and build the data for the distributed system to process.
        distrib_run_build_orientation_vectors requires the attitude, two way travel time, ping time index, and the
        starting orientation of the tx and rx.

        Parameters
        ----------
        ra
            the raw_ping associated with this system
        idx_by_chunk
            list of dataarrays, values are the integer indexes of the pings to use, coords are the time of ping
        timestmp
            timestamp of the installation parameters instance used
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        silent
            if True, does not print out the log messages

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_build_orientation_vectors
        """

        raw_att = self.multibeam.raw_att
        latency = self.motion_latency

        if latency and not silent:
            self.logger.info('applying {}ms of latency to attitude...'.format(latency))

        if not silent:
            self.logger.info('calculating 3d transducer orientation for:')
        tx_roll_mountingangle = self.multibeam.xyzrph[prefixes[0] + '_r'][timestmp]
        tx_pitch_mountingangle = self.multibeam.xyzrph[prefixes[0] + '_p'][timestmp]
        tx_yaw_mountingangle = self.multibeam.xyzrph[prefixes[0] + '_h'][timestmp]
        tx_orientation = [self.ideal_tx_vec, tx_roll_mountingangle, tx_pitch_mountingangle, tx_yaw_mountingangle, timestmp]
        if not silent:
            self.logger.info('transducer {} mounting angles: roll={} pitch={} yaw={}'.format(prefixes[0], tx_roll_mountingangle,
                                                                                             tx_pitch_mountingangle, tx_yaw_mountingangle))

        rx_roll_mountingangle = self.multibeam.xyzrph[prefixes[1] + '_r'][timestmp]
        rx_pitch_mountingangle = self.multibeam.xyzrph[prefixes[1] + '_p'][timestmp]
        rx_yaw_mountingangle = self.multibeam.xyzrph[prefixes[1] + '_h'][timestmp]
        rx_orientation = [self.ideal_rx_vec, rx_roll_mountingangle, rx_pitch_mountingangle, rx_yaw_mountingangle, timestmp]
        if not silent:
            self.logger.info('transducer {} mounting angles: roll={} pitch={} yaw={}'.format(prefixes[1], rx_roll_mountingangle,
                                                                                             rx_pitch_mountingangle, rx_yaw_mountingangle))

        data_for_workers = []
        for chnk in idx_by_chunk:
            try:
                worker_att = self.client.scatter(slice_xarray_by_dim(raw_att, start_time=chnk.time.min() - 1, end_time=chnk.time.max() + 1))
                worker_twtt = self.client.scatter(ra.traveltime[chnk.values])
                worker_delay = self.client.scatter(ra.delay[chnk.values])
                worker_tx_tstmp_idx = self.client.scatter(ra.time[chnk.values])
            except:  # get here if client is closed or doesnt exist
                worker_att = slice_xarray_by_dim(raw_att, start_time=chnk.time.min() - 1, end_time=chnk.time.max() + 1)
                worker_twtt = ra.traveltime[chnk.values]
                worker_delay = ra.delay[chnk.values]
                worker_tx_tstmp_idx = ra.time[chnk.values]
            data_for_workers.append([worker_att, worker_twtt, worker_delay, worker_tx_tstmp_idx, tx_orientation, rx_orientation, latency])
        return data_for_workers

    def _generate_chunks_bpv(self, ra: xr.Dataset, idx_by_chunk: list, timestmp: str, run_index: int, silent: bool = False):
        """
        Take a single system, and build the data for the distributed system to process.
        distrib_run_build_beam_pointing_vector requires the heading, beampointingangle, tx tiltangle, tx/rx orientation,
        ping time index, and indicators whether or not the sonar heads were installed in a reverse fashion.

        Parameters
        ----------
        ra
            the raw_ping associated with this system
        idx_by_chunk
            list of dataarrays, values are the integer indexes of the pings to use, coords are the time of ping
        timestmp
            timestamp of the installation parameters instance used
        run_index
            the run counter that we are currently on, used in the in memory workflow to figure out which intermediate chunks of data to use
        silent
            if True, does not print out the log messages

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_build_beam_pointing_vector
        """
        latency = self.motion_latency
        if not silent:
            self.logger.info('transducers mounted backwards - TX: {} RX: {}'.format(self.tx_reversed, self.rx_reversed))

        data_for_workers = []
        for cnt, chnk in enumerate(idx_by_chunk):
            if 'orientation' in self.intermediate_dat[ra.system_identifier]:
                # workflow for data that is not written to disk.  Preference given for data in memory
                intermediate_index = cnt + run_index
                tx_rx_data = self.intermediate_dat[ra.system_identifier]['orientation'][timestmp][intermediate_index][0]
                try:
                    tx_rx_data = self.client.submit(_drop_list_element, tx_rx_data, -1)
                except:  # client is not setup, run locally
                    tx_rx_data = _drop_list_element(tx_rx_data, -1)
            else:
                # workflow for data that is written to disk
                try:  # drop the processing status record that is unnecessary
                    tx_rx_data = self.client.scatter([ra.tx[chnk], ra.rx[chnk]])
                except:  # client is not setup, run locally
                    tx_rx_data = [ra.tx[chnk], ra.rx[chnk]]

            heading = get_beamwise_interpolation(chnk.time + latency, ra.delay[chnk.values], self.multibeam.raw_att.heading)
            try:
                fut_hdng = self.client.scatter(heading)
                fut_bpa = self.client.scatter(ra.beampointingangle[chnk.values])
                fut_tilt = self.client.scatter(ra.tiltangle[chnk.values])
            except:  # client is not setup, run locally
                fut_hdng = heading
                fut_bpa = ra.beampointingangle[chnk.values]
                fut_tilt = ra.tiltangle[chnk.values]
            data_for_workers.append([fut_hdng, fut_bpa, fut_tilt, tx_rx_data, self.tx_reversed, self.rx_reversed])
        return data_for_workers

    def _generate_chunks_svcorr(self, ra: xr.Dataset, cast_chunks: list, casts: list,
                                prefixes: str, timestmp: str, addtl_offsets: list, run_index: int, silent: bool = False):
        """
        Take a single sector, and build the data for the distributed system to process.  Svcorrect requires the
        relative azimuth (to ship heading) and the corrected beam pointing angle (corrected for attitude/mounting angle)

        Parameters
        ----------
        ra
            xarray dataset for the rawping dataset we are working with
        cast_chunks
            list of lists, each sub-list is [xarray Datarray with times/indices for the chunk, integer index of the cast that
            applies to that chunk]
        casts
            list of [depth values, sv values] for each cast
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp of the installation parameters instance used
        total offsets
            [float, additional x offset, float, additional y offset, float, additional z offset]
        run_index
            the run counter that we are currently on, used in the in memory workflow to figure out which intermediate chunks of data to use
        silent
            if True, does not print out the log messages

        Returns
        -------
        list
            list of lists, each list contains future objects for distributed_run_sv_correct
        """

        data_for_workers = []
        if any(c[1] is None for c in cast_chunks):
            self.logger.error('Unable to sound velocity correct, one of the data chunks was unable to find a cast, see log for more details')
            raise ValueError('Unable to sound velocity correct, one of the data chunks was unable to find a cast, see log for more details')

        # this should be the transducer to waterline, positive down
        refpt = self.multibeam.return_prefix_for_rp()
        z_pos = -float(self.multibeam.xyzrph[prefixes[refpt[2]] + '_z'][timestmp]) + float(self.multibeam.xyzrph['waterline'][timestmp])
        # if we have no soundspeed data, just use the first entry in the cast
        if ra.soundspeed.values[0] == 0.0:
            self.logger.warning('svcorrect: Found surface sound speed values of 0.0, using the first entry of the cast instead.')
            sspeed = xr.full_like(ra.soundspeed, casts[0][1][0])
        else:
            sspeed = ra.soundspeed
        try:
            twtt_data = self.client.scatter([ra.traveltime[d[0]] for d in cast_chunks])
            ss_data = self.client.scatter([sspeed[d[0]] for d in cast_chunks])
            casts = self.client.scatter(casts)
            addtl_offsets = self.client.scatter([addtl for addtl in addtl_offsets])
        except:  # client is not setup, run locally
            twtt_data = [ra.traveltime[d[0]] for d in cast_chunks]
            ss_data = [sspeed[d[0]] for d in cast_chunks]

        for cnt, dat in enumerate(cast_chunks):
            intermediate_index = cnt + run_index
            if 'bpv' in self.intermediate_dat[ra.system_identifier]:
                # workflow for data that is not written to disk.  Preference given for data in memory
                bpv_data = self.intermediate_dat[ra.system_identifier]['bpv'][timestmp][intermediate_index][0]
                try:  # drop the processing status record that is unnecessary
                    bpv_data = self.client.submit(_drop_list_element, bpv_data, -1)
                except:  # client is not setup, run locally
                    bpv_data = _drop_list_element(bpv_data, -1)
            else:
                # workflow for data that is written to disk, break it up according to cast_chunks
                try:
                    bpv_data = self.client.scatter([ra.rel_azimuth[dat[0]], ra.corr_pointing_angle[dat[0]]])
                except:  # client is not setup, run locally
                    bpv_data = [ra.rel_azimuth[dat[0]], ra.corr_pointing_angle[dat[0]]]
            data_for_workers.append([casts[dat[1]], bpv_data, twtt_data[cnt], ss_data[cnt], z_pos, addtl_offsets[cnt]])
        return data_for_workers

    def _generate_chunks_georef(self, ra: xr.Dataset, idx_by_chunk: xr.DataArray,
                                prefixes: str, timestmp: str, z_offset: float, prefer_pp_nav: bool,
                                vdatum_directory: str, run_index: int, silent: bool = False):
        """
        Take a single sector, and build the data for the distributed system to process.  Georeference requires the
        sv_corrected acrosstrack/alongtrack/depthoffsets, as well as navigation, heading, heave and the quality
        factor data to build x/y/z/uncertainty.

        Parameters
        ----------
        ra
            xarray Dataset for the raw_ping instance selected for processing
        idx_by_chunk
            xarray Datarray, values are the integer indexes of the pings to use, coords are the time of ping
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp of the installation parameters instance used
        z_offset
            reference point to transmitter
        prefer_pp_nav
            if True will use post-processed navigation/height (SBET)
        vdatum_directory
            if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here
        run_index
            the run counter that we are currently on, used in the in memory workflow to figure out which intermediate chunks of data to use
        silent
            if True, does not print out the log messages

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_georeference, see georef_xyz
        """

        latency = self.motion_latency
        tx_tstmp_idx = xr.concat([idx.time for idx in idx_by_chunk], dim='time')
        if latency and not silent:
            self.logger.info('Applying motion latency of {} seconds'.format(latency))

        if prefer_pp_nav and self.has_sbet:
            if not silent:
                self.logger.info('Using post processed navigation...')
            lat = xr.concat([ra.sbet_latitude[chnk] for chnk in idx_by_chunk], dim='time')
            lon = xr.concat([ra.sbet_longitude[chnk] for chnk in idx_by_chunk], dim='time')
            alt = xr.concat([ra.sbet_altitude[chnk] for chnk in idx_by_chunk], dim='time')
            input_datum = ra.sbet_datum
            self._using_sbet = True
        else:
            if not silent:
                self.logger.info('Using raw navigation...')
            lat = xr.concat([ra.latitude[chnk] for chnk in idx_by_chunk], dim='time')
            lon = xr.concat([ra.longitude[chnk] for chnk in idx_by_chunk], dim='time')
            try:
                alt = xr.concat([ra.altitude[chnk] for chnk in idx_by_chunk], dim='time')
            except:  # no raw altitude for some reason...
                if self.vert_ref in kluster_variables.ellipse_based_vertical_references:
                    raise ValueError('georef_xyz: No raw altitude found, and {} is an ellipsoidally based vertical reference'.format(self.vert_ref))
                else:  # we can continue because we aren't going to use altitude anyway
                    alt = xr.zeros_like(lon)
            try:
                input_datum = ra.input_datum
            except AttributeError:
                if not silent:
                    self.logger.warning('No input datum attribute found, assuming WGS84')
                input_datum = 'WGS84'
            self._using_sbet = False

        isvalid, newcrs = validate_kluster_input_datum(input_datum)
        if not isvalid:
            self.logger.error('_generate_chunks_georef: {} not supported.  Only supports WGS84, NAD83 or custom epsg integer code'.format(input_datum))
            raise ValueError('_generate_chunks_georef: {} not supported.  Only supports WGS84, NAD83 or custom epsg integer code'.format(input_datum))
        input_datum = newcrs

        if ('heading' in ra) and ('heave' in ra) and not self.motion_latency:
            hdng = ra.heading
            hve = ra.heave
        else:
            if not silent:
                self.logger.info('Using raw attitude...')
            rawatt = interp_across_chunks(self.multibeam.raw_att, tx_tstmp_idx + latency, daskclient=self.client)
            hdng = rawatt.heading
            hve = rawatt.heave

        wline = float(self.multibeam.xyzrph['waterline'][str(timestmp)])
        tidecorr = None
        if self.vert_ref in kluster_variables.ellipse_based_vertical_references:
            alt = self.determine_altitude_corr(alt, self.multibeam.raw_att, tx_tstmp_idx + latency, prefixes, timestmp)
        else:
            hve = self.determine_induced_heave(ra, hve, self.multibeam.raw_att, tx_tstmp_idx + latency, prefixes, timestmp)
            if self.vert_ref == 'Aviso MLLW':
                region = determine_aviso_grid(lon[0].values)
                if not silent:
                    self.logger.info(f'Aviso tides: building MLLW corrector values for region={region}')
                tidecorr = aviso_tide_correct(lat.values, lon.values, lat.time.values, region, 'MLLW')

        data_for_workers = []
        min_chunk_index = np.min([idx.min() for idx in idx_by_chunk])

        for cnt, chnk in enumerate(idx_by_chunk):
            intermediate_index = cnt + run_index
            if 'sv_corr' in self.intermediate_dat[ra.system_identifier]:
                # workflow for data that is not written to disk.  Preference given for data in memory
                sv_data = self.intermediate_dat[ra.system_identifier]['sv_corr'][timestmp][intermediate_index][0]
                try:  # drop the processing status record that is unnecessary
                    sv_data = self.client.submit(_drop_list_element, sv_data, -1)
                except:  # client is not setup, run locally
                    sv_data = _drop_list_element(sv_data, -1)
            else:
                try:  # workflow for data that is written to disk
                    sv_data = self.client.scatter([ra.alongtrack[chnk], ra.acrosstrack[chnk], ra.depthoffset[chnk]])
                except:  # client is not setup, run locally
                    sv_data = [ra.alongtrack[chnk], ra.acrosstrack[chnk], ra.depthoffset[chnk]]

            # latency workflow is kind of strange.  We want to get data where the time equals the chunk time.  Which
            #   means we have to apply the latency to the chunk time.  But then we need to remove the latency from the
            #   data time so that it aligns with ping time again for writing to disk.
            if latency:
                chnk = chnk.assign_coords({'time': chnk.time.time + latency})
            chnk_vals = chnk.values - min_chunk_index
            try:
                if alt is None:
                    fut_alt = alt
                else:
                    fut_alt = self.client.scatter(alt[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                if tidecorr is None:
                    fut_tide = tidecorr
                else:
                    fut_tide = self.client.scatter(tidecorr[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                fut_lon = self.client.scatter(lon[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                fut_lat = self.client.scatter(lat[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                fut_hdng = self.client.scatter(hdng[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                fut_hve = self.client.scatter(hve[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
            except:  # client is not setup, run locally
                if alt is None:
                    fut_alt = alt
                else:
                    fut_alt = alt[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                if tidecorr is None:
                    fut_tide = tidecorr
                else:
                    fut_tide = tidecorr[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_lon = lon[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_lat = lat[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_hdng = hdng[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_hve = hve[chnk_vals].assign_coords({'time': chnk.time.time - latency})
            data_for_workers.append([sv_data, fut_alt, fut_lon, fut_lat, fut_hdng, fut_hve, wline, self.vert_ref,
                                     input_datum, self.horizontal_crs, z_offset, vdatum_directory, fut_tide])
        return data_for_workers

    def _generate_chunks_tpu(self, ra: xr.Dataset, idx_by_chunk: xr.DataArray, prefixes: str, timestmp: str, run_index: int, silent: bool = False):
        """
        Take a single sector, and build the data for the distributed system to process.  Georeference requires the
        sv_corrected acrosstrack/alongtrack/depthoffsets, as well as navigation, heading, heave and the quality
        factor data to build x/y/z/uncertainty.

        Parameters
        ----------
        ra
            xarray Dataset for the raw_ping instance selected for processing
        idx_by_chunk
            xarray Datarray, values are the integer indexes of the pings to use, coords are the time of ping
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp of the installation parameters instance used
        run_index
            the run counter that we are currently on, used in the in memory workflow to figure out which intermediate chunks of data to use
        silent
            if True, does not print out the log messages

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_georeference, see georef_xyz
        """

        latency = self.motion_latency
        tx_tstmp_idx = xr.concat([idx.time for idx in idx_by_chunk], dim='time')
        if latency and not silent:
            self.logger.info('Applying motion latency of {} seconds'.format(latency))

        if 'qualityfactor' not in self.multibeam.raw_ping[0]:
            self.logger.error("_generate_chunks_tpu: sonar uncertainty ('qualityfactor') must exist to calculate uncertainty")
            return None

        roll = interp_across_chunks(self.multibeam.raw_att['roll'], tx_tstmp_idx + latency, daskclient=self.client)

        first_mbes_file = list(ra.multibeam_files.keys())[0]
        mbes_ext = os.path.splitext(first_mbes_file)[1]
        if mbes_ext in kluster_variables.sonar_uses_quality_factor:  # for .all files, quality factor is an int representing scaled std dev
            qf_type = 'kongsberg'
        elif mbes_ext in kluster_variables.sonar_uses_ifremer:  # for .kmall files, quality factor is a percentage of water depth, see IFREMER formula
            qf_type = 'ifremer'
        else:
            raise ValueError('Found multibeam file with {} extension, only {} supported by kluster'.format(mbes_ext, kluster_variables.supported_sonar))

        data_for_workers = []

        # set the first chunk of the first write to build the tpu sample image, provide a path to the folder to save in
        image_generation = [False] * len(idx_by_chunk)
        if not silent:
            image_generation[0] = os.path.join(self.multibeam.converted_pth, 'ping_' + ra.system_identifier + '.zarr')

        tpu_params = self.multibeam.return_tpu_parameters(timestmp)
        # tx/rx opening angles added in kluster 0.9.3, before it was just 'beam_opening_angle' and used as the rx angle
        if prefixes[1] + '_opening_angle' in tpu_params:  # this is data processed after kluster 0.9.3
            tpu_params.pop(prefixes[0] + '_opening_angle')
            tpu_params['beam_opening_angle'] = tpu_params.pop(prefixes[1] + '_opening_angle')

        for cnt, chnk in enumerate(idx_by_chunk):
            if 'georef' in self.intermediate_dat[ra.system_identifier]:
                raise NotImplementedError('_generate_chunks_tpu: in memory workflow not currently implemented for compute tpu')
            intermediate_index = cnt + run_index
            raw_point = ra.beampointingangle[chnk.values]
            if self.rx_reversed:
                # if reversed, we have to reverse the raw angles to match the already reversed corr angles
                #  also load the numpy array, as leaving it as an xarray seems to cause problems with xarray ops later
                raw_point = raw_point[..., ::-1].values
            if 'datum_uncertainty' in ra and self.vert_ref not in ['waterline', 'ellipse']:
                datum_unc = ra.datum_uncertainty[chnk.values]
            else:
                datum_unc = None
            try:
                fut_corr_point = self.client.scatter(ra.corr_pointing_angle[chnk.values])
                fut_raw_point = self.client.scatter(raw_point)
                fut_acrosstrack = self.client.scatter(ra.acrosstrack[chnk.values])
                fut_depthoffset = self.client.scatter(ra.depthoffset[chnk.values])
                fut_soundspeed = self.client.scatter(ra.soundspeed[chnk.values])
                fut_qualityfactor = self.client.scatter(ra.qualityfactor[chnk.values])
                fut_datumuncertainty = self.client.scatter(datum_unc)
                try:  # pospac uncertainty available
                    fut_npe = self.client.scatter(ra.sbet_north_position_error[chnk.values])
                    fut_epe = self.client.scatter(ra.sbet_east_position_error[chnk.values])
                    fut_dpe = self.client.scatter(ra.sbet_down_position_error[chnk.values])
                    fut_rpe = self.client.scatter(ra.sbet_roll_error[chnk.values])
                    fut_ppe = self.client.scatter(ra.sbet_pitch_error[chnk.values])
                    fut_hpe = self.client.scatter(ra.sbet_heading_error[chnk.values])
                except:  # rely on static values
                    fut_npe = None
                    fut_epe = None
                    fut_dpe = None
                    fut_rpe = None
                    fut_ppe = None
                    fut_hpe = None
                # latency workflow is kind of strange.  We want to get data where the time equals the chunk time.  Which
                #   means we have to apply the latency to the chunk time.  But then we need to remove the latency from the
                #   data time so that it aligns with ping time again for writing to disk.
                if latency:
                    chnk = chnk.assign_coords({'time': chnk.time.time + latency})
                fut_roll = self.client.scatter(roll.where(roll['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
            except:  # client is not setup, run locally
                fut_corr_point = ra.corr_pointing_angle[chnk.values]
                fut_raw_point = raw_point
                fut_acrosstrack = ra.acrosstrack[chnk.values]
                fut_depthoffset = ra.depthoffset[chnk.values]
                fut_soundspeed = ra.soundspeed[chnk.values]
                fut_qualityfactor = ra.qualityfactor[chnk.values]
                fut_datumuncertainty = datum_unc
                try:  # pospac uncertainty available
                    fut_npe = ra.sbet_north_position_error[chnk.values]
                    fut_epe = ra.sbet_east_position_error[chnk.values]
                    fut_dpe = ra.sbet_down_position_error[chnk.values]
                    fut_rpe = ra.sbet_roll_error[chnk.values]
                    fut_ppe = ra.sbet_pitch_error[chnk.values]
                    fut_hpe = ra.sbet_heading_error[chnk.values]
                except:  # rely on static values
                    fut_npe = None
                    fut_epe = None
                    fut_dpe = None
                    fut_rpe = None
                    fut_ppe = None
                    fut_hpe = None
                if latency:
                    chnk = chnk.assign_coords({'time': chnk.time.time + latency})
                fut_roll = roll.where(roll['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
            data_for_workers.append([fut_roll, fut_raw_point, fut_corr_point, fut_acrosstrack, fut_depthoffset, fut_soundspeed,
                                     fut_datumuncertainty, tpu_params, fut_qualityfactor, fut_npe, fut_epe, fut_dpe,
                                     fut_rpe, fut_ppe, fut_hpe, qf_type, self.vert_ref, image_generation[cnt]])
        return data_for_workers

    def initialize_intermediate_data(self, sec_ident: str, ky: str):
        """
        self.intermediate_dat is the storage for all the futures generated by the main processes
        (get_orientation_vectors, get_beam_pointing_vectors, etc.).  It is organized by sector identifier/process key.

        This method will initialize the storage for a new sector identifier/process key.

        Parameters
        ----------
        sec_ident
            raw_ping sector identifier, ex: '40107_1_320000'
        ky
            process key, one of 'orientation', 'bpv', etc.
        """

        try:
            self.intermediate_dat[sec_ident][ky] = {}
        except (KeyError, TypeError):
            try:
                # picking this back up from reloading, or the first operation run
                # have to rebuild this dict
                self.intermediate_dat[sec_ident] = {}
                self.intermediate_dat[sec_ident][ky] = {}
            except (KeyError, TypeError):
                # first run of any process
                self.intermediate_dat = {}
                self.intermediate_dat[sec_ident] = {}
                self.intermediate_dat[sec_ident][ky] = {}

    def _validate_subset_time(self, subset_time: list, dump_data: bool):
        """
        Validation routine for the subset_time processing option.  A quirk of writing to zarr datastores, is we can't
        just write the 50th element without writing the first 49 first.  So all our writes have to be checked to
        make sure we don't leave a time gap when compared to the converted data.  This validation routine checks that.

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore
        """
        if subset_time is not None and dump_data:
            if isinstance(subset_time[0], list):
                first_subset_time = subset_time[0][0]
                last_subset_time = subset_time[-1][-1]
            else:
                first_subset_time = subset_time[0]
                last_subset_time = subset_time[-1]
            for ra in self.multibeam.raw_ping:
                sysid = ra.system_identifier
                # check to see if this dataset is within the subset time
                if ra.time[0] <= last_subset_time or ra.time[-1] >= first_subset_time:
                    # nothing written to disk yet, first run has to include the first time
                    if 'tx' not in list(ra.keys()):
                        if first_subset_time > np.min(ra.time):
                            msg = 'get_orientation_vectors: {}: If your first run of this function uses subset_time, it must include the first ping.'.format(sysid)
                            raise NotImplementedError(msg)

    def _validate_get_orientation_vectors(self, subset_time: list, dump_data: bool):
        """
        Validation routine for running get_orientation_vectors.  Ensures you have all the data you need before kicking
        off the process

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore
        """

        self._validate_subset_time(subset_time, dump_data)
        req = 'traveltime'
        if req not in list(self.multibeam.raw_ping[0].keys()):
            err = 'get_orientation_vectors: unable to find {}'.format(req)
            err += ' in ping data {}.  You must run read_from_source first.'.format(self.multibeam.raw_ping[0].sector_identifier)
            self.logger.error(err)
            raise ValueError(err)
        if self.multibeam.raw_att is None:
            err = 'get_orientation_vectors: unable to find raw attitude. You must run read_from_source first.'
            self.logger.error(err)
            raise ValueError(err)

    def _validate_get_beam_pointing_vectors(self, subset_time: list, dump_data: bool):
        """
        Validation routine for running get_beam_pointing_vectors.  Ensures you have all the data you need before kicking
        off the process

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore
        """
        self._validate_subset_time(subset_time, dump_data)

        # first check to see if there is any data in memory.  If so, we just assume that you have the data you need.
        if self.intermediate_dat is not None:
            for rawping in self.multibeam.raw_ping:
                if 'orientation' in self.intermediate_dat[rawping.system_identifier]:
                    return

        required = ['tx', 'rx', 'beampointingangle', 'tiltangle']
        for req in required:
            if req not in list(self.multibeam.raw_ping[0].keys()):
                err = 'get_beam_pointing_vectors: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run get_orientation_vectors first.'.format(self.multibeam.raw_ping[0].system_identifier)
                self.logger.error(err)
                raise ValueError(err)

    def _validate_sv_correct(self, subset_time: list, dump_data: bool):
        """
        Validation routine for running sv_correct.  Ensures you have all the data you need before kicking
        off the process

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore
        """
        self._validate_subset_time(subset_time, dump_data)

        # first check to see if there is any data in memory.  If so, we just assume that you have the data you need.
        if self.intermediate_dat is not None:
            for rawping in self.multibeam.raw_ping:
                if 'bpv' in self.intermediate_dat[rawping.system_identifier]:
                    return

        required = ['rel_azimuth', 'corr_pointing_angle']
        for req in required:
            if req not in list(self.multibeam.raw_ping[0].keys()):
                err = 'sv_correct: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run get_beam_pointing_vectors first.'.format(
                    self.multibeam.raw_ping[0].system_identifier)
                self.logger.error(err)
                raise ValueError(err)

    def _validate_georef_xyz(self, subset_time: list, dump_data: bool):
        """
        Validation routine for running georef_xyz.  Ensures you have all the data you need before kicking
        off the process

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore
        """
        self._validate_subset_time(subset_time, dump_data)

        if self.vert_ref is None:
            self.logger.error("georef_xyz: set_vertical_reference must be run before georef_xyz")
            raise ValueError('georef_xyz: set_vertical_reference must be run before georef_xyz')
        if self.vert_ref not in kluster_variables.vertical_references:
            self.logger.error("georef_xyz: {} must be one of {}".format(self.vert_ref, kluster_variables.vertical_references))
            raise ValueError("georef_xyz: {} must be one of ".format(self.vert_ref))
        if self.horizontal_crs is None:
            self.logger.error('georef_xyz: horizontal_crs object not found.  Please run Fqpr.construct_crs first.')
            raise ValueError('georef_xyz: horizontal_crs object not found.  Please run Fqpr.construct_crs first.')
        if self.vert_ref in kluster_variables.ellipse_based_vertical_references:
            if 'altitude' not in self.multibeam.raw_ping[0] and 'sbet_altitude' not in self.multibeam.raw_ping[0]:
                self.logger.error('georef_xyz: You must provide altitude for vert_ref=ellipse, not found in ping record or post processed navigation.')
                raise ValueError('georef_xyz: You must provide altitude for vert_ref=ellipse, not found in ping record or post processed navigation.')
        if self.vert_ref in kluster_variables.vdatum_vertical_references:
            if not vyperdatum_found:
                self.logger.error('georef_xyz: {} provided but vyperdatum is not found'.format(self.vert_ref))
                raise ValueError('georef_xyz: {} provided but vyperdatum is not found'.format(self.vert_ref))

        # first check to see if there is any data in memory.  If so, we just assume that you have the data you need.
        if self.intermediate_dat is not None:
            for rawping in self.multibeam.raw_ping:
                if 'sv_corr' in self.intermediate_dat[rawping.system_identifier]:
                    return

        required = ['alongtrack', 'acrosstrack', 'depthoffset']
        for req in required:
            if req not in list(self.multibeam.raw_ping[0].keys()):
                err = 'georef_xyz: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run sv_correct first.'.format(
                    self.multibeam.raw_ping[0].system_identifier)
                self.logger.error(err)
                raise ValueError(err)

    def _overwrite_georef_stats(self):
        """
        Each georeference run (assuming it is not a subset operation) will overwrite the attributed georeference
        max min values.  Have to write to disk with write_attribute and also set the currently loaded instance. Otherwise
        we would have to do the costly reload_pingrecords for the inmemory ping record to match the disk copy.
        """

        if not self.subset.is_subset:  # this is not a subset operation, overwrite the global min/max values
            if 'x' in self.multibeam.raw_ping[0]:  # if they ran georeference but did not dump the data, it is still a future and inaccessible
                minx = min([np.nanmin(rp.x) for rp in self.multibeam.raw_ping])
                miny = min([np.nanmin(rp.y) for rp in self.multibeam.raw_ping])
                minz = round(np.float64(min([np.nanmin(rp.z) for rp in self.multibeam.raw_ping])), 3)  # cast as f64 to deal with json serializable error in zarr write attributes
                maxx = max([np.nanmax(rp.x) for rp in self.multibeam.raw_ping])
                maxy = max([np.nanmax(rp.y) for rp in self.multibeam.raw_ping])
                maxz = round(np.float64(max([np.nanmax(rp.z) for rp in self.multibeam.raw_ping])), 3)
                geohash_by_line = self.subset_variables_by_line(['geohash'])
                geohash_dict = {}
                for mline, linedataset in geohash_by_line.items():
                    if linedataset is None:
                        geohash_dict[mline] = []
                    else:
                        geohash_dict[mline] = [x.decode() for x in np.unique(linedataset.geohash).tolist()]
                newattr = {'min_x': minx, 'min_y': miny, 'min_z': minz, 'max_x': maxx, 'max_y': maxy, 'max_z': maxz, 'geohashes': geohash_dict}
                self.write_attribute_to_ping_records(newattr)

    def _validate_calculate_total_uncertainty(self, subset_time: list, dump_data: bool):
        """
        Validation routine for running calculate_total_uncertainty.  Ensures you have all the data you need before kicking
        off the process

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore
        """

        self._validate_subset_time(subset_time, dump_data)

        # no in memory workflow built just yet

        if self.vert_ref is None:
            self.logger.error('calculate_total_uncertainty: set_vertical_reference must be run before calculate_total_uncertainty')
            raise ValueError('calculate_total_uncertainty: set_vertical_reference must be run before calculate_total_uncertainty')
        if self.vert_ref not in kluster_variables.vertical_references:
            self.logger.error("calculate_total_uncertainty: {} must be one of {}".format(self.vert_ref, kluster_variables.vertical_references))
            raise ValueError("calculate_total_uncertainty: {} must be one of {}".format(self.vert_ref, kluster_variables.vertical_references))

        required = ['corr_pointing_angle', 'beampointingangle', 'acrosstrack', 'depthoffset', 'soundspeed', 'qualityfactor']
        for req in required:
            if req not in list(self.multibeam.raw_ping[0].keys()):
                err = 'calculate_total_uncertainty: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run georef_xyz first.'.format(
                    self.multibeam.raw_ping[0].system_identifier)
                self.logger.error(err)
                raise ValueError(err)

    def _validate_post_processed_navigation(self, navfiles: list, errorfiles: list = None, logfiles: list = None,
                                            overwrite: bool = False):
        """
        Validate the input navigation files to ensure the import_post_processed_navigation process will work.  The big
        things to check include ensuring that navfiles/errorfiles/logfiles are all the same size (as they should be
        one to one) and that there are no files that we are importing that exist in the input dataset.  We check the
        latter by comparing file names and start/end times.

        Might be that we need to dig deeper, compare whole records to ensure we get no duplicates.  That might be a
        feature for a later date.

        Parameters
        ----------
        navfiles
            list of postprocessed navigation file paths
        errorfiles
            list of postprocessed error file paths.  If provided, must be same number as nav files
        logfiles
            list of export log file paths associated with navfiles.  If provided, must be same number as nav files
        overwrite
            if True, will not filter out files that exist, all valid files will be accepted

        Returns
        -------
        navfiles
            verified list of postprocessed navigation file paths with duplicates removed
        errorfiles
            verified list of postprocessed error file paths with duplicates removed
        logfiles
            verified list of export log file paths associated with navfiles with duplicates removed
        """

        if self.multibeam is None:
            raise ValueError('Expect multibeam records before importing post processed navigation')

        if errorfiles is not None:
            if len(navfiles) != len(errorfiles):
                raise ValueError('Expect the same number of nav/error files: \n\n{}\n{}'.format(navfiles, errorfiles))
        else:
            errorfiles = [None] * len(navfiles)

        if logfiles is not None:
            if len(navfiles) != len(logfiles):
                raise ValueError('Expect the same number of nav/log files: \n\n{}\n{}'.format(navfiles, logfiles))
        else:
            logfiles = [None] * len(navfiles)

        # remove any duplicate files, these would be files that already exist in the Fqpr instance.  Check by comparing
        #  file name and the start/end time of the navfile.
        if not overwrite:
            rp = self.multibeam.raw_ping[0]
            duplicate_navfiles = []
            for new_file in navfiles:
                root, filename = os.path.split(new_file)
                if 'nav_files' in rp and filename in rp.nav_files:
                    new_file_times = fast_read_sbet_metadata(new_file)
                    if rp.nav_files[filename] == new_file_times:
                        duplicate_navfiles.append(new_file)
            for fil in duplicate_navfiles:
                print('{} is already a converted navigation file within this dataset'.format(fil))
                navfiles_index = navfiles.index(fil)
                if errorfiles is not None:
                    errorfiles.remove(errorfiles[navfiles_index])
                if logfiles is not None:
                    logfiles.remove(logfiles[navfiles_index])
                navfiles.remove(fil)

        return navfiles, errorfiles, logfiles

    def _validate_raw_navigation(self, navfiles: list, overwrite: bool = False):
        """
        Validate the input navigation files to ensure the overwrite_raw_navigation process will work.  Only allow
        duplicate files if overwrite is True.

        Parameters
        ----------
        navfiles
            list of raw navigation file paths
        overwrite
            if True, will not filter out files that exist, all valid files will be accepted

        Returns
        -------
        navfiles
            verified list of raw navigation file paths with duplicates removed
        """

        if self.multibeam is None:
            raise ValueError('Expect multibeam records before importing post processed navigation')

        # remove any duplicate files, these would be files that already exist in the Fqpr instance.  Check by comparing
        #  file name and the start/end time of the navfile.
        if not overwrite:
            duplicate_navfiles = []
            for new_file in navfiles:
                root, filename = os.path.split(new_file)
                if 'pos_files' in self.multibeam.raw_ping[0].attrs:
                    if filename in self.multibeam.raw_ping[0].pos_files:
                        duplicate_navfiles.append(new_file)
            for fil in duplicate_navfiles:
                print('{} is already a converted navigation file within this dataset'.format(fil))
                navfiles.remove(fil)

        return navfiles

    def import_post_processed_navigation(self, navfiles: list, errorfiles: list = None, logfiles: list = None,
                                         weekstart_year: int = None, weekstart_week: int = None,
                                         override_datum: str = None, override_grid: str = None,
                                         override_zone: str = None, override_ellipsoid: str = None,
                                         max_gap_length: float = 1.0, overwrite: bool = False):
        """
        Load from post processed navigation files (currently just SBET and SMRMSG) to get lat/lon/altitude as well
        as 3d error for further processing.  Will save as variables/attributes within the ping record for the nearest
        data point to each ping time.

        Parameters
        ----------
        navfiles
            list of postprocessed navigation file paths
        errorfiles
            list of postprocessed error file paths.  If provided, must be same number as nav files
        logfiles
            list of export log file paths associated with navfiles.  If provided, must be same number as nav files
        weekstart_year
            if you aren't providing a logfile, must provide the year of the sbet here
        weekstart_week
            if you aren't providing a logfile, must provide the week of the sbet here
        override_datum
            provide a string datum identifier if you want to override what is read from the log or you don't have a
            log, ex: 'NAD83 (2011)'
        override_grid
            provide a string grid identifier if you want to override what is read from the log or you don't have a log,
            ex: 'Universal Transverse Mercator'
        override_zone
            provide a string zone identifier if you want to override what is read from the log or you don't have a log,
             ex: 'UTM North 20 (66W to 60W)'
        override_ellipsoid
            provide a string ellipsoid identifier if you want to override what is read from the log or you don't have a
            log, ex: 'GRS80'
        max_gap_length
            maximum allowable gap in the sbet in seconds, excluding gaps found in raw navigation
        overwrite
            if True, will include files that are already in the navigation dataset as valid
        """

        self.logger.info('****Importing post processed navigation****\n')
        starttime = perf_counter()

        navfiles, errorfiles, logfiles = self._validate_post_processed_navigation(navfiles, errorfiles, logfiles, overwrite)
        if not navfiles:
            self.logger.error('import_post_processed_navigation: No valid navigation files to import')
            return

        try:
            navdata = return_xarray_from_sbet(navfiles, smrmsgfiles=errorfiles, logfiles=logfiles, weekstart_year=weekstart_year,
                                              weekstart_week=weekstart_week, override_datum=override_datum, override_grid=override_grid,
                                              override_zone=override_zone, override_ellipsoid=override_ellipsoid)
        except:
            navdata = None
        if not navdata:
            self.logger.error('import_post_processed_navigation: Unable to read from {}'.format(navfiles))
            return

        for rp in self.multibeam.raw_ping:
            if navdata.time.values[0] > rp.time.values[-1] or navdata.time.values[-1] < rp.time.values[0]:
                self.logger.warning('No overlap found between ping data and SBET navigation.')
                self.logger.warning('Raw navigation: UTC seconds from {} to {}.  SBET data: UTC seconds from {} to {}'.format(rp.time.values[0], rp.time.values[-1],
                                                                                                                navdata.time.values[0], navdata.time.values[-1]))
                continue

            # find the nearest new record to each existing navigation record, trim off the time period greater than max sbet time
            #  (should probably just have a max gap time in interp_across_chunks)
            nav_wise_data = interp_across_chunks(navdata, rp.time, 'time')
            gap_mask = np.logical_or(nav_wise_data.time < navdata.time[0] - max_gap_length,
                                     nav_wise_data.time > navdata.time[-1] + max_gap_length)
            for ky in nav_wise_data.variables.keys():
                if ky != 'time':
                    nav_wise_data[ky][gap_mask] = np.nan
            self.logger.info('{}: Writing {} new SBET navigation records'.format(rp.system_identifier, nav_wise_data.time.shape[0]))

            # find gaps that don't line up with existing nav gaps (like time between multibeam files)
            gaps = compare_and_find_gaps(self.multibeam.raw_ping[0], navdata, max_gap_length=max_gap_length, dimname='time')
            if gaps.any():
                self.logger.info('Found gaps > {} in comparison between post processed navigation and realtime.  Will not process soundings found in these gaps.'.format(max_gap_length))
                nav_wise_data = nav_wise_data.load()
                for gp in gaps:
                    self.logger.info('mintime: {}, maxtime: {}, gap length {}'.format(gp[0], gp[1], gp[1] - gp[0]))
                    gap_mask = np.logical_and(nav_wise_data.time < gp[1], nav_wise_data.time > gp[0])
                    for ky in nav_wise_data.variables.keys():
                        if ky != 'time':
                            if ky in rp:
                                nav_wise_data[ky][gap_mask] = rp[ky][gap_mask]
                            else:
                                nav_wise_data[ky][gap_mask] = np.nan

            navdata_attrs = nav_wise_data.attrs
            navdata_times = [nav_wise_data.time]
            try:
                nav_wise_data = self.client.scatter(nav_wise_data)
            except:  # not using dask distributed client
                pass
            outfold, _ = self.write('ping', [nav_wise_data], time_array=navdata_times, attributes=navdata_attrs, sys_id=rp.system_identifier)

        if self.multibeam.raw_ping[0].current_processing_status >= 4:
            # have to start over at georeference now, if there isn't any postprocessed navigation
            self.write_attribute_to_ping_records({'current_processing_status': 3})
            self.logger.info('Setting processing status to 3, starting over at georeferencing')
        self.multibeam.reload_pingrecords(skip_dask=self.client is None)

        endtime = perf_counter()
        self.logger.info('****Importing post processed navigation complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def remove_post_processed_navigation(self):
        """
        import_post_processed_navigation will write navigation and navigation related attributes to the Fqpr instance.
        This method will remove all variables and attributes related to post processed navigation.  If the current processing
        status of this Fqpr is greater than or equal to georeference, this method will also write a new current processing status
        informing the user/intelligence module to restart processing at georeferencing.
        """

        self.logger.info('****Removing post processed navigation****\n')
        starttime = perf_counter()

        if self.has_sbet:
            expected_records = kluster_variables.variables_by_key['processed navigation']
            expected_attributes = ['nav_error_files', 'nav_files', 'sbet_datum', 'sbet_ellipsoid', 'sbet_logging_rate_hz',
                                   'sbet_mission_date']
            for rpindex in range(len(self.multibeam.raw_ping)):  # for each sonar head (raw_ping)...
                for rec in expected_records:
                    try:  # remove the currently loaded xarray dataset variable
                        self.multibeam.raw_ping[rpindex] = self.multibeam.raw_ping[rpindex].drop(rec)
                    except:
                        print('WARNING: Unable to find loaded data matching record "{}"'.format(rec))
                    try:  # then remove the matching zarr data on disk
                        self.delete('ping', rec, self.multibeam.raw_ping[rpindex].system_identifier)
                    except:
                        print('WARNING: Unable to find data on disk matching record "{}" for sonar {}'.format(rec,
                                                                                                              self.multibeam.raw_ping[rpindex].system_identifier))
                for recattr in expected_attributes:
                    try:
                        self.multibeam.raw_ping[rpindex].attrs.pop(recattr)
                    except:
                        print('WARNING: Unable to find loaded attribute data matching attribute "{}"'.format(recattr))
                    try:
                        self.remove_attribute('ping', recattr, self.multibeam.raw_ping[0].system_identifier)
                    except:
                        print('WARNING: Unable to find data on disk matching attribute "{}" for sonar {}'.format(recattr,
                                                                                                                 self.multibeam.raw_ping[rpindex].system_identifier))
            if self.multibeam.raw_ping[0].current_processing_status >= 4:
                # have to start over at georeference now, if you removed sbet data
                self.write_attribute_to_ping_records({'current_processing_status': 3})
                self.logger.info('Setting processing status to 3, starting over at georeferencing')
        else:
            print('No post processed navigation found to remove')

        endtime = perf_counter()
        self.logger.info('****Removing post processed navigation complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def overwrite_raw_navigation(self, navfiles: list, weekstart_year: int, weekstart_week: int, overwrite: bool = False):
        """
        Load from raw navigation files (currently just POS MV .000) to get lat/lon/altitude.  Will overwrite the original
        raw navigation zarr rootgroup, so you can compare pos mv to sbet.

        No interpolation is done, but it will slice the incoming data to the time extents of the raw navigation and
        identify time gaps larger than the provided max_gap_length in seconds.

        Parameters
        ----------
        navfiles
            list of postprocessed navigation file paths
        weekstart_year
            must provide the year of the pos mv file here
        weekstart_week
            must provide the week of the pos mv file here
        overwrite
            if True, will include files that are already in the navigation dataset as valid
        """

        self.logger.info('****Overwriting raw navigation****\n')
        starttime = perf_counter()

        navfiles = self._validate_raw_navigation(navfiles, overwrite)

        try:
            navdata = return_xarray_from_posfiles(navfiles, weekstart_year=weekstart_year, weekstart_week=weekstart_week)
        except:
            navdata = None
        if not navdata:
            print('Unable to generate xarray dataset from {}'.format(navfiles))
            return
        for rp in self.multibeam.raw_ping:
            if navdata.time.values[0] > rp.time.values[-1] or navdata.time.values[-1] < rp.time.values[0]:
                print('{}: No overlap found between POS data and raw navigation, probably due to incorrect date entered.')
                print('Raw navigation: UTC seconds from {} to {}.  POS data: UTC seconds from {} to {}'.format(rp.time.values[0], rp.time.values[-1],
                                                                                                               navdata.time.values[0], navdata.time.values[-1]))
                continue
            # find the nearest new record to each existing navigation record
            nav_wise_data = interp_across_chunks(navdata, rp.time, 'time')
            print('{}: Overwriting with {} new navigation records'.format(rp.system_identifier, nav_wise_data.time.shape[0]))
            nan_check = np.isnan(nav_wise_data.latitude)
            if nan_check.any():
                print('{}: Found {} records that are not in the new navigation data, keeping these original values'.format(rp.system_identifier, np.count_nonzero(nan_check)))
                nav_wise_data = nav_wise_data.dropna('time', how='any')

            navdata_attrs = nav_wise_data.attrs
            navdata_times = [nav_wise_data.time]
            try:
                nav_wise_data = self.client.scatter(nav_wise_data)
            except:  # not using dask distributed client
                pass
            outfold, _ = self.write('ping', [nav_wise_data], time_array=navdata_times, attributes=navdata_attrs, sys_id=rp.system_identifier)

        if self.multibeam.raw_ping[0].current_processing_status >= 4 and not self.has_sbet:
            # have to start over at georeference now, if there isn't any postprocessed navigation
            self.write_attribute_to_ping_records({'current_processing_status': 3})
            self.logger.info('Setting processing status to 3, starting over at georeferencing')
        self.multibeam.reload_pingrecords(skip_dask=self.client is None)

        endtime = perf_counter()
        self.logger.info('****Overwriting raw navigation complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def interp_to_ping_record(self, sources: Union[xr.Dataset, list], attributes: dict = None):
        """
        Take in a dataset that is not at ping time (raw navigation, attitude, etc.) and interpolate it to ping time
        and save it to the raw ping datasets.

        Parameters
        ----------
        sources
            one or more datasets that you want to interpolate and save to the raw ping datastores
        attributes
            optional attributes to write to the zarr datastore
        """

        if not isinstance(sources, list):
            sources = [sources]
        if attributes is None:
            attributes = {}

        for src in sources:
            self.logger.info('****Performing interpolation of {}****\n'.format(list(src.keys())))
        starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        for rp in self.multibeam.raw_ping:
            for source in sources:
                ping_wise_data = interp_across_chunks(source, rp.time, 'time').chunk(kluster_variables.ping_chunk_size)
                ping_wise_times = [ping_wise_data.time]
                try:
                    ping_wise_data = self.client.scatter(ping_wise_data)
                except:  # not using dask distributed client
                    pass
                self.write('ping', [ping_wise_data], time_array=ping_wise_times, attributes=attributes, sys_id=rp.system_identifier)
        self.multibeam.reload_pingrecords(skip_dask=skip_dask)
        endtime = perf_counter()
        self.logger.info('****Interpolation complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def initial_att_interpolation(self):
        """
        We provide as an optional step in self.get_orientation_vectors (or run separately) the ability to interpolate
        the raw attitude and navigation to the ping record times and save these records to disk.  Otherwise,
        each time attitude/navigation is needed by the processing module, it will be interpolated then.
        """

        needs_interp = None
        if ('roll' not in list(self.multibeam.raw_ping[0].keys())) or \
                ('pitch' not in list(self.multibeam.raw_ping[0].keys())) or \
                ('heave' not in list(self.multibeam.raw_ping[0].keys())) or \
                ('heading' not in list(self.multibeam.raw_ping[0].keys())):
            needs_interp = [self.multibeam.raw_att]
        if needs_interp:
            self.interp_to_ping_record(needs_interp, {'attitude_source': 'multibeam'})

    def get_orientation_vectors(self, subset_time: list = None, dump_data: bool = True, initial_interp: bool = False):
        """
        Using attitude angles, mounting angles, build the tx/rx vectors that represent the orientation of the tx/rx at
        time of transmit/receive.   Sends the data and calculations to the cluster, receive futures objects back.
        Use the dump_data/delete_futs to interact with the futures object.

        | To process only a section of the dataset, use subset_time.
        | ex: subset_time=[1531317999, 1531321000] means only process times that are from 1531317999 to 1531321000
        | ex: subset_time=[[1531317999, 1531318885], [1531318886, 1531321000]] means only process times that are
                from either 1531317999 to 1531318885 or 1531318886 to 1531321000

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the tx/rx vectors to the multibeam datastore.  Set this to false for an entirely in memory
            workflow
        initial_interp
            if True, will interpolate attitude to the ping record and store in the raw_ping datasets.  This
            is not mandatory for processing, but useful for other kluster functions post processing.
        """

        self._validate_get_orientation_vectors(subset_time, dump_data)
        if initial_interp:  # optional step if you want to save interpolated attitude/navigation at ping time to disk
            self.initial_att_interpolation()
        if dump_data:
            self.logger.info('****Building tx/rx vectors at time of transmit/receive****\n')
            starttime = perf_counter()  # use starttime to time the process
            # each run of this process overwrites existing offsets/angles with the currently set ones
            self.write_attribute_to_ping_records({'xyzrph': self.multibeam.xyzrph})

        skip_dask = False  # skip dask will allow us to process without dask distributed
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        # systems lets us loop through...
        #  - each raw_ping dataset, which corresponds to each head of the sonar (two iterations for dual head sonar)
        #    - within each raw_ping, the installation parameter records, as there may be multiple (recording changes
        #      in waterline or something like that)
        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)

        # for each serial number combination...only one loop here unless you have a dual head system
        for s_cnt, system in enumerate(systems):
            if system is None:  # get here if one of the heads is disabled (set to None)
                continue
            ra = self.multibeam.raw_ping[s_cnt]  # raw ping record
            sys_ident = ra.system_identifier
            if dump_data:
                self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            # when we process, we store the futures in self.intermediate_dat, so we can access it later
            self.initialize_intermediate_data(sys_ident, 'orientation')
            # get the settings we want to use for this sector, controls the amount of data we pass at once
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            # for each installation parameters record...
            for applicable_index, timestmp, prefixes in system:
                if dump_data:
                    self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                self.generate_starter_orientation_vectors(prefixes, timestmp)
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'orientation', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask)
                else:
                    if dump_data:
                        self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))

            if dump_data:
                del self.intermediate_dat[sys_ident]['orientation']
        if dump_data:
            # after each full processing step, reload the raw_ping datasets to get the new metadata
            self.multibeam.reload_pingrecords(skip_dask=skip_dask)
            self.subset.redo_subset()
            endtime = perf_counter()
            self.logger.info('****Get Orientation Vectors complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def get_beam_pointing_vectors(self, subset_time: list = None, dump_data: bool = True):
        """
        Beam pointing vector is the beam specific vector that arises from the intersection of the tx ping and rx cone
        of sensitivity.  Points at that area.  Is in the geographic coordinate system, built using the tx/rx at time of
        ping/receive.  Sends the data and calculations to the cluster, receive futures objects back.
        Use the dump_data/delete_futs to interact with the futures object.

        | To process only a section of the dataset, use subset_time.
        | ex: subset_time=[1531317999, 1531321000] means only process times that are from 1531317999 to 1531321000
        | ex: subset_time=[[1531317999, 1531318885], [1531318886, 1531321000]] means only process times that are
                from either 1531317999 to 1531318885 or 1531318886 to 1531321000

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore.  Set this to false for an entirely in memory
            workflow
        """

        self._validate_get_beam_pointing_vectors(subset_time, dump_data)
        if dump_data:
            self.logger.info('****Building beam specific pointing vectors****\n')
            starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            if system is None:  # get here if one of the heads is disabled (set to None)
                continue
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            if dump_data:
                self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'bpv')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                if dump_data:
                    self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                self.generate_starter_orientation_vectors(prefixes, timestmp)
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'bpv', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask)
                else:
                    if dump_data:
                        self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))
            if dump_data:
                del self.intermediate_dat[sys_ident]['bpv']
        if dump_data:
            self.multibeam.reload_pingrecords(skip_dask=skip_dask)
            self.subset.redo_subset()
            endtime = perf_counter()
            self.logger.info('****Beam Pointing Vector generation complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def sv_correct(self, add_cast_files: Union[str, list] = None, cast_selection_method: str = 'nearest_in_time',
                   subset_time: list = None, dump_data: bool = True):
        """
        Apply sv cast/surface sound speed to raytrace.  Generates xyz for each beam.
        Currently only supports nearest-in-time for selecting the cast for each chunk.   Sends the data and
        calculations to the cluster, receive futures objects back.  Use the dump_data/delete_futs to interact with
        the futures object.

        | To process only a section of the dataset, use subset_time.
        | ex: subset_time=[1531317999, 1531321000] means only process times that are from 1531317999 to 1531321000
        | ex: subset_time=[[1531317999, 1531318885], [1531318886, 1531321000]] means only process times that are
                from either 1531317999 to 1531318885 or 1531318886 to 1531321000

        Parameters
        ----------
        add_cast_files
            either a list of files to include or the path to a directory containing files.  These are in addition to
            the casts in the ping dataset.
        cast_selection_method
            the method used to select the cast that goes with each chunk of the dataset, one of ['nearest_in_time',
            'nearest_in_time_four_hours', 'nearest_in_distance', 'nearest_in_distance_four_hours']
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore.  Set this to false for an entirely in memory
            workflow
        """

        self._validate_sv_correct(subset_time, dump_data)
        if dump_data:
            self.logger.info('****Correcting for sound velocity****\n')
            starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        if add_cast_files:
            self.import_sound_velocity_files(add_cast_files)

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            if system is None:  # get here if one of the heads is disabled (set to None)
                continue
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            if dump_data:
                self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'sv_corr')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                if dump_data:
                    self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'sv_corr', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask,
                                                 cast_selection_method=cast_selection_method)
                else:
                    if dump_data:
                        self.logger.info('No pings found for {}-{}'.format(ra.system_identifier, timestmp))
            if dump_data:
                del self.intermediate_dat[sys_ident]['sv_corr']

        if dump_data:
            self.multibeam.reload_pingrecords(skip_dask=skip_dask)
            self.subset.redo_subset()
            endtime = perf_counter()
            self.logger.info('****Sound Velocity complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def georef_xyz(self, subset_time: list = None, prefer_pp_nav: bool = True, dump_data: bool = True,
                   vdatum_directory: str = None):
        """
        Use the raw attitude/navigation to transform the vessel relative along/across/down offsets to georeferenced
        soundings.  Will support transformation to geographic and projected coordinate systems and with a vertical
        reference that you select.

        If uncertainty is included in the source data, will calculate the unc based on depth.

        First does a forward transformation using the geoid provided in horizontal_crs
        Then does a transformation from geographic to projected, if that is included in horizontal_crs

        Uses pyproj to do all transformations.  User must run self.construct_crs first to establish the destination
        datum and ellipsoid.

        Sends the data and calculations to the cluster, receive futures objects back.  Use the dump_data/delete_futs to
        interact with the futures object.

        | To process only a section of the dataset, use subset_time.
        | ex: subset_time=[1531317999, 1531321000] means only process times that are from 1531317999 to 1531321000
        | ex: subset_time=[[1531317999, 1531318885], [1531318886, 1531321000]] means only process times that are
                from either 1531317999 to 1531318885 or 1531318886 to 1531321000

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        prefer_pp_nav
            if True will use post-processed navigation/height (SBET)
        dump_data
            if True dump the futures to the multibeam datastore.  Set this to false for an entirely in memory
            workflow
        vdatum_directory
            if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here
        """

        self._validate_georef_xyz(subset_time, dump_data)
        if dump_data:
            self.logger.info('****Georeferencing sound velocity corrected beam offsets****\n')
            starttime = perf_counter()
            self.write_attribute_to_ping_records({'xyzrph': self.multibeam.xyzrph})
            self.logger.info('Using pyproj CRS: {}'.format(self.horizontal_crs.to_string()))

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            if system is None:  # get here if one of the heads is disabled (set to None)
                continue
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            if dump_data:
                self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'georef')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                if dump_data:
                    self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'georef', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask,
                                                 prefer_pp_nav=prefer_pp_nav, vdatum_directory=vdatum_directory)
                else:
                    if dump_data:
                        self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))
            if dump_data:
                del self.intermediate_dat[sys_ident]['georef']

        if dump_data:
            self.multibeam.reload_pingrecords(skip_dask=skip_dask)
            self._overwrite_georef_stats()
            self.subset.redo_subset()
            endtime = perf_counter()
            self.logger.info('****Georeferencing sound velocity corrected beam offsets complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def calculate_total_uncertainty(self, subset_time: list = None, dump_data: bool = True):
        """
        Use the tpu module to calculate total horizontal uncertainty and total vertical uncertainty for each sounding.
        See tpu.py for more information

        | To process only a section of the dataset, use subset_time.
        | ex: subset_time=[1531317999, 1531321000] means only process times that are from 1531317999 to 1531321000
        | ex: subset_time=[[1531317999, 1531318885], [1531318886, 1531321000]] means only process times that are
                from either 1531317999 to 1531318885 or 1531318886 to 1531321000

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the futures to the multibeam datastore.  Set this to false for an entirely in memory
            workflow
        """

        self._validate_calculate_total_uncertainty(subset_time, dump_data)
        if dump_data:
            self.logger.info('****Calculating total uncertainty****\n')
            starttime = perf_counter()
            self.write_attribute_to_ping_records({'xyzrph': self.multibeam.xyzrph})

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            if system is None:  # get here if one of the heads is disabled (set to None)
                continue
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            if dump_data:
                self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'tpu')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                if dump_data:
                    self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                self.generate_starter_orientation_vectors(prefixes, timestmp)  # have to include this to know if rx is reversed to reverse raw beam angles
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'tpu', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask)
                else:
                    self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))

        if dump_data:
            self.multibeam.reload_pingrecords(skip_dask=skip_dask)
            self.subset.redo_subset()
            endtime = perf_counter()
            self.logger.info('****Calculating total uncertainty complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def export_pings_to_file(self, output_directory: str = None, file_format: str = 'csv', csv_delimiter=' ',
                             filter_by_detection: bool = True, format_type: str = 'xyz', z_pos_down: bool = True, export_by_identifiers: bool = True):
        """
        Run the export module to export point cloud relevant data to file, see export.export_pings_to_file
        """
        written_files = self.export.export_pings_to_file(output_directory=output_directory, file_format=file_format,
                                                         csv_delimiter=csv_delimiter, filter_by_detection=filter_by_detection,
                                                         format_type=format_type, z_pos_down=z_pos_down,
                                                         export_by_identifiers=export_by_identifiers)
        return written_files

    def export_lines_to_file(self, linenames: list = None, output_directory: str = None, file_format: str = 'csv', csv_delimiter=' ',
                             filter_by_detection: bool = True, format_type: str = 'xyz', z_pos_down: bool = True, export_by_identifiers: bool = True):
        """
        Run the export module to export only the data belonging to the given lines to file, see export.export_lines_to_file
        """
        written_files = self.export.export_lines_to_file(linenames=linenames, output_directory=output_directory,
                                                         file_format=file_format, csv_delimiter=csv_delimiter,
                                                         filter_by_detection=filter_by_detection, format_type=format_type,
                                                         z_pos_down=z_pos_down, export_by_identifiers=export_by_identifiers)
        return written_files

    def export_soundings_to_file(self, datablock: list, output_directory: str = None, file_format: str = 'csv',
                                 csv_delimiter=' ', filter_by_detection: bool = True, format_type: str = 'xyz',
                                 z_pos_down: bool = True):
        """
        Run the export module to export given soundings to file, see export.export_soundings_to_file
        """

        self.export.export_soundings_to_file(datablock, output_directory, file_format, csv_delimiter, filter_by_detection,
                                             format_type, z_pos_down)

    def export_tracklines_to_file(self, linenames: list = None, output_file: str = None, file_format: str = 'GPKG'):
        """
        Run the export module to export the navigation to vector file, see export.export_tracklines_to_file
        """

        self.export.export_tracklines_to_file(linenames, output_file, file_format)

    def export_variable(self, dataset_name: str, var_name: str, dest_path: str, reduce_method: str = None,
                        zero_centered: bool = False):
        """
        Run the export module to export the given variable to csv, writing to the provided path, see export.export_variable_to_csv
        """

        self.export.export_variable_to_csv(dataset_name, var_name, dest_path, reduce_method, zero_centered)

    def export_dataset(self, dataset_name: str, dest_path: str):
        """
        Run the export module to export each variable in the given dataset to one csv, writing to the provided path, see export.export_dataset_to_csv
        """

        self.export.export_dataset_to_csv(dataset_name, dest_path)

    def run_filter(self, filtername: str, *args, selected_index: list = None, save_to_disk: bool = True, **kwargs):
        """
        Run the filter module with the provided filtername, will match the filename of the filter python file.

        Parameters
        ----------
        filtername
            name of the file that you want to load
        selected_index
            optional list of 1d boolean arrays representing the flattened index of those values to retain.  Used mainly
            in Points View filtering, where you have a (time,beam) space but only want to retain the beams shown in
            Points View.
        save_to_disk
            if True, will save the new sounding status to disk
        """

        return self.filter.run_filter(filtername, selected_index, save_to_disk=save_to_disk, **kwargs)

    def _submit_data_to_cluster(self, rawping: xr.Dataset, mode: str, idx_by_chunk: list, max_chunks_at_a_time: int,
                                timestmp: str, prefixes: str, dump_data: bool = True, skip_dask: bool = False,
                                prefer_pp_nav: bool = True, vdatum_directory: str = None,
                                cast_selection_method: str = 'nearest_in_time'):
        """
        For all of the main processes, we break up our inputs into chunks, appended to a list (data_for_workers).
        Knowing the capacity of the cluster memory, we can determine how many chunks to run at a time
        (max_chunks_at_a_time) and submit map those chunks to the cluster workers.  Append the resulting futures
        to the futures_repo and wait inbetween mappings.  This limits the memory used so that we don't run out.

        Parameters
        ----------
        rawping
            xarray Dataset for the ping records
        mode
            one of ['orientation', 'bpv', sv_corr', 'georef', 'tpu']
        idx_by_chunk
            values are the integer indexes of the pings to use, coords are the time of ping
        max_chunks_at_a_time
            maximum number of data chunks to load and process at a time
        timestmp
            timestamp of the installation parameters instance used
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        dump_data
            if True dump the tx/rx vectors to the multibeam datastore.  Set this to false for an entirely in memory
            workflow
        prefer_pp_nav
            if True will use post-processed navigation/height (SBET)
        vdatum_directory
            if 'NOAA MLLW' 'NOAA MHW' is the vertical reference, a path to the vdatum directory is required here
        skip_dask
            if True will not use the dask.distributed client to submit tasks, will run locally instead
        cast_selection_method
            the method used to select the cast that goes with each chunk of the dataset, one of ['nearest_in_time',
            'nearest_in_time_four_hours', 'nearest_in_distance', 'nearest_in_distance_four_hours']
        """

        # clear out the intermediate data just in case there is old data there
        sys_ident = rawping.system_identifier
        self.intermediate_dat[sys_ident][mode][timestmp] = []
        tot_runs = int(np.ceil(len(idx_by_chunk) / max_chunks_at_a_time))
        for rn in range(tot_runs):
            silent = (rn != 0) or not dump_data  # only messages for the first chunk, and only when we are writing to disk
            starttime = perf_counter()
            start_r = rn * max_chunks_at_a_time
            end_r = min(start_r + max_chunks_at_a_time, len(idx_by_chunk))  # clamp for last run
            idx_by_chunk_subset = idx_by_chunk[start_r:end_r].copy()
            start_run_index = rn * max_chunks_at_a_time

            if mode == 'orientation':
                kluster_function = distrib_run_build_orientation_vectors
                chunk_function = self._generate_chunks_orientation
                comp_time = 'orientation_time_complete'
                chunkargs = [rawping, idx_by_chunk_subset, timestmp, prefixes]
            elif mode == 'bpv':
                kluster_function = distrib_run_build_beam_pointing_vector
                chunk_function = self._generate_chunks_bpv
                comp_time = 'bpv_time_complete'
                chunkargs = [rawping, idx_by_chunk_subset, timestmp, start_run_index]
            elif mode == 'sv_corr':
                kluster_function = distributed_run_sv_correct
                chunk_function = self._generate_chunks_svcorr
                comp_time = 'sv_time_complete'
                profnames, casts, cast_times, castlocations = self.return_all_profiles()
                if cast_selection_method == 'nearest_in_time':
                    cast_chunks = self.return_cast_idx_nearestintime(idx_by_chunk_subset, silent=silent)
                elif cast_selection_method == 'nearest_in_time_four_hours':
                    cast_chunks = self.return_cast_idx_nearestintime_fourhours(idx_by_chunk_subset, silent=silent)
                elif cast_selection_method == 'nearest_in_distance':
                    cast_chunks = self.return_cast_idx_nearestindistance(idx_by_chunk_subset, silent=silent)
                elif cast_selection_method == 'nearest_in_distance_four_hours':
                    cast_chunks = self.return_cast_idx_nearestindistance_fourhours(idx_by_chunk_subset, silent=silent)
                else:
                    msg = f'unexpected cast selection method "{cast_selection_method}", must be one of ' \
                          f'{kluster_variables.cast_selection_methods} as of 0.9.6.  Defaulting to nearest_in_time.'
                    self.logger.warning(msg)
                    cast_chunks = self.return_cast_idx_nearestintime(idx_by_chunk_subset, silent=silent)
                self.svmethod = cast_selection_method
                addtl_offsets = self.return_additional_xyz_offsets(rawping, prefixes, timestmp, idx_by_chunk_subset)
                chunkargs = [rawping, cast_chunks, casts, prefixes, timestmp, addtl_offsets, start_run_index]
            elif mode == 'georef':
                refpt = self.multibeam.return_prefix_for_rp()
                kluster_function = distrib_run_georeference
                chunk_function = self._generate_chunks_georef
                comp_time = 'georef_time_complete'
                z_offset = float(self.multibeam.xyzrph[prefixes[refpt[2]] + '_z'][timestmp])
                chunkargs = [rawping, idx_by_chunk_subset, prefixes, timestmp, z_offset, prefer_pp_nav, vdatum_directory, start_run_index]
            elif mode == 'tpu':
                kluster_function = distrib_run_calculate_tpu
                chunk_function = self._generate_chunks_tpu
                comp_time = 'tpu_time_complete'
                chunkargs = [rawping, idx_by_chunk_subset, prefixes, timestmp, start_run_index]
            else:
                self.logger.error('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')
                raise ValueError('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')

            data_for_workers = chunk_function(*chunkargs, silent=silent)
            try:
                futs = self.client.map(kluster_function, data_for_workers)
                if self.show_progress:
                    progress(futs, multi=False)
                endtimes = [len(c) for c in idx_by_chunk]
                futs_with_endtime = [[f, endtimes[cnt]] for cnt, f in enumerate(futs)]
                self.intermediate_dat[sys_ident][mode][timestmp].extend(futs_with_endtime)
                wait(self.intermediate_dat[sys_ident][mode][timestmp])
            except:  # get here if client is closed or not setup
                for cnt, dat in enumerate(data_for_workers):
                    endtime = len(idx_by_chunk[cnt])
                    data = kluster_function(dat)
                    self.intermediate_dat[sys_ident][mode][timestmp].append([data, endtime])
            if dump_data:
                self.__setattr__(comp_time, datetime.utcnow().strftime('%c'))
                self.write_intermediate_futs_to_zarr(mode, rawping.system_identifier, timestmp, skip_dask=skip_dask)
                endtime = perf_counter()
                self.logger.info('Processing chunk {} out of {} complete: {}'.format(rn + 1, tot_runs,
                                                                                     seconds_to_formatted_string(int(endtime - starttime))))
        if mode == 'georef' and self.vert_ref == 'Aviso MLLW':  # free up the memory associated with the aviso model after all runs
            aviso_clear_model()

    def write_intermediate_futs_to_zarr(self, mode: str, sys_ident: str, timestmp: str, skip_dask: bool = False):
        """
        Flush some of the intermediate data that was mapped to the cluster (and lives in futures objects) to disk, puts
        it in the multibeam, as the time dimension should be the same.  Mode allows for selecting the output from one
        of the main processes for writing.

        Parameters
        ----------
        mode
            one of ['orientation', 'bpv', sv_corr', 'georef', 'tpu']
        sys_ident
            the multibeam system identifier attribute, used as a key to find the intermediate data
        timestmp
            timestamp of the installation parameters instance used
        skip_dask
            if True will not use the dask.distributed client to submit tasks, will run locally instead
        """

        if mode == 'orientation':
            mode_settings = ['orientation', ['tx', 'rx', 'processing_status'], 'orientation vectors',
                             {'_compute_orientation_complete': self.orientation_time_complete,
                              'current_processing_status': 1,
                              'reference': {'tx': 'unit vector', 'rx': 'unit vector'},
                              'units': {'tx': ['+ forward', '+ starboard', '+ down'],
                                        'rx': ['+ forward', '+ starboard', '+ down']}}]
        elif mode == 'bpv':
            mode_settings = ['bpv', ['rel_azimuth', 'corr_pointing_angle', 'processing_status'], 'beam pointing vectors',
                             {'_compute_beam_vectors_complete': self.bpv_time_complete,
                              'current_processing_status': 2,
                              'reference': {'rel_azimuth': 'vessel heading',
                                            'corr_pointing_angle': 'vertical in geographic reference frame'},
                              'units': {'rel_azimuth': 'radians', 'corr_pointing_angle': 'radians'}}]
        elif mode == 'sv_corr':
            mode_settings = ['sv_corr', ['alongtrack', 'acrosstrack', 'depthoffset', 'processing_status'], 'sv corrected data',
                             {'svmode': self.svmethod, '_sound_velocity_correct_complete': self.sv_time_complete,
                              'current_processing_status': 3,
                              'reference': {'alongtrack': 'reference point', 'acrosstrack': 'reference point',
                                            'depthoffset': 'transmitter'},
                              'units': {'alongtrack': 'meters (+ forward)', 'acrosstrack': 'meters (+ starboard)',
                                        'depthoffset': 'meters (+ down)'}}]
        elif mode == 'georef':
            crs = self.horizontal_crs.to_epsg()
            if self.vert_ref == 'NOAA MLLW':
                vertcrs = vertical_datum_to_wkt('mllw', crs, self.multibeam.raw_ping[0].min_lon, self.multibeam.raw_ping[0].min_lat,
                                                self.multibeam.raw_ping[0].max_lon, self.multibeam.raw_ping[0].max_lat)
            elif self.vert_ref == 'NOAA MHW':
                vertcrs = vertical_datum_to_wkt('mhw', crs, self.multibeam.raw_ping[0].min_lon, self.multibeam.raw_ping[0].min_lat,
                                                self.multibeam.raw_ping[0].max_lon, self.multibeam.raw_ping[0].max_lat)
            elif self.vert_ref == 'ellipse':
                vertcrs = vertical_datum_to_wkt('ellipse', crs, self.multibeam.raw_ping[0].min_lon, self.multibeam.raw_ping[0].min_lat,
                                                self.multibeam.raw_ping[0].max_lon, self.multibeam.raw_ping[0].max_lat)
            else:
                vertcrs = 'Unknown'
            if self._using_sbet:
                navigation_source = 'sbet'
            else:
                navigation_source = 'multibeam'
            mode_settings = ['georef', ['x', 'y', 'z', 'corr_heave', 'corr_altitude', 'datum_uncertainty', 'geohash', 'processing_status'],
                             'georeferenced soundings data',
                             {'horizontal_crs': crs, 'vertical_reference': self.vert_ref,
                              'vertical_crs': vertcrs, 'navigation_source': navigation_source,
                              '_georeference_soundings_complete': self.georef_time_complete,
                              'current_processing_status': 4,
                              'reference': {'x': 'reference', 'y': 'reference', 'z': 'reference',
                                            'corr_heave': 'transmitter', 'corr_altitude': 'transmitter to ellipsoid'},
                              'units': {'x': 'meters (+ forward)', 'y': 'meters (+ starboard)',
                                        'z': 'meters (+ down)', 'corr_heave': 'meters (+ down)',
                                        'corr_altitude': 'meters (+ up)'}}]
        elif mode == 'tpu':
            mode_settings = ['tpu', ['tvu', 'thu', 'processing_status'],
                             'total horizontal and vertical uncertainty',
                             {'_total_uncertainty_complete': self.tpu_time_complete,
                              'vertical_reference': self.vert_ref,
                              'current_processing_status': 5,
                              'reference': {'tvu': 'None', 'thu': 'None'},
                              'units': {'tvu': 'meters (+ down)', 'thu': 'meters'}}]
        else:
            self.logger.error('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')
            raise ValueError('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')

        futs_data = []
        for f in self.intermediate_dat[sys_ident][mode_settings[0]][timestmp]:
            try:
                futs_data.extend([self.client.submit(combine_arrays_to_dataset, f[0], mode_settings[1])])
            except:  # client is not setup or closed, this is if you want to run on just your machine
                futs_data.extend([combine_arrays_to_dataset(f[0], mode_settings[1])])

        if futs_data:
            if not skip_dask:
                time_arrs = self.client.gather(self.client.map(_return_xarray_time, futs_data))
            else:
                time_arrs = [_return_xarray_time(tr) for tr in futs_data]
            self.write('ping', futs_data, attributes=mode_settings[3], time_array=time_arrs, sys_id=sys_ident,
                       skip_dask=skip_dask)

        self.intermediate_dat[sys_ident][mode_settings[0]][timestmp] = []

    def return_total_pings(self, min_time: float = None, max_time: float = None):
        """
        Get the total ping count, optionally within the provided mintime maxtime range

        Parameters
        ----------
        min_time
            the minimum time desired from the raw_ping dataset
        max_time
            the maximum time desired from the raw_ping dataset

        Returns
        -------
        int
            total number of pings for this dataset
        """

        total_pings = 0
        if min_time is not None or max_time is not None:
            if min_time is None:
                min_time = np.min([rp.time.values[0] for rp in self.multibeam.raw_ping])
            if max_time is None:
                max_time = np.max([rp.time.values[-1] for rp in self.multibeam.raw_ping])
            for ra in self.multibeam.raw_ping:
                slice_ra = slice_xarray_by_dim(ra, dimname='time', start_time=min_time, end_time=max_time)
                try:
                    total_pings += slice_ra.time.shape[0]
                except AttributeError:  # no pings in this slice
                    pass
        else:
            for rp in self.multibeam.raw_ping:
                total_pings += rp.time.shape[0]

        return total_pings

    def return_total_soundings(self, min_time: float = None, max_time: float = None):
        """
        Return the number of soundings in all systems within this fqpr instance, optionally within the provided mintime maxtime range

        Parameters
        ----------
        min_time
            the minimum time desired from the raw_ping dataset
        max_time
            the maximum time desired from the raw_ping dataset

        Returns
        -------
        int
            total number of soundings in the dataset
        """

        totalcount = 0
        if min_time is not None or max_time is not None:
            if min_time is None:
                min_time = np.min([rp.time.values[0] for rp in self.multibeam.raw_ping])
            if max_time is None:
                max_time = np.max([rp.time.values[-1] for rp in self.multibeam.raw_ping])
            for ra in self.multibeam.raw_ping:
                slice_ra = slice_xarray_by_dim(ra, dimname='time', start_time=min_time, end_time=max_time)
                try:
                    totalcount += np.count_nonzero(~np.isnan(slice_ra.frequency))
                except AttributeError:  # no pings in this slice
                    pass
        else:
            # count all the valid entries for each sounding in the frequency variable.  Use frequency because it is cheaper
            #   to load into memory vs the float32 variables
            for rp in self.multibeam.raw_ping:
                totalcount += np.count_nonzero(~np.isnan(rp.frequency))

        return totalcount

    def return_cast_dict(self):
        """
        Return a dictionary object combining the profile data and the attribution for each cast

        Returns
        -------
        dict
            dictionary of all the data for each profile, key is profile attribute name, ex: 'profile_1495563079'
        """
        return_dict = {}
        for attribute in self.multibeam.raw_ping[0].attrs:
            if attribute.find('profile') != -1:
                cast_time = attribute.split('_')[1]
                attribution = json.loads(self.multibeam.raw_ping[0].attrs['attributes_' + cast_time])
                attribution['time'] = int(cast_time)
                attribution['data'] = json.loads(self.multibeam.raw_ping[0].attrs[attribute])
                return_dict[attribute] = attribution
        return return_dict

    def subset_by_time(self, mintime: float = None, maxtime: float = None):
        """
        Use subset module to trim the fqpr instance to the given time range
        """

        return self.subset.subset_by_time(mintime, maxtime)

    def subset_by_times(self, time_segments: list):
        """
        Use subset module to trim the fqpr instance to the given time ranges
        """

        self.subset.subset_by_times(time_segments)

    def subset_by_time_and_beam(self, subset_time: np.ndarray, subset_beam: np.ndarray):
        """
        Use subset module to subset by time,beam provided, returns a 1d boolean mask for each sonar head
        """

        return self.subset.subset_by_time_and_beam(subset_time, subset_beam)

    def subset_by_lines(self, line_names: Union[str, list]):
        """
        Use subset module to trim the fqpr instance to the given lines
        """

        self.subset.subset_by_lines(line_names)

    def subset_variables(self, variable_selection: list, ping_times: Union[np.array, float, tuple] = None,
                         skip_subset_by_time: bool = False, filter_by_detection: bool = False):
        """
        Take specific variable names and return just those variables in a new xarray dataset, see subset module.
        """

        return self.subset.subset_variables(variable_selection, ping_times, skip_subset_by_time, filter_by_detection)

    def subset_variables_by_line(self, variable_selection: list, line_names: Union[str, list] = None, ping_times: tuple = None,
                                 filter_by_detection: bool = False):
        """
        Apply subset_variables to get the data split up into lines for the variable_selection provided, see subset module
        """

        return self.subset.subset_variables_by_line(variable_selection, line_names, ping_times, filter_by_detection)

    def restore_subset(self):
        """
        Restores the original data if subset_by_time has been run.
        """

        self.subset.restore_subset()

    def return_line_dict(self, line_names: Union[str, list] = None, ping_times: tuple = None):
        """
        Return all the lines with associated start and stop times for all sectors in the fqpr dataset.

        If line_names is provide, only return line data for those lines.  If ping_times is provided, trim all lines
        or drop lines that are not within the ping_times tuple (starttime in utc seconds, endtime in utc seconds)

        Parameters
        ----------
        line_names
            if provided, only returns data for the line(s), otherwise, returns data for all lines
        ping_times
            time to select the dataset by, must be a tuple of (min time, max time) in utc seconds.  If None, will use
            the full min/max time of the dataset

        Returns
        -------
        dict
            dictionary of names/start and stop times for all lines, ex: {'0022_20190716_232128_S250.all':
            [1563319288.304, 1563319774.876]}
        """

        mfiles = deepcopy(self.multibeam.raw_ping[0].multibeam_files)
        if line_names:
            if isinstance(line_names, str):
                line_names = [line_names]
            [mfiles.pop(lnme) for lnme in self.multibeam.raw_ping[0].multibeam_files.keys() if lnme not in line_names]
        if not mfiles and line_names:
            # print('No lines found in dataset, looked only for {}.  Dataset lines: {}'.format(line_names, list(mfiles.keys())))
            return {}
        elif not mfiles:
            # print('No lines found in dataset.')
            return {}
        if ping_times:
            try:
                sel_start_time, sel_end_time = float(ping_times[0]), float(ping_times[1])
            except:
                raise ValueError('return_line_dict: ping_times must be a tuple of (start time utc seconds, end time utc seconds): {}'.format(ping_times))
            corrected_mfiles = {}  # we need to trim the line start/end times by the given ping_times
            for linename in mfiles.keys():
                starttime, endtime = mfiles[linename][0], mfiles[linename][1]
                if starttime > sel_end_time:
                    continue
                if endtime < sel_start_time:
                    continue
                if starttime <= sel_start_time:
                    starttime = sel_start_time
                if endtime >= sel_end_time:
                    endtime = sel_end_time
                corrected_mfiles[linename] = mfiles[linename]
                corrected_mfiles[linename][0] = starttime
                corrected_mfiles[linename][1] = endtime
            mfiles = corrected_mfiles
        return mfiles

    def calc_min_var(self, varname: str = 'depthoffset'):
        """
        For given variable, return the minimum value found across all sectors

        Parameters
        ----------
        varname
            name of the variable you are interested in

        Returns
        -------
        float
            minimum value across all sectors
        """

        mins = np.array([])
        for ping in self.multibeam.raw_ping:
            mins = np.append(mins, float(ping[varname].min()))
        return mins.min()

    def calc_max_var(self, varname: str = 'depthoffset'):
        """
        For given variable, return the maximum value found across all sectors

        Parameters
        ----------
        varname
            name of the variable you are interested in

        Returns
        -------
        float
            maximum value across all sectors
        """

        maxs = np.array([])
        for ping in self.multibeam.raw_ping:
            maxs = np.append(maxs, float(ping[varname].max()))
        return maxs.max()

    def intersects(self, min_y: float, max_y: float, min_x: float, max_x: float, geographic: bool = True):
        """
        Check if the provided extents intersect with this fqpr instance.  Requires georeferencing has been performed

        Parameters
        ----------
        min_y
            minimum northing/latitude of extents
        max_y
            maximum northing/latitude of extents
        min_x
            minimum easting/longitude of extents
        max_x
            maximum easting/longitude of extents
        geographic
            if True, autotransforms to projected, if False, uses the northing/easting

        Returns
        -------
        bool
            True if the extents provided intersect with the fqpr instance, False if they do not
        """

        if 'min_x' in self.multibeam.raw_ping[0].attrs:
            fqpr_max_x = self.multibeam.raw_ping[0].max_x
            fqpr_min_x = self.multibeam.raw_ping[0].min_x
            fqpr_max_y = self.multibeam.raw_ping[0].max_y
            fqpr_min_y = self.multibeam.raw_ping[0].min_y
        else:
            print('Unable to query by northing/easting, georeference has not been performed')
            return False

        if geographic:
            trans = Transformer.from_crs(CRS.from_epsg(kluster_variables.epsg_wgs84),
                                         CRS.from_epsg(self.multibeam.raw_ping[0].horizontal_crs), always_xy=True)
            min_x, min_y = trans.transform(min_x, min_y)
            max_x, max_y = trans.transform(max_x, max_y)

        in_bounds = False
        if (min_x <= fqpr_max_x) and (max_x >= fqpr_min_x):
            if (min_y <= fqpr_max_y) and (max_y >= fqpr_min_y):
                in_bounds = True
        return in_bounds

    def return_unique_mode(self):
        """
        Finds the unique mode entries in raw_ping Datasets.  If there is more than one unique mode, return them in order
        of most often found.

        Returns
        -------
        np.array
            array of mode settings
        """

        mode = np.unique(np.concatenate([np.unique(f.mode) for f in self.multibeam.raw_ping]))
        if len(mode) > 1:
            counts = []
            for m in mode:
                counts.append(np.sum([np.count_nonzero(f.mode.where(f.mode == m)) for f in self.multibeam.raw_ping]))
            self.logger.info('Found multiple unique mode entries in the dataset:')
            for idx, cnts in enumerate(counts):
                self.logger.info('{}: {} times'.format(mode[idx], cnts))
        return mode

    def return_rounded_frequency(self):
        """
        Returns the frequency rounded to match the freq settings commonly given with sonar manufacturer settings.  If
        you have entries like [270000, 290000, 310000, 330000], it returns [300000].  If its something like [69000, 71000]
        it returns [70000].

        Returns
        -------
        np.array
            array of rounded frequencies
        """

        freq_numbers = np.unique([np.unique(rp.frequency) for rp in self.multibeam.raw_ping])
        lens = np.max(np.unique([len(str(id)) for id in freq_numbers]))

        freqs = [f for f in freq_numbers if len(str(f)) == lens]
        digits = -(len(str(freqs[0])) - 1)
        rounded_freqs = list(np.unique([np.around(f, digits) for f in freqs]))

        return rounded_freqs

    def return_lines_for_times(self, times: np.array):
        """
        Given the 1d array of times (utc seconds), return a same size object array with the string value of the line
        file name that matches the time.

        Parameters
        ----------
        times
            1d numpy array of times in utc seconds

        Returns
        -------
        np.array
            1d object array of the string file name for the multibeam file that encompasses each time
        """

        lines = np.full(times.shape[0], '', dtype=object)
        # we shoudn't have to sort this dict, should be sorted naturally, but odd things can happen when
        # user appends new data to existing storage.
        mbeslines = {k: v for k, v in sorted(self.multibeam.raw_ping[0].multibeam_files.items(),
                                             key=lambda item: item[1][0])}
        numlines = len(mbeslines)
        for cnt, (ln, ln_times) in enumerate(mbeslines.items()):
            ln_times[0] = ln_times[0] - 1  # small buffer for ping times slightly outside
            ln_times[1] = ln_times[1] + 1
            applicable_idx = np.logical_and(times >= ln_times[0], times <= ln_times[1])
            lines[applicable_idx] = ln
            if cnt == 0:  # handle times slightly less than the first lines logged starttime
                applicable_idx = times < ln_times[0]
                lines[applicable_idx] = ln
            elif cnt == numlines - 1:  # handle times slightly past the last lines logged endtime
                applicable_idx = times > ln_times[1]
                lines[applicable_idx] = ln

        return lines

    def return_line_time(self, line_name: str):
        """
        Return the start and end time for the given line name

        Parameters
        ----------
        line_name
            file name for the multibeam file, ex: 0000_testhis.all

        Returns
        -------
        float
            start time in utc seconds for the line
        float
            end time in utc seconds for the line
        """

        line_dict = self.return_line_dict()
        sortedlines = sorted(line_dict, key=lambda item: item[0])  # first item in values is the minimum time
        if line_name in line_dict.keys():
            line_times = [line_dict[line_name][0], line_dict[line_name][1]]
            if len(list(line_dict.keys())) == 1:  # only one line
                return float(np.min([rp.time.values[0] for rp in self.multibeam.raw_ping])),\
                       float(np.max([rp.time.values[-1] for rp in self.multibeam.raw_ping]))
            elif line_name == sortedlines[0]:  # first line, correct for any small discrepancy between metadata and data
                return float(np.min([rp.time.values[0] for rp in self.multibeam.raw_ping])), line_times[1]
            elif line_name == sortedlines[-1]:  # last line
                return line_times[0], float(np.max([rp.time.values[-1] for rp in self.multibeam.raw_ping]))
            else:
                return line_times[0], line_times[1]
        else:
            return None, None

    def return_line_xyzrph(self, line_name: str):
        """
        Return only the relevant xyzrph (kluster vessel config data) entries for the given line name.

        Parameters
        ----------
        line_name
            file name of the multibeam line

        Returns
        -------
        dict
            xyzrph trimmed to only the relevant entries for the line
        """

        start_time, end_time = self.return_line_time(line_name)
        if start_time and end_time:
            return trim_xyzrprh_to_times(self.multibeam.xyzrph, start_time, end_time)
        else:
            print('return_line_xyzrph: Line {} is not a part of this converted data instance'.format(line_name))
            return None

    def return_soundings_in_polygon(self, polygon: np.ndarray, geographic: bool = True,
                                    variable_selection: tuple = ('head', 'x', 'y', 'z', 'tvu', 'detectioninfo', 'time', 'beam'),
                                    isolate_head: int = None):
        """
        Using provided coordinates (in either horizontal_crs projected or geographic coordinates), return the soundings
        and sounding attributes for all soundings within the coordinates, see subset module.  Also sets the ping_filter
        attribute which can be used with set_variable_by_filter get_variable_by_filter
        """

        datablock = self.subset.return_soundings_in_polygon(polygon, geographic, variable_selection, isolate_head=isolate_head)
        return datablock

    def set_filter_by_polygon(self, polygon: np.ndarray, geographic: bool = True):
        """
        Alternative way to set the ping_filter attribute which can be used with set_variable_by_filter
        get_variable_by_filter, see subset module.
        """

        self.subset.set_filter_by_polygon(polygon, geographic)

    def set_variable_by_filter(self, var_name: str = 'detectioninfo', newval: Union[np.array, int, str, float] = 2, selected_index: list = None):
        """
        ping_filter is set upon selecting points in 2d/3d in Kluster.  See return_soundings_in_polygon.  Here we can take
        those points and set one of the variables with new data.  Optionally, you can include a selected_index that is a list
        of flattened indices to points in the ping_filter that you want to super-select, see subset module.
        """

        self.subset.set_variable_by_filter(var_name, newval, selected_index)

    def get_variable_by_filter(self, var_name: str, selected_index: list = None, by_sonar_head: bool = False):
        """
        ping_filter is set upon selecting points in 2d/3d in Kluster.  See return_soundings_in_polygon.  Here we can take
        those points and get one of the variables individually.  This is going to be faster than running return_soundings_in_polygon
        again and is kind of an added feature for just getting one other variable.

        Optionally, you can include a selected_index that is a list of flattened indices to points in the ping_filter
        that you want to super-select, see subset module.
        """

        return self.subset.get_variable_by_filter(var_name, selected_index, by_sonar_head)

    def return_processing_dashboard(self):
        """
        Return the necessary data for a dashboard like view of this fqpr instance.  Currently we are concerned with
        the total multibeam files associated with instance, and the processing status of each sector at a sounding level.

        | The returned dict object looks something like this:
        |
        | {'sounding_status': {'40072_0_260000': {'converted': 0, 'orientation': 0, 'beamvector': 0, 'soundvelocity': 0,
        |                                         'georeference': 0, 'tpu': 7536046},
        |                      '40072_0_290000': {'converted': 0, 'orientation': 0, 'beamvector': 0, 'soundvelocity': 0,
        |                                         'georeference': 0, 'tpu': 7536046}, ...
        |  'last_run': {'40072_0_260000': {'_conversion_complete': 'Tue Nov 24 12:42:41 2020', '_compute_orientation_complete': 'Tue Nov 24 12:44:21 2020',
        |                                  '_compute_beam_vectors_complete': 'Tue Nov 24 12:46:20 2020', '_sound_velocity_correct_complete': 'Tue Nov 24 12:48:25 2020',
        |                                  '_georeference_soundings_complete': 'Tue Nov 24 12:50:04 2020', '_total_uncertainty_complete': 'Tue Nov 24 12:51:55 2020'},
        |               '40072_0_290000': {'_conversion_complete': 'Tue Nov 24 12:42:41 2020', '_compute_orientation_complete': 'Tue Nov 24 12:44:40 2020',
        |                                  '_compute_beam_vectors_complete': 'Tue Nov 24 12:46:40 2020', '_sound_velocity_correct_complete': 'Tue Nov 24 12:48:39 2020',
        |                                  '_georeference_soundings_complete': 'Tue Nov 24 12:50:21 2020', '_total_uncertainty_complete': 'Tue Nov 24 12:52:14 2020'}, ...
        |  'multibeam_files': {'0000_202003_S222_EM2040.all': [1584426535.491, 1584426638.015], '0001_202003_S222_EM2040.all': [1584427154.74, 1584427341.396],
        |                      '0002_202003_S222_EM2040.all': [1584427786.983, 1584427894.186], '0003_202003_S222_EM2040.all': [1584428272.65, 1584428465.862], ...

        Returns
        -------
        dict
            processing status at the sector level
        """

        dashboard = {'sounding_status': {}, 'last_run': {}, 'multibeam_files': self.multibeam.raw_ping[0].multibeam_files}
        status_lookup = self.multibeam.raw_ping[0].status_lookup
        for ra in self.multibeam.raw_ping:
            dashboard['sounding_status'][ra.system_identifier] = {i: 0 for i in list(status_lookup.values())}
            unique_status, cnts = np.unique(ra.processing_status, return_counts=True)
            for i in list(status_lookup.keys()):
                if int(i) in unique_status:
                    dashboard['sounding_status'][ra.system_identifier][status_lookup[str(i)]] = cnts[list(unique_status).index(int(i))]

            dashboard['last_run'][ra.system_identifier] = {k: '' for k in kluster_variables.processing_log_names}
            for ky in list(dashboard['last_run'][ra.system_identifier].keys()):
                if ky in ra.attrs:
                    dashboard['last_run'][ra.system_identifier][ky] = ra.attrs[ky]
        return dashboard

    def return_next_action(self, new_vertical_reference: str = None, new_coordinate_system: CRS = None, new_offsets: bool = False,
                           new_angles: bool = False, new_tpu: bool = False, new_input_datum: str = None,
                           new_waterline: bool = False, process_mode: str = 'normal', cast_selection_method: str = 'nearest_in_time'):
        """
        Determine the next action to take, building the arguments for the fqpr_convenience.process_multibeam function.
        Uses the processing status, which is updated as a process is completed at a sounding level.

        0 = conversion
        1 = orientation
        2 = beam vectors
        3 = sound velocity
        4 = georeference
        5 = tpu

        Used in fqpr_intelligence in generating processing actions to take as data is converted/updated.

        Needs some more sophistication with time ranges (i.e. navigation was added, but only for xxxxxx.xx-xxxxxxxx.xx
        time range, only process this segment)

        Parameters
        ----------
        new_vertical_reference
            If the user sets a new vertical reference that does not match the existing one, this will trigger a processing
            action starting at georeferencing
        new_coordinate_system
            If the user sets a new coordinate system that does not match the existing one, this will trigger a
            processing action starting at georeferencing
        new_offsets
            True if new offsets have been set, requires processing starting at sound velocity correction
        new_angles
            True if new mounting angles have been set, requires the full processing stack to be run
        new_tpu
            True if new tpu values have been set, requires compute TPU to run
        new_input_datum
            None, if there is no change to the input datum requested, otherwise this is the new input datum we need to set,
            should trigger a new processing action starting at georeferencing
        new_waterline
            True if a new waterline value has been set, requires processing starting at sound velocity correction
        process_mode
            one of the following process modes:
            - normal = generate the next processing action using the current_processing_status attribute as normal
            - reprocess = perform a full reprocess of the dataset ignoring the current_processing_status
            - convert_only = only convert incoming data, return no processing actions
            - concatenate = process line by line if there is no processed data for that line
        cast_selection_method
            the method used to select the cast that goes with each chunk of the dataset, one of ['nearest_in_time',
            'nearest_in_time_four_hours', 'nearest_in_distance', 'nearest_in_distance_four_hours']

        Returns
        -------
        list
            list of processing arguments to feed fqpr_convenience.process_multibeam
        dict
            dict of processing keyword arguments to feed fqpr_convenience.process_multibeam
        """

        min_status = self.multibeam.raw_ping[0].current_processing_status
        args = [self]
        kwargs = {}
        if process_mode == 'reprocess':
            min_status = 1
        elif process_mode == 'convert_only':
            return args, kwargs
        elif process_mode == 'concatenate':
            nextline = self.return_next_unprocessed_line()
            if nextline:
                kwargs['only_this_line'] = nextline
                min_status = 1
            else:
                return args, kwargs

        new_diff_vertref = False
        new_diff_coordinate = False
        new_diff_inputdatum = False
        default_use_epsg = False
        default_use_coord = True
        default_epsg = None
        input_datum = None
        default_coord_system = kluster_variables.default_coordinate_system
        default_vert_ref = kluster_variables.default_vertical_reference
        if new_input_datum:
            if self.has_sbet:  # doesn't matter, because sbet datum is going to trump new input datum
                new_diff_inputdatum = False
            elif self.input_datum == new_input_datum:  # they match, no action necessary
                new_diff_inputdatum = False
            else:
                new_diff_inputdatum = True
                input_datum = new_input_datum
        if 'horizontal_crs' in self.multibeam.raw_ping[0].attrs:
            try:
                existing_epsg = int(self.multibeam.raw_ping[0].attrs['horizontal_crs'])
            except:
                raise ValueError('return_next_action: Unable to convert current coordinate system to epsg: {}'.format(
                    self.horizontal_crs))
        else:
            existing_epsg = 1  # georeference has not been run yet, so we use this default value that always fails the next check
        if new_coordinate_system:
            try:
                new_epsg = new_coordinate_system.to_epsg()
            except:
                raise ValueError('return_next_action: Unable to convert new coordinate system to epsg: {}'.format(new_coordinate_system))
            default_use_epsg = True
            default_use_coord = False
            default_epsg = new_epsg
            default_coord_system = None
            if new_epsg != existing_epsg and new_epsg and existing_epsg:
                new_diff_coordinate = True
        elif existing_epsg != 1:
            default_use_epsg = True
            default_use_coord = False
            default_epsg = existing_epsg
            default_coord_system = None
        if new_vertical_reference:
            if new_vertical_reference != self.vert_ref:
                new_diff_vertref = True
                default_vert_ref = new_vertical_reference

        if min_status < 5 or new_diff_coordinate or new_diff_inputdatum or new_diff_vertref or new_offsets or new_angles or new_waterline or new_tpu:
            kwargs['run_orientation'] = False
            kwargs['run_beam_vec'] = False
            kwargs['run_svcorr'] = False
            kwargs['run_georef'] = False
            kwargs['run_tpu'] = True
        if min_status < 4 or new_diff_coordinate or new_diff_inputdatum or new_diff_vertref or new_offsets or new_angles or new_waterline:
            kwargs['run_georef'] = True
            kwargs['use_epsg'] = default_use_epsg
            kwargs['use_coord'] = default_use_coord
            kwargs['epsg'] = default_epsg
            kwargs['input_datum'] = input_datum
            kwargs['coord_system'] = default_coord_system
            kwargs['vert_ref'] = default_vert_ref
        if min_status < 3 or new_offsets or new_angles or new_waterline:
            kwargs['run_svcorr'] = True
            kwargs['add_cast_files'] = []
            kwargs['cast_selection_method'] = cast_selection_method
        if min_status < 2 or new_angles:
            kwargs['run_orientation'] = True
            kwargs['orientation_initial_interpolation'] = False
            kwargs['run_beam_vec'] = True

        return args, kwargs


def get_ping_times(pingrec_time: xr.DataArray, idx: xr.DataArray):
    """
    Given a rangeangle Dataset and an index of values that we are interested in from the Dataset, return the ping
    time, which is just the time coordinate of the rangeangle Dataset

    Parameters
    ----------
    pingrec_time
        xarray Dataarray, time coordinate from ping record
    idx
        xarray Dataarray, times of interest from the DataSet

    Returns
    -------
    xr.DataArray
        1 dim ping times from the DataSet
    """

    tx_tstmp_idx = pingrec_time.where(idx, drop=True)
    return tx_tstmp_idx


def _return_xarray_time(xarrs: Union[xr.DataArray, xr.Dataset]):
    """
    Access xarray object and return the time dimension.

    Parameters
    ----------
    xarrs
        Dataset or DataArray that we want the time array from

    Returns
    -------
    xarray DataArray
        time array
    """

    return xarrs['time']


def _drop_list_element(data_list: list, drop_this_one: int):
    data_list.pop(drop_this_one)
    return data_list


def validate_kluster_input_datum(new_datum: Union[str, int]):
    """
    Check the given datum string identifier or epsg code for a valid kluster datum.

    Parameters
    ----------
    new_datum

    Returns
    -------

    """
    new_datum = str(new_datum)
    is_valid = True
    if new_datum.lower() == 'nad83':
        new_datum = CRS.from_epsg(kluster_variables.epsg_nad83)
    elif new_datum.lower() == 'wgs84':
        new_datum = CRS.from_epsg(kluster_variables.epsg_wgs84)
    else:
        try:
            new_datum = CRS.from_epsg(int(new_datum))
            if new_datum.is_projected:
                print(f'validate_kluster_input_datum: was given a projected crs, but input crs must be geographic: {new_datum}')
                is_valid = False
            elif new_datum.coordinate_system.axis_list[0].unit_name not in ['degree', 'degrees']:
                print(f'validate_kluster_input_datum: expected a crs to be provided with units of degrees: {new_datum}')
                is_valid = False
        except:
            print('validate_kluster_datum: {} not supported.  Only supports WGS84, NAD83 or custom epsg integer code'.format(new_datum))
            is_valid = False
    return is_valid, new_datum
