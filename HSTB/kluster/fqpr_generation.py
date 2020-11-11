import os
from typing import Union, Callable
from datetime import datetime
from time import perf_counter
import xarray as xr
import numpy as np
import laspy
from dask.distributed import wait, progress
from pyproj import CRS

from HSTB.kluster.orientation import distrib_run_build_orientation_vectors
from HSTB.kluster.beampointingvector import distrib_run_build_beam_pointing_vector
from HSTB.kluster.svcorrect import get_sv_files_from_directory, return_supported_casts_from_list, SoundSpeedProfile, \
    distributed_run_sv_correct
from HSTB.kluster.georeference import distrib_run_georeference
from HSTB.kluster.tpu import distrib_run_calculate_tpu
from HSTB.kluster.xarray_conversion import BatchRead
from HSTB.kluster.xarray_helpers import combine_arrays_to_dataset, compare_and_find_gaps, distrib_zarr_write, \
    divide_arrays_by_time_index, interp_across_chunks, reload_zarr_records, slice_xarray_by_dim, stack_nan_array, \
    get_write_indices_zarr
from HSTB.kluster.dask_helpers import DaskProcessSynchronizer, dask_find_or_start_client, get_number_of_workers
from HSTB.kluster.rotations import return_attitude_rotation_matrix
from HSTB.kluster.logging_conf import return_logger
from HSTB.kluster.pydro_helpers import is_pydro
from HSTB.kluster.pdal_entwine import build_entwine_points
from HSTB.drivers.sbet import sbet_to_xarray

debug = False


class Fqpr:
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
    source_dat
        instance of xarray_conversion BatchRead class
    motion_latency
        optional motion latency adjustment
    address
        passed to dask_find_or_start_client to setup dask cluster
    show_progress
        If true, uses dask.distributed.progress.  Disabled for GUI, as it generates too much text
    """

    def __init__(self, source_dat: BatchRead = None, motion_latency: float = 0.0, address: str = None, show_progress: bool = True):
        self.source_dat = source_dat
        self.intermediate_dat = None
        self.soundings_path = ''
        self.soundings = None
        self.ppnav_path = ''
        self.ppnav_dat = None
        self.xyz_crs = None
        self.vert_ref = None
        self.motion_latency = motion_latency

        self.client = None
        self.address = address
        self.show_progress = show_progress
        self.cast_files = None
        self.soundspeedprofiles = None
        self.cast_chunks = None

        self.tx_vecs = None
        self.rx_vecs = None
        self.tx_reversed = False
        self.rx_reversed = False
        self.ideal_tx_vec = None
        self.ideal_rx_vec = None

        self.orientation_time_complete = ''
        self.bpv_time_complete = ''
        self.sv_time_complete = ''
        self.georef_time_complete = ''
        self.tpu_time_complete = ''

        self.logfile = None
        self.logger = None
        self.initialize_log()

    def initialize_log(self):
        """
        Initialize the fqpr logger using the source_dat logfile attribute.

        | self.logfile is the path to the text log that the logging module uses
        | self.logger is the logging.Logger object
        """

        if self.logger is None and self.source_dat.logfile is not None and self.source_dat is not None:
            self.logfile = self.source_dat.logfile
            self.logger = return_logger(__name__, self.logfile)

    def set_vertical_reference(self, vert_ref: str):
        """
        Set the Fqpr instance vertical reference.  This will feed into the georef and calculate tpu processes.

        If the new vert_ref conflicts with an existing written vert_ref, issue a warning.

        Parameters
        ----------
        vert_ref
            vertical reference for the survey, one of ['ellipse', 'waterline']
        """

        if 'vertical_reference' in self.source_dat.raw_ping[0].attrs:
            if vert_ref != self.source_dat.raw_ping[0].vertical_reference:
                self.logger.warning('Setting vertical reference to {} when existing vertical reference is {}'.format(vert_ref, self.source_dat.raw_ping[0].vertical_reference))
                self.logger.warning('You will need to georeference and calculate total uncertainty again')
        if vert_ref not in ['ellipse', 'waterline']:
            self.logger.error("Unable to set vertical reference to {}: expected one of ['ellipse', 'waterline']".format(vert_ref))
            raise ValueError("Unable to set vertical reference to {}: expected one of ['ellipse', 'waterline']".format(vert_ref))
        self.vert_ref = vert_ref

    def read_from_source(self):
        """
        Activate rawdat object's appropriate read class
        """

        if self.source_dat is not None:
            self.client = self.source_dat.client  # mbes read is first, pull dask distributed client from it
            if self.source_dat.raw_ping is None:
                self.source_dat.read()
        else:
            self.client = dask_find_or_start_client(address=self.address)
        self.initialize_log()

    def reload_soundings_records(self, skip_dask: bool = False):
        """
        Reload the zarr datastore for the xyz record using the xyz_path attribute

        Parameters
        ----------
        skip_dask
            if True, will open zarr datastore without Dask synchronizer object
        """

        self.soundings = reload_zarr_records(self.soundings_path, skip_dask)

    def reload_ppnav_records(self, skip_dask: bool = False):
        """
        Reload the zarr datastore for the ppnav record using the ppnav_path attribute

        Parameters
        ----------
        skip_dask
            if True, will open zarr datastore without Dask synchronizer object
        """

        self.ppnav_dat = reload_zarr_records(self.ppnav_path, skip_dask)

    def construct_crs(self, epsg: str = None, datum: str = 'NAD83', projected: bool = True, vert_ref: str = None):
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
            vertical reference for the survey, one of ['ellipse', 'waterline']
        """

        datum = datum.upper()
        if epsg is not None:
            self.xyz_crs = CRS.from_epsg(int(epsg))
        elif epsg is None and not projected:
            if datum == 'NAD83':
                self.xyz_crs = CRS.from_epsg(6319)
            elif datum == 'WGS84':
                self.xyz_crs = CRS.from_epsg(4326)
            else:
                self.logger.error('{} not supported.  Only supports WGS84 and NAD83'.format(datum))
                raise ValueError('{} not supported.  Only supports WGS84 and NAD83'.format(datum))
        elif epsg is None and projected:
            zone = self.source_dat.return_utm_zone_number()
            if datum == 'NAD83':
                self.xyz_crs = CRS.from_proj4('+proj=utm +zone={} +ellps=GRS80 +datum=NAD83'.format(zone))
            elif datum == 'WGS84':
                self.xyz_crs = CRS.from_proj4('+proj=utm +zone={} +ellps=WGS84 +datum=WGS84'.format(zone))
            else:
                self.logger.error('{} not supported.  Only supports WGS84 and NAD83'.format(datum))
                raise ValueError('{} not supported.  Only supports WGS84 and NAD83'.format(datum))
        if vert_ref is not None:
            self.set_vertical_reference(vert_ref)

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
            sectors = self.source_dat.return_sector_time_indexed_array()
            for s_cnt, sector in enumerate(sectors):
                for applicable_index, timestmp, prefixes in sector:
                    txrx = prefixes
                    tstmp = timestmp
                    break
                break

        # start with ideal vectors, flip sign if tx/rx is installed backwards
        # ideal tx vector is aligned with the x axis (forward)
        tx_heading = abs(float(self.source_dat.xyzrph[txrx[0] + '_h'][tstmp]))
        if (tx_heading > 90) and (tx_heading < 270):
            self.ideal_tx_vec = np.array([-1, 0, 0])
            self.tx_reversed = True
        else:
            self.ideal_tx_vec = np.array([1, 0, 0])

        # ideal rx vector is aligned with the y axis (stbd)
        rx_heading = abs(float(self.source_dat.xyzrph[txrx[1] + '_h'][tstmp]))
        if (rx_heading > 90) and (rx_heading < 270):
            self.ideal_rx_vec = np.array([0, -1, 0])
            self.rx_reversed = True
        else:
            self.ideal_rx_vec = np.array([0, 1, 0])

    def get_cast_files(self, src: Union[str, list]):
        """
        Load to self.cast_files the file paths to the sv casts of interest.

        Parameters
        ----------
        src
            either a list of files to include or the path to a directory containing files
        """

        if type(src) is str:
            svfils = get_sv_files_from_directory(src)
        elif type(src) is list:
            svfils = return_supported_casts_from_list(src)
        else:
            self.logger.error('Provided source is neither a path or a list of files.  Please provide one of those.')
            raise TypeError('Provided source is neither a path or a list of files.  Please provide one of those.')

        if self.cast_files is None:
            self.cast_files = svfils
        elif isinstance(self.cast_files, list):
            self.cast_files.extend(sv for sv in svfils if sv not in self.cast_files)
        else:
            raise ValueError('Found sound velocity casts not provided as a list: {}'.format(self.cast_files))

    def setup_casts(self, surf_sound_speed: xr.DataArray, z_pos: float, add_cast_files: Union[str, list] = None):
        """
        Using all the profiles in the rangeangle dataset as well as externally provided casts as files, generate
        SoundSpeedProfile objects and build the lookup tables.

        Parameters
        ----------
        surf_sound_speed
            1dim array of surface sound speed values, coords = timestamp
        z_pos
            z value of the transducer position in the watercolumn from the waterline
        add_cast_files
            optional - either a list of sv files or the path to a directory of files

        Returns
        -------
        list
            a list of SoundSpeedProfile objects with constructed lookup tables
        """

        # get the svp files and the casts in the mbes converted data
        if add_cast_files is not None:
            self.get_cast_files(add_cast_files)
        mbes_profs = self.source_dat.return_all_profiles()

        # convert to SoundSpeedProfile objects
        rangeangle_casts = []
        if mbes_profs:
            for castname, data in mbes_profs.items():
                try:
                    casttime = float(castname.split('_')[1])
                    cst_object = SoundSpeedProfile(data, z_pos, surf_sound_speed, prof_time=casttime,
                                                             prof_type='raw_ping')
                    cst_object.generate_lookup_table()
                    rangeangle_casts.append(cst_object)
                except ValueError:
                    self.logger.error('Profile attribute name in ping DataSet must include timestamp, ex: "profile_1495599960"')
                    raise ValueError('Profile attribute name in ping DataSet must include timestamp, ex: "profile_1495599960"')

        # include additional casts from sv files as SoundSpeedProfile objects
        additional_casts = []
        if self.cast_files is not None:
            for f in self.cast_files:
                cst_object = SoundSpeedProfile(f, z_pos, surf_sound_speed, prof_type='caris_svp')
                cst_object.generate_lookup_table()
                additional_casts.append(cst_object)

        # retain all the casts that are unique in time, preferring the svp file ones (they have location and stuff)
        final_casts = additional_casts
        cast_tstmps = [cst.prof_time for cst in final_casts]
        new_casts = [cst for cst in rangeangle_casts if cst.prof_time not in cast_tstmps]
        return final_casts + new_casts

    def setup_casts_for_sec_by_index(self, sector_index: int, applicable_index: xr.DataArray, prefixes: str,
                                     timestmp: str, add_cast_files: Union[str, list] = None):
        """
        Generate cast objects for the given sector across all values given for waterline

        Originally started with building all casts for the first sector and using those cast objects across all other
        sectors (as the z pos is basically the same) but ran into the issue where different sectors would start at
        different times in the file.

        Also, we don't return the whole class, as it has things in it that cause dask to freeze on scatter.  Dask
        apparently really only guarantees to work with numpy/numpy derivatives.  So we have to return the attributes
        that are necessary for sv correct.

        Parameters
        ----------
        sector_index
            index of sector
        applicable_index
            boolean mask for the data associated with this installation parameters instance
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp of the installation parameters instance used
        add_cast_files
            optional - either a list of sv files or the path to a directory of files

        Returns
        -------
        list
            a list of lists of the attributes within the SoundSpeedProfile objects, including constructed lookup
            tables for each waterline value in installation parameters.  Ideally, I could return the objects themselves,
            but you cannot scatter/map custom classes effectively in Dask.
        """

        self.cast_chunks[sector_index] = {}

        ss_by_sec = self.source_dat.raw_ping[sector_index].soundspeed.where(applicable_index, drop=True)
        # this should be the transducer to waterline, positive down
        z_pos = -float(self.source_dat.xyzrph[prefixes[0] + '_z'][timestmp]) + float(self.source_dat.xyzrph['waterline'][timestmp])
        cst = self.setup_casts(ss_by_sec, z_pos, add_cast_files)
        cast_size = np.sum([c.__sizeof__() for c in cst])
        cast_data = [[c.prof_time, c.dim_angle, c.dim_raytime, c.lkup_across_dist, c.lkup_down_dist, c.corr_profile_lkup] for c in cst]
        self.logger.info('built {} total cast objects, total size = {} bytes'.format(len(cst), cast_size))
        return cast_data

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
            the applicable_index generated from return_sector_time_indexed_array
        pings_per_chunk
            number of pings in each worker chunk

        Returns
        -------
        list
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping
        """

        msk = idx_mask.values
        index_timevals = np.arange(np.count_nonzero(msk))
        idx = xr.DataArray(index_timevals, dims=('time',), coords={'time': idx_mask.time[msk]})

        if len(idx) < pings_per_chunk:
            # not enough data to warrant multiple chunks
            idx_by_chunk = [idx]
        else:
            split_indices = [pings_per_chunk * (i + 1) for i in
                             range(int(np.floor(idx.shape[0] / pings_per_chunk)))]
            idx_by_chunk = np.array_split(idx, split_indices, axis=0)
        return idx_by_chunk

    def return_cast_idx_nearestintime(self, cast_times: list, idx_by_chunk: list):
        """
        Need to find the cast associated with each chunk of data.  Currently we just take the average chunk time and
        find the closest cast time, and assign that cast.  We also need the index of the chunk in the original size
        dataset, as we built the casts based on the original size soundvelocity dataarray.

        Parameters
        ----------
        cast_times
            list of floats, time each cast was taken
        idx_by_chunk
            list of xarray Datarrays, values are the integer indexes of the pings to use, coords are the time of ping

        Returns
        -------
        data
            list of lists, each sub-list is [xarray Datarray with times/indices for the chunk, index of the cast that
            applies to that chunk]
        """

        data = []
        casts_used = []
        for chnk in idx_by_chunk:
            # get average chunk time and find the nearest cast to that time.  Retain the index of that cast object.
            avgtme = float(chnk.time.mean())
            cst = np.argmin([np.abs(c - avgtme) for c in cast_times])
            data.append([chnk, cst])
            if cast_times[cst] not in casts_used:
                casts_used.append(cast_times[cst])

        self.logger.info('nearest-in-time: applying casts {}'.format(casts_used))
        return data

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

        if self.source_dat.is_dual_head():  # dual dual systems
            if not self.is_primary_system(ra.sector_identifier):  # secondary head
                self.logger.info('Building induced heave for secondary system in dual head arrangement')

                # lever arms for secondary head to ref pt
                secondary_x_lever = float(self.source_dat.xyzrph[prefixes[0] + '_x'][timestmp])
                secondary_y_lever = float(self.source_dat.xyzrph[prefixes[0] + '_y'][timestmp])
                secondary_z_lever = float(self.source_dat.xyzrph[prefixes[0] + '_z'][timestmp])

                # lever arms for primary head to ref pt
                if prefixes[0] == 'tx_port':
                    prim_prefix = 'tx_stbd'
                else:
                    prim_prefix = 'tx_port'
                primary_x_lever = float(self.source_dat.xyzrph[prim_prefix + '_x'][timestmp])
                primary_y_lever = float(self.source_dat.xyzrph[prim_prefix + '_y'][timestmp])
                primary_z_lever = float(self.source_dat.xyzrph[prim_prefix + '_z'][timestmp])

                final_lever = np.array([primary_x_lever - secondary_x_lever, primary_y_lever - secondary_y_lever,
                                        primary_z_lever - secondary_z_lever])
            else:
                self.logger.info('No induced heave in primary system in dual head arrangement')
                return hve
        else:
            self.logger.info('No induced heave in primary system in single head arrangement')
            return hve

        self.logger.info('Rotating: {}'.format(final_lever))

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

        x_lever = float(self.source_dat.xyzrph[prefixes[0] + '_x'][timestmp])
        y_lever = float(self.source_dat.xyzrph[prefixes[0] + '_y'][timestmp])
        z_lever = float(self.source_dat.xyzrph[prefixes[0] + '_z'][timestmp])
        if x_lever or y_lever or z_lever:
            # There exists a lever arm between tx and rp, and the altitude is at the rp
            #  - svcorrected offsets are at tx/rx so there will be a correction necessary to use altitude
            rp_to_tx_leverarm = np.array([-x_lever, -y_lever, -z_lever])
            self.logger.info('Applying altitude correction for RP to TX offset: {}'.format(rp_to_tx_leverarm))

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
        else:
            self.logger.info('no altitude correction for RP at TX')

        return alt

    def return_additional_xyz_offsets(self, prefixes: str, sec_info: dict, timestmp: str):
        """
        Apply tx to reference point offset to beams.

        All the kongsberg sonars have additional offsets in the installation parameters document listed as the difference
        between the measured center of the transducer and the phase center of the transducer.  Here we get those values
        for the provided system (we've previously stored them in the xyzrph data)

        Parameters
        ----------
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        sec_info
            sector info, see parse_sect_info_from_identifier
        timestmp
            timestamp for the appropriate xyzrph record

        Returns
        -------
        list
            [float, additional x offset, float, additional y offset, float, additional z offset]
        """

        x_off_ky = prefixes[0] + '_x_' + sec_info['sector']
        x_base_offset = prefixes[0] + '_x'
        if x_off_ky in self.source_dat.xyzrph:
            addtl_x = float(self.source_dat.xyzrph[x_base_offset][timestmp]) + float(self.source_dat.xyzrph[x_off_ky][timestmp])
        else:
            addtl_x = float(self.source_dat.xyzrph[x_base_offset][timestmp])

        y_off_ky = prefixes[0] + '_y_' + sec_info['sector']
        y_base_offset = prefixes[0] + '_y'
        if y_off_ky in self.source_dat.xyzrph:
            addtl_y = float(self.source_dat.xyzrph[y_base_offset][timestmp]) + float(self.source_dat.xyzrph[y_off_ky][timestmp])
        else:
            addtl_y = float(self.source_dat.xyzrph[y_base_offset][timestmp])

        z_off_ky = prefixes[0] + '_z_' + sec_info['sector']
        # z_base_offset = prefixes[0] + '_z'
        if z_off_ky in self.source_dat.xyzrph:
            addtl_z = float(self.source_dat.xyzrph[z_off_ky][timestmp])
        else:
            addtl_z = 0  # z included at cast creation, we will apply this for real in georeference bathy

        total_offsets = [addtl_x, addtl_y, addtl_z]
        self.logger.info('Applying offsets: {}'.format(total_offsets))
        return total_offsets

    def get_cluster_params(self, sector_identifier: str):
        """
        Attempt to figure out what the chunk size and number of chunks at a time parameters should be given the dims
        of the dataset.  It's pretty rough, definitely needs something more sophisticated, but this serves as a place
        holder.

        Basically uses the avg number of beams per ping and the worker memory size to get the chunk sizes (in time)

        Parameters
        ----------
        sector_identifier
            identifier for the sector in format 'serialnumber_sectornumber_freq', ex: '125125_0_200000'

        Returns
        -------
        int
            number of pings in each chunk
        int
            number of chunks to run at once
        """

        if self.source_dat is None:
            self.logger.info('Read from data first, source_dat is None')
            return
        # This is the old way.  We would scale the chunksize on each run depending on the capabilities of the cluster.
        # Unfortunately, it turns out you always want the written chunks to be of the same size, because you can't
        # really change the zarr chunk size.  So instead, just return a constant that we made up that gets you near the
        # desired 1MB per chunk that Zarr recommends.
        #
        #  We need to get an idea of how many beams there are in an average ping in this sector
        # bpa = self.source_dat.select_array_from_rangeangle('beampointingangle', sector_identifier)
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
            totchunks = 4
        return self.source_dat.ping_chunksize, totchunks

    def _generate_chunks_orientation(self, ra: xr.Dataset, raw_att: xr.Dataset, idx_by_chunk: xr.DataArray,
                                     twtt_by_idx: xr.DataArray, applicable_index: xr.DataArray,
                                     timestmp: str, prefixes: str):
        """
        Take a single sector, and build the data for the distributed system to process.
        distrib_run_build_orientation_vectors requires the attitude, two way travel time, ping time index, and the
        starting orientation of the tx and rx.

        Parameters
        ----------
        ra
            the raw_ping associated with this sector
        raw_att
            raw attitude Dataset including roll, pitch, yaw
        idx_by_chunk
            values are the integer indexes of the pings to use, coords are the time of ping
        twtt_by_idx
            two way travel time for this sector
        applicable_index
            boolean mask for the data associated with this installation parameters instance
        timestmp
            timestamp of the installation parameters instance used
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_build_orientation_vectors
        """

        latency = self.motion_latency
        if latency:
            self.logger.info('applying {}ms of latency to attitude...'.format(latency))
        tx_tstmp_idx = get_ping_times(ra.time, applicable_index)
        self.logger.info('preparing to process {} pings'.format(len(tx_tstmp_idx)))

        self.logger.info('calculating 3d transducer orientation for:')
        tx_roll_mountingangle = self.source_dat.xyzrph[prefixes[0] + '_r'][timestmp]
        tx_pitch_mountingangle = self.source_dat.xyzrph[prefixes[0] + '_p'][timestmp]
        tx_yaw_mountingangle = self.source_dat.xyzrph[prefixes[0] + '_h'][timestmp]
        tx_orientation = [self.ideal_tx_vec, tx_roll_mountingangle, tx_pitch_mountingangle, tx_yaw_mountingangle, timestmp]
        self.logger.info('transducer {} mounting angles: roll={} pitch={} yaw={}'.format(prefixes[0], tx_roll_mountingangle,
                                                                                         tx_pitch_mountingangle, tx_yaw_mountingangle))

        rx_roll_mountingangle = self.source_dat.xyzrph[prefixes[1] + '_r'][timestmp]
        rx_pitch_mountingangle = self.source_dat.xyzrph[prefixes[1] + '_p'][timestmp]
        rx_yaw_mountingangle = self.source_dat.xyzrph[prefixes[1] + '_h'][timestmp]
        rx_orientation = [self.ideal_rx_vec, rx_roll_mountingangle, rx_pitch_mountingangle, rx_yaw_mountingangle, timestmp]
        self.logger.info('transducer {} mounting angles: roll={} pitch={} yaw={}'.format(prefixes[1], rx_roll_mountingangle,
                                                                                         rx_pitch_mountingangle, rx_yaw_mountingangle))

        data_for_workers = []
        for chnk in idx_by_chunk:
            try:
                worker_att = self.client.scatter(slice_xarray_by_dim(raw_att, start_time=chnk.time.min() - 1, end_time=chnk.time.max() + 1))
                worker_twtt = self.client.scatter(twtt_by_idx[chnk])
                worker_tx_tstmp_idx = self.client.scatter(tx_tstmp_idx[chnk])
            except:  # get here if client is closed or doesnt exist
                worker_att = slice_xarray_by_dim(raw_att, start_time=chnk.time.min() - 1, end_time=chnk.time.max() + 1)
                worker_twtt = twtt_by_idx[chnk]
                worker_tx_tstmp_idx = tx_tstmp_idx[chnk]
            data_for_workers.append([worker_att, worker_twtt, worker_tx_tstmp_idx, tx_orientation, rx_orientation, latency])
        return data_for_workers

    def _generate_chunks_bpv(self, raw_hdng: xr.DataArray, idx_by_chunk: xr.DataArray, tx_tstmp_idx: xr.DataArray,
                             applicable_index: xr.DataArray, sec_ident: str, timestmp: str):
        """
        Take a single sector, and build the data for the distributed system to process.
        distrib_run_build_beam_pointing_vector requires the heading, beampointingangle, tx tiltangle, tx/rx orientation,
        ping time index, and indicators whether or not the sonar heads were installed in a reverse fashion.

        Parameters
        ----------
        raw_hdng
            heading for entire time of all input data
        idx_by_chunk
            values are the integer indexes of the pings to use, coords are the time of ping
        tx_tstmp_idx
            1d array of ping times
        applicable_index
            boolean mask for the data associated with this installation parameters instance
        sec_ident
            sector identifier string
        timestmp
            timestamp of the installation parameters instance used

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_build_beam_pointing_vector
        """
        latency = self.motion_latency
        self.logger.info('preparing to process {} pings'.format(len(tx_tstmp_idx)))
        self.logger.info('transducers mounted backwards - TX: {} RX: {}'.format(self.tx_reversed, self.rx_reversed))

        bpa = self.source_dat.select_array_from_rangeangle('beampointingangle', sec_ident).where(applicable_index, drop=True)
        tilt = self.source_dat.select_array_from_rangeangle('tiltangle', sec_ident).where(applicable_index, drop=True)
        if 'heading' in self.source_dat.raw_ping[0]:
            self.logger.info('Using pre-interpolated heading saved to disk...')
            heading = self.source_dat.select_array_from_rangeangle('heading', sec_ident).where(applicable_index, drop=True)
        else:
            heading = interp_across_chunks(raw_hdng, tx_tstmp_idx + latency, daskclient=self.client)

        if 'orientation' in self.intermediate_dat[sec_ident]:
            # workflow for data that is not written to disk.  Preference given for data in memory
            tx_rx_futs = [f[0] for f in self.intermediate_dat[sec_ident]['orientation'][timestmp]] * len(idx_by_chunk)
            try:
                tx_rx_data = self.client.map(divide_arrays_by_time_index,
                                             tx_rx_futs, [chnk for chnk in idx_by_chunk])
            except:  # client is not setup, run locally
                tx_rx_data = []
                for cnt, fut in enumerate(tx_rx_futs):
                    tx_rx_data.append(divide_arrays_by_time_index(fut, idx_by_chunk[cnt]))
        else:
            # workflow for data that is written to disk
            tx_idx = self.source_dat.select_array_from_rangeangle('tx', sec_ident).where(applicable_index, drop=True)
            rx_idx = self.source_dat.select_array_from_rangeangle('rx', sec_ident).where(applicable_index, drop=True)
            try:
                tx_rx_data = self.client.scatter([[tx_idx[chnk], rx_idx[chnk]] for chnk in idx_by_chunk])
            except:  # client is not setup, run locally
                tx_rx_data = [[tx_idx[chnk], rx_idx[chnk]] for chnk in idx_by_chunk]

        data_for_workers = []
        for cnt, chnk in enumerate(idx_by_chunk):
            try:
                fut_hdng = self.client.scatter(heading[chnk])
                fut_tx_tstmp_idx = self.client.scatter(tx_tstmp_idx[chnk])
                fut_bpa = self.client.scatter(bpa[chnk])
                fut_tilt = self.client.scatter(tilt[chnk])
            except:  # client is not setup, run locally
                fut_hdng = heading[chnk]
                fut_tx_tstmp_idx = tx_tstmp_idx[chnk]
                fut_bpa = bpa[chnk]
                fut_tilt = tilt[chnk]
            data_for_workers.append([fut_hdng, fut_bpa, fut_tilt, tx_rx_data[cnt], fut_tx_tstmp_idx, self.tx_reversed,
                                     self.rx_reversed])
        return data_for_workers

    def _generate_chunks_svcorr(self, casts: list, cast_chunks: list, sec_ident: str, applicable_index: xr.DataArray,
                                timestmp: str, addtl_offsets: list):
        """
        Take a single sector, and build the data for the distributed system to process.  Svcorrect requires the
        relative azimuth (to ship heading) and the corrected beam pointing angle (corrected for attitude/mounting angle)

        Parameters
        ----------
        casts
            a list of the attributes within the SoundSpeedProfile object associated with this sector, including
            constructed lookup tables for each waterline value in installation parameters.
        cast_chunks
            list of lists, each sub-list is [timestamps for the chunk, index of the chunk in the original array, index
            of the cast that applies to that chunk]
        sec_ident
            sector identifier string
        applicable_index
            xarray Dataarray, boolean mask for the data associated with this installation parameters instance
        timestmp
            timestamp of the installation parameters instance used
        total offsets
            [float, additional x offset, float, additional y offset, float, additional z offset]

        Returns
        -------
        list
            list of lists, each list contains future objects for distributed_run_sv_correct
        """

        self.logger.info('dividing into {} data chunks for workers...'.format(len(cast_chunks)))
        data_for_workers = []
        if 'bpv' in self.intermediate_dat[sec_ident]:
            # workflow for data that is not written to disk.  Preference given for data in memory
            bpv_futs = [f[0] for f in self.intermediate_dat[sec_ident]['bpv'][timestmp]] * len(cast_chunks)
            try:
                bpv_data = self.client.map(divide_arrays_by_time_index, bpv_futs, [d[0] for d in cast_chunks])
            except:  # client is not setup, run locally
                bpv_data = []
                for cnt, fut in enumerate(bpv_futs):
                    bpv_data.append(divide_arrays_by_time_index(fut, cast_chunks[cnt][0]))
        else:
            # workflow for data that is written to disk, break it up according to cast_chunks
            rel_azimuth_idx = self.source_dat.select_array_from_rangeangle('rel_azimuth', sec_ident).where(applicable_index, drop=True)
            corr_angle_idx = self.source_dat.select_array_from_rangeangle('corr_pointing_angle', sec_ident).where(applicable_index, drop=True)
            try:
                bpv_data = self.client.scatter([[rel_azimuth_idx[d[0]], corr_angle_idx[d[0]]] for d in cast_chunks])
            except:  # client is not setup, run locally
                bpv_data = [[rel_azimuth_idx[d[0]], corr_angle_idx[d[0]]] for d in cast_chunks]

        twtt = self.source_dat.select_array_from_rangeangle('traveltime', sec_ident).where(applicable_index, drop=True)
        try:
            twtt_data = self.client.scatter([twtt[d[0]] for d in cast_chunks])
            casts = self.client.scatter(casts)
        except:  # client is not setup, run locally
            twtt_data = [twtt[d[0]] for d in cast_chunks]

        # data_idx = self.client.scatter([d[0].values for d in cast_chunks])

        for cnt, dat in enumerate(cast_chunks):
            data_for_workers.append([casts[dat[1]], bpv_data[cnt], twtt_data[cnt], None, addtl_offsets])
        return data_for_workers

    def _generate_chunks_georef(self, ra: xr.Dataset, idx_by_chunk: xr.DataArray, applicable_index: xr.DataArray,
                                prefixes: str, timestmp: str, z_offset: float, prefer_pp_nav: bool):
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
        applicable_index
            xarray Dataarray, boolean mask for the data associated with this installation parameters instance
        prefixes
            prefix identifier for the tx/rx, will vary for dual head systems
        timestmp
            timestamp of the installation parameters instance used
        z_offset
            reference point to transmitter
        prefer_pp_nav
            if True will use post-processed navigation/height (SBET)

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_georeference, see georef_xyz
        """

        latency = self.motion_latency
        sec_ident = ra.sector_identifier
        if latency:
            self.logger.info('Applying motion latency of {}ms'.format(latency))
        tx_tstmp_idx = get_ping_times(ra.time, applicable_index)
        self.logger.info('preparing to process {} pings'.format(len(tx_tstmp_idx)))

        if ('latitude' in ra) and ('longitude' in ra) and ('altitude' in ra):
            self.logger.info('Using pre-interpolated attitude/navigation saved to disk...')
            lat = self.source_dat.select_array_from_rangeangle('latitude', sec_ident).where(applicable_index, drop=True)
            lon = self.source_dat.select_array_from_rangeangle('longitude', sec_ident).where(applicable_index, drop=True)
            alt = self.source_dat.select_array_from_rangeangle('altitude', sec_ident).where(applicable_index, drop=True)
        elif prefer_pp_nav and (self.ppnav_dat is not None):
            self.logger.info('Using post processed navigation...')
            nav = interp_across_chunks(self.ppnav_dat, tx_tstmp_idx + latency, daskclient=self.client)
            lat = nav.latitude
            lon = nav.longitude
            alt = nav.altitude
        else:
            nav = interp_across_chunks(self.source_dat.raw_nav, tx_tstmp_idx + latency, daskclient=self.client)
            lat = nav.latitude
            lon = nav.longitude
            if 'altitude' in nav:  # for seapath systems, there is no altitude in the record
                alt = nav.altitude
            else:
                alt = None

        if ('heading' in ra) and ('heave' in ra):
            hdng = self.source_dat.select_array_from_rangeangle('heading', sec_ident).where(applicable_index, drop=True)
            hve = self.source_dat.select_array_from_rangeangle('heave', sec_ident).where(applicable_index, drop=True)
        else:
            hdng = interp_across_chunks(self.source_dat.raw_att['heading'], tx_tstmp_idx + latency, daskclient=self.client)
            hve = interp_across_chunks(self.source_dat.raw_att['heave'], tx_tstmp_idx + latency, daskclient=self.client)

        wline = float(self.source_dat.xyzrph['waterline'][str(timestmp)])

        if self.vert_ref == 'ellipse':
            alt = self.determine_altitude_corr(alt, self.source_dat.raw_att, tx_tstmp_idx + latency, prefixes, timestmp)
        else:
            hve = self.determine_induced_heave(ra, hve, self.source_dat.raw_att, tx_tstmp_idx + latency, prefixes, timestmp)

        if 'sv_corr' not in self.intermediate_dat[sec_ident]:
            altrack = self.source_dat.select_array_from_rangeangle('alongtrack', sec_ident).where(applicable_index, drop=True)
            actrack = self.source_dat.select_array_from_rangeangle('acrosstrack', sec_ident).where(applicable_index, drop=True)
            dpthoff = self.source_dat.select_array_from_rangeangle('depthoffset', sec_ident).where(applicable_index, drop=True)

        data_for_workers = []

        for chnk in idx_by_chunk:
            if latency:
                chnk = chnk.assign_coords({'time': chnk.time.time + latency})
            if 'sv_corr' in self.intermediate_dat[sec_ident]:
                # workflow for data that is not written to disk.  Preference given for data in memory
                sv_data = self.intermediate_dat[sec_ident]['sv_corr'][timestmp][0][0]
            else:
                # workflow for data that is written to disk
                try:
                    sv_data = self.client.scatter([altrack[chnk], actrack[chnk], dpthoff[chnk]])
                except:  # client is not setup, run locally
                    sv_data = [altrack[chnk], actrack[chnk], dpthoff[chnk]]
            try:
                if alt is None:
                    fut_alt = alt
                else:
                    fut_alt = self.client.scatter(alt[chnk])
                fut_lon = self.client.scatter(lon[chnk])
                fut_lat = self.client.scatter(lat[chnk])
                fut_hdng = self.client.scatter(hdng[chnk])
                fut_hve = self.client.scatter(hve[chnk])
            except:  # client is not setup, run locally
                if alt is None:
                    fut_alt = alt
                else:
                    fut_alt = alt[chnk]
                fut_lon = lon[chnk]
                fut_lat = lat[chnk]
                fut_hdng = hdng[chnk]
                fut_hve = hve[chnk]
            data_for_workers.append([sv_data, fut_alt, fut_lon, fut_lat, fut_hdng, fut_hve, wline, self.vert_ref, self.xyz_crs, z_offset])
        return data_for_workers

    def _generate_chunks_tpu(self, ra: xr.Dataset, idx_by_chunk: xr.DataArray, applicable_index: xr.DataArray):
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
        applicable_index
            xarray Dataarray, boolean mask for the data associated with this installation parameters instance

        Returns
        -------
        list
            list of lists, each list contains future objects for distrib_run_georeference, see georef_xyz
        """

        latency = self.motion_latency
        if latency:
            self.logger.info('Applying motion latency of {}ms'.format(latency))
        tx_tstmp_idx = get_ping_times(ra.time, applicable_index)
        self.logger.info('preparing to process {} pings'.format(len(tx_tstmp_idx)))

        if 'qualityfactor' not in self.source_dat.raw_ping[0]:
            self.logger.error("_generate_chunks_tpu: sonar uncertainty ('qualityfactor') must exist to calculate uncertainty")
            return None
        if self.ppnav_dat is not None:
            ppnav = interp_across_chunks(self.ppnav_dat, tx_tstmp_idx + latency, daskclient=self.client)

        roll = interp_across_chunks(self.source_dat.raw_att['roll'], tx_tstmp_idx + latency, daskclient=self.client)

        corr_point = self.source_dat.select_array_from_rangeangle('corr_pointing_angle', ra.sector_identifier).where(applicable_index, drop=True)
        raw_point = self.source_dat.select_array_from_rangeangle('beampointingangle', ra.sector_identifier).where(applicable_index, drop=True)
        acrosstrack = self.source_dat.select_array_from_rangeangle('acrosstrack', ra.sector_identifier).where(applicable_index, drop=True)
        depthoffset = self.source_dat.select_array_from_rangeangle('depthoffset', ra.sector_identifier).where(applicable_index, drop=True)
        soundspeed = self.source_dat.select_array_from_rangeangle('soundspeed', ra.sector_identifier).where(applicable_index, drop=True)
        qf = self.source_dat.select_array_from_rangeangle('qualityfactor', ra.sector_identifier).where(applicable_index, drop=True)

        first_mbes_file = list(ra.multibeam_files.keys())[0]
        is_kongsberg_all = os.path.splitext(first_mbes_file)[1] == '.all'
        if is_kongsberg_all:  # for .all files, quality factor is an int representing scaled std dev
            qf_type = 'kongsberg'
        else:  # for .kmall files, quality factor is a percentage of water depth, see IFREMER formula
            qf_type = 'ifremer'

        data_for_workers = []

        # set the first chunk to build the tpu sample image, provide a path to the folder to save in
        image_generation = [False] * len(idx_by_chunk)
        image_generation[0] = os.path.join(self.source_dat.converted_pth, 'ping_' + ra.sector_identifier + '.zarr')

        for cnt, chnk in enumerate(idx_by_chunk):
            if latency:
                chnk = chnk.assign_coords({'time': chnk.time.time + latency})
            try:
                fut_roll = self.client.scatter(roll.where(roll['time'] == chnk.time, drop=True))
                fut_corr_point = self.client.scatter(corr_point[chnk])
                fut_raw_point = self.client.scatter(raw_point[chnk])
                fut_acrosstrack = self.client.scatter(acrosstrack[chnk])
                fut_depthoffset = self.client.scatter(depthoffset[chnk])
                fut_soundspeed = self.client.scatter(soundspeed[chnk])
                fut_qualityfactor = self.client.scatter(qf[chnk])
                try:  # pospac uncertainty available
                    fut_npe = self.client.scatter(ppnav.north_position_error.where(ppnav.north_position_error['time'] == chnk.time, drop=True))
                    fut_epe = self.client.scatter(ppnav.east_position_error.where(ppnav.east_position_error['time'] == chnk.time, drop=True))
                    fut_dpe = self.client.scatter(ppnav.down_position_error.where(ppnav.down_position_error['time'] == chnk.time, drop=True))
                    fut_rpe = self.client.scatter(ppnav.roll_error.where(ppnav.roll_error['time'] == chnk.time, drop=True))
                    fut_ppe = self.client.scatter(ppnav.pitch_error.where(ppnav.pitch_error['time'] == chnk.time, drop=True))
                    fut_hpe = self.client.scatter(ppnav.heading_error.where(ppnav.heading_error['time'] == chnk.time, drop=True))
                except:  # rely on static values
                    fut_npe = None
                    fut_epe = None
                    fut_dpe = None
                    fut_rpe = None
                    fut_ppe = None
                    fut_hpe = None
            except:  # client is not setup, run locally
                fut_roll = roll.where(roll['time'] == chnk.time, drop=True)
                fut_corr_point = corr_point[chnk]
                fut_raw_point = raw_point[chnk]
                fut_acrosstrack = acrosstrack[chnk]
                fut_depthoffset = depthoffset[chnk]
                fut_soundspeed = soundspeed[chnk]
                fut_qualityfactor = qf[chnk]
                try:  # pospac uncertainty available
                    fut_npe = ppnav.north_position_error.where(ppnav.north_position_error['time'] == chnk.time, drop=True)
                    fut_epe = ppnav.east_position_error.where(ppnav.east_position_error['time'] == chnk.time, drop=True)
                    fut_dpe = ppnav.down_position_error.where(ppnav.down_position_error['time'] == chnk.time, drop=True)
                    fut_rpe = ppnav.roll_error.where(ppnav.roll_error['time'] == chnk.time, drop=True)
                    fut_ppe = ppnav.pitch_error.where(ppnav.pitch_error['time'] == chnk.time, drop=True)
                    fut_hpe = ppnav.heading_error.where(ppnav.heading_error['time'] == chnk.time, drop=True)
                except:  # rely on static values
                    fut_npe = None
                    fut_epe = None
                    fut_dpe = None
                    fut_rpe = None
                    fut_ppe = None
                    fut_hpe = None

            data_for_workers.append([fut_roll, fut_raw_point, fut_corr_point, fut_acrosstrack, fut_depthoffset, fut_soundspeed,
                                     self.source_dat.tpu_parameters, fut_qualityfactor, fut_npe, fut_epe, fut_dpe,
                                     fut_rpe, fut_ppe, fut_hpe, qf_type, self.vert_ref, image_generation[cnt]])
        return data_for_workers

    def _generate_chunks_xyzdat_old(self, variable_name: str, finallength: int, var_dtype: np.dtype,
                                    add_idx_vars: bool = True, s_index: list = None):
        """
        Flatten and chunk the non-NaN values for each sector.  Currently an issue with beam number, beam number saved
        this way is from zero for each sector.

        DEPRECATED: See the new _generate_chunks_xyzdat

        Parameters
        ----------
        variable_name
            variable identifier for the array to write.  ex: 'z' or 'tvu'
        finallength
            total number of soundings to write, ends up as the total length of the resulting dataset
        var_dtype
            pass in a numpy dtype to astype the array before writing
        add_idx_vars
            if True, write out the supporting arrays as well.  Only need to do this once, generally the first run.
        s_index
            list of the valid indices for variable if we need to subset.  Detectioninfo will have non-NaN values for
            all soundings, but if we get bad values in beam vector generation, we might have NaNs interspersed in the
            xyz arrays.  Use s_index to shorten the given variable (detectioninfo) to the non-NaN indices of the other
            arrays.

        Returns
        -------
        list
            each element is a future object pointing to a dataset to write out in memory
        list
            chunk indices for each chunk
        dict
            chunk sizes for the write, zarr wants explicit chunksizes for each array that cannot change after array
            creation.  chunk sizes can be greater than data size.
        """

        self.logger.info('Constructing dataset for variable "{}"'.format(variable_name))

        # use the raw_ping chunksize to chunk the reformed pings.
        data_for_workers = []
        sector_possible = self.return_sector_ids()

        # get the non NaN data for the variable name across all raw_pings (all sectors)
        if s_index is None:
            vals = np.concatenate([self.source_dat.select_array_from_rangeangle(variable_name, rp.sector_identifier)
                                   for rp in self.source_dat.raw_ping])
            actual_soundings_idx = ~np.isnan(vals)
        else:
            vals = np.concatenate([rp[variable_name].stack(stck=('time', 'beam')) for rp in self.source_dat.raw_ping])
            actual_soundings_idx = np.concatenate(s_index)

        # only need to do this once, all the other var writes will just be the variable specified
        if add_idx_vars:
            sec_ids = np.concatenate(
                [[sector_possible.index(rp.sector_identifier)] * rp.time.shape[0] for rp in self.source_dat.raw_ping])
            tms = np.concatenate([rp.time.values for rp in self.source_dat.raw_ping])

            self.logger.info('Constructing dataset for variable "beam_idx"')
            bms = np.tile(self.source_dat.raw_ping[0].beam.values, (vals.shape[0], 1))
            bms = bms[actual_soundings_idx]

            self.logger.info('Constructing dataset for variable "sector_idx"')
            sec_ids = np.tile(np.expand_dims(sec_ids, 1), (1, len(self.source_dat.raw_ping[0].beam.values)))
            sec_ids = sec_ids[actual_soundings_idx]

            self.logger.info('Constructing dataset for variable "time_idx"')
            tms = np.tile(np.expand_dims(tms, 1), (1, len(self.source_dat.raw_ping[0].beam.values)))
            tms = tms[actual_soundings_idx]

        vals = vals[actual_soundings_idx]

        # 1000000 soundings gets you about 1MB chunks, which is what zarr recommends
        chnksize = np.min([finallength, 1000000])
        chnks = [[i * chnksize, i * chnksize + chnksize] for i in range(int(finallength / chnksize))]
        chnks[-1][1] = len(vals)
        chnksize_dict = {'sounding': (1000000,), 'beam_idx': (1000000,), 'thu': (1000000,),
                         'sector_idx': (1000000,), 'time_idx': (1000000,), 'tvu': (1000000,), 'x': (1000000,),
                         'y': (1000000,), 'z': (1000000,)}
        for c in chnks:
            vrs = {variable_name: (['sounding'], vals[c[0]:c[1]].astype(var_dtype))}
            if add_idx_vars:
                vrs['sounding'] = np.arange(c[0], c[1], dtype=np.int64)
                vrs['time_idx'] = (['sounding'], tms[c[0]:c[1]])
                vrs['sector_idx'] = (['sounding'], sec_ids[c[0]:c[1]].astype(np.uint8))
                vrs['beam_idx'] = (['sounding'], bms[c[0]:c[1]].astype(np.uint16))
            ds = xr.Dataset(data_vars=vrs)
            data_for_workers.append(self.client.scatter(ds))

        return data_for_workers, chnks, chnksize_dict

    def _generate_chunks_xyzdat(self, variable_name: str):
        """
        Merge the desired vars across sectors to reform pings, and build the data for the distributed system to process.
        Export_xyzdat requires a full dataset flattened to a 'soundings' dimension but retaining the sectorwise
        indexing.

        Parameters
        ----------
        variable_name
            variable identifier for the array to write.  ex: 'z' or 'tvu'

        Returns
        -------
        list
            each element is a future object pointing to a dataset to write out in memory
        list
            chunk indices for each chunk
        dict
            chunk sizes for the write, zarr wants explicit chunksizes for each array that cannot change after array
            creation.  chunk sizes can be greater than data size.
        """

        if variable_name not in self.source_dat.raw_ping[0]:
            self.logger.warning('Skipping variable "{}", not found in dataset.'.format(variable_name))
            return None, None, None
        self.logger.info('Constructing dataset for variable "{}"'.format(variable_name))

        # use the raw_ping chunksize to chunk the reformed pings.
        data_for_workers = []
        unique_times_across_sectors = self.return_unique_times_across_sectors()
        var_data, sectors, tims = self.reform_2d_vars_across_sectors_at_time([variable_name], unique_times_across_sectors)

        # flatten to get the 1d sounding data
        tims = np.tile(np.expand_dims(tims, 1), (1, var_data.shape[2])).ravel()
        beams = np.tile(np.arange(0, var_data.shape[2], 1), (var_data.shape[1], 1)).ravel().astype(np.uint16)
        counter = np.repeat(np.arange(0, var_data.shape[1], 1)[:, np.newaxis], var_data.shape[2], axis=1).ravel().astype(np.uint32)
        var_data = var_data.ravel()
        sectors = sectors.ravel()

        finallength = len(var_data)

        # 1000000 soundings gets you about 1MB chunks, which is what zarr recommends
        chnksize = np.min([finallength, 1000000])
        chnks = [[i * chnksize, i * chnksize + chnksize] for i in range(int(finallength / chnksize))]
        chnks[-1][1] = finallength
        chnksize_dict = {'beam_number': (1000000,), 'sector': (1000000,), 'time': (1000000,), 'ping_counter': (1000000,),
                         'thu': (1000000,), 'tvu': (1000000,), 'x': (1000000,), 'y': (1000000,), 'z': (1000000,)}
        dtype_dict = {'x': np.float64, 'y': np.float64, 'z': np.float32, 'tvu': np.float32, 'thu': np.float32}

        for c in chnks:
            vrs = {variable_name: (['time'], var_data[c[0]:c[1]].astype(dtype_dict[variable_name]))}
            coords = {'beam_number': (['time'], beams[c[0]:c[1]]), 'time': tims[c[0]:c[1]],
                      'sector': (['time'], sectors[c[0]:c[1]]), 'ping_counter': (['time'], counter[c[0]:c[1]])}
            ds = xr.Dataset(vrs, coords)
            data_for_workers.append(self.client.scatter(ds))

        return data_for_workers, chnks, chnksize_dict

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

    def import_post_processed_navigation(self, navfiles: list, errorfiles: list = None, logfiles: list = None,
                                         weekstart_year: int = None, weekstart_week: int = None,
                                         override_datum: str = None, override_grid: str = None,
                                         override_zone: str = None, override_ellipsoid: str = None,
                                         max_gap_length: float = 1.0):
        """
        Load from post processed navigation files (currently just SBET and SMRMSG) to get lat/lon/altitude as well
        as 3d error for further processing.  Will save to a zarr rootgroup alongside the raw navigation, so you can
        compare and select as you wish.

        No interpolation is done, but it will slice the incoming data to the time extents of the raw navigation and
        identify time gaps larger than the provided max_gap_length in seconds.

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
        """

        self.logger.info('****Importing post processed navigation****\n')
        starttime = perf_counter()

        if self.source_dat is None:
            raise ValueError('Expect multibeam records before importing post processed navigation')

        if errorfiles is not None:
            if len(navfiles) != len(errorfiles):
                raise ValueError('Expect the same number of nav/error files: \n\n{}\n{}'.format(navfiles, errorfiles))
            err = errorfiles
        else:
            err = [None] * len(navfiles)

        if logfiles is not None:
            if len(navfiles) != len(logfiles):
                raise ValueError('Expect the same number of nav/log files: \n\n{}\n{}'.format(navfiles, logfiles))
            log = logfiles
        else:
            log = [None] * len(navfiles)

        newdata = []
        for cnt, nav in enumerate(navfiles):
            newdata.append(sbet_to_xarray(nav, smrmsgfile=err[cnt], logfile=log[cnt], weekstart_year=weekstart_year,
                                          weekstart_week=weekstart_week, override_datum=override_datum,
                                          override_grid=override_grid, override_zone=override_zone,
                                          override_ellipsoid=override_ellipsoid))
        navdata = xr.concat(newdata, dim='time')
        del newdata
        navdata = navdata.sortby('time', ascending=True)  # sbet files might be in any time order

        # retain only nav records that are within existing nav times
        navdata = slice_xarray_by_dim(navdata, 'time', start_time=float(self.source_dat.raw_nav.time.min()),
                                      end_time=float(self.source_dat.raw_nav.time.max()))
        if navdata is None:
            raise ValueError('Unable to find timestamps in SBET that align with the raw navigation.')
        navdata.attrs['reference'] = {'latitude': 'reference point', 'longitude': 'reference point',
                                      'altitude': 'reference point'}

        # find gaps that don't line up with existing nav gaps (like time between multibeam files)
        gaps = compare_and_find_gaps(self.source_dat.raw_nav, navdata, max_gap_length=max_gap_length, dimname='time')
        if gaps:
            self.logger.info('Found gaps > {} in comparison between post processed navigation and realtime.'.format(max_gap_length))
            for gp in gaps:
                self.logger.info('mintime: {}, maxtime: {}, gap length {}'.format(gp[0], gp[1], gp[1] - gp[0]))

        outfold = os.path.join(self.source_dat.converted_pth, 'ppnav.zarr')
        chunk_sizes = {k: self.source_dat.nav_chunksize for k in list(navdata.variables.keys())}  # 50000 to match the raw_nav
        sync = DaskProcessSynchronizer(outfold)
        data_locs, finalsize = get_write_indices_zarr(outfold, [navdata.time])
        navdata_attrs = navdata.attrs
        try:
            navdata = self.client.scatter(navdata)
        except:  # not using dask distributed client
            pass
        distrib_zarr_write(outfold, [navdata], navdata_attrs, chunk_sizes, data_locs, finalsize, sync, self.client,
                           append_dim='time', merge=False, show_progress=self.show_progress)

        self.ppnav_path = outfold
        self.reload_ppnav_records()

        self.interp_to_ping_record(self.ppnav_dat, {'navigation_source': 'sbet'})

        endtime = perf_counter()
        self.logger.info('****Importing post processed navigation complete: {}s****\n'.format(round(endtime - starttime, 1)))

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

        sectors = self.return_sector_ids()
        for sec in sectors:
            raw_ping = self.source_dat.return_ping_by_sector(sec)
            outfold_sec = os.path.join(self.source_dat.converted_pth, 'ping_' + sec + '.zarr')
            sync = DaskProcessSynchronizer(outfold_sec)

            for source in sources:
                ping_wise_data = interp_across_chunks(source, raw_ping.time, 'time').chunk(self.source_dat.ping_chunksize)
                chunk_sizes = {k: self.source_dat.ping_chunksize for k in list(ping_wise_data.variables.keys())}
                data_locs, finalsize = get_write_indices_zarr(outfold_sec, [ping_wise_data.time])
                try:
                    ping_wise_data = self.client.scatter(ping_wise_data)
                except:  # not using dask distributed client
                    pass
                distrib_zarr_write(outfold_sec, [ping_wise_data], attributes, chunk_sizes, data_locs, finalsize, sync,
                                   self.client, append_dim='time', merge=True, show_progress=self.show_progress)
                attributes = {}
        self.source_dat.reload_pingrecords(skip_dask=skip_dask)
        endtime = perf_counter()
        self.logger.info('****Interpolation complete: {}s****\n'.format(round(endtime - starttime, 1)))

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
            if True dump the tx/rx vectors to the source_data datastore
        """
        if subset_time is not None and dump_data:
            if isinstance(subset_time[0], list):
                first_subset_time = subset_time[0][0]
                last_subset_time = subset_time[-1][-1]
            else:
                first_subset_time = subset_time[0]
                last_subset_time = subset_time[-1]
            for ra in self.source_dat.raw_ping:
                secid = ra.sector_identifier
                # check to see if this sector is within the subset time
                if np.logical_and(ra.time <= last_subset_time, ra.time >= first_subset_time).any():
                    # nothing written to disk yet, first run has to include the first time
                    if 'tx' not in list(ra.keys()):
                        if first_subset_time > np.min(ra.time):
                            msg = 'get_orientation_vectors: {}: If your first run of this function uses subset_time, it must include the first ping.'.format(secid)
                            raise NotImplementedError(msg)
                    # data written already, just make sure we aren't creating a gap
                    else:
                        try:
                            last_written_time = ra.time[np.where(np.isnan(ra.tx.values))[0][0]]
                        except IndexError:  # no NaNs, array is complete so we are all good here
                            continue

                        if first_subset_time > last_written_time:
                            msg = 'get_orientation_vectors: {}: saved arrays must not have a time gap, subset_time must start at the last written time.'.format(secid)
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
            if True dump the tx/rx vectors to the source_data datastore
        """

        self._validate_subset_time(subset_time, dump_data)
        req = 'traveltime'
        if req not in list(self.source_dat.raw_ping[0].keys()):
            err = 'get_orientation_vectors: unable to find {}'.format(req)
            err += ' in ping data {}.  You must run read_from_source first.'.format(self.source_dat.raw_ping[0].sector_identifier)
            self.logger.error(err)
            raise ValueError(err)
        if self.source_dat.raw_att is None:
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
            if True dump the tx/rx vectors to the source_data datastore
        """
        self._validate_subset_time(subset_time, dump_data)

        # first check to see if there is any data in memory.  If so, we just assume that you have the data you need.
        if self.intermediate_dat is not None:
            for rawping in self.source_dat.raw_ping:
                if 'orientation' in self.intermediate_dat[rawping.sector_identifier]:
                    print('get_beam_pointing_vectors: in memory workflow')
                    return

        required = ['tx', 'rx', 'beampointingangle', 'tiltangle']
        for req in required:
            if req not in list(self.source_dat.raw_ping[0].keys()):
                err = 'get_beam_pointing_vectors: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run get_orientation_vectors first.'.format(self.source_dat.raw_ping[0].sector_identifier)
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
            if True dump the tx/rx vectors to the source_data datastore
        """
        self._validate_subset_time(subset_time, dump_data)

        # first check to see if there is any data in memory.  If so, we just assume that you have the data you need.
        if self.intermediate_dat is not None:
            for rawping in self.source_dat.raw_ping:
                if 'bpv' in self.intermediate_dat[rawping.sector_identifier]:
                    print('sv_correct: in memory workflow')
                    return

        required = ['rel_azimuth', 'corr_pointing_angle']
        for req in required:
            if req not in list(self.source_dat.raw_ping[0].keys()):
                err = 'sv_correct: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run get_beam_pointing_vectors first.'.format(
                    self.source_dat.raw_ping[0].sector_identifier)
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
            if True dump the tx/rx vectors to the source_data datastore
        """
        self._validate_subset_time(subset_time, dump_data)

        if self.vert_ref is None:
            self.logger.error("georef_xyz: set_vertical_reference must be run before georef_xyz")
            raise ValueError('georef_xyz: set_vertical_reference must be run before georef_xyz')
        if self.vert_ref not in ['ellipse', 'waterline']:
            self.logger.error("georef_xyz: {} must be one of 'ellipse', 'waterline'".format(self.vert_ref))
            raise ValueError("georef_xyz: {} must be one of 'ellipse', 'waterline'".format(self.vert_ref))
        if self.xyz_crs is None:
            self.logger.error('georef_xyz: xyz_crs object not found.  Please run Fqpr.construct_crs first.')
            raise ValueError('georef_xyz: xyz_crs object not found.  Please run Fqpr.construct_crs first.')
        if self.vert_ref == 'ellipse':
            if 'altitude' not in self.source_dat.raw_ping[0] and 'altitude' not in self.source_dat.raw_nav:
                self.logger.error('georef_xyz: You must provide altitude for vert_ref=ellipse, not found in raw navigation or ping records.')
                raise ValueError('georef_xyz: You must provide altitude for vert_ref=ellipse, not found in raw navigation or ping records.')

        # first check to see if there is any data in memory.  If so, we just assume that you have the data you need.
        if self.intermediate_dat is not None:
            for rawping in self.source_dat.raw_ping:
                if 'sv_corr' in self.intermediate_dat[rawping.sector_identifier]:
                    print('georef_xyz: in memory workflow')
                    return

        required = ['alongtrack', 'acrosstrack', 'depthoffset']
        for req in required:
            if req not in list(self.source_dat.raw_ping[0].keys()):
                err = 'georef_xyz: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run sv_correct first.'.format(
                    self.source_dat.raw_ping[0].sector_identifier)
                self.logger.error(err)
                raise ValueError(err)

    def _validate_calculate_total_uncertainty(self, subset_time: list, dump_data: bool):
        """
        Validation routine for running calculate_total_uncertainty.  Ensures you have all the data you need before kicking
        off the process

        Parameters
        ----------
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the tx/rx vectors to the source_data datastore
        """

        self._validate_subset_time(subset_time, dump_data)

        # no in memory workflow built just yet

        if self.vert_ref is None:
            self.logger.error('calculate_total_uncertainty: set_vertical_reference must be run before calculate_total_uncertainty')
            raise ValueError('calculate_total_uncertainty: set_vertical_reference must be run before calculate_total_uncertainty')
        if self.vert_ref not in ['ellipse', 'waterline']:
            self.logger.error("calculate_total_uncertainty: {} must be one of 'ellipse', 'waterline'".format(self.vert_ref))
            raise ValueError("calculate_total_uncertainty: {} must be one of 'ellipse', 'waterline'".format(self.vert_ref))
        if self.vert_ref == 'ellipse':
            if self.ppnav_dat is None:
                self.logger.error("calculate_total_uncertainty: with vert_ref={} you must provide post processed navigation".format(self.vert_ref))
                raise ValueError("calculate_total_uncertainty: with vert_ref={} you must provide post processed navigation".format(self.vert_ref))
            elif 'down_position_error' not in self.ppnav_dat:
                self.logger.error("calculate_total_uncertainty: with vert_ref={} you must provide sbet error".format(self.vert_ref))
                raise ValueError("calculate_total_uncertainty: with vert_ref={} you must provide sbet error".format(self.vert_ref))

        required = ['corr_pointing_angle', 'beampointingangle', 'acrosstrack', 'depthoffset', 'soundspeed', 'qualityfactor']
        for req in required:
            if req not in list(self.source_dat.raw_ping[0].keys()):
                err = 'calculate_total_uncertainty: unable to find {}'.format(req)
                err += ' in ping data {}.  You must run georef_xyz first.'.format(
                    self.source_dat.raw_ping[0].sector_identifier)
                self.logger.error(err)
                raise ValueError(err)

    def get_orientation_vectors(self, subset_time: list = None, dump_data: bool = True, delete_futs: bool = True,
                                initial_interp: bool = True):
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
            if True dump the tx/rx vectors to the source_data datastore.  Set this to false for an entirely in memory
            workflow
        delete_futs
            if True remove the futures objects after data is dumped.
        initial_interp
            if True, will interpolate attitude/navigation to the ping record and store in the raw_ping datasets.  This
            is not mandatory for processing, but useful for other kluster functions post processing.
        """

        self._validate_get_orientation_vectors(subset_time, dump_data)
        if initial_interp:
            needs_interp = []
            if ('latitude' not in list(self.source_dat.raw_ping[0].keys())) or \
                    ('altitude' not in list(self.source_dat.raw_ping[0].keys())) or \
                    ('latitude' not in list(self.source_dat.raw_ping[0].keys())):
                needs_interp.append(self.source_dat.raw_nav)
            if ('roll' not in list(self.source_dat.raw_ping[0].keys())) or \
                    ('pitch' not in list(self.source_dat.raw_ping[0].keys())) or \
                    ('heave' not in list(self.source_dat.raw_ping[0].keys())) or \
                    ('heading' not in list(self.source_dat.raw_ping[0].keys())):
                needs_interp.append(self.source_dat.raw_att)
            if needs_interp:
                self.interp_to_ping_record(needs_interp, {'attitude_source': 'multibeam', 'navigation_source': 'multibeam'})

        self.logger.info('****Building tx/rx vectors at time of transmit/receive****\n')
        starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        sectors = self.source_dat.return_sector_time_indexed_array(subset_time=subset_time)
        raw_att = self.source_dat.raw_att

        for s_cnt, sector in enumerate(sectors):
            ra = self.source_dat.raw_ping[s_cnt]
            sec_ident = ra.sector_identifier
            sec_info = self.parse_sect_info_from_identifier(sec_ident)
            self.logger.info('sector info: ' + ', '.join(['{}: {}'.format(k, v) for k, v in sec_info.items()]))
            self.initialize_intermediate_data(sec_ident, 'orientation')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params(sec_ident)

            for applicable_index, timestmp, prefixes in sector:
                self.logger.info('using installation params {}'.format(timestmp))
                self.generate_starter_orientation_vectors(prefixes, timestmp)
                twtt_by_idx = self.source_dat.select_array_from_rangeangle('traveltime', ra.sector_identifier).where(applicable_index, drop=True)
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this sector that align with this installation parameter record
                    data_for_workers = self._generate_chunks_orientation(ra, raw_att, idx_by_chunk, twtt_by_idx,
                                                                         applicable_index, timestmp, prefixes)
                    self.intermediate_dat[sec_ident]['orientation'][timestmp] = []
                    self._submit_data_to_cluster(data_for_workers, distrib_run_build_orientation_vectors,
                                                 max_chunks_at_a_time, idx_by_chunk,
                                                 self.intermediate_dat[sec_ident]['orientation'][timestmp])
                else:
                    self.logger.info('No pings found for {}-{}'.format(sec_ident, timestmp))
            if dump_data:
                self.orientation_time_complete = datetime.utcnow().strftime('%c')
                self.write_intermediate_futs_to_zarr('orientation', only_sec_idx=s_cnt, delete_futs=delete_futs, skip_dask=skip_dask)
        self.source_dat.reload_pingrecords(skip_dask=skip_dask)

        endtime = perf_counter()
        self.logger.info('****Get Orientation Vectors complete: {}s****\n'.format(round(endtime - starttime, 1)))

    def get_beam_pointing_vectors(self, subset_time: list = None, dump_data: bool = True, delete_futs: bool = True):
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
            if True dump the tx/rx vectors to the source_data datastore.  Set this to false for an entirely in memory
            workflow
        delete_futs
            if True remove the futures objects after data is dumped.
        """

        self._validate_get_beam_pointing_vectors(subset_time, dump_data)
        self.logger.info('****Building beam specific pointing vectors****\n')
        starttime = perf_counter()
        raw_hdng = self.source_dat.raw_att['heading']

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        sectors = self.source_dat.return_sector_time_indexed_array(subset_time=subset_time)
        for s_cnt, sector in enumerate(sectors):
            ra = self.source_dat.raw_ping[s_cnt]
            sec_ident = ra.sector_identifier
            sec_info = self.parse_sect_info_from_identifier(sec_ident)
            self.logger.info('sector info: ' + ', '.join(['{}: {}'.format(k, v) for k, v in sec_info.items()]))
            self.initialize_intermediate_data(sec_ident, 'bpv')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params(sec_ident)

            for applicable_index, timestmp, prefixes in sector:
                self.logger.info('using installation params {}'.format(timestmp))
                tx_tstmp_idx = get_ping_times(ra.time, applicable_index)
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this sector that align with this installation parameter record
                    data_for_workers = self._generate_chunks_bpv(raw_hdng, idx_by_chunk, tx_tstmp_idx, applicable_index, sec_ident, timestmp)
                    self.intermediate_dat[sec_ident]['bpv'][timestmp] = []
                    self._submit_data_to_cluster(data_for_workers,
                                                 distrib_run_build_beam_pointing_vector,
                                                 max_chunks_at_a_time, idx_by_chunk,
                                                 self.intermediate_dat[sec_ident]['bpv'][timestmp])
                else:
                    self.logger.info('No pings found for {}-{}'.format(sec_ident, timestmp))
            if dump_data:
                self.bpv_time_complete = datetime.utcnow().strftime('%c')
                self.write_intermediate_futs_to_zarr('bpv', only_sec_idx=s_cnt, delete_futs=delete_futs, skip_dask=skip_dask)
        self.source_dat.reload_pingrecords(skip_dask=skip_dask)

        endtime = perf_counter()
        self.logger.info('****Beam Pointing Vector generation complete: {}s****\n'.format(round(endtime - starttime, 1)))

    def sv_correct(self, add_cast_files: list = None, subset_time: list = None, dump_data: bool = True,
                   delete_futs: bool = True):
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
        subset_time
            List of unix timestamps in seconds, used as ranges for times that you want to process.
        dump_data
            if True dump the tx/rx vectors to the source_data datastore.  Set this to false for an entirely in memory
            workflow
        delete_futs
            if True remove the futures objects after data is dumped.
        """

        self._validate_sv_correct(subset_time, dump_data)
        self.logger.info('****Correcting for sound velocity****\n')
        starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        sectors = self.source_dat.return_sector_time_indexed_array(subset_time=subset_time)
        self.cast_chunks = {}
        for s_cnt, sector in enumerate(sectors):
            ra = self.source_dat.raw_ping[s_cnt]
            sec_ident = ra.sector_identifier
            sec_info = self.parse_sect_info_from_identifier(sec_ident)
            self.logger.info('sector info: ' + ', '.join(['{}: {}'.format(k, v) for k, v in sec_info.items()]))
            self.initialize_intermediate_data(sec_ident, 'sv_corr')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params(sec_ident)

            for applicable_index, timestmp, prefixes in sector:
                self.logger.info('using installation params {}'.format(timestmp))
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this sector that align with this installation parameter record
                    cast_objects = self.setup_casts_for_sec_by_index(s_cnt, applicable_index, prefixes, timestmp, add_cast_files)
                    cast_times = [c[0] for c in cast_objects]
                    cast_chunks = self.return_cast_idx_nearestintime(cast_times, idx_by_chunk)
                    addtl_offsets = self.return_additional_xyz_offsets(prefixes, sec_info, timestmp)
                    data_for_workers = self._generate_chunks_svcorr(cast_objects, cast_chunks, sec_ident, applicable_index, timestmp, addtl_offsets)
                    self.intermediate_dat[sec_ident]['sv_corr'][timestmp] = []
                    self._submit_data_to_cluster(data_for_workers, distributed_run_sv_correct,
                                                 max_chunks_at_a_time, [c[0].time for c in cast_chunks],
                                                 self.intermediate_dat[sec_ident]['sv_corr'][timestmp])
                    self.cast_chunks[s_cnt][timestmp] = cast_chunks
                else:
                    self.logger.info('No pings found for {}-{}'.format(sec_ident, timestmp))
            if dump_data:
                self.sv_time_complete = datetime.utcnow().strftime('%c')
                self.write_intermediate_futs_to_zarr('sv_corr', only_sec_idx=s_cnt, delete_futs=delete_futs, skip_dask=skip_dask)
        self.source_dat.reload_pingrecords(skip_dask=skip_dask)

        endtime = perf_counter()
        self.logger.info('****Sound Velocity complete: {}s****\n'.format(round(endtime - starttime, 1)))

    def georef_xyz(self, subset_time: list = None, prefer_pp_nav: bool = True, dump_data: bool = True, delete_futs: bool = True):
        """
        Use the raw attitude/navigation to transform the vessel relative along/across/down offsets to georeferenced
        soundings.  Will support transformation to geographic and projected coordinate systems and with a vertical
        reference that you select.

        If uncertainty is included in the source data, will calculate the unc based on depth.

        First does a forward transformation using the geoid provided in xyz_crs
        Then does a transformation from geographic to projected, if that is included in xyz_crs

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
            if True dump the tx/rx vectors to the source_data datastore.  Set this to false for an entirely in memory
            workflow
        delete_futs
            if True remove the futures objects after data is dumped.
        """

        self._validate_georef_xyz(subset_time, dump_data)
        self.logger.info('****Georeferencing sound velocity corrected beam offsets****\n')
        starttime = perf_counter()

        self.logger.info('Using pyproj CRS: {}'.format(self.xyz_crs.to_string()))

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        sectors = self.source_dat.return_sector_time_indexed_array(subset_time=subset_time)
        for s_cnt, sector in enumerate(sectors):
            ra = self.source_dat.raw_ping[s_cnt]
            sec_ident = ra.sector_identifier
            sec_info = self.parse_sect_info_from_identifier(sec_ident)
            self.logger.info('sector info: ' + ', '.join(['{}: {}'.format(k, v) for k, v in sec_info.items()]))
            self.initialize_intermediate_data(sec_ident, 'xyz')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params(sec_ident)

            for applicable_index, timestmp, prefixes in sector:
                self.logger.info('using installation params {}'.format(timestmp))
                z_offset = float(self.source_dat.xyzrph[prefixes[0] + '_z'][timestmp])
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this sector that align with this installation parameter record
                    data_for_workers = self._generate_chunks_georef(ra, idx_by_chunk, applicable_index, prefixes,
                                                                    timestmp, z_offset, prefer_pp_nav)
                    self.intermediate_dat[sec_ident]['xyz'][timestmp] = []
                    self._submit_data_to_cluster(data_for_workers, distrib_run_georeference,
                                                 max_chunks_at_a_time, idx_by_chunk,
                                                 self.intermediate_dat[sec_ident]['xyz'][timestmp])
                else:
                    self.logger.info('No pings found for {}-{}'.format(sec_ident, timestmp))
            if dump_data:
                self.georef_time_complete = datetime.utcnow().strftime('%c')
                self.write_intermediate_futs_to_zarr('georef', only_sec_idx=s_cnt, delete_futs=delete_futs, skip_dask=skip_dask)
        self.source_dat.reload_pingrecords(skip_dask=skip_dask)

        endtime = perf_counter()
        self.logger.info('****Georeferencing sound velocity corrected beam offsets complete: {}s****\n'.format(round(endtime - starttime, 1)))

    def calculate_total_uncertainty(self, subset_time: list = None, dump_data: bool = True, delete_futs: bool = True):
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
            if True dump the tx/rx vectors to the source_data datastore.  Set this to false for an entirely in memory
            workflow
        delete_futs
            if True remove the futures objects after data is dumped.
        """

        self._validate_calculate_total_uncertainty(subset_time, dump_data)
        self.logger.info('****Calculating total uncertainty****\n')
        starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        sectors = self.source_dat.return_sector_time_indexed_array(subset_time=subset_time)
        for s_cnt, sector in enumerate(sectors):
            ra = self.source_dat.raw_ping[s_cnt]
            sec_ident = ra.sector_identifier
            sec_info = self.parse_sect_info_from_identifier(sec_ident)
            self.logger.info('sector info: ' + ', '.join(['{}: {}'.format(k, v) for k, v in sec_info.items()]))
            self.initialize_intermediate_data(sec_ident, 'tpu')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params(sec_ident)

            for applicable_index, timestmp, prefixes in sector:
                self.logger.info('using installation params {}'.format(timestmp))
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this sector that align with this installation parameter record
                    data_for_workers = self._generate_chunks_tpu(ra, idx_by_chunk, applicable_index)
                    if data_for_workers is not None:
                        self.intermediate_dat[sec_ident]['tpu'][timestmp] = []
                        self._submit_data_to_cluster(data_for_workers, distrib_run_calculate_tpu,
                                                     max_chunks_at_a_time, idx_by_chunk,
                                                     self.intermediate_dat[sec_ident]['tpu'][timestmp])
                else:
                    self.logger.info('No pings found for {}-{}'.format(sec_ident, timestmp))
            if dump_data:
                self.tpu_time_complete = datetime.utcnow().strftime('%c')
                self.write_intermediate_futs_to_zarr('tpu', only_sec_idx=s_cnt, delete_futs=delete_futs, skip_dask=skip_dask)
        self.source_dat.reload_pingrecords(skip_dask=skip_dask)

        endtime = perf_counter()
        self.logger.info('****Calculating total uncertainty complete: {}s****\n'.format(round(endtime - starttime, 1)))

    def export_pings_to_file(self, outfold: str = None, file_format: str = 'csv', filter_by_detection: bool = True):
        """
        Uses the output of georef_along_across_depth to build sounding exports.  Currently you can export to csv or las
        file formats, see file_format argument.

        If you export to las and want to retain rejected soundings under the noise classification, set
        filter_by_detection to False.

        Filters using the detectioninfo variable if present in source_dat and filter_by_detection is set.

        Will generate an xyz file for each sector in source_dat.  Results in one xyz file for each freq/sector id/serial
        number combination.

        entwine export will build las first, and then entwine from las

        Parameters
        ----------
        outfold
            optional, destination directory for the xyz exports, otherwise will auto export next to converted data
        file_format
            optional, destination file format, default is csv file, options include ['csv', 'las', 'entwine']
        filter_by_detection
            optional, if True will only write soundings that are not rejected
        """

        if 'x' not in self.source_dat.raw_ping[0]:
            self.logger.error('No xyz data found')
            return
        if file_format not in ['csv', 'las', 'entwine']:
            self.logger.error('Only csv, las and entwine format options supported at this time')
            return
        if file_format == 'entwine' and not is_pydro():
            self.logger.error(
                'Only pydro environments support entwine tile building.  Please see https://entwine.io/configuration.html for instructions on installing entwine if you wish to use entwine outside of Kluster.  Kluster exported las files will work with the entwine build command')

        if outfold is None:
            outfold = self.source_dat.converted_pth

        self.logger.info('****Exporting xyz data to {}****'.format(file_format))
        starttime = perf_counter()

        tstmp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if file_format == 'csv':
            fldrname = 'csv_export'
        elif file_format in ['las', 'entwine']:
            fldrname = 'las_export'
        try:
            fldr_path = os.path.join(outfold, fldrname)
            os.mkdir(fldr_path)
        except FileExistsError:
            fldr_path = os.path.join(outfold, fldrname + '_{}'.format(tstmp))
            os.mkdir(fldr_path)

        for rp in self.source_dat.raw_ping:
            self.logger.info('Operating on sector {}'.format(rp.sector_identifier))
            if filter_by_detection and 'detectioninfo' not in rp:
                self.logger.error('Unable to filter by detection type, detectioninfo not found')
                return

            uncertainty_included = False
            x_idx, x_stck = stack_nan_array(rp['x'], stack_dims=('time', 'beam'))
            y_idx, y_stck = stack_nan_array(rp['y'], stack_dims=('time', 'beam'))
            z_idx, z_stck = stack_nan_array(rp['z'], stack_dims=('time', 'beam'))
            if 'tvu' in rp:
                uncertainty_included = True
                unc_idx, unc_stck = stack_nan_array(rp['tvu'], stack_dims=('time', 'beam'))

            # build mask with kongsberg detection info
            classification = None
            valid_detections = None
            if 'detectioninfo' in rp:
                dinfo = self.source_dat.select_array_from_rangeangle('detectioninfo', rp.sector_identifier)
                filter_stck = dinfo.values[x_idx]
                # filter_idx, filter_stck = stack_nan_array(dinfo, stack_dims=('time', 'beam'))
                valid_detections = filter_stck != 2
                tot = len(filter_stck)
                tot_valid = np.count_nonzero(valid_detections)
                tot_invalid = tot - tot_valid
                self.logger.info('{}: {} total soundings, {} retained, {} filtered'.format(rp.sector_identifier, tot, tot_valid,
                                                                                tot_invalid))
            # filter points by mask
            unc = None
            if filter_by_detection and valid_detections is not None:
                x = x_stck[valid_detections]
                y = y_stck[valid_detections]
                z = z_stck[valid_detections]
                classification = filter_stck[valid_detections]
                if uncertainty_included:
                    unc = unc_stck[valid_detections]
            else:
                x = x_stck
                y = y_stck
                z = z_stck
                if 'detectioninfo' in rp:
                    classification = filter_stck
                if uncertainty_included:
                    unc = unc_stck

            if file_format == 'csv':
                dest_path = os.path.join(fldr_path, rp.sector_identifier + '.xyz')

                self.logger.info('writing to {}'.format(dest_path))
                if uncertainty_included:
                    np.savetxt(dest_path, np.c_[x, y, z, unc], ['%3.3f', '%2.3f', '%4.3f', '%4.3f'])
                else:
                    np.savetxt(dest_path, np.c_[x, y, z], ['%3.3f', '%2.3f', '%4.3f'])
            elif file_format in ['las', 'entwine']:
                dest_path = os.path.join(fldr_path, rp.sector_identifier + '.las')
                self.logger.info('writing to {}'.format(dest_path))
                x = np.round(x.values, 2)
                y = np.round(y.values, 2)
                z = np.round(z.values, 3)
                gps_time = rp.time[x_idx[0]]
                if filter_by_detection and valid_detections is not None:
                    gps_time = gps_time[valid_detections]
                hdr = laspy.header.Header(file_version=1.4, point_format=3)  # pt format 3 includes GPS time
                hdr.x_scale = 0.01  # xyz precision, las stores data as int
                hdr.y_scale = 0.01
                hdr.z_scale = 0.001
                # offset apparently used to store only differences, but you still write the actual value?  needs more understanding.
                hdr.x_offset = np.floor(float(x.min()))
                hdr.y_offset = np.floor(float(y.min()))
                hdr.z_offset = np.floor(float(z.min()))
                outfile = laspy.file.File(dest_path, mode='w', header=hdr)
                outfile.x = x
                outfile.y = y
                outfile.z = z
                outfile.gps_time = gps_time
                if classification is not None:
                    classification[np.where(classification < 2)] = 1  # 1 = Unclassified according to LAS spec
                    classification[np.where(classification == 2)] = 7  # 7 = Low Point (noise) according to LAS spec
                    outfile.classification = classification.astype(np.int8)
                if unc is not None:  # putting it in Intensity for now as integer mm, Intensity is an int16 field
                    outfile.intensity = (unc.values * 1000).astype(np.int16)
                outfile.close()

        if file_format == 'entwine':
            las_fldr = fldr_path
            try:
                fldr_path = os.path.join(outfold, 'entwine_export')
                os.mkdir(fldr_path)
            except FileExistsError:
                fldr_path = os.path.join(outfold, 'entwine_export_{}'.format(tstmp))
                os.mkdir(fldr_path)
            build_entwine_points(las_fldr, fldr_path)

        endtime = perf_counter()
        self.logger.info('****Exporting xyz data to {} complete: {}s****\n'.format(file_format,
                                                                                   round(endtime - starttime, 1)))

    def _export_pings_to_dataset_old(self, outfold: str = None, validate: bool = False):
        """
        Write out data variable by variable to the final sounding data store.  First write will write out the useful
        indexes as well (time, sector, etc.).  Requires existence of georeferenced soundings to perform this function.

        We no longer reform pings from the sector datasets, we just write them out sector by sector.  This means that
        the sounding set is not necessarily organized geographically or in time order.  I don't think this matters,
        but i'll leave this here in case future me wants to embarass past me.

        Future me: this causes the beams that are indexed from 0 to max beam for each SECTOR to be included as is in the
        dataset.  We want beams indexed from 0 to max beam for each PING.  which means we need to reform the pings to
        get the actual beam number.  Which leads to the new export_pings_to_dataset method.  This is outdated.

        Parameters
        ----------
        outfold
            destination directory for the xyz exports
        validate
            if True will use assert statement to verify that the number of soundings between pre and post exported
            data is equal
        """

        self.logger.info('\n****Exporting xyz data to dataset****')
        starttime = perf_counter()

        if 'x' not in self.source_dat.raw_ping[0]:
            self.logger.error('No xyz data found')
            return

        if outfold is None:
            outfold = os.path.join(self.source_dat.converted_pth, 'soundings.zarr')
        if os.path.exists(outfold):
            self.logger.error('export_pings_to_dataset: dataset exists already ({}), please remove and run'.format(outfold))
            raise NotImplementedError('export_pings_to_dataset: dataset exists already ({}), please remove and run'.format(outfold))

        sync = DaskProcessSynchronizer(outfold)

        vars_of_interest = ('x', 'y', 'z', 'tvu', 'thu')
        dtype_of_interest = (np.float32, np.float32, np.float32, np.float32, np.float32)

        finallength = self.return_sounding_count()

        # build the attributes we want in the final array.  Everything from the raw_ping plus what we need to make
        #    sense of our new indexes seems good.
        exist_attrs = self.source_dat.raw_ping[0].attrs.copy()
        secs = self.return_sector_ids()
        exist_attrs['serial_number_identifier'] = [int(self.parse_sect_info_from_identifier(f)['serial_number']) for f in secs]
        exist_attrs['frequency_identifier'] = [int(self.parse_sect_info_from_identifier(f)['frequency']) for f in secs]
        exist_attrs['sector_identifier'] = [int(self.parse_sect_info_from_identifier(f)['sector']) for f in secs]
        exist_attrs['xyzdat_export_time'] = datetime.utcnow().strftime('%c')

        detectioninfo_len = np.sum([np.count_nonzero(~np.isnan(self.source_dat.select_array_from_rangeangle('detectioninfo', ra.sector_identifier))) for ra in self.source_dat.raw_ping])
        x_len = np.sum([np.count_nonzero(~np.isnan(ra.x)) for ra in self.source_dat.raw_ping])

        sounding_index = None
        if detectioninfo_len != x_len:
            self.logger.warning('Found uneven arrays, only including detection info where there are valid soundings:')
            self.logger.warning('soundings: {}'.format(x_len))
            self.logger.warning('detectioninfo: {}'.format(detectioninfo_len))
            sounding_index = [~np.isnan(ra.x.stack(stck=('time', 'beam'))) for ra in self.source_dat.raw_ping]

        for cnt, v in enumerate(vars_of_interest):
            add_idx_vars = False
            merge = False
            s_index = None
            if cnt == 0:
                # for the first one, write the beam/time/sec index variable arrays as well
                add_idx_vars = True
            else:
                # after the first write where we create the dataset, we need to flag subsequent writes as merge
                merge = True
            if v == 'detectioninfo':
                s_index = sounding_index

            data_for_workers, write_chnk_idxs, chunk_sizes = self._generate_chunks_xyzdat_old(v, finallength,
                                                                                          dtype_of_interest[cnt],
                                                                                          add_idx_vars=add_idx_vars,
                                                                                          s_index=s_index)
            final_size = write_chnk_idxs[-1][-1]
            fpths = distrib_zarr_write(outfold, data_for_workers, exist_attrs, chunk_sizes, write_chnk_idxs, final_size,
                                       sync, self.client, append_dim='sounding', merge=merge,
                                       show_progress=self.show_progress)

        self.soundings_path = outfold
        self.reload_soundings_records()

        if validate:  # ensure the sounding count matches
            pre_soundings_count = np.sum([np.count_nonzero(~np.isnan(f.x)) for f in self.source_dat.raw_ping])
            post_soundings_count = self.soundings.sounding.shape[0]
            assert pre_soundings_count == post_soundings_count
            self.logger.info('export_pings_to_dataset validated successfully')

        endtime = perf_counter()
        self.logger.info('****Exporting xyz data to dataset complete: {}s****\n'.format(round(endtime - starttime, 1)))

    def export_pings_to_dataset(self, outfold: str = None, validate: bool = False):
        """
        Write out data variable by variable to the final sounding data store.  Requires existence of georeferenced
        soundings to perform this function.

        Use reform_2d_vars_across_sectors_at_time to build the arrays before exporting.  Only necessary to attain
        the actual beam number of each sounding.  This is the difference between this method and the original.

        Parameters
        ----------
        outfold
            destination directory for the xyz exports
        validate
            if True will use assert statement to verify that the number of soundings between pre and post exported
            data is equal
        """

        self.logger.info('\n****Exporting xyz data to dataset****')
        starttime = perf_counter()

        if 'x' not in self.source_dat.raw_ping[0]:
            self.logger.error('No xyz data found')
            return

        if outfold is None:
            outfold = os.path.join(self.source_dat.converted_pth, 'soundings.zarr')
        if os.path.exists(outfold):
            self.logger.error(
                'export_pings_to_dataset: dataset exists already ({}), please remove and run'.format(outfold))
            raise NotImplementedError(
                'export_pings_to_dataset: dataset exists already ({}), please remove and run'.format(outfold))

        sync = DaskProcessSynchronizer(outfold)

        vars_of_interest = ('x', 'y', 'z', 'tvu', 'thu')

        # build the attributes we want in the final array.  Everything from the raw_ping plus what we need to make
        #    sense of our new indexes seems good.
        exist_attrs = self.source_dat.raw_ping[0].attrs.copy()
        secs = self.return_sector_ids()
        exist_attrs['serial_number_identifier'] = [int(self.parse_sect_info_from_identifier(f)['serial_number']) for f in secs]
        exist_attrs['frequency_identifier'] = [int(self.parse_sect_info_from_identifier(f)['frequency']) for f in secs]
        exist_attrs['sector_identifier'] = [int(self.parse_sect_info_from_identifier(f)['sector']) for f in secs]
        exist_attrs['xyzdat_export_time'] = datetime.utcnow().strftime('%c')

        for cnt, v in enumerate(vars_of_interest):
            merge = False
            if cnt != 0:
                # after the first write where we create the dataset, we need to flag subsequent writes as merge
                merge = True

            data_for_workers, write_chnk_idxs, chunk_sizes = self._generate_chunks_xyzdat(v)
            if data_for_workers is not None:
                final_size = write_chnk_idxs[-1][-1]
                fpths = distrib_zarr_write(outfold, data_for_workers, exist_attrs, chunk_sizes, write_chnk_idxs,
                                           final_size, sync, self.client, append_dim='time', merge=merge,
                                           show_progress=self.show_progress)
        self.soundings_path = outfold
        self.reload_soundings_records()

        if validate:
            # ensure the sounding count matches
            pre_soundings_count = np.sum([np.count_nonzero(~np.isnan(f.x)) for f in self.source_dat.raw_ping])
            post_soundings_count = self.soundings.time.shape[0]
            assert pre_soundings_count == post_soundings_count
            self.logger.info('export_pings_to_dataset validated successfully')

        endtime = perf_counter()
        self.logger.info('****Exporting xyz data to dataset complete: {}s****\n'.format(round(endtime - starttime, 1)))

    def _submit_data_to_cluster(self, data_for_workers: list, kluster_function: Callable, max_chunks_at_a_time: int,
                                idx_by_chunk: xr.DataArray, futures_repo: list):
        """
        For all of the main processes, we break up our inputs into chunks, appended to a list (data_for_workers).
        Knowing the capacity of the cluster memory, we can determine how many chunks to run at a time
        (max_chunks_at_a_time) and submit map those chunks to the cluster workers.  Append the resulting futures
        to the futures_repo and wait inbetween mappings.  This limits the memory used so that we don't run out.

        Parameters
        ----------
        data_for_workers
            list of lists, each sub list is all the inputs for the given function
        kluster_function
            one of the distrib functions, i.e. distrib_run_build_orientation_vectors,
            distrib_run_build_beam_pointing_vector, etc.
        max_chunks_at_a_time
            the max number of chunks we run at once
        idx_by_chunk
            values are the integer indexes of the pings to use, coords are the time of ping
        futures_repo
            where we want to store the futures objects, later used in writing to disk
        """

        try:
            tot_runs = int(np.ceil(len(data_for_workers) / max_chunks_at_a_time))
            for rn in range(tot_runs):
                start_r = rn * max_chunks_at_a_time
                end_r = min(start_r + max_chunks_at_a_time, len(data_for_workers))  # clamp for last run
                futs = self.client.map(kluster_function, data_for_workers[start_r:end_r])
                if self.show_progress:
                    progress(futs)
                endtimes = [len(c) for c in idx_by_chunk[start_r:end_r]]
                futs_with_endtime = [[f, endtimes[cnt]] for cnt, f in enumerate(futs)]
                futures_repo.extend(futs_with_endtime)
                wait(futures_repo)
        except:  # get here if client is closed or not setup
            for cnt, dat in enumerate(data_for_workers):
                endtime = len(idx_by_chunk[cnt])
                data = kluster_function(dat)
                futures_repo.append([data, endtime])

    def write_intermediate_futs_to_zarr(self, mode: str, only_sec_idx: int = None, outfold: str = None,
                                        delete_futs: bool = False, skip_dask: bool = False):
        """
        Flush some of the intermediate data that was mapped to the cluster (and lives in futures objects) to disk, puts
        it in the source_data, as the time dimension should be the same.  Mode allows for selecting the output from one
        of the main processes for writing.

        Delete futures to clear up memory if desired.

        Reload the source_data so that the object sees the updated zarr variables (reload_pingrecords)

        Parameters
        ----------
        mode
            one of ['orientation', 'bpv', sv_corr', 'georef']
        only_sec_idx
            optional, if this is not None, will only write the futures associated with the sector that has the given
            index
        outfold
            optional, output folder path, not including the zarr folder will use source_dat.converted_pth if not
            provided
        delete_futs
            if True will delete futures after writing data to disk
        skip_dask
            if True will not use the dask.distributed client to submit tasks, will run locally instead
        """

        ping_chunks = {'time': (self.source_dat.ping_chunksize,), 'beam': (400,), 'xyz': (3,),
                       'tx': (self.source_dat.ping_chunksize, 3), 'rx': (self.source_dat.ping_chunksize, 400, 3),
                       'rel_azimuth': (self.source_dat.ping_chunksize, 400),
                       'corr_pointing_angle': (self.source_dat.ping_chunksize, 400),
                       'alongtrack': (self.source_dat.ping_chunksize, 400),
                       'acrosstrack': (self.source_dat.ping_chunksize, 400),
                       'depthoffset': (self.source_dat.ping_chunksize, 400),
                       'x': (self.source_dat.ping_chunksize, 400), 'y': (self.source_dat.ping_chunksize, 400),
                       'z': (self.source_dat.ping_chunksize, 400), 'thu': (self.source_dat.ping_chunksize, 400),
                       'tvu': (self.source_dat.ping_chunksize, 400),
                       'corr_heave': (self.source_dat.ping_chunksize,),
                       'corr_altitude': (self.source_dat.ping_chunksize,)}

        if mode == 'orientation':
            mode_settings = ['orientation', ['tx', 'rx'], 'orientation vectors',
                             {'_compute_orientation_complete': self.orientation_time_complete,
                              'reference': {'tx': 'unit vector', 'rx': 'unit vector'},
                              'units': {'tx': ['+ forward', '+ starboard', '+ down'],
                                        'rx': ['+ forward', '+ starboard', '+ down']}}]
        elif mode == 'bpv':
            mode_settings = ['bpv', ['rel_azimuth', 'corr_pointing_angle'], 'beam pointing vectors',
                             {'_compute_beam_vectors_complete': self.bpv_time_complete,
                              'reference': {'rel_azimuth': 'vessel heading',
                                            'corr_pointing_angle': 'vertical in geographic reference frame'},
                              'units': {'rel_azimuth': 'radians', 'corr_pointing_angle': 'radians'}}]
        elif mode == 'sv_corr':
            mode_settings = ['sv_corr', ['alongtrack', 'acrosstrack', 'depthoffset'], 'sv corrected data',
                             {'svmode': 'nearest in time', '_sound_velocity_correct_complete': self.sv_time_complete,
                              'reference': {'alongtrack': 'reference', 'acrosstrack': 'reference',
                                            'depthoffset': 'transmitter'},
                              'units': {'alongtrack': 'meters (+ forward)', 'acrosstrack': 'meters (+ starboard)',
                                        'depthoffset': 'meters (+ down)'}}]
        elif mode == 'georef':
            mode_settings = ['xyz', ['x', 'y', 'z', 'corr_heave', 'corr_altitude'],
                             'georeferenced soundings data',
                             {'xyz_crs': self.xyz_crs.to_epsg(), 'vertical_reference': self.vert_ref,
                              '_georeference_soundings_complete': self.georef_time_complete,
                              'reference': {'x': 'reference', 'y': 'reference', 'z': 'reference',
                                            'corr_heave': 'transmitter', 'corr_altitude': 'transmitter to ellipsoid'},
                              'units': {'x': 'meters (+ forward)', 'y': 'meters (+ starboard)',
                                        'z': 'meters (+ down)', 'corr_heave': 'meters (+ down)',
                                        'corr_altitude': 'meters (+ down)'}}]
        elif mode == 'tpu':
            mode_settings = ['tpu', ['tvu', 'thu'],
                             'total horizontal and vertical uncertainty',
                             {'_total_uncertainty_complete': self.tpu_time_complete,
                              'vertical_reference': self.vert_ref,
                              'reference': {'tvu': 'None', 'thu': 'None'},
                              'units': {'tvu': 'meters (+ down)', 'thu': 'meters'}}]
        else:
            self.logger.error('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')
            raise ValueError('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')

        self.logger.info('Writing {} to disk...'.format(mode_settings[2]))
        starttime = perf_counter()

        if outfold is None:
            outfold = self.source_dat.converted_pth  # parent folder to all the currently written data
        sync = None
        if self.client is not None:
            sync = DaskProcessSynchronizer(outfold)

        sectors = self.source_dat.return_sector_time_indexed_array()
        for s_cnt, sector in enumerate(sectors):  # for each sector
            if only_sec_idx is not None:  # isolate just one sector
                if s_cnt != only_sec_idx:
                    continue
            ra = self.source_dat.raw_ping[s_cnt]
            sec_ident = ra.sector_identifier
            outfold_sec = os.path.join(outfold, 'ping_' + sec_ident + '.zarr')
            futs_data = []
            for applicable_index, timestmp, prefixes in sector:  # for each unique install parameter entry in that sector
                if timestmp in self.intermediate_dat[sec_ident][mode_settings[0]]:
                    futs = self.intermediate_dat[sec_ident][mode_settings[0]][timestmp]
                    for f in futs:
                        try:
                            futs_data.extend([self.client.submit(combine_arrays_to_dataset, f[0], mode_settings[1])])
                        except:  # client is not setup or closed, this is if you want to run on just your machine
                            futs_data.extend([combine_arrays_to_dataset(f[0], mode_settings[1])])
            if futs_data:
                time_arrs = self.client.gather(self.client.map(_return_xarray_time, futs_data))
                data_locs, finalsize = get_write_indices_zarr(outfold_sec, time_arrs)
                fpths = distrib_zarr_write(outfold_sec, futs_data, mode_settings[3], ping_chunks, data_locs, finalsize,
                                           sync, self.client, merge=True, skip_dask=skip_dask, show_progress=self.show_progress)
            if delete_futs:
                del self.intermediate_dat[sec_ident][mode_settings[0]]

        endtime = perf_counter()
        self.logger.info('Writing {} to disk complete: {}s\n'.format(mode_settings[2], round(endtime - starttime, 1)))

    def return_xyz(self, start_time: float = None, end_time: float = None, include_unc: bool = False):
        """
        Iterate through all the raw_ping datasets and append all the xyz records together into a list of one dimensional
        arrays.  If start time and/or end time are provided, it will only include xyz records that are within that time
        period.  If start time and end time are not provided, it returns all the xyz records in all the raw_ping datasets.

        If include_unc and there is uncertainty in the raw_ping, it will include that as well.

        Parameters
        ----------
        start_time
            start time in utc seconds
        end_time
            end time in utc seconds
        include_unc
            if true it will also include uncertainty in the list

        Returns
        -------
        list
            list of numpy arrays for either x,y,z or x,y,z,uncertainty if include_unc
        """

        if 'x' not in self.source_dat.raw_ping[0]:
            print('return_xyz: unable to find georeferenced xyz for {}'.format(self.source_dat.converted_pth))
            return None

        if 'tvu' not in self.source_dat.raw_ping[0] and include_unc:
            print('return_xyz: unable to find uncertainty for {}'.format(self.source_dat.converted_pth))
            return None

        data = []
        xyz = [[], [], [], []]
        for rp in self.source_dat.raw_ping:
            rp_sliced = slice_xarray_by_dim(rp, 'time', start_time=start_time, end_time=end_time)
            if rp_sliced is not None:
                x_idx, x_stck = stack_nan_array(rp_sliced['x'], stack_dims=('time', 'beam'))
                y_idx, y_stck = stack_nan_array(rp_sliced['y'], stack_dims=('time', 'beam'))
                z_idx, z_stck = stack_nan_array(rp_sliced['z'], stack_dims=('time', 'beam'))

                xyz[0].append(x_stck)
                xyz[1].append(y_stck)
                xyz[2].append(z_stck)
                if 'tvu' in rp and include_unc:
                    unc_idx, unc_stck = stack_nan_array(rp['tvu'], stack_dims=('time', 'beam'))
                    xyz[3].append(unc_stck)

        if xyz[0]:
            if include_unc and xyz[3]:
                data = [np.concatenate(xyz[0]), np.concatenate(xyz[1]), np.concatenate(xyz[2]), np.concatenate(xyz[3])]
            else:
                data = [np.concatenate(xyz[0]), np.concatenate(xyz[1]), np.concatenate(xyz[2])]

        return data

    def return_sector_ids(self):
        """
        Return a list containing the sector ids across all sectors in the fqpr

        dual head sonars will have pings that are the same in time and ping counter.  Only serial number can be
        used to distinguish.  All we need to do is ensure primary comes first in the loop.

        Returns
        -------
        list
            sector ids as strings
        """

        ids = []
        for ra in self.source_dat.raw_ping:
            ids.append(ra.sector_identifier)
        if self.source_dat.is_dual_head():
            primary_sn = str(self.source_dat.raw_ping[0].system_serial_number[0])
            ids.sort(key=lambda x: x.split('_')[0] == primary_sn, reverse=True)
        return ids

    def is_primary_system(self, sector_identifier: str):
        """
        Take the provided sector identifier string and return boolean for whether or not it is primary system.  We'll
        know because primary/secondary system identifer is stored as an attribute in the raw_ping datasets.

        Will return True for all sectors if this is not a dual head (only dual head has secondary system)

        Parameters
        ----------
        sector_identifier
            identifier for the sector in format 'serialnumber_sectornumber_freq', ex: '125125_0_200000'

        Returns
        -------
        bool
            if provided sector_identifier is the primary system, returns true.  else returns false
        """

        # use the attributes from the first raw_ping record as they all have the same attributes
        primary_sn = str(self.source_dat.raw_ping[0].system_serial_number[0])
        sector_sn = self.parse_sect_info_from_identifier(sector_identifier)['serial_number']
        return primary_sn == sector_sn

    def parse_sect_info_from_identifier(self, sector_identifier: str):
        """
        Take the format used for sector ids and return the encoded info.

        EX: '40111_1_290000' => serial number = 40111, sector index = 1, frequency = 290000 hz

        Parameters
        ----------
        sector_identifier
            identifier for the sector in format 'serialnumber_sectornumber_freq', ex: '125125_0_200000'

        Returns
        -------
        dict
            dict containing serial number, sector and frequency, all as string
        """

        sec_info = sector_identifier.split('_')
        return {'serial_number': sec_info[0], 'sector': sec_info[1], 'frequency': sec_info[2]}

    def return_total_pings(self, only_these_counters: Union[np.array, int] = None, min_time: float = None,
                           max_time: float = None):
        """
        Use the sector identifiers to determine the total pings for this fqpr instance.  Sectors are split by serial
        number/sector index/frequency, so to get the total pings, we just find all the serial number/frequency
        combinations and add the time sizes together

        Parameters
        ----------
        only_these_counters
            an array or a single ping counter value to subset the raw_ping dataset by.
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
        secs = self.return_sector_ids()
        secs_info = [self.parse_sect_info_from_identifier(s) for s in secs]
        sec_index = secs_info[0]['sector']
        selected_secs = [secs[secs_info.index(info)] for info in secs_info if info['sector'] == sec_index]
        for sec in selected_secs:
            raw_ping = self.source_dat.return_ping_by_sector(sec)
            if min_time is not None:
                raw_ping = raw_ping.where(raw_ping.time >= min_time, drop=True)
            if max_time is not None:
                raw_ping = raw_ping.where(raw_ping.time <= max_time, drop=True)
            if only_these_counters is not None:
                total_pings += np.count_nonzero(np.isin(only_these_counters, raw_ping.counter))
            else:
                total_pings += raw_ping.time.size
        return total_pings

    def _reform_build_pings(self, total_pings: int, secs: list, ping_counters: np.array, variable_selection: list,
                            max_possible_beams: int = 3000, min_time: float = None, max_time: float = None):
        """
        Admittedly kind of a messy way to reform pings from the split up sector Dataset.  We use the ping counter and
        time of ping to do this.  Find all the beams associated with the counter/time and toss them into an array, that
        has a max length of max_possible_beams.  Make sure max_possible_beams is large enough to contain beams + NaNs,
        the more sectors you have, the more beam padding you'll have.

        Parameters
        ----------
        total_pings
            expected number of pings
        secs
            sector ids for all sectors
        ping_counters
            ping counters for all pings
        variable_selection
            strings for all variables you want to reform across sectors
        max_possible_beams
            some arbitrarily large number of beams to ensure that you capture all beams + NaNs for empty beams in
            square sector arrays
        min_time
            limit accessible pings by minimum time
        max_time
            limit accessible pings by maximum time

        Returns
        -------
        np.array
            data for given variable names at that time for all sectors
        np.array
            1d array containing times for each ping
        np.array
            1d array containing string values indicating the sector each beam value comes from
        """

        # 2000, an arbitrarily large number to hold all possible beams plus NaNs
        #    these arrays are where we are going to put all the different sectors for each ping counter
        out = np.full((len(variable_selection), total_pings, max_possible_beams), np.nan)
        out_sec = np.empty((1, total_pings, max_possible_beams), dtype=np.uint16)
        out_tms = np.full((1, total_pings, len(secs)), np.nan)

        # make the second dim long enough for duplicate ping counters in dual head
        #    these arrays serve as indexes for pushing sectors into the out arrays
        data_strt = np.zeros((len(variable_selection), len(ping_counters) * 4), dtype=int)
        data_end = np.zeros((len(variable_selection), len(ping_counters) * 4), dtype=int)

        # build the out arrays by just shoving in blocks of ping/beam data, NaNs and all.  We use the expected shape
        #    and numpy isnan later to reform the 400 beam pings later on
        for s_cnt, sec in enumerate(secs):
            # counter_idxs is the ping counter index for where this sector is active
            counter_times = self.source_dat.return_active_sectors_for_ping_counter(s_cnt, ping_counters)
            if min_time is not None:
                counter_times[counter_times < min_time] = 0.0
            if max_time is not None:
                counter_times[counter_times > max_time] = 0.0
            counter_idxs = np.array(np.where(counter_times != 0)).ravel()

            if np.any(counter_times):
                time_idx = counter_times[counter_times != 0]
                for v_cnt, dattype in enumerate(variable_selection):
                    dat = self.source_dat.select_array_from_rangeangle(dattype, sec).sel(time=time_idx)
                    data_end[v_cnt, counter_idxs] += dat.shape[1]
                    out[v_cnt, counter_idxs, data_strt[v_cnt, counter_idxs[0]]:data_end[v_cnt, counter_idxs[0]]] = dat
                    if v_cnt == 0:
                        out_tms[v_cnt, counter_idxs, s_cnt] = time_idx
                        out_sec[v_cnt, counter_idxs, data_strt[v_cnt, counter_idxs[0]]:data_end[v_cnt, counter_idxs[0]]] = s_cnt
                    data_strt[v_cnt, counter_idxs] = data_end[v_cnt, counter_idxs]

        return out, out_tms, out_sec

    def _reform_correct_for_different_lengths(self, out: np.array, variable_selection: tuple,
                                              actual_var_lengths: np.array, maxbeamnumber: int):
        """
        Finding instances of variables having a different amount of NaNs built in to them.  I saw this on the EM2040
        dual head system on the Hassler, where some Data78 instances had traveltime, but no beampointingangle.  It is
        kind of strange.  Here we just take the shortest variable (after NaNs are removed) and use that as the index
        for the other variables.

        Parameters
        ----------
        out
            (number of variables, number of pings, 2000) for the dataset still containing NaN
        variable_selection
            str identifiers for each variable, ex: 'x' or 'traveltime'
        actual_var_lengths
            number of nans found for each variable in out
        maxbeamnumber
            maximum number of beams for this system

        Returns
        -------
        np.ndarray
            (number of variables, number of pings, number of beams) for the dataset without NaN
        tuple
            (number of variables, number of pings, number of beams)
        tuple
            (1, number of pings, number of beams)
        np.ndarray
            (number of variables, number of pings, number of beams) for all non-NaN values in out
        """

        shortest_arr = int(np.where(actual_var_lengths == np.amin(actual_var_lengths))[0])
        shortest_idx = ~np.isnan(out[shortest_arr])
        self.logger.info('Found some variables with different lengths, resizing to {}'.format(
            variable_selection[shortest_arr]))
        final_idx = np.zeros_like(out, dtype=bool)
        for cnt, i in enumerate(actual_var_lengths):
            self.logger.info('{}: {}'.format(variable_selection[cnt], i))
            final_idx[cnt] = shortest_idx
        finalout = out[final_idx]
        revised_pingcount = int(len(finalout) / len(variable_selection) / maxbeamnumber)
        expected_shape = (len(variable_selection), revised_pingcount, maxbeamnumber)
        expected_sec_shape = (1, revised_pingcount, maxbeamnumber)
        finalout = finalout.reshape(expected_shape)
        return finalout, expected_shape, expected_sec_shape, final_idx

    def reform_2d_vars_across_sectors_at_time(self, variable_selection: list, ping_times: Union[np.array, float],
                                              maxbeamnumber: int = 400):
        """
        Take specific variable names and a time, return the array across all sectors merged into one block.

        Do this by finding the ping counter number(s) at that time, and return the full ping(s) associated with that
        ping counter number.

        EM2040c will have multiple pings at the same time with different ping counters.  Sometimes there is a slight
        time delay.  Most of the time not.  Tracking ping counter solves this

        DualHead sonar will have multiple pings at the same time with the same ping counter.  Ugh.  Only way of
        differentiating is through the serial number associated with that sector.

        Parameters
        ----------
        variable_selection
            variable names you want from the fqpr sectors
        ping_times
            time to select the dataset by
        maxbeamnumber
            maximum number of beams for the system

        Returns
        -------
        np.array
            data for given variable names at that time for all sectors
        np.array
            1d array containing string values indicating the sector each beam value comes from
        np.array
            1d array containing times for each ping
        """
        self.source_dat.correct_for_counter_reset()
        ping_counters = self.source_dat.return_ping_counters_at_time(ping_times)
        secs = self.return_sector_ids()
        if isinstance(ping_times, float):
            min_time, max_time = ping_times, ping_times
        else:
            min_time, max_time = ping_times[0], ping_times[-1]

        total_pings = self.return_total_pings(only_these_counters=ping_counters, min_time=min_time, max_time=max_time)

        # after merging sectors, shape of final array should be as follows
        expected_shape = (len(variable_selection), int(total_pings), maxbeamnumber)
        expected_sec_shape = (1, int(total_pings), maxbeamnumber)
        out, out_tms, out_sec = self._reform_build_pings(total_pings, secs, ping_counters, variable_selection,
                                                         min_time=min_time, max_time=max_time)

        # reform pings using the expected shape
        if np.any(out):
            final_idx = ~np.isnan(out)
            finalout = out[final_idx]
            finaltms = out_tms[~np.isnan(out_tms)]
            try:
                finalout = finalout.reshape(expected_shape)
                finalsec = out_sec[np.expand_dims(final_idx[0], axis=0)].reshape(expected_sec_shape)
            except ValueError:
                # no good, we couldn't predict the shape, either we got the ping count wrong (they skipped a ping or
                # something) or the beam count isn't 400.  Try letting numpy figure out ping / beam count
                try:
                    new_shape = (expected_shape[0], -1, expected_shape[2])
                    finalout = finalout.reshape(new_shape)
                    finalsec = out_sec[np.expand_dims(final_idx[0], axis=0)].reshape(new_shape)
                    print('Expected shape {}, ended up using {} by autodetecting the ping count'.format(expected_shape, new_shape))
                except ValueError:
                    new_shape = (expected_shape[0], expected_shape[1], -1)
                    finalout = finalout.reshape(new_shape)
                    finalsec = out_sec[np.expand_dims(final_idx[0], axis=0)].reshape(new_shape)
                    print('Expected shape {}, ended up using {} by autodetecting the beam number'.format(expected_shape, new_shape))
            # now get time per ping, just use the time from the first sector
            finaltms = finaltms[::int(finaltms.shape[0] / finalout.shape[1])]
            if finaltms.shape[0] != finalout.shape[1]:
                print('After resampling, expected time dim equal to total pings {}, found {}'.format(finalout.shape[1],
                                                                                                     finaltms.shape[0]))
            return finalout, finalsec, finaltms
        else:
            self.logger.error('Unable to find records for {} for time {}'.format(variable_selection, ping_times))
            return None, None, None

    def reform_1d_vars_across_sectors_at_time(self, variable_selection: list, ping_times: Union[np.array, float],
                                              serial_number: str = None):
        """
        Method for taking one dimensional (time) variable names and returning the values at the given times (ping_times)
        across all sectors.  ping_times can be from any/all sector(s)

        An optional serial number can be provided to isolate one system in dual head configuration

        Parameters
        ----------
        variable_selection
            variable names you want from the fqpr sectors
        ping_times
            time to select the dataset by
        serial_number
            serial number identifier for dual head systems, if not provided will prefer the first serial number found

        Returns
        -------
        np.array
            data for given variable names at that time for all sectors (variable, ping)
        np.array
            1d array containing times for each ping
        """

        self.source_dat.correct_for_counter_reset()
        ping_counters = self.source_dat.return_ping_counters_at_time(ping_times)
        secs = self.return_sector_ids()
        if isinstance(ping_times, float):
            min_time, max_time = ping_times, ping_times
        else:
            min_time, max_time = ping_times[0], ping_times[-1]
        total_pings = self.return_total_pings(only_these_counters=ping_counters)

        if serial_number is not None:
            ser_num = str(serial_number)
        else:
            ser_num = self.parse_sect_info_from_identifier(secs[0])['serial_number']
        secs = [s for s in secs if self.parse_sect_info_from_identifier(s)['serial_number'] == ser_num]
        if not secs:
            print('No sectors found matching serial number {}'.format(ser_num))
            return

        out = np.full((len(variable_selection), total_pings), np.nan)
        out_tms = np.full((1, total_pings), np.nan)

        for s_cnt, sec in enumerate(secs):
            counter_times = self.source_dat.return_active_sectors_for_ping_counter(s_cnt, ping_counters)
            counter_times[counter_times < min_time] = 0.0
            counter_times[counter_times > max_time] = 0.0
            counter_idxs = np.array(np.where(counter_times != 0)).ravel()
            if np.any(counter_times):
                time_idx = counter_times[counter_times != 0]
                for v_cnt, dattype in enumerate(variable_selection):
                    dat = self.source_dat.select_array_from_rangeangle(dattype, sec).sel(time=time_idx)
                    out[v_cnt, counter_idxs] = dat
                    if v_cnt == 0:
                        out_tms[v_cnt, counter_idxs] = time_idx
        return out, out_tms

    def return_times_across_sectors(self):
        """
        Return all the times that are within at least one sector in the fqpr dataset.  If a time shows up twice
        (EM2040c will do this, with two sectors and no delay between them), return the array sorted so that the time
        shows up twice in the returned array as well.

        Returns
        -------
        np.array
            1d array of timestamps
        """

        tims = np.concatenate([ra.time.values for ra in self.source_dat.raw_ping])
        tims.sort()
        return tims

    def return_sounding_count(self):
        """
        Return the number of soundings in the processed georeference data.  Requires georeferenced processed data
        in this Fqpr instance

        Returns
        -------
        int
            total number of soundings in the dataset
        """

        if 'x' not in self.source_dat.raw_ping[0]:
            self.logger.error('No xyz data found')
            return None

        totalcount = 0
        for rp in self.source_dat.raw_ping:
            totalcount += np.count_nonzero(~np.isnan(rp.x.values))
        return totalcount

    def return_unique_times_across_sectors(self):
        """
        Return all the unique times that are within at least one sector in the fqpr dataset

        Returns
        -------
        np.array
            1d array of timestamps
        """

        return np.unique(np.concatenate([ra.time.values for ra in self.source_dat.raw_ping]))

    def return_line_dict(self):
        """
        Return all the lines with associated start and stop times for all sectors in the fqpr dataset

        Returns
        -------
        dict
            dictionary of names/start and stop times for all lines, ex: {'0022_20190716_232128_S250.all':
            [1563319288.304, 1563319774.876]}
        """

        return self.source_dat.raw_ping[0].multibeam_files

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
        for ping in self.source_dat.raw_ping:
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
        for ping in self.source_dat.raw_ping:
            maxs = np.append(maxs, float(ping[varname].max()))
        return maxs.max()

    def return_unique_mode(self):
        """
        Finds the unique mode entries in raw_ping Datasets.  If there is more than one unique mode, return them in order
        of most often found.

        Returns
        -------
        np.array
            array of mode settings
        """

        if 'sub_mode' in self.source_dat.raw_ping[0]:
            mode = np.unique(np.concatenate([np.unique(f.sub_mode) for f in self.source_dat.raw_ping]))
        else:
            mode = np.unique(np.concatenate([np.unique(f.mode) for f in self.source_dat.raw_ping]))
        if len(mode) > 1:
            counts = []
            for m in mode:
                counts.append(np.sum([np.count_nonzero(f.mode.where(f.mode == m)) for f in self.source_dat.raw_ping]))
            self.logger.info('Found multiple unique mode entries in the dataset:')
            for idx, cnts in enumerate(counts):
                self.logger.info('{}: {} times'.format(mode[idx], cnts))
        return mode

    def return_rounded_freq(self):
        """
        Returns the frequency rounded to match the freq settings commonly given with sonar manufacturer settings.  If
        you have entries like [270000, 290000, 310000, 330000], it returns [300000].  If its something like [69000, 71000]
        it returns [70000].

        Returns
        -------
        np.array
            array of rounded frequencies
        """

        rounded_freqs = []
        f_idents = self.soundings.frequency_identifier
        lens = np.unique([len(str(id)) for id in f_idents])
        for l in lens:  # all freqs are of the same length (should be true)
            freqs = [f for f in f_idents if len(str(f)) == l]
            digits = -(len(str(freqs[0])) - 1)
            rounded_freqs.extend(list(np.unique([np.around(f, digits) for f in freqs])))
        if len(rounded_freqs) > 1:
            self.logger.info('Found multiple unique rounded frequencies: {}'.format(rounded_freqs))
        return rounded_freqs

    def return_downsampled_navigation(self, sample: float = 0.01, start_time: float = None, end_time: float = None):
        """
        Given sample rate in seconds, downsample the raw navigation to give lat lon points.  Used for plotting lines
        currently.

        Parameters
        ----------
        sample
            time in seconds to downsample
        start_time
            if provided will allow you to only return navigation after this time.  Selects the nearest time value to
            the one provided.
        end_time
            if provided will allow you to only return navigation before this time.  Selects the nearest time value to
            the one provided.

        Returns
        -------
        xr.DataArray
            latitude at the provided sample rate between start and end times if provided
        xr.DataArray
            longitude at the provided sample rate between start and end times if provided
        """

        if self.source_dat.raw_nav is None:
            print('Unable to find raw_nav for {}'.format(self.source_dat.converted_pth))

        rnav = slice_xarray_by_dim(self.source_dat.raw_nav, 'time', start_time=start_time, end_time=end_time)
        first_idx = int(np.abs(rnav.time - (rnav.time[0] + sample)).argmin())
        idxs = np.arange(0, len(rnav.time), first_idx)
        sampl_nav = rnav.isel(time=idxs)

        return sampl_nav.latitude, sampl_nav.longitude


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
