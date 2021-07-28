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
import matplotlib.path as mpl_path

from HSTB.kluster.modules.orientation import distrib_run_build_orientation_vectors
from HSTB.kluster.modules.beampointingvector import distrib_run_build_beam_pointing_vector
from HSTB.kluster.modules.svcorrect import get_sv_files_from_directory, return_supported_casts_from_list, \
    distributed_run_sv_correct, cast_data_from_file
from HSTB.kluster.modules.georeference import distrib_run_georeference, datum_to_wkt, vyperdatum_found
from HSTB.kluster.modules.tpu import distrib_run_calculate_tpu
from HSTB.kluster.xarray_conversion import BatchRead
from HSTB.kluster.modules.visualizations import FqprVisualizations
from HSTB.kluster.modules.export import FqprExport
from HSTB.kluster.xarray_helpers import combine_arrays_to_dataset, compare_and_find_gaps, divide_arrays_by_time_index, \
    interp_across_chunks, reload_zarr_records, slice_xarray_by_dim, stack_nan_array, get_beamwise_interpolation
from HSTB.kluster.backends._zarr import ZarrBackend
from HSTB.kluster.dask_helpers import dask_find_or_start_client, get_number_of_workers
from HSTB.kluster.fqpr_helpers import build_crs, seconds_to_formatted_string
from HSTB.kluster.rotations import return_attitude_rotation_matrix
from HSTB.kluster.logging_conf import return_logger
from HSTB.drivers.sbet import sbets_to_xarray, sbet_fast_read_start_end_time
from HSTB.drivers.PCSio import posfiles_to_xarray
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
        self.navigation_path = ''
        self.navigation = None
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

        self.orientation_time_complete = ''
        self.bpv_time_complete = ''
        self.sv_time_complete = ''
        self.georef_time_complete = ''
        self.tpu_time_complete = ''

        self.backup_fqpr = {}
        self.subset_mintime = 0
        self.subset_maxtime = 0
        self.ping_filter = None  # see return_soundings_in_box

        # plotting module
        self.plot = FqprVisualizations(self)
        # export module
        self.export = FqprExport(self)

        self.logfile = None
        self.logger = None
        self.initialize_log()

    def close(self):
        """
        Must forcibly close the logging handlers to allow the data written to disk to be moved or deleted.
        """
        if self.client is not None:
            if self.client.status in ("running", "connecting"):
                self.client.close()
        if self.logger is not None:
            handlers = self.logger.handlers[:]
            for handler in handlers:
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)
            self.logger = None
        if self.multibeam is not None:
            if self.multibeam.logger is not None:
                handlers = self.multibeam.logger.handlers[:]
                for handler in handlers:
                    handler.flush()
                    handler.close()
                    self.multibeam.logger.removeHandler(handler)
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

    def read_from_source(self):
        """
        Activate rawdat object's appropriate read class
        """

        if self.multibeam is not None:
            self.client = self.multibeam.client  # mbes read is first, pull dask distributed client from it
            if self.multibeam.raw_ping is None:
                self.multibeam.read()
            self.output_folder = self.multibeam.converted_pth
        else:
            self.client = dask_find_or_start_client(address=self.address)
        self.initialize_log()

    def reload_ppnav_records(self, skip_dask: bool = False):
        """
        Reload the zarr datastore for the ppnav record using the navigation_path attribute

        Parameters
        ----------
        skip_dask
            if True, will open zarr datastore without Dask synchronizer object
        """

        self.navigation = reload_zarr_records(self.navigation_path, skip_dask)

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

    def import_sound_velocity_files(self, src: Union[str, list]):
        """
        Load to self.cast_files the file paths to the sv casts of interest.

        Parameters
        ----------
        src
            either a list of files to include or the path to a directory containing sv files (only supporting .svp currently)
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
            for f in svfils:
                data, locs, times, name = cast_data_from_file(f)
                for cnt, dat in enumerate(data):
                    cst_name = 'profile_{}'.format(int(times[cnt]))
                    attrs_name = 'attributes_{}'.format(int(times[cnt]))
                    if cst_name not in self.multibeam.raw_ping[0].attrs:
                        attr_dict[attrs_name] = json.dumps({'location': locs[cnt], 'source': name})
                        cast_dict[cst_name] = json.dumps([list(d) for d in dat.items()])

            self.write_attribute_to_ping_records(cast_dict)
            self.write_attribute_to_ping_records(attr_dict)

            new_cast_names = list(cast_dict.keys())
            applicable_casts = self.return_applicable_casts()
            new_applicable_casts = [nc for nc in new_cast_names if nc in applicable_casts]
            if new_applicable_casts:
                if self.multibeam.raw_ping[0].current_processing_status >= 3:  # have to start over at sound velocity now
                    self.write_attribute_to_ping_records({'current_processing_status': 2})

            self.multibeam.reload_pingrecords(skip_dask=self.client is None)
            self.logger.info('Successfully imported {} new casts'.format(len(cast_dict)))
        else:
            self.logger.warning('Unable to import casts from {}'.format(src))

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

    def return_cast_idx_nearestintime(self, cast_times: list, idx_by_chunk: list, silent: bool = False):
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
        silent
            if True, will not print out messages

        Returns
        -------
        data
            list of lists, each sub-list is [xarray Datarray with times/indices for the chunk, integer index of the cast that
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

        if not silent:
            self.logger.info('nearest-in-time: selecting nearest cast for each {} pings...'.format(kluster_variables.ping_chunk_size))
        return data

    def return_applicable_casts(self, method='nearestintime'):
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
        profnames, casts, cast_times, castlocations = self.multibeam.return_all_profiles()
        for s_cnt, system in enumerate(systems):
            ra = self.multibeam.raw_ping[s_cnt]
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()
            for applicable_index, timestmp, prefixes in system:
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if method == 'nearestintime':
                    cast_chunks = self.return_cast_idx_nearestintime(cast_times, idx_by_chunk, silent=True)
                    final_idxs += [c[1] for c in cast_chunks]
        final_idxs = np.unique(final_idxs).tolist()
        return [profnames[idx] for idx in final_idxs]

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
                self.logger.info('Building induced heave for secondary system in dual head arrangement')

                # lever arms for secondary head to ref pt
                secondary_x_lever = float(self.multibeam.xyzrph[prefixes[0] + '_x'][timestmp])
                secondary_y_lever = float(self.multibeam.xyzrph[prefixes[0] + '_y'][timestmp])
                secondary_z_lever = float(self.multibeam.xyzrph[prefixes[0] + '_z'][timestmp])

                # lever arms for primary head to ref pt
                if prefixes[0] == 'tx_port':
                    prim_prefix = 'tx_stbd'
                else:
                    prim_prefix = 'tx_port'
                primary_x_lever = float(self.multibeam.xyzrph[prim_prefix + '_x'][timestmp])
                primary_y_lever = float(self.multibeam.xyzrph[prim_prefix + '_y'][timestmp])
                primary_z_lever = float(self.multibeam.xyzrph[prim_prefix + '_z'][timestmp])

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

        x_lever = float(self.multibeam.xyzrph[prefixes[0] + '_x'][timestmp])
        y_lever = float(self.multibeam.xyzrph[prefixes[0] + '_y'][timestmp])
        z_lever = float(self.multibeam.xyzrph[prefixes[0] + '_z'][timestmp])
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

        x_base_offset = prefixes[0] + '_x'
        y_base_offset = prefixes[0] + '_y'

        # z included at cast creation, we will apply this in georeference bathy
        # z_base_offset = prefixes[0] + '_z'
        addtl_offsets = []
        for cnt, chnk in enumerate(idx_by_chunk):
            sector_by_beam = ra.txsector_beam[chnk].values
            x_offsets_by_beam = np.zeros(sector_by_beam.shape, dtype=np.float32)
            y_offsets_by_beam = np.zeros(sector_by_beam.shape, dtype=np.float32)
            z_offsets_by_beam = np.zeros(sector_by_beam.shape, dtype=np.float32)

            sector_possibilities = np.unique(sector_by_beam)
            for sector in sector_possibilities:
                x_off_ky = prefixes[0] + '_x_' + str(sector)
                y_off_ky = prefixes[0] + '_y_' + str(sector)
                z_off_ky = prefixes[0] + '_z_' + str(sector)

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

    def _generate_chunks_bpv(self, ra: xr.Dataset, idx_by_chunk: list, timestmp: str, silent: bool = False):
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

        if 'orientation' in self.intermediate_dat[ra.system_identifier]:
            # workflow for data that is not written to disk.  Preference given for data in memory
            tx_rx_futs = [f[0] for f in self.intermediate_dat[ra.system_identifier]['orientation'][timestmp]] * len(idx_by_chunk)
            try:
                tx_rx_data = self.client.map(divide_arrays_by_time_index, tx_rx_futs, [chnk for chnk in idx_by_chunk])
            except:  # client is not setup, run locally
                tx_rx_data = []
                for cnt, fut in enumerate(tx_rx_futs):
                    tx_rx_data.append(divide_arrays_by_time_index(fut, idx_by_chunk[cnt]))
        else:
            # workflow for data that is written to disk
            try:
                tx_rx_data = self.client.scatter([[ra.tx[chnk], ra.rx[chnk]] for chnk in idx_by_chunk])
            except:  # client is not setup, run locally
                tx_rx_data = [[ra.tx[chnk], ra.rx[chnk]] for chnk in idx_by_chunk]

        data_for_workers = []
        for cnt, chnk in enumerate(idx_by_chunk):
            heading = get_beamwise_interpolation(chnk.time + latency, ra.delay[chnk.values], self.multibeam.raw_att.heading)
            try:
                fut_hdng = self.client.scatter(heading)
                fut_bpa = self.client.scatter(ra.beampointingangle[chnk.values])
                fut_tilt = self.client.scatter(ra.tiltangle[chnk.values])
            except:  # client is not setup, run locally
                fut_hdng = heading
                fut_bpa = ra.beampointingangle[chnk.values]
                fut_tilt = ra.tiltangle[chnk.values]
            data_for_workers.append([fut_hdng, fut_bpa, fut_tilt, tx_rx_data[cnt], self.tx_reversed, self.rx_reversed])
        return data_for_workers

    def _generate_chunks_svcorr(self, ra: xr.Dataset, cast_chunks: list, casts: list,
                                prefixes: str, timestmp: str, addtl_offsets: list, silent: bool = False):
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
        silent
            if True, does not print out the log messages

        Returns
        -------
        list
            list of lists, each list contains future objects for distributed_run_sv_correct
        """

        data_for_workers = []
        if 'bpv' in self.intermediate_dat[ra.system_identifier]:
            # workflow for data that is not written to disk.  Preference given for data in memory
            bpv_futs = [f[0] for f in self.intermediate_dat[ra.system_identifier]['bpv'][timestmp]] * len(cast_chunks)
            try:
                bpv_data = self.client.map(divide_arrays_by_time_index, bpv_futs, [d[0] for d in cast_chunks])
            except:  # client is not setup, run locally
                bpv_data = []
                for cnt, fut in enumerate(bpv_futs):
                    bpv_data.append(divide_arrays_by_time_index(fut, cast_chunks[cnt][0]))
        else:
            # workflow for data that is written to disk, break it up according to cast_chunks
            try:
                bpv_data = self.client.scatter([[ra.rel_azimuth[d[0]], ra.corr_pointing_angle[d[0]]] for d in cast_chunks])
            except:  # client is not setup, run locally
                bpv_data = [[ra.rel_azimuth[d[0]], ra.corr_pointing_angle[d[0]]] for d in cast_chunks]

        # this should be the transducer to waterline, positive down
        z_pos = -float(self.multibeam.xyzrph[prefixes[0] + '_z'][timestmp]) + float(self.multibeam.xyzrph['waterline'][timestmp])

        try:
            twtt_data = self.client.scatter([ra.traveltime[d[0]] for d in cast_chunks])
            ss_data = self.client.scatter([ra.soundspeed[d[0]] for d in cast_chunks])
            casts = self.client.scatter(casts)
            addtl_offsets = self.client.scatter([addtl for addtl in addtl_offsets])
        except:  # client is not setup, run locally
            twtt_data = [ra.traveltime[d[0]] for d in cast_chunks]
            ss_data = [ra.soundspeed[d[0]] for d in cast_chunks]

        for cnt, dat in enumerate(cast_chunks):
            data_for_workers.append([casts[dat[1]], bpv_data[cnt], twtt_data[cnt], ss_data[cnt], z_pos, addtl_offsets[cnt]])
        return data_for_workers

    def _generate_chunks_georef(self, ra: xr.Dataset, idx_by_chunk: xr.DataArray,
                                prefixes: str, timestmp: str, z_offset: float, prefer_pp_nav: bool,
                                vdatum_directory: str, silent: bool = False):
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
            self.logger.info('Applying motion latency of {}ms'.format(latency))
        try:
            if ra.input_datum == 'NAD83':
                input_datum = CRS.from_epsg(kluster_variables.epsg_nad83)
            elif ra.input_datum == 'WGS84':
                input_datum = CRS.from_epsg(kluster_variables.epsg_wgs84)
            else:
                self.logger.error('{} not supported.  Only supports WGS84 and NAD83'.format(ra.input_datum))
                raise ValueError('{} not supported.  Only supports WGS84 and NAD83'.format(ra.input_datum))
        except AttributeError:
            if not silent:
                self.logger.warning('No input datum attribute found, assuming WGS84')
            input_datum = CRS.from_epsg(kluster_variables.epsg_wgs84)

        if prefer_pp_nav and isinstance(self.navigation, xr.Dataset):
            if not silent:
                self.logger.info('Using post processed navigation...')
            nav = interp_across_chunks(self.navigation, tx_tstmp_idx + latency, daskclient=self.client)
            lat = nav.latitude
            lon = nav.longitude
            alt = nav.altitude
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

        if self.vert_ref in kluster_variables.ellipse_based_vertical_references:
            alt = self.determine_altitude_corr(alt, self.multibeam.raw_att, tx_tstmp_idx + latency, prefixes, timestmp)
        else:
            hve = self.determine_induced_heave(ra, hve, self.multibeam.raw_att, tx_tstmp_idx + latency, prefixes, timestmp)

        data_for_workers = []
        min_chunk_index = np.min([idx.min() for idx in idx_by_chunk])

        for chnk in idx_by_chunk:
            if 'sv_corr' in self.intermediate_dat[ra.system_identifier]:
                # workflow for data that is not written to disk.  Preference given for data in memory
                sv_data = self.intermediate_dat[ra.system_identifier]['sv_corr'][timestmp][0][0]
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
                fut_lon = self.client.scatter(lon[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                fut_lat = self.client.scatter(lat[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                fut_hdng = self.client.scatter(hdng[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
                fut_hve = self.client.scatter(hve[chnk_vals].assign_coords({'time': chnk.time.time - latency}))
            except:  # client is not setup, run locally
                if alt is None:
                    fut_alt = alt
                else:
                    fut_alt = alt[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_lon = lon[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_lat = lat[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_hdng = hdng[chnk_vals].assign_coords({'time': chnk.time.time - latency})
                fut_hve = hve[chnk_vals].assign_coords({'time': chnk.time.time - latency})
            data_for_workers.append([sv_data, fut_alt, fut_lon, fut_lat, fut_hdng, fut_hve, wline, self.vert_ref,
                                     input_datum, self.horizontal_crs, z_offset, vdatum_directory])
        return data_for_workers

    def _generate_chunks_tpu(self, ra: xr.Dataset, idx_by_chunk: xr.DataArray, timestmp: str, silent: bool = False):
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
        timestmp
            timestamp of the installation parameters instance used
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
            self.logger.info('Applying motion latency of {}ms'.format(latency))

        if 'qualityfactor' not in self.multibeam.raw_ping[0]:
            self.logger.error("_generate_chunks_tpu: sonar uncertainty ('qualityfactor') must exist to calculate uncertainty")
            return None
        if isinstance(self.navigation, xr.Dataset):
            ppnav = interp_across_chunks(self.navigation, tx_tstmp_idx + latency, daskclient=self.client)

        roll = interp_across_chunks(self.multibeam.raw_att['roll'], tx_tstmp_idx + latency, daskclient=self.client)

        first_mbes_file = list(ra.multibeam_files.keys())[0]
        mbes_ext = os.path.splitext(first_mbes_file)[1]
        if mbes_ext in kluster_variables.multibeam_uses_quality_factor:  # for .all files, quality factor is an int representing scaled std dev
            qf_type = 'kongsberg'
        elif mbes_ext in kluster_variables.multibeam_uses_ifremer:  # for .kmall files, quality factor is a percentage of water depth, see IFREMER formula
            qf_type = 'ifremer'
        else:
            raise ValueError('Found multibeam file with {} extension, only {} supported by kluster'.format(mbes_ext, kluster_variables.supported_multibeam))

        data_for_workers = []

        # set the first chunk of the first write to build the tpu sample image, provide a path to the folder to save in
        image_generation = [False] * len(idx_by_chunk)
        if not silent:
            image_generation[0] = os.path.join(self.multibeam.converted_pth, 'ping_' + ra.system_identifier + '.zarr')

        tpu_params = self.multibeam.return_tpu_parameters(timestmp)
        for cnt, chnk in enumerate(idx_by_chunk):
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

                # latency workflow is kind of strange.  We want to get data where the time equals the chunk time.  Which
                #   means we have to apply the latency to the chunk time.  But then we need to remove the latency from the
                #   data time so that it aligns with ping time again for writing to disk.
                if latency:
                    chnk = chnk.assign_coords({'time': chnk.time.time + latency})
                fut_roll = self.client.scatter(roll.where(roll['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
                try:  # pospac uncertainty available
                    fut_npe = self.client.scatter(ppnav.north_position_error.where(ppnav.north_position_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
                    fut_epe = self.client.scatter(ppnav.east_position_error.where(ppnav.east_position_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
                    fut_dpe = self.client.scatter(ppnav.down_position_error.where(ppnav.down_position_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
                    fut_rpe = self.client.scatter(ppnav.roll_error.where(ppnav.roll_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
                    fut_ppe = self.client.scatter(ppnav.pitch_error.where(ppnav.pitch_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
                    fut_hpe = self.client.scatter(ppnav.heading_error.where(ppnav.heading_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency}))
                except:  # rely on static values
                    fut_npe = None
                    fut_epe = None
                    fut_dpe = None
                    fut_rpe = None
                    fut_ppe = None
                    fut_hpe = None
            except:  # client is not setup, run locally
                fut_corr_point = ra.corr_pointing_angle[chnk.values]
                fut_raw_point = raw_point
                fut_acrosstrack = ra.acrosstrack[chnk.values]
                fut_depthoffset = ra.depthoffset[chnk.values]
                fut_soundspeed = ra.soundspeed[chnk.values]
                fut_qualityfactor = ra.qualityfactor[chnk.values]
                fut_datumuncertainty = datum_unc

                if latency:
                    chnk = chnk.assign_coords({'time': chnk.time.time + latency})
                fut_roll = roll.where(roll['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
                try:  # pospac uncertainty available
                    fut_npe = ppnav.north_position_error.where(ppnav.north_position_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
                    fut_epe = ppnav.east_position_error.where(ppnav.east_position_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
                    fut_dpe = ppnav.down_position_error.where(ppnav.down_position_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
                    fut_rpe = ppnav.roll_error.where(ppnav.roll_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
                    fut_ppe = ppnav.pitch_error.where(ppnav.pitch_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
                    fut_hpe = ppnav.heading_error.where(ppnav.heading_error['time'] == chnk.time, drop=True).assign_coords({'time': chnk.time.time - latency})
                except:  # rely on static values
                    fut_npe = None
                    fut_epe = None
                    fut_dpe = None
                    fut_rpe = None
                    fut_ppe = None
                    fut_hpe = None

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
                # check to see if this sector is within the subset time
                if np.logical_and(ra.time <= last_subset_time, ra.time >= first_subset_time).any():
                    # nothing written to disk yet, first run has to include the first time
                    if 'tx' not in list(ra.keys()):
                        if first_subset_time > np.min(ra.time):
                            msg = 'get_orientation_vectors: {}: If your first run of this function uses subset_time, it must include the first ping.'.format(sysid)
                            raise NotImplementedError(msg)
                    # data written already, just make sure we aren't creating a gap
                    else:
                        try:
                            last_written_time = ra.time[np.where(np.isnan(ra.tx.values))[0][0]]
                        except IndexError:  # no NaNs, array is complete so we are all good here
                            continue

                        if first_subset_time > last_written_time:
                            msg = 'get_orientation_vectors: {}: saved arrays must not have a time gap, subset_time must start at the last written time.'.format(sysid)
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
                    print('get_beam_pointing_vectors: in memory workflow')
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
                    print('sv_correct: in memory workflow')
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
            if 'altitude' not in self.multibeam.raw_ping[0] and self.navigation is None:
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
                    print('georef_xyz: in memory workflow')
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

        if self.backup_fqpr == {}:  # this is not a subset operation, overwrite the global min/max values
            if 'x' in self.multibeam.raw_ping[0]:  # if they ran georeference but did not dump the data, it is still a future and inaccessible
                minx = min([np.nanmin(rp.x) for rp in self.multibeam.raw_ping])
                miny = min([np.nanmin(rp.y) for rp in self.multibeam.raw_ping])
                minz = round(np.float64(min([np.nanmin(rp.z) for rp in self.multibeam.raw_ping])), 3)  # cast as f64 to deal with json serializable error in zarr write attributes
                maxx = max([np.nanmax(rp.x) for rp in self.multibeam.raw_ping])
                maxy = max([np.nanmax(rp.y) for rp in self.multibeam.raw_ping])
                maxz = round(np.float64(max([np.nanmax(rp.z) for rp in self.multibeam.raw_ping])), 3)
                newattr = {'min_x': minx, 'min_y': miny, 'min_z': minz, 'max_x': maxx, 'max_y': maxy, 'max_z': maxz}
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
        if isinstance(self.navigation, xr.Dataset) and not overwrite:
            duplicate_navfiles = []
            for new_file in navfiles:
                root, filename = os.path.split(new_file)
                if filename in self.navigation.nav_files:
                    new_file_times = sbet_fast_read_start_end_time(new_file)
                    if self.navigation.nav_files[filename] == new_file_times:
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
        overwrite
            if True, will include files that are already in the navigation dataset as valid
        """

        self.logger.info('****Importing post processed navigation****\n')
        starttime = perf_counter()

        navfiles, errorfiles, logfiles = self._validate_post_processed_navigation(navfiles, errorfiles, logfiles, overwrite)
        if not navfiles:
            print('import_post_processed_navigation: No valid navigation files to import')
            return

        try:
            navdata = sbets_to_xarray(navfiles, smrmsgfiles=errorfiles, logfiles=logfiles, weekstart_year=weekstart_year,
                                      weekstart_week=weekstart_week, override_datum=override_datum, override_grid=override_grid,
                                      override_zone=override_zone, override_ellipsoid=override_ellipsoid)
        except:
            navdata = None
        if not navdata:
            print('import_post_processed_navigation: Unable to read from {}'.format(navfiles))
            return

        # retain only nav records that are within existing ping times
        mintime = min([rp.time.values[0] for rp in self.multibeam.raw_ping])
        maxtime = min([rp.time.values[-1] for rp in self.multibeam.raw_ping])
        navdata = slice_xarray_by_dim(navdata, 'time', start_time=float(mintime) - 2, end_time=float(maxtime) + 2)
        if navdata is None:
            print('import_post_processed_navigation: Unable to find timestamps in SBET that align with the raw navigation.')
            return

        print('Writing {} new post processed navigation records'.format(navdata.time.shape[0]))

        navdata.attrs['reference'] = {'latitude': 'reference point', 'longitude': 'reference point',
                                      'altitude': 'reference point'}
        if self.multibeam.raw_ping[0].current_processing_status >= 4:  # have to start over at georeference now
            self.write_attribute_to_ping_records({'current_processing_status': 3})

        # find gaps that don't line up with existing nav gaps (like time between multibeam files)
        # gaps = compare_and_find_gaps(self.multibeam.raw_ping[0], navdata, max_gap_length=max_gap_length, dimname='time')
        # if gaps.any():
        #     self.logger.info('Found gaps > {} in comparison between post processed navigation and realtime.'.format(max_gap_length))
        #     for gp in gaps:
        #         self.logger.info('mintime: {}, maxtime: {}, gap length {}'.format(gp[0], gp[1], gp[1] - gp[0]))

        navdata_attrs = navdata.attrs
        navdata_times = [navdata.time]
        try:
            navdata = self.client.scatter(navdata)
        except:  # not using dask distributed client
            pass

        outfold, _ = self.write('ppnav', [navdata], time_array=navdata_times, attributes=navdata_attrs)
        self.navigation_path = outfold
        self.reload_ppnav_records()
        self.multibeam.reload_pingrecords(skip_dask=self.client is None)

        endtime = perf_counter()
        self.logger.info('****Importing post processed navigation complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def overwrite_raw_navigation(self, navfiles: list, weekstart_year: int, weekstart_week: int,
                                 max_gap_length: float = 1.0, overwrite: bool = False):
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
        max_gap_length
            maximum allowable gap in the pos file in seconds, excluding gaps found in raw navigation
        overwrite
            if True, will include files that are already in the navigation dataset as valid
        """

        self.logger.info('****Overwriting raw navigation****\n')
        starttime = perf_counter()

        navfiles = self._validate_raw_navigation(navfiles, overwrite)

        try:
            navdata = posfiles_to_xarray(navfiles, weekstart_year=weekstart_year, weekstart_week=weekstart_week)
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

        if self.multibeam.raw_ping[0].current_processing_status >= 4 and not isinstance(self.navigation, xr.Dataset):
            # have to start over at georeference now, if there isn't any postprocessed navigation
            self.write_attribute_to_ping_records({'current_processing_status': 3})
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
            if True, will interpolate attitude/navigation to the ping record and store in the raw_ping datasets.  This
            is not mandatory for processing, but useful for other kluster functions post processing.
        """

        self._validate_get_orientation_vectors(subset_time, dump_data)
        if initial_interp:  # optional step if you want to save interpolated attitude/navigation at ping time to disk
            self.initial_att_nav_interpolation()

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
            ra = self.multibeam.raw_ping[s_cnt]  # raw ping record
            sys_ident = ra.system_identifier
            self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            # when we process, we store the futures in self.intermediate_dat, so we can access it later
            self.initialize_intermediate_data(sys_ident, 'orientation')
            # get the settings we want to use for this sector, controls the amount of data we pass at once
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            # for each installation parameters record...
            for applicable_index, timestmp, prefixes in system:
                self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                self.generate_starter_orientation_vectors(prefixes, timestmp)
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'orientation', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask)
                else:
                    self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))

            if dump_data:
                del self.intermediate_dat[sys_ident]['orientation']
        # after each full processing step, reload the raw_ping datasets to get the new metadata
        self.multibeam.reload_pingrecords(skip_dask=skip_dask)
        if self.subset_mintime and self.subset_maxtime:
            self.subset_by_time(self.subset_mintime, self.subset_maxtime)

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
        self.logger.info('****Building beam specific pointing vectors****\n')
        starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'bpv')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                self.generate_starter_orientation_vectors(prefixes, timestmp)
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'bpv', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask)
                else:
                    self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))
            if dump_data:
                del self.intermediate_dat[sys_ident]['bpv']

        self.multibeam.reload_pingrecords(skip_dask=skip_dask)
        if self.subset_mintime and self.subset_maxtime:
            self.subset_by_time(self.subset_mintime, self.subset_maxtime)

        endtime = perf_counter()
        self.logger.info('****Beam Pointing Vector generation complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def sv_correct(self, add_cast_files: Union[str, list] = None, subset_time: list = None, dump_data: bool = True):
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
            if True dump the futures to the multibeam datastore.  Set this to false for an entirely in memory
            workflow
        """

        self._validate_sv_correct(subset_time, dump_data)
        self.logger.info('****Correcting for sound velocity****\n')
        starttime = perf_counter()

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        if add_cast_files:
            self.import_sound_velocity_files(add_cast_files)

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'sv_corr')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'sv_corr', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask)
                else:
                    self.logger.info('No pings found for {}-{}'.format(ra.system_identifier, timestmp))
            if dump_data:
                del self.intermediate_dat[sys_ident]['sv_corr']

        self.multibeam.reload_pingrecords(skip_dask=skip_dask)
        if self.subset_mintime and self.subset_maxtime:
            self.subset_by_time(self.subset_mintime, self.subset_maxtime)

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
        self.logger.info('****Georeferencing sound velocity corrected beam offsets****\n')
        starttime = perf_counter()
        self.write_attribute_to_ping_records({'xyzrph': self.multibeam.xyzrph})

        self.logger.info('Using pyproj CRS: {}'.format(self.horizontal_crs.to_string()))

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'georef')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'georef', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask,
                                                 prefer_pp_nav=prefer_pp_nav, vdatum_directory=vdatum_directory)
                else:
                    self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))
            if dump_data:
                del self.intermediate_dat[sys_ident]['georef']

        self.multibeam.reload_pingrecords(skip_dask=skip_dask)
        self._overwrite_georef_stats()

        if self.subset_mintime and self.subset_maxtime:
            self.subset_by_time(self.subset_mintime, self.subset_maxtime)

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
        self.logger.info('****Calculating total uncertainty****\n')
        starttime = perf_counter()
        self.write_attribute_to_ping_records({'xyzrph': self.multibeam.xyzrph})

        skip_dask = False
        if self.client is None:  # small datasets benefit from just running it without dask distributed
            skip_dask = True

        systems = self.multibeam.return_system_time_indexed_array(subset_time=subset_time)
        for s_cnt, system in enumerate(systems):
            ra = self.multibeam.raw_ping[s_cnt]
            sys_ident = ra.system_identifier
            self.logger.info('Operating on system serial number = {}'.format(sys_ident))
            self.initialize_intermediate_data(sys_ident, 'tpu')
            pings_per_chunk, max_chunks_at_a_time = self.get_cluster_params()

            for applicable_index, timestmp, prefixes in system:
                self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                self.generate_starter_orientation_vectors(prefixes, timestmp)  # have to include this to know if rx is reversed to reverse raw beam angles
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)
                if len(idx_by_chunk[0]):  # if there are pings in this system that align with this installation parameter record
                    self._submit_data_to_cluster(ra, 'tpu', idx_by_chunk, max_chunks_at_a_time,
                                                 timestmp, prefixes, dump_data=dump_data, skip_dask=skip_dask)
                else:
                    self.logger.info('No pings found for {}-{}'.format(sys_ident, timestmp))
        self.multibeam.reload_pingrecords(skip_dask=skip_dask)
        if self.subset_mintime and self.subset_maxtime:
            self.subset_by_time(self.subset_mintime, self.subset_maxtime)

        endtime = perf_counter()
        self.logger.info('****Calculating total uncertainty complete: {}****\n'.format(seconds_to_formatted_string(int(endtime - starttime))))

    def export_pings_to_file(self, output_directory: str = None, file_format: str = 'csv', csv_delimiter=' ',
                             filter_by_detection: bool = True, z_pos_down: bool = True, export_by_identifiers: bool = True):
        """
        Uses the output of georef_along_across_depth to build sounding exports.  Currently you can export to csv or las
        file formats, see file_format argument.

        If you export to las and want to retain rejected soundings under the noise classification, set
        filter_by_detection to False.

        Filters using the detectioninfo variable if present in multibeam and filter_by_detection is set.

        Will generate an xyz file for each sector in multibeam.  Results in one xyz file for each freq/sector id/serial
        number combination.

        entwine export will build las first, and then entwine from las

        Parameters
        ----------
        output_directory
            optional, destination directory for the xyz exports, otherwise will auto export next to converted data
        file_format
            optional, destination file format, default is csv file, options include ['csv', 'las', 'entwine']
        csv_delimiter
            optional, if you choose file_format=csv, this will control the delimiter
        filter_by_detection
            optional, if True will only write soundings that are not rejected
        z_pos_down
            if True, will export soundings with z positive down (this is the native Kluster convention)
        export_by_identifiers
            if True, will generate separate files for each combination of serial number/sector/frequency

        Returns
        -------
        list
            list of written file paths
        """
        written_files = self.export.export_pings_to_file(output_directory=output_directory, file_format=file_format,
                                                         csv_delimiter=csv_delimiter, filter_by_detection=filter_by_detection,
                                                         z_pos_down=z_pos_down, export_by_identifiers=export_by_identifiers)
        return written_files

    def _submit_data_to_cluster(self, rawping: xr.Dataset, mode: str, idx_by_chunk: list, max_chunks_at_a_time: int,
                                timestmp: str, prefixes: str, dump_data: bool = True, skip_dask: bool = False,
                                prefer_pp_nav: bool = True, vdatum_directory: str = None):
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
        """

        # clear out the intermediate data just in case there is old data there
        sys_ident = rawping.system_identifier
        self.intermediate_dat[sys_ident][mode][timestmp] = []
        tot_runs = int(np.ceil(len(idx_by_chunk) / max_chunks_at_a_time))
        for rn in range(tot_runs):
            starttime = perf_counter()
            start_r = rn * max_chunks_at_a_time
            end_r = min(start_r + max_chunks_at_a_time, len(idx_by_chunk))  # clamp for last run
            idx_by_chunk_subset = idx_by_chunk[start_r:end_r].copy()

            if mode == 'orientation':
                kluster_function = distrib_run_build_orientation_vectors
                chunk_function = self._generate_chunks_orientation
                comp_time = 'orientation_time_complete'
                chunkargs = [rawping, idx_by_chunk_subset, timestmp, prefixes]
            elif mode == 'bpv':
                kluster_function = distrib_run_build_beam_pointing_vector
                chunk_function = self._generate_chunks_bpv
                comp_time = 'bpv_time_complete'
                chunkargs = [rawping, idx_by_chunk_subset, timestmp]
            elif mode == 'sv_corr':
                kluster_function = distributed_run_sv_correct
                chunk_function = self._generate_chunks_svcorr
                comp_time = 'sv_time_complete'
                profnames, casts, cast_times, castlocations = self.multibeam.return_all_profiles()
                cast_chunks = self.return_cast_idx_nearestintime(cast_times, idx_by_chunk_subset, silent=(rn != 0))
                addtl_offsets = self.return_additional_xyz_offsets(rawping, prefixes, timestmp, idx_by_chunk_subset)
                chunkargs = [rawping, cast_chunks, casts, prefixes, timestmp, addtl_offsets]
            elif mode == 'georef':
                kluster_function = distrib_run_georeference
                chunk_function = self._generate_chunks_georef
                comp_time = 'georef_time_complete'
                z_offset = float(self.multibeam.xyzrph[prefixes[0] + '_z'][timestmp])
                chunkargs = [rawping, idx_by_chunk_subset, prefixes, timestmp, z_offset, prefer_pp_nav, vdatum_directory]
            elif mode == 'tpu':
                kluster_function = distrib_run_calculate_tpu
                chunk_function = self._generate_chunks_tpu
                comp_time = 'tpu_time_complete'
                chunkargs = [rawping, idx_by_chunk_subset, timestmp]
            else:
                self.logger.error('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')
                raise ValueError('Mode must be one of ["orientation", "bpv", "sv_corr", "georef", "tpu"]')

            data_for_workers = chunk_function(*chunkargs, silent=(rn != 0))
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
                             {'svmode': 'nearest in time', '_sound_velocity_correct_complete': self.sv_time_complete,
                              'current_processing_status': 3,
                              'reference': {'alongtrack': 'reference', 'acrosstrack': 'reference',
                                            'depthoffset': 'transmitter'},
                              'units': {'alongtrack': 'meters (+ forward)', 'acrosstrack': 'meters (+ starboard)',
                                        'depthoffset': 'meters (+ down)'}}]
        elif mode == 'georef':
            crs = self.horizontal_crs.to_epsg()
            if crs is None:  # gets here if there is no valid EPSG for this transformation
                crs = self.horizontal_crs.to_string()
            if self.vert_ref == 'NOAA MLLW':
                vertcrs = datum_to_wkt('mllw', self.multibeam.raw_ping[0].min_lon, self.multibeam.raw_ping[0].min_lat,
                                       self.multibeam.raw_ping[0].max_lon, self.multibeam.raw_ping[0].max_lat)
            elif self.vert_ref == 'NOAA MHW':
                vertcrs = datum_to_wkt('mhw', self.multibeam.raw_ping[0].min_lon, self.multibeam.raw_ping[0].min_lat,
                                       self.multibeam.raw_ping[0].max_lon, self.multibeam.raw_ping[0].max_lat)
            else:
                vertcrs = 'Unknown'
            mode_settings = ['georef', ['x', 'y', 'z', 'corr_heave', 'corr_altitude', 'datum_uncertainty', 'processing_status'],
                             'georeferenced soundings data',
                             {'horizontal_crs': crs, 'vertical_reference': self.vert_ref,
                              'vertical_crs': vertcrs,
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

        if 'x' not in self.multibeam.raw_ping[0]:
            print('return_xyz: unable to find georeferenced xyz for {}'.format(self.multibeam.converted_pth))
            return None

        if 'tvu' not in self.multibeam.raw_ping[0] and include_unc:
            print('return_xyz: unable to find uncertainty for {}'.format(self.multibeam.converted_pth))
            return None

        if start_time is not None or start_time is not None:
            self.subset_by_time(start_time, end_time)

        data = []
        xyz = [[], [], [], []]
        for rp in self.multibeam.raw_ping:
            x_idx, x_stck = stack_nan_array(rp['x'], stack_dims=('time', 'beam'))
            y_idx, y_stck = stack_nan_array(rp['y'], stack_dims=('time', 'beam'))
            z_idx, z_stck = stack_nan_array(rp['z'], stack_dims=('time', 'beam'))

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

        if start_time is not None or start_time is not None:
            self.restore_subset()

        return data

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
        We save the line start/end time as an attribute within each raw_ping record.  Use this method to pull out
        just the data that is within the mintime/maxtime range (inclusive mintime, exclusive maxtime).  The class will
        then only have access to data within that time period.

        To return to the full original dataset, use restore_subset

        Parameters
        ----------
        mintime
            minimum time of the subset, if not provided and maxtime is, use the minimum time of the datasets
        maxtime
            maximum time of the subset, if not provided and mintime is, use the maximum time of the datasets
        """

        if mintime is None and maxtime is not None:
            mintime = np.min([rp.time.values[0] for rp in self.multibeam.raw_ping])
        if maxtime is None and mintime is not None:
            maxtime = np.max([rp.time.values[-1] for rp in self.multibeam.raw_ping])
        if mintime is None and maxtime is None:
            raise ValueError('subset_by_time: either mintime or maxtime must be provided to subset by time')

        if self.backup_fqpr != {}:
            self.restore_subset()
        self.subset_mintime = mintime
        self.subset_maxtime = maxtime
        self.backup_fqpr['raw_ping'] = [ping.copy() for ping in self.multibeam.raw_ping]
        self.backup_fqpr['raw_att'] = self.multibeam.raw_att.copy()

        slice_raw_ping = []
        for ra in self.multibeam.raw_ping:
            slice_ra = slice_xarray_by_dim(ra, dimname='time', start_time=mintime, end_time=maxtime)
            slice_raw_ping.append(slice_ra)
        self.multibeam.raw_ping = slice_raw_ping
        self.multibeam.raw_att = slice_xarray_by_dim(self.multibeam.raw_att, dimname='time', start_time=mintime, end_time=maxtime)
        if isinstance(self.navigation, xr.Dataset):  # if self.navigation is a dataset, make a backup
            self.backup_fqpr['ppnav'] = self.navigation.copy()
            self.navigation = slice_xarray_by_dim(self.navigation, dimname='time', start_time=mintime, end_time=maxtime)

    def restore_subset(self):
        """
        Restores the original data if subset_by_time has been run.
        """

        if self.backup_fqpr != {}:
            self.multibeam.raw_ping = self.backup_fqpr['raw_ping']
            self.multibeam.raw_att = self.backup_fqpr['raw_att']
            if 'ppnav' in self.backup_fqpr:
                self.navigation = self.backup_fqpr['ppnav']
            self.backup_fqpr = {}
            self.subset_maxtime = 0
            self.subset_mintime = 0
        else:
            self.logger.error('restore_subset: no subset found to restore from')
            raise ValueError('restore_subset: no subset found to restore from')

    def subset_variables(self, variable_selection: list, ping_times: Union[np.array, float, tuple] = None, skip_subset_by_time: bool = False):
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
            time to select the dataset by, can either be an array of times (will use the min/max of the array to subset),
            a float for a single time, or a tuple of (min time, max time).  If None, will use the min/max of the dataset
        skip_subset_by_time
            if True, will not run the subset by time method

        Returns
        -------
        xr.Dataset
            Dataset with the
        """

        if not skip_subset_by_time:
            if ping_times is None:
                ping_times = (np.min([rp.time.values[0] for rp in self.multibeam.raw_ping]),
                              np.max([rp.time.values[-1] for rp in self.multibeam.raw_ping]))

            if isinstance(ping_times, float):
                min_time, max_time = ping_times, ping_times
            elif isinstance(ping_times, tuple):
                min_time, max_time = ping_times
            else:
                min_time, max_time = float(np.min(ping_times)), float(np.max(ping_times))

            self.subset_by_time(min_time, max_time)

        dataset_variables = {}
        maxbeams = 0
        times = np.concatenate([rp.time.values for rp in self.multibeam.raw_ping]).flatten()
        systems = np.concatenate([[rp.system_identifier] * rp.time.shape[0] for rp in self.multibeam.raw_ping]).flatten().astype(np.int32)
        for var in variable_selection:
            if self.multibeam.raw_ping[0][var].ndim == 2:
                if self.multibeam.raw_ping[0][var].dims == ('time', 'beam'):
                    dataset_variables[var] = (['time', 'beam'], np.concatenate([rp[var] for rp in self.multibeam.raw_ping]))
                    newmaxbeams = self.multibeam.raw_ping[0][var].shape[1]
                    if maxbeams and maxbeams != newmaxbeams:
                        raise ValueError('Found multiple max beam number values for the different ping datasets, {} and {}, beam shapes must match'.format(maxbeams, newmaxbeams))
                    else:
                        maxbeams = newmaxbeams
                else:
                    raise ValueError('Only time and beam dimensions are suppoted, found {} for {}'.format(self.multibeam.raw_ping[0][var].dims, var))
            elif self.multibeam.raw_ping[0][var].ndim == 1:
                if self.multibeam.raw_ping[0][var].dims == ('time',):
                    dataset_variables[var] = (['time'], np.concatenate([rp[var] for rp in self.multibeam.raw_ping]))
                else:
                    raise ValueError('Only time dimension is suppoted, found {} for {}'.format(self.multibeam.raw_ping[0][var].dims, var))
            else:
                raise ValueError('Only 2 and 1 dimension variables are supported, {}} is {} dim'.format(var, self.multibeam.raw_ping[0][var].ndim))

        dataset_variables['system_identifier'] = (['time'], systems)
        if maxbeams:  # when variables are a mix of time time/beam dimensions
            coords = {'time': times, 'beam': np.arange(maxbeams)}
        else:  # when variables are just time dimension
            coords = {'time': times}
        dset = xr.Dataset(dataset_variables, coords)
        dset = dset.sortby('time')

        if not skip_subset_by_time:
            self.restore_subset()

        return dset

    def return_line_dict(self):
        """
        Return all the lines with associated start and stop times for all sectors in the fqpr dataset

        Returns
        -------
        dict
            dictionary of names/start and stop times for all lines, ex: {'0022_20190716_232128_S250.all':
            [1563319288.304, 1563319774.876]}
        """

        return self.multibeam.raw_ping[0].multibeam_files

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

        if 'sub_mode' in self.multibeam.raw_ping[0]:
            mode = np.unique(np.concatenate([np.unique(f.sub_mode) for f in self.multibeam.raw_ping]))
        else:
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
        for ln, ln_times in mbeslines.items():
            # first line time bounds sometimes does not cover the first few pings
            ln_times[0] = ln_times[0] - 2
            # same with last line, except extend the last time a bit
            ln_times[1] = ln_times[1] + 2
            applicable_idx = np.logical_and(times >= ln_times[0], times <= ln_times[1])
            lines[applicable_idx] = ln
        return lines

    def _soundings_by_poly(self, polygon: np.ndarray):
        """
        Return soundings and sounding attributes that are within the box formed by the provided coordinates.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon, (x, y) in Fqpr CRS

        Returns
        -------
        list
            list of 1d numpy arrays for the x coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the y coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the z coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the tvu value of the soundings in the box
        list
            list of 1d numpy arrays for the rejected flag of the soundings in the box
        list
            list of 1d numpy arrays for the time of the soundings in the box
        list
            list of 1d numpy arrays for the beam number of the soundings in the box
        """

        x = []
        y = []
        z = []
        tvu = []
        rejected = []
        pointtime = []
        beam = []
        self.ping_filter = []
        polypath = mpl_path.Path(polygon)
        for rp in self.multibeam.raw_ping:
            if 'z' not in self.multibeam.raw_ping[0]:
                continue
            filt = polypath.contains_points(np.c_[rp.x.values.ravel(), rp.y.values.ravel()])
            self.ping_filter.append(filt)
            if filt.any():
                xval = rp.x.values.ravel()[filt]
                if xval.any():
                    x.append(xval)
                    y.append(rp.y.values.ravel()[filt])
                    z.append(rp.z.values.ravel()[filt])
                    tvu.append(rp.tvu.values.ravel()[filt])
                    rejected.append(rp.detectioninfo.values.ravel()[filt])
                    # have to get time for each beam to then make the filter work
                    pointtime.append((rp.time.values[:, np.newaxis] * np.ones_like(rp.x)).ravel()[filt])
                    beam.append((rp.beam.values[np.newaxis, :] * np.ones_like(rp.x)).ravel()[filt])
        return x, y, z, tvu, rejected, pointtime, beam

    def _swaths_by_poly(self, polygon: np.ndarray):
        """
        Return soundings and sounding attributes that are a part of swaths within the box formed by the provided
        coordinates.  Only returns complete swaths.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon, (longitude, latitude) in geographic coords

        Returns
        -------
        list
            list of 1d numpy arrays for the acrosstrack coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the y coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the z coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the tvu value of the soundings in the box
        list
            list of 1d numpy arrays for the rejected flag of the soundings in the box
        list
            list of 1d numpy arrays for the time of the soundings in the box
        list
            list of 1d numpy arrays for the beam number of the soundings in the box
        """

        x = []
        y = []
        z = []
        tvu = []
        rejected = []
        pointtime = []
        beam = []
        self.ping_filter = []
        nv = self.multibeam.raw_ping[0]
        polypath = mpl_path.Path(polygon)
        filt = polypath.contains_points(np.vstack([nv.longitude.values, nv.latitude.values]))
        time_sel = nv.time.where(filt, drop=True).values

        if time_sel.any():
            # if selecting swaths of multiple lines, there will be time gaps
            time_gaps = np.where(np.diff(time_sel) > 1)[0]
            if time_gaps.any():
                time_segments = []
                strt = 0
                for gp in time_gaps:
                    time_segments.append(time_sel[strt:gp + 1])
                    strt = gp + 1
                time_segments.append(time_sel[strt:])
            else:
                time_segments = [time_sel]

            for timeseg in time_segments:
                seg_filter = []
                mintime, maxtime = timeseg.min(), timeseg.max()
                for rp in self.multibeam.raw_ping:
                    ping_filter = np.logical_and(rp.time >= mintime, rp.time <= maxtime)
                    seg_filter.append([mintime, maxtime, ping_filter])
                    if ping_filter.any():
                        pings = rp.where(ping_filter, drop=True)
                        x.append(pings.acrosstrack.values.ravel())
                        y.append(pings.alongtrack.values.ravel())
                        z.append(pings.z.values.ravel())
                        tvu.append(pings.tvu.values.ravel())
                        rejected.append(pings.detectioninfo.values.ravel())
                        # have to get time for each beam to then make the filter work
                        pointtime.append((pings.time.values[:, np.newaxis] * np.ones_like(pings.x)).ravel())
                        beam.append((pings.beam.values[np.newaxis, :] * np.ones_like(pings.x)).ravel())
                self.ping_filter.append(seg_filter)
        return x, y, z, tvu, rejected, pointtime, beam

    def return_soundings_in_polygon(self, polygon: np.ndarray, geographic: bool = True,
                                    full_swath: bool = False):
        """
        Using provided coordinates (in either horizontal_crs projected or geographic coordinates), return the soundings
        and sounding attributes for all soundings within the coordinates.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon,  (longitude, latitude) in degrees
        geographic
            If True, the coordinates provided are geographic (latitude/longitude)
        full_swath
            If True, only returns the full swaths whose navigation is within the provided box

        Returns
        -------
        list
            list of 1d numpy arrays for the x coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the y coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the z coordinate of the soundings in the box
        list
            list of 1d numpy arrays for the tvu value of the soundings in the box
        list
            list of 1d numpy arrays for the rejected flag of the soundings in the box
        list
            list of 1d numpy arrays for the time of the soundings in the box
        list
            list of 1d numpy arrays for the beam number of the soundings in the box
        """

        if 'horizontal_crs' not in self.multibeam.raw_ping[0].attrs or 'z' not in self.multibeam.raw_ping[0].variables.keys():
            raise ValueError('Georeferencing has not been run yet, you must georeference before you can get soundings')
        if full_swath and not geographic:
            raise NotImplementedError('full swath mode can only be used in geographic mode')

        if not full_swath:
            if geographic:
                trans = Transformer.from_crs(CRS.from_epsg(kluster_variables.epsg_wgs84),
                                             CRS.from_epsg(self.multibeam.raw_ping[0].horizontal_crs), always_xy=True)
                polyx, polyy = trans.transform(polygon[:, 0], polygon[:, 1])
                polygon = np.c_[polyx, polyy]
            x, y, z, tvu, rejected, pointtime, beam = self._soundings_by_poly(polygon)
        else:
            x, y, z, tvu, rejected, pointtime, beam = self._swaths_by_poly(polygon)

        if len(x) > 1:
            x = np.concatenate(x)
            y = np.concatenate(y)
            z = np.concatenate(z)
            tvu = np.concatenate(tvu)
            rejected = np.concatenate(rejected)
            pointtime = np.concatenate(pointtime)
            beam = np.concatenate(beam)
        elif len(x) == 1:
            x = x[0]
            y = y[0]
            z = z[0]
            tvu = tvu[0]
            rejected = rejected[0]
            pointtime = pointtime[0]
            beam = beam[0]
        else:
            x = None
            y = None
            z = None
            tvu = None
            rejected = None
            pointtime = None
            beam = None
        return x, y, z, tvu, rejected, pointtime, beam

    def set_variable_by_filter(self, var_name: str = 'detectioninfo', newval: Union[int, str, float] = 2):
        if self.ping_filter is None:
            print('No soundings selected to set a variable.')
            return
        if not isinstance(self.ping_filter[0], np.ndarray):  # must be swaths_by_box, not supported
            raise NotImplementedError('Have not built the selecting by filter for swaths_by_box yet')
        for cnt, rp in enumerate(self.multibeam.raw_ping):
            filt = self.ping_filter[cnt]
            var_vals = rp[var_name].values.ravel()
            var_vals[filt] = newval
            var_vals.reshape(rp[var_name].shape)
            # still need to write to disk....

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

            dashboard['last_run'][ra.system_identifier] = {'_conversion_complete': '', '_compute_orientation_complete': '',
                                                           '_compute_beam_vectors_complete': '', '_sound_velocity_correct_complete': '',
                                                           '_georeference_soundings_complete': '', '_total_uncertainty_complete': ''}
            for ky in list(dashboard['last_run'][ra.system_identifier].keys()):
                if ky in ra.attrs:
                    dashboard['last_run'][ra.system_identifier][ky] = ra.attrs[ky]
        return dashboard

    def return_next_action(self, new_vertical_reference: str = None, new_coordinate_system: CRS = None, new_offsets: bool = False,
                           new_angles: bool = False, new_tpu: bool = False, new_waterline: bool = False, full_reprocess: bool = False):
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
        new_waterline
            True if a new waterline value has been set, requires processing starting at sound velocity correction
        full_reprocess
            True if you want to trigger a full reprocessing of this instance
        """

        min_status = self.multibeam.raw_ping[0].current_processing_status
        args = [self]
        kwargs = {}
        if full_reprocess:
            min_status = 1

        new_diff_vertref = False
        new_diff_coordinate = False
        default_use_epsg = False
        default_use_coord = True
        default_epsg = None
        default_coord_system = kluster_variables.default_coordinate_system
        default_vert_ref = kluster_variables.default_vertical_reference
        if new_coordinate_system:
            try:
                new_epsg = new_coordinate_system.to_epsg()
            except:
                raise ValueError('return_next_action: Unable to convert new coordinate system to epsg: {}'.format(new_coordinate_system))
            if 'horizontal_crs' in self.multibeam.raw_ping[0].attrs:
                try:
                    existing_epsg = int(self.multibeam.raw_ping[0].attrs['horizontal_crs'])
                except:
                    raise ValueError('return_next_action: Unable to convert current coordinate system to epsg: {}'.format(self.horizontal_crs))
            else:
                existing_epsg = 1  # georeference has not been run yet, so we use this default value that always fails the next check
            if new_epsg != existing_epsg and new_epsg and existing_epsg:
                new_diff_coordinate = True
                default_use_epsg = True
                default_use_coord = False
                default_epsg = new_epsg
                default_coord_system = None
        if new_vertical_reference:
            if new_vertical_reference != self.vert_ref:
                new_diff_vertref = True
                default_vert_ref = new_vertical_reference

        if min_status < 5 or new_diff_coordinate or new_diff_vertref or new_offsets or new_angles or new_waterline or new_tpu:
            kwargs['run_orientation'] = False
            kwargs['run_beam_vec'] = False
            kwargs['run_svcorr'] = False
            kwargs['run_georef'] = False
            kwargs['run_tpu'] = True
        if min_status < 4 or new_diff_coordinate or new_diff_vertref or new_offsets or new_angles or new_waterline:
            kwargs['run_georef'] = True
            kwargs['use_epsg'] = default_use_epsg
            kwargs['use_coord'] = default_use_coord
            kwargs['epsg'] = default_epsg
            kwargs['coord_system'] = default_coord_system
            kwargs['vert_ref'] = default_vert_ref
        if min_status < 3 or new_offsets or new_angles or new_waterline:
            kwargs['run_svcorr'] = True
            kwargs['add_cast_files'] = []
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
