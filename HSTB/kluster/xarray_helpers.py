import os
import numpy as np
import json
import xarray as xr
import time

import zarr

from dask.distributed import wait, Client, progress
from xarray.core.combine import _infer_concat_order_from_positions, _nested_combine
from typing import Union, Callable, Tuple, Any


def retry_call(callabl: Callable, args=None, kwargs=None, exceptions: Tuple[Any, ...] = (),
               retries: int = 50, wait: float = 0.5):
    """
    Make several attempts to invoke the callable. If one of the given exceptions
    is raised, wait the given period of time and retry up to the given number of
    retries.
    """

    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}

    for attempt in range(1, retries+1):
        try:
            return callabl(*args, **kwargs)
        except exceptions:
            if attempt < retries:
                time.sleep(wait)
            else:
                print('WARNING: attempted {} retries at {} second interval, unable to complete process'.format(retries, wait))
                return callabl(*args, **kwargs)


def search_not_sorted(base: np.ndarray, search_array: np.ndarray):
    """
    Implement a way to find the indices where search_array is within base when base is not sorted.  I've found that
    simply sorting and running searchsorted is the fastest way to handle this.  I even tested against iterating through
    the array with Numba, and it was close, but this was faster.

    Parameters
    ----------
    base
        the array you want to search against
    search_array
        the array to search with

    Returns
    -------
    np.ndarray
        indices of where search_array is within base
    """

    if not set(search_array).issubset(set(base)):
        raise ValueError('search must be a subset of master')

    sorti = np.argsort(base)
    # get indices in sorted version
    tmpind = np.searchsorted(base, search_array, sorter=sorti)
    final_inds = sorti[tmpind]

    return final_inds


class ZarrWrite:
    """
    Class for handling writing xarray data to Zarr.  I started off using the xarray to_zarr functions, but I found
    that they could not handle changes in size/distributed writes very well, so I came up with my own.  This class
    currently supports:

    |  1. writing to zarr from dask map function, see distrib_zarr_write
    |  2. writing data with a larger expand dimension than currently exists in zarr (think new data has more beams)
    |  3. writing new variable to existing zarr data store (must match existing data dimensions)
    |  4. appending to existing zarr by filling in the last zarr chunk with data and then writing new chunks (only last
                chunk of zarr array is allowed to not be of length equal to zarr chunk size)
    """
    def __init__(self, zarr_path: str, desired_chunk_shape: dict = None, append_dim: str = 'time', expand_dim: str = 'beam',
                 float_no_data_value: float = np.nan, int_no_data_value: int = 999):
        """
        Initialize zarr write class

        Parameters
        ----------
        zarr_path
            str, full file path to where you want the zarr data store to be written to
        desired_chunk_shape
            dict, keys are dimension names, vals are the chunk size for that dimension
        append_dim
            str, dimension name that you are appending to (generally time)
        expand_dim
            str, dimension name that you need to expand if necessary (generally beam)
        float_no_data_value
            float, no data value for variables that are dtype float
        int_no_data_value
            int, no data value for variables that are dtype int
        """

        self.zarr_path = zarr_path
        self.desired_chunk_shape = desired_chunk_shape
        self.append_dim = append_dim
        self.expand_dim = expand_dim
        self.float_no_data_value = float_no_data_value
        self.int_no_data_value = int_no_data_value

        self.rootgroup = None
        self.zarr_array_names = []

        self.merge_chunks = False

        if self.zarr_path is not None:
            self.open()
        else:
            print('Warning: starting zarr_write with an empty rootgroup, writing to disk not supported')
            self.rootgroup = zarr.group()

    def open(self):
        """
        Open the zarr data store, will create a new one if it does not exist.  Get all the existing array names.
        """

        sync = zarr.ProcessSynchronizer(self.zarr_path + '.sync')
        self.rootgroup = zarr.open(self.zarr_path, mode='a', synchronizer=sync)
        self.get_array_names()

    def get_array_names(self):
        """
        Get all the existing array names as a list of strings and set self.zarr_array_names with that list
        """

        self.zarr_array_names = [t for t in self.rootgroup.array_keys()]

    def _attributes_only_unique_profile(self, attrs: dict):
        """
        Given attribute dict from dataset (attrs) retain only unique sound velocity profiles

        Parameters
        ----------
        attrs
            input attribution from converted dataset

        Returns
        -------
        dict
            attrs with only unique sv profiles
        """

        try:
            new_profs = [x for x in attrs.keys() if x[0:7] == 'profile']
            curr_profs = [x for x in self.rootgroup.attrs.keys() if x[0:7] == 'profile']
            current_vals = [self.rootgroup.attrs[p] for p in curr_profs]
            for prof in new_profs:
                val = attrs[prof]
                if val in current_vals:
                    try:  # find matching attribute key if exists
                        tstmp = prof.split('_')[1]
                        matching_attr = 'attributes_{}'.format(tstmp)
                        if matching_attr in attrs:
                            attrs.pop(matching_attr)
                    except:
                        pass
                    attrs.pop(prof)
        except:
            pass
        return attrs

    def _attributes_only_unique_runtime(self, attrs: dict):
        """
        Given attribute dict from dataset (attrs) retain only unique runtime settings dicts

        Parameters
        ----------
        attrs
            input attribution from converted dataset

        Returns
        -------
        dict
            attrs with only unique runtime settings dicts
        """
        try:
            new_settings = [x for x in attrs.keys() if x[0:7] == 'runtime']
            curr_settings = [x for x in self.rootgroup.attrs.keys() if x[0:7] == 'runtime']
            current_vals = [self.rootgroup.attrs[p] for p in curr_settings]
            for sett in new_settings:
                val = attrs[sett]
                if val in current_vals:
                    attrs.pop(sett)
        except:
            pass
        return attrs

    def _attributes_only_unique_settings(self, attrs: dict):
        """
        Given attribute dict from dataset (attrs) retain only unique settings dicts

        Parameters
        ----------
        attrs
            input attribution from converted dataset

        Returns
        -------
        dict
            attrs with only unique settings dicts
        """
        try:
            new_settings = [x for x in attrs.keys() if x[0:7] == 'install']
            curr_settings = [x for x in self.rootgroup.attrs.keys() if x[0:7] == 'install']
            current_vals = [self.rootgroup.attrs[p] for p in curr_settings]
            for sett in new_settings:
                val = attrs[sett]
                if val in current_vals:
                    attrs.pop(sett)
        except:
            pass
        return attrs

    def _attributes_only_unique_xyzrph(self, attrs: dict):
        """
        Given attribute dict from dataset (attrs) retain only unique xyzrph constructs

        xyzrph is constructed in processing as the translated settings

        Parameters
        ----------
        attrs
            input attribution from converted dataset

        Returns
        -------
        dict
            attrs with only unique xyzrph timestamped records
        """

        try:
            new_xyz = attrs['xyzrph']
            new_tstmps = list(new_xyz[list(new_xyz.keys())[0]].keys())
            curr_xyz = self.rootgroup.attrs['xyzrph']
            curr_tstmps = list(curr_xyz[list(curr_xyz.keys())[0]].keys())

            curr_vals = []
            for tstmp in curr_tstmps:
                curr_vals.append([curr_xyz[x][tstmp] for x in curr_xyz])
            for tstmp in new_tstmps:
                new_val = [new_xyz[x][tstmp] for x in new_xyz]
                if new_val in curr_vals:
                    for ky in new_xyz:
                        new_xyz[ky].pop(tstmp)
            if not new_xyz[list(new_xyz.keys())[0]]:
                attrs.pop('xyzrph')
        except:
            pass
        return attrs

    def write_attributes(self, attrs: dict):
        """
        Write out attributes to the zarr data store

        Parameters
        ----------
        attrs
            attributes associated with this zarr rootgroup
        """
        if attrs is not None:
            attrs = self._attributes_only_unique_profile(attrs)
            attrs = self._attributes_only_unique_settings(attrs)
            attrs = self._attributes_only_unique_runtime(attrs)
            attrs = self._attributes_only_unique_xyzrph(attrs)
            _my_xarr_to_zarr_writeattributes(self.rootgroup, attrs)

    def _check_fix_rootgroup_expand_dim(self, xarr: xr.Dataset):
        """
        Check if this xarr is greater in the exand dimension (probably beam) than the existing rootgroup beam array.  If it is,
        we'll need to expand the rootgroup to cover the max beams of the xarr.

        Parameters
        ----------
        xarr
            data that we are trying to write to rootgroup

        Returns
        -------
        bool
            if True expand the rootgroup expand dimension
        """

        if (self.expand_dim in self.rootgroup) and (self.expand_dim in xarr):
            last_expand = self.rootgroup[self.expand_dim].size
            if last_expand < xarr[self.expand_dim].shape[0]:
                return True  # last expand dim isn't long enough, need to fix the chunk
            else:
                return False  # there is a chunk there, but it is of size equal to desired
        else:
            return False  # first write

    def _get_arr_nodatavalue(self, arr_dtype: np.dtype):
        """
        Given the dtype of the array, determine the appropriate no data value.  Fall back on empty string if not int or
        float.

        Parameters
        ----------
        arr_dtype
            numpy dtype, dtype of input array

        Returns
        -------
        Union[str, int, float]
            no data value, one of [self.float_no_data_value, self.int_no_data_value, '']
        """

        isfloat = np.issubdtype(arr_dtype, np.floating)
        if isfloat:
            nodata = self.float_no_data_value
        else:
            isint = np.issubdtype(arr_dtype, np.integer)
            if isint:
                nodata = self.int_no_data_value
            else:
                nodata = ''
        return nodata

    def fix_rootgroup_expand_dim(self, xarr: xr.Dataset):
        """
        Once we've determined that the xarr Dataset expand_dim is greater than the rootgroup expand_dim, expand the
        rootgroup expand_dim to match the xarr.  Fill the empty space with the appropriate no data value.

        Parameters
        ----------
        xarr
            data that we are trying to write to rootgroup
        """

        curr_expand_dim_size = self.rootgroup[self.expand_dim].size
        for var in self.zarr_array_names:
            newdat = None
            newshp = None
            if var == self.expand_dim:
                newdat = np.arange(xarr[self.expand_dim].shape[0])
                newshp = xarr[self.expand_dim].shape
            elif self.rootgroup[var].ndim >= 2:
                if self.rootgroup[var].shape[1] == curr_expand_dim_size:  # you found an array with a beam dimension
                    nodata_value = self._get_arr_nodatavalue(self.rootgroup[var].dtype)
                    newdat = self._inflate_expand_dim(self.rootgroup[var], xarr[self.expand_dim].shape[0], nodata_value)
                    newshp = list(self.rootgroup[var].shape)
                    newshp[1] = xarr[self.expand_dim].shape[0]
                    newshp = tuple(newshp)
            if newdat is not None:
                self.rootgroup[var].resize(newshp)
                self.rootgroup[var][:] = newdat

    def _inflate_expand_dim(self, input_arr: Union[np.array, zarr.Array, xr.DataArray],
                            expand_dim_size: int, nodata: Union[int, float, str]):
        """
        Take in the rootgroup and expand the beam dimension to the expand_dim_size, filling the empty space with the
        nodata value.

        Parameters
        ----------
        input_arr
            numpy like object, includes zarr.core.Array and xarray.core.dataarray.DataArray, data that we want to expand to match the expand dim size
        expand_dim_size
            size of the expand_dim (probably beam) that we need
        nodata
            one of [self.float_no_data_value, self.int_no_data_value, '']

        Returns
        -------
        Union[np.array, zarr.Array, xr.DataArray]
            input_arr with expanded beam dimension
        """

        if input_arr.ndim == 3:
            appended_data = np.full((input_arr.shape[0], expand_dim_size - input_arr.shape[1], input_arr.shape[2]), nodata)
        else:
            appended_data = np.full((input_arr.shape[0], expand_dim_size - input_arr.shape[1]), nodata)
        new_arr = np.concatenate((input_arr, appended_data), axis=1)
        return new_arr

    def correct_rootgroup_dims(self, xarr: xr.Dataset):
        """
        Correct for when the input xarray Dataset shape is greater than the rootgroup shape.  Most likely this is when
        the input xarray Dataset is larger in the beam dimension than the existing rootgroup arrays.

        Parameters
        ----------
        xarr
            xarray Dataset, data that we are trying to write to rootgroup
        """

        if self._check_fix_rootgroup_expand_dim(xarr):
            self.fix_rootgroup_expand_dim(xarr)

    def _write_adjust_max_beams(self, startingshp: tuple):
        """
        The first write in appending to an existing zarr data store will resize that zarr array to the expected size
        of the new data + old data.  We provide the expected shape when we write, but that shape is naive to the
        beam dimension of the existing data.  Here we correct that.

        Parameters
        ----------
        startingshp
            tuple, expected shape of the appended data + existing data

        Returns
        -------
        tuple
            same shape, but with beam dimension corrected for the existing data
        """

        if len(startingshp) >= 2:
            current_max_beams = self.rootgroup['beam'].shape[0]
            startingshp = list(startingshp)
            startingshp[1] = current_max_beams
            startingshp = tuple(startingshp)
        return startingshp

    def _write_determine_shape(self, var: str, dims_of_arrays: dict, finalsize: int = None):
        """
        Given the size information and dimension names for the given variable, determine the axis to append to and the
        expected shape for the rootgroup array.

        Parameters
        ----------
        var
            name of the array, ex: 'beampointingangle'
        dims_of_arrays
            where keys are array names and values list of dims/shape.  Example: 'beampointingangle': [['time', 'sector', 'beam'], (5000, 3, 400)]
        finalsize
            will resize zarr to the expected final size after all writes have been performed. If None, will generate
            desired shape but not be used

        Returns
        -------
        int
            index of the time dimension
        int
            length of the time dimension for the input xarray Dataset
        tuple
            desired shape for the rootgroup array, might be modified later for total beams if necessary. if finalsize
            is None (the case when this is not the first write in a set of distributed writes) this is still returned but not used.
        """

        if var in ['beam', 'xyz']:
            # only need time dim info for time dependent variables
            timaxis = None
            timlength = None
            startingshp = dims_of_arrays[var][1]
        else:
            # want to get the length of the time dimension, so you know which dim to append to
            timaxis = dims_of_arrays[var][0].index(self.append_dim)
            timlength = dims_of_arrays[var][1][timaxis]
            startingshp = tuple(
                finalsize if dims_of_arrays[var][1].index(x) == timaxis else x for x in dims_of_arrays[var][1])
        return timaxis, timlength, startingshp

    def _write_existing_rootgroup(self, xarr: xr.Dataset, data_loc_copy: Union[list, np.ndarray], var_name: str, dims_of_arrays: dict,
                                  chunksize: tuple, timlength: int, timaxis: int, startingshp: tuple):
        """
        A slightly different operation than _write_new_dataset_rootgroup.  To write to an existing rootgroup array,
        we use the data_loc as an index and create a new zarr array from the xarray Dataarray.  The data_loc is only
        used if the var is a time based array.

        Parameters
        ----------
        xarr
            data to write to zarr
        data_loc_copy
            either [start time index, end time index] for xarr, ex: [0,1000] if xarr time dimension is 1000 long,
            or np.array([4,5,6,7,1,2...]) for when data might not be continuous and we need to use a boolean mask
        var_name
            variable name
        dims_of_arrays
            where keys are array names and values list of dims/shape.  Example: 'beampointingangle': [['time', 'sector', 'beam'], (5000, 3, 400)]
        chunksize
            chunk shape used to create the zarr array
        timlength
            Length of the time dimension for the input xarray Dataset
        timaxis
            index of the time dimension
        startingshp
            desired shape for the rootgroup array, might be modified later for total beams if necessary.  if finalsize
            is None (the case when this is not the first write in a set of distributed writes) this is still returned but not used.
        """

        # array to be written
        xarr_data = xarr[var_name].values
        if startingshp is not None:
            startingshp = self._write_adjust_max_beams(startingshp)
            self.rootgroup[var_name].resize(startingshp)

        if isinstance(data_loc_copy, list):  # [start index, end index]
            # the last write will often be less than the block size.  This is allowed in the zarr store, but we
            #    need to correct the index for it.
            if timlength != data_loc_copy[1] - data_loc_copy[0]:
                data_loc_copy[1] = data_loc_copy[0] + timlength

            # location for new data, assume constant chunksize (as we are doing this outside of this function)
            chunk_time_range = slice(data_loc_copy[0], data_loc_copy[1])
            # use the chunk_time_range for writes unless this variable is a non-time dim array (beam for example)
            chunk_idx = tuple(
                chunk_time_range if dims_of_arrays[var_name][1].index(i) == timaxis else slice(0, i) for i in
                dims_of_arrays[var_name][1])
            self.rootgroup[var_name][chunk_idx] = zarr.array(xarr_data, shape=dims_of_arrays[var_name][1], chunks=chunksize)
        else:  # np.array([4,5,6,1,2,3,8,9...]), indices of the new data, might not be sorted
            sorted_order = data_loc_copy.argsort()
            xarr_data = xarr_data[sorted_order]
            data_loc_copy = data_loc_copy[sorted_order]
            zarr_mask = np.zeros_like(self.rootgroup[var_name], dtype=bool)
            zarr_mask[data_loc_copy] = True
            # seems to require me to ravel first, examples only show setting with integer, not sure what is going on here
            self.rootgroup[var_name].set_mask_selection(zarr_mask, xarr_data.ravel())

    def _write_new_dataset_rootgroup(self, xarr: xr.Dataset, var_name: str, dims_of_arrays: dict, chunksize: tuple,
                                     startingshp: tuple):
        """
        Create a new rootgroup array from the input xarray Dataarray.  Use startingshp to resize the array to the
        expected shape of the array after ALL writes.  This must be the first write if there are multiple distributed
        writes.

        Parameters
        ----------
        xarr
            data to write to zarr
        var_name
            variable name
        dims_of_arrays
            where keys are array names and values list of dims/shape.  Example: 'beampointingangle': [['time', 'sector', 'beam'], (5000, 3, 400)]
        chunksize
            chunk shape used to create the zarr array
        startingshp
            desired shape for the rootgroup array, might be modified later for total beams if necessary. if finalsize
            is None (the case when this is not the first write in a set of distributed writes) this is still returned but not used.
        """

        sync = None
        if self.zarr_path:
            sync = zarr.ProcessSynchronizer(self.zarr_path + '.sync')
        newarr = self.rootgroup.create_dataset(var_name, shape=dims_of_arrays[var_name][1], chunks=chunksize,
                                               dtype=xarr[var_name].dtype, synchronizer=sync,
                                               fill_value=self._get_arr_nodatavalue(xarr[var_name].dtype))
        newarr[:] = xarr[var_name].values
        newarr.resize(startingshp)

    def write_to_zarr(self, xarr: xr.Dataset, attrs: dict, dataloc: Union[list, np.ndarray], finalsize: int = None):
        """
        Take the input xarray Dataset and write each variable as arrays in a zarr rootgroup.  Write the attributes out
        to the rootgroup as well.  Dataloc determines the index the incoming data is written to.  A new write might
        have a dataloc of [0,100], if the time dim of the xarray Dataset was 100 long.  A write to an existing zarr
        rootgroup might have a dataloc of [300,400] if we were appending to it.

        Parameters
        ----------
        xarr
            xarray Dataset, data to write to zarr
        attrs
            attributes we want written to zarr rootgroup
        dataloc
            either [start time index, end time index] for xarr, ex: [0,1000] if xarr time dimension is 1000 long,
            or np.array([4,5,6,7,1,2...]) for when data might not be continuous and we need to use a boolean mask
        finalsize
            optional, int, if provided will resize zarr to the expected final size after all writes have been
            performed.  (We need to resize the zarr for that expected size before writing)

        Returns
        -------
        str
            path to zarr data store
        """

        if finalsize is not None:
            self.correct_rootgroup_dims(xarr)
        self.get_array_names()
        dims_of_arrays = _my_xarr_to_zarr_build_arraydimensions(xarr)
        self.write_attributes(attrs)

        for var in dims_of_arrays:
            already_written = var in self.zarr_array_names
            if var in ['beam', 'xyz'] and already_written:
                # no append_dim (usually time) component to these arrays
                # You should only have to write this once, if beam dim expands, correct_rootgroup_dims handles it
                continue

            timaxis, timlength, startingshp = self._write_determine_shape(var, dims_of_arrays, finalsize)
            chunksize = self.desired_chunk_shape[var]
            if isinstance(dataloc, list):
                data_loc_copy = dataloc.copy()
            else:
                data_loc_copy = dataloc

            # shape is extended on append.  chunks will always be equal to shape, as each run of this function will be
            #     done on one chunk of data by one worker
            if var in self.zarr_array_names:
                if finalsize is not None:  # appending data, first write contains the final shape of the data
                    self._write_existing_rootgroup(xarr, data_loc_copy, var, dims_of_arrays, chunksize, timlength,
                                                   timaxis, startingshp)
                else:
                    self._write_existing_rootgroup(xarr, data_loc_copy, var, dims_of_arrays, chunksize, timlength,
                                                   timaxis, None)
            else:
                self._write_new_dataset_rootgroup(xarr, var, dims_of_arrays, chunksize, startingshp)

            # _ARRAY_DIMENSIONS is used by xarray for connecting dimensions with zarr arrays
            self.rootgroup[var].attrs['_ARRAY_DIMENSIONS'] = dims_of_arrays[var][0]
        return self.zarr_path


def zarr_write(zarr_path: str, xarr: xr.Dataset, attrs: dict, desired_chunk_shape: dict, dataloc: Union[list, np.ndarray],
               append_dim: str = 'time', finalsize: int = None):
    """
    Convenience function for writing with ZarrWrite

    Parameters
    ----------
    zarr_path
        path to zarr data store
    xarr
        xarray Dataset, data to write to zarr
    attrs
        attributes we want written to zarr rootgroup
    desired_chunk_shape
        variable name: chunk size as tuple, for each variable in the input xarr
    dataloc
        either [start time index, end time index] for xarr, ex: [0,1000] if xarr time dimension is 1000 long,
        or np.array([4,5,6,7,1,2...]) for when data might not be continuous and we need to use a boolean mask
    append_dim
        dimension name that you are appending to (generally time)
    finalsize
        optional, if provided will resize zarr to the expected final size after all writes have been performed.  (We
        need to resize the zarr for that expected size before writing)

    Returns
    -------
    str
        path to zarr data store
    """

    zw = ZarrWrite(zarr_path, desired_chunk_shape, append_dim=append_dim)
    zarr_path = retry_call(zw.write_to_zarr, (xarr, attrs, dataloc), {'finalsize': finalsize}, exceptions=(PermissionError,))
    return zarr_path


def distrib_zarr_write(zarr_path: str, xarrays: list, attributes: dict, chunk_sizes: dict, data_locs: list,
                       finalsize: int, client: Client, append_dim: str = 'time',
                       write_in_parallel: bool = False, skip_dask: bool = False,
                       show_progress: bool = True):
    """
    A function for using the ZarrWrite class to write data to disk.  xarr and attrs are written to the datastore at
    zarr_path.  We use the function (and not the class directly) in Dask when we map it across all the workers.  Dask
    serializes data when mapping, so passing classes causes issues.

    Currently we wait between each write.  This seems to deal with the occassional permissions error that pops up
    when letting dask write in parallel.  Maybe we aren't using the sync object correctly?  Needs more testing.

    Parameters
    ----------
    zarr_path
        path to zarr data store
    xarrays
        list of xarray Datasets, data to write to zarr
    attributes
        attributes we want written to zarr rootgroup
    chunk_sizes
        variable name: chunk size as tuple, for each variable in the input xarr
    data_locs
        list of lists, either [start time index, end time index] for xarr, ex: [0,1000] if xarr time dimension is 1000 long,
        or np.array([4,5,6,7,1,2...]) for when data might not be continuous and we need to use a boolean mask
    finalsize
        the final size of the time dimension of the written data, we resize the zarr to this size on the first write
    client
        dask.distributed.Client, the client we are submitting the tasks to
    append_dim
        dimension name that you are appending to (generally time)
    write_in_parallel
        if True and skip_dask is False, will use the first write to set up the zarr datastore, and all subsequent
        writes will be done in parallel.  We have this as optional, because on government machines (I suspect due to
        the antivirus scans) this can occassionally fail, where parallel writes generate permission denied errors.
        For now we leave this default off.
    skip_dask
        if True, skip the dask client mapping as you are not running dask distributed
    show_progress
        If true, uses dask.distributed.progress.  Disabled for GUI, as it generates too much text

    Returns
    -------
    list
        futures objects containing the path to the zarr rootgroup.
    """

    if skip_dask:  # run the zarr write process without submitting to a dask client
        for cnt, arr in enumerate(xarrays):
            if cnt == 0:
                futs = [zarr_write(zarr_path, arr, attributes, chunk_sizes, data_locs[cnt],
                        append_dim=append_dim, finalsize=finalsize)]
            else:
                futs.append([zarr_write(zarr_path, xarrays[cnt], None, chunk_sizes, data_locs[cnt],
                             append_dim=append_dim)])
    else:
        futs = [client.submit(zarr_write, zarr_path, xarrays[0], attributes, chunk_sizes, data_locs[0],
                              append_dim=append_dim, finalsize=finalsize)]
        if show_progress:
            progress(futs, multi=False)
        wait(futs)
        if len(xarrays) > 1:
            for i in range(len(xarrays) - 1):
                futs.append(client.submit(zarr_write, zarr_path, xarrays[i + 1], None, chunk_sizes,
                                          data_locs[i + 1], append_dim=append_dim))
                if not write_in_parallel:  # wait on each future, write one data chunk at a time
                    wait(futs)
            if write_in_parallel:  # don't wait on the futures until you append all of them
                if show_progress:
                    progress(futs, multi=False)
                wait(futs)
    return futs


def zarr_write_attributes(zarr_path: str, attrs: dict):
    """
    Convenience function for writing attribution to kluster zarr datastore.  We do many things with incoming attribution
    in terms of the rules for appending/replacing (see ZarrWrite.write_attributes) so this exists to write using
    those rules

    Parameters
    ----------
    zarr_path
        path to zarr data store
    attrs
        attributes we want written to zarr rootgroup
    """
    zw = ZarrWrite(zarr_path)
    zw.write_attributes(attrs)


def my_open_mfdataset(paths: list, chnks: dict = None, concat_dim: str = 'time', compat: str = 'no_conflicts',
                      data_vars: str = 'all', coords: str = 'different', join: str = 'outer'):
    """
    Trying to address the limitations of the existing xr.open_mfdataset function.  This is my modification using
    the existing function and tweaking to resolve the issues i've found.

    (see https://github.com/pydata/xarray/blob/master/xarray/backends/api.py)

    | Current issues with open_mfdataset (1/8/2020):
    | 1. open_mfdataset only uses the attrs from the first nc file
    | 2. open_mfdataset will not run with parallel=True or with the distributed.LocalCluster running
    | 3. open_mfdataset infers time order from position.  (I could just sort outside of the function, but i kinda
    | like it this way anyway.  Also a re-indexing would probably resolve this.)

    Only resolved item = 1 so far.  See https://github.com/pydata/xarray/issues/3684

    Parameters
    ----------
    paths
        list of file paths to existing netcdf stores
    chnks
        if provided, used to load dataset into chunks
    concat_dim
        dimension to concatenate along
    compat
        String indicating how to compare non-concatenated variables of the same name for potential conflicts
    data_vars
        which variables will be concatenated
    coords
        which coordinate variables will be concatenated
    join
        String indicating how to combine differing indexes (excluding dim) in objects

    Returns
    -------
    xr.Dataset
        attributes, variables, dimensions of combined netCDF files.  Returns dask arrays, compute to access local numpy array.
    """

    # ensure file paths are valid
    pth_chk = np.all([os.path.exists(x) for x in paths])
    if not pth_chk:
        raise ValueError('Check paths supplied to function.  Some/all files do not exist.')

    # sort by filename index, e.g. rangeangle_0.nc, rangeangle_1.nc, rangeangle_2.nc, etc.
    idxs = [int(os.path.splitext(os.path.split(x)[1])[0].split('_')[1]) for x in paths]
    sortorder = sorted(range(len(idxs)), key=lambda k: idxs[k])

    # sort_paths are the paths in sorted order by the filename index
    sort_paths = [paths[p] for p in sortorder]

    # build out the arugments for the nested combine
    if isinstance(concat_dim, (str, xr.DataArray)) or concat_dim is None:
        concat_dim = [concat_dim]
    combined_ids_paths = _infer_concat_order_from_positions(sort_paths)
    ids, paths = (list(combined_ids_paths.keys()), list(combined_ids_paths.values()))
    if chnks is None:
        chnks = {}

    datasets = [xr.open_dataset(p, engine='netcdf4', chunks=chnks, lock=None, autoclose=None) for p in paths]

    combined = _nested_combine(datasets, concat_dims=concat_dim, compat=compat, data_vars=data_vars,
                               coords=coords, ids=ids, join=join)
    combined.attrs = combine_xr_attributes(datasets)
    return combined


def xarr_to_netcdf(xarr: xr.Dataset, pth: str, fname: str, attrs: dict = None, idx: int = None):
    """
    Takes in an xarray Dataset and pushes it to netcdf.
    For use with the output from combine_xarrs and/or _sequential_to_xarray

    Parameters
    ----------
    xarr
        Dataset to save
    pth
        Path to the folder to contain the written netcdf file
    fname
        base file name for the netcdf file
    attrs
        optional attribution to store in the netcdf store
    idx
        optional file name index

    Returns
    -------
    str
        path to the netcdf file
    """

    if idx is not None:
        finalpth = os.path.join(pth, os.path.splitext(fname)[0] + '_{}.nc'.format(idx))
    else:
        finalpth = os.path.join(pth, fname)

    if attrs is not None:
        xarr.attrs = attrs

    xarr.to_netcdf(path=finalpth, format='NETCDF4', engine='netcdf4')
    return finalpth


def xarr_to_zarr(xarr: xr.Dataset, outputpth: str, attrs: dict = None):
    """
    Takes in an xarray Dataset and pushes it to zarr store.

    Must be run once to generate new store.  Successive runs append, see mode flag

    Parameters
    ----------
    xarr
        xarray Dataset to write to zarr
    outputpth
        path to the zarr rootgroup folder to write
    attrs
        optional attribution to write to zarr

    Returns
    -------
    str
        path to the zarr group
    """

    # grpname = str(datetime.now().strftime('%H%M%S%f'))
    if attrs is not None:
        xarr.attrs = attrs

    if not os.path.exists(outputpth):
        xarr.to_zarr(outputpth, mode='w-', compute=False)
    else:
        sync = zarr.ProcessSynchronizer(outputpth + '.sync')
        xarr.to_zarr(outputpth, mode='a', synchronizer=sync, compute=False, append_dim='time')

    return outputpth


def _my_xarr_to_zarr_build_arraydimensions(xarr: xr.Dataset):
    """
    Build out dimensions/shape of arrays in xarray into a dict so that we can use it with the zarr writer.

    Parameters
    ----------
    xarr
        xarray Dataset, one chunk of the final range_angle/attitude/navigation xarray Dataset we are writing

    Returns
    -------
    dict
        where keys are array names and values list of dims/shape.  Example: 'beampointingangle': [['time', 'sector', 'beam'], (5000, 3, 400)]
    """

    dims_of_arrays = {}
    arrays_in_xarr = list(xarr.variables.keys())
    for arr in arrays_in_xarr:
        if xarr[arr].dims and xarr[arr].shape:  # only return arrays that have dimensions/shape
            dims_of_arrays[arr] = [xarr[arr].dims, xarr[arr].shape, xarr[arr].chunks]
    return dims_of_arrays


def _my_xarr_to_zarr_writeattributes(rootgroup: zarr.hierarchy.Group, attrs: dict):
    """
    Take the attributes generated with combine_xr_attributes and write them to the final datastore

    Parameters
    ----------
    rootgroup
        zarr datastore group for one of range_angle/attitude/navigation
    attrs
        dictionary of combined attributes from xarray datasets, None if no attributes exist
    """

    if attrs is not None:
        for att in attrs:

            # ndarray is not json serializable
            if isinstance(attrs[att], np.ndarray):
                attrs[att] = attrs[att].tolist()

            if att not in rootgroup.attrs:
                try:
                    rootgroup.attrs[att] = attrs[att]
                except:
                    print('Unable to assign {} to key {}'.format(attrs[att], att))
            else:
                if isinstance(attrs[att], list):
                    try:
                        for sub_att in attrs[att]:
                            if sub_att not in rootgroup.attrs[att]:
                                rootgroup.attrs[att].append(sub_att)
                    except:
                        print('Unable to append to {} with value {}'.format(att, attrs[att]))
                elif isinstance(attrs[att], dict) and att != 'status_lookup':
                    # have to load update and save to update dict attributes for some reason
                    try:
                        dat = rootgroup.attrs[att]
                        dat.update(attrs[att])
                        rootgroup.attrs[att] = dat
                    except:
                        print('Unable to update {} with value {}'.format(att, dat))
                else:
                    try:
                        rootgroup.attrs[att] = attrs[att]
                    except:
                        print('Unable to replace {} with value {}'.format(att, dat))


def resize_zarr(zarrpth: str, finaltimelength: int = None):
    """
    Takes in the path to a zarr group and resizes the time dimension according to the provided finaltimelength

    Parameters
    ----------
    zarrpth
        path to a zarr group on the filesystem
    finaltimelength
        new length for the time dimension
    """

    # the last write will often be less than the block size.  This is allowed in the zarr store, but we
    #    need to correct the index for it.
    rootgroup = zarr.open(zarrpth, mode='r+')
    if finaltimelength is None:
        finaltimelength = np.count_nonzero(~np.isnan(rootgroup['time']))
    for var in rootgroup.arrays():
        if var[0] not in ['beam', 'sector', 'xyz']:
            varname = var[0]
            dims = rootgroup[varname].attrs['_ARRAY_DIMENSIONS']
            time_index = dims.index('time')
            new_shape = list(rootgroup[varname].shape)
            new_shape[time_index] = finaltimelength
            rootgroup[varname].resize(tuple(new_shape))


def combine_xr_attributes(datasets: list):
    """
    xarray open_mfdataset only retains the attributes of the first dataset.  We store profiles and installation
    parameters in datasets as they arise.  We need to combine the attributes across all datasets for our final
    dataset.

    Designed for the ping record, with filenames, survey identifiers, etc.  Will also accept min/max stats from navigation

    Parameters
    ----------
    datasets
        list of xarray.Datasets representing range_angle for our workflow.  Can be any dataset object though.  We are
        just storing attributes in the range_angle one so far.

    Returns
    -------
    dict
        contains all unique attributes across all dataset, will append unique prim/secondary serial numbers and ignore duplicate settings entries
    """

    finaldict = {}

    buffered_settings = ''
    buffered_runtime_settings = ''

    fnames = []
    survey_nums = []
    cast_dump = {}
    attrs_dump = {}

    if type(datasets) != list:
        datasets = [datasets]

    try:
        all_attrs = [datasets[x].attrs for x in range(len(datasets))]
    except AttributeError:
        all_attrs = datasets

    for d in all_attrs:
        for k, v in d.items():
            # settings gets special treatment for a few reasons...
            if k[0:7] == 'install':
                vals = json.loads(v)  # stored as a json string for serialization reasons
                try:
                    fname = vals.pop('raw_file_name')
                    if fname not in fnames:
                        # keep .all file names for their own attribute
                        fnames.append(fname)
                except KeyError:  # key exists in .all file but not in .kmall
                    pass
                    # print('{}: Unable to find "raw_file_name" key'.format(k))
                try:
                    sname = vals.pop('survey_identifier')
                    if sname not in survey_nums:
                        # keep survey identifiers for their own attribute
                        survey_nums.append(sname)
                except KeyError:  # key exists in .all file but not in .kmall
                    pass
                    # print('{}: Unable to find "raw_file_name" key'.format(k))
                vals = json.dumps(vals)

                # This is for the duplicate entries, just ignore these
                if vals == buffered_settings:
                    pass
                # this is for the first settings entry
                elif not buffered_settings:
                    buffered_settings = vals
                    finaldict[k] = vals
                # all unique entries after the first are saved
                else:
                    finaldict[k] = vals
            elif k[0:7] == 'runtime':
                vals = json.loads(v)  # stored as a json string for serialization reasons
                # we pop out these three keys because they are unique across all runtime params.  You end up with like
                # fourty records, all with only them being unique.  Not useful.  Rather only store important differences.
                try:
                    counter = vals.pop('Counter')
                except KeyError:  # key exists in .all file but not in .kmall
                    counter = ''
                    # print('{}: Unable to find "raw_file_name" key'.format(k))
                try:
                    mindepth = vals.pop('MinDepth')
                except KeyError:  # key exists in .all file but not in .kmall
                    mindepth = ''
                    # print('{}: Unable to find "MinDepth" key'.format(k))
                try:
                    maxdepth = vals.pop('MaxDepth')
                except KeyError:  # key exists in .all file but not in .kmall
                    maxdepth = ''
                    # print('{}: Unable to find "MaxDepth" key'.format(k))
                vals = json.dumps(vals)

                # This is for the duplicate entries, just ignore these
                if vals == buffered_runtime_settings:
                    pass
                # this is for the first settings entry
                elif not buffered_runtime_settings:
                    buffered_runtime_settings = vals
                    vals = json.loads(v)
                    vals['Counter'] = counter
                    finaldict[k] = json.dumps(vals)
                # all unique entries after the first are saved
                else:
                    vals = json.loads(v)
                    vals['Counter'] = counter
                    finaldict[k] = json.dumps(vals)

            # save all unique serial numbers
            elif k in ['system_serial_number', 'secondary_system_serial_number'] and k in list(finaldict.keys()):
                if finaldict[k] != v:
                    finaldict[k] = np.array(finaldict[k])
                    finaldict[k] = np.append(finaldict[k], v)
            # save all casts, use this to only pull the first unique cast later (casts are being saved in each line
            #   with a time stamp of when they appear in the data.  Earliest time represents the closest to the actual
            #   cast time).
            elif k[0:7] == 'profile':
                cast_dump[k] = v
            elif k[0:11] == 'attributes_':
                attrs_dump[k] = v
            elif k[0:3] == 'min':
                if k in finaldict:
                    finaldict[k] = np.min([v, finaldict[k]])
                else:
                    finaldict[k] = v
            elif k[0:3] == 'max':
                if k in finaldict:
                    finaldict[k] = np.max([v, finaldict[k]])
                else:
                    finaldict[k] = v
            elif k not in finaldict:
                finaldict[k] = v

    if fnames:
        finaldict['system_serial_number'] = finaldict['system_serial_number'].tolist()
        finaldict['secondary_system_serial_number'] = finaldict['secondary_system_serial_number'].tolist()
        finaldict['multibeam_files'] = list(np.unique(sorted(fnames)))
    if survey_nums:
        finaldict['survey_number'] = list(np.unique(survey_nums))
    if cast_dump:
        sorted_kys = sorted(cast_dump)
        unique_casts = []
        for k in sorted_kys:
            tstmp = k.split('_')[1]
            matching_attr = 'attributes_{}'.format(tstmp)
            if cast_dump[k] not in unique_casts:
                unique_casts.append(cast_dump[k])
                finaldict[k] = cast_dump[k]
                finaldict[matching_attr] = attrs_dump[matching_attr]
    return finaldict


def divide_arrays_by_time_index(arrs: list, idx: np.array):
    """
    Simple method for indexing a list of arrays

    Parameters
    ----------
    arrs
        list of xarray DataArray or Dataset objects
    idx
        numpy array index

    Returns
    -------
    list
        list of indexed xarray DataArray or Dataset objects
    """

    dat = []
    for ar in arrs:
        dat.append(ar[idx])
    return dat


def combine_arrays_to_dataset(arrs: list, arrnames: list):
    """
    Build a dataset from a list of Xarray DataArrays, given a list of names for each array.

    Parameters
    ----------
    arrs
        xarray DataArrays you want in your xarray Dataset
    arrnames
        string name identifiers for each array, will be the variable name in the Dataset

    Returns
    -------
    xr.Dataset
        xarray Dataset with variables equal to the provided arrays
    """

    if len(arrs) != len(arrnames):
        raise ValueError('Please provide an equal number of names to dataarrays')
    dat = {a: arrs[arrnames.index(a)] for a in arrnames}
    dset = xr.Dataset(dat)
    return dset


def my_xarr_add_attribute(attrs: dict, outputpth: str):
    """
    Add the provided attrs dict to the existing attribution of the zarr instance at outputpth

    Parameters
    ----------
    attrs
        dictionary of combined attributes from xarray datasets, None if no attributes exist
    outputpth
        path to zarr group to either be created or append to

    Returns
    -------
    str
        path to the final zarr group
    """

    # mode 'a' means read/write, create if doesnt exist
    sync = zarr.ProcessSynchronizer(outputpth + '.sync')
    rootgroup = zarr.open(outputpth, mode='a', synchronizer=sync)
    _my_xarr_to_zarr_writeattributes(rootgroup, attrs)
    return outputpth


def _get_indices_dataset_notexist(input_time_arrays):
    running_total = 0
    write_indices = []
    for input_time in input_time_arrays:
        write_indices.append([0 + running_total, len(input_time) + running_total])
        running_total += len(input_time)
    return write_indices, running_total


def _get_indices_dataset_exists(input_time_arrays, zarr_time):
    running_total = 0
    write_indices = []
    for input_time in input_time_arrays:
        input_is_in_zarr = np.isin(input_time, zarr_time)
        if input_is_in_zarr.any():  # this input array is at least partly in this datastore already
            if not input_is_in_zarr.all():  # this input array is only partly in this datastore
                raise NotImplementedError(
                    'get_write_indices_zarr: processed data is only partly within the existing zarr dataset, must be entirely within or without')
            else:
                # input data is entirely within the existing zarr data, the input_time is going to be sorted, but the zarr
                # time will be in the order of lines received and saved to disk.  Have to get indices of input_time in zarr_time
                if isinstance(input_time, xr.DataArray):
                    input_time = input_time.values
                write_indices.append(search_not_sorted(zarr_time, input_time))
        else:  # zarr datastore exists, but this data is not in it.  Append to the existing datastore
            write_indices.append([len(zarr_time) + running_total, len(zarr_time) + len(input_time) + running_total])
            running_total += len(input_time)
    return write_indices, running_total


def get_write_indices_zarr(output_pth: str, input_time_arrays: list, index_dim='time'):
    """
    In Kluster, we parallel process the multibeam data and write it out to zarr chunks.  Here we need to figure out
    if the input data should be appended or if it should overwrite existing data.  This is controlled by the returned
    list of data locations.

    Take the dimension we are using as the index (usually time) and see where the input arrays fit in

    the list of write indices could include:
    - [startidx, endidx] when the written data is new and not in the zarr store yet
    - np.array(0,1,2,3....) when the written data is in the zarr store and may not be continuous

    Parameters
    ----------
    output_pth
        str, path to the zarr rootgroup
    input_time_arrays
        list of xarray DataArray, the time dimension for each input array
    index_dim
        str identifier for the dimension name that we are using as the index.  Generally time.

    Returns
    -------
    list
        write indices to use to write the input_time_arrays to the zarr datastore at outputpth
    int
        final size of the rootgroup after the write, needed to resize zarr to the appropriate length
    """

    zarr_time = np.array([])
    mintimes = [float(i.min()) for i in input_time_arrays]
    if not (np.diff(mintimes) > 0).all():  # input arrays are not time sorted
        raise NotImplementedError('get_write_indices_zarr: input arrays are out of order in time')

    if os.path.exists(output_pth):
        rootgroup = zarr.open(output_pth, mode='r')  # only opens if the path exists
        zarr_time = rootgroup[index_dim]
        write_indices, running_total = _get_indices_dataset_exists(input_time_arrays, zarr_time)
    else:  # datastore doesn't exist, we just return the write indices equal to the shape of the input arrays
        write_indices, running_total = _get_indices_dataset_notexist(input_time_arrays)
    final_size = np.max([write_indices[-1][-1], len(zarr_time)])
    return write_indices, final_size


def _interp_across_chunks_xarrayinterp(xarr: Union[xr.Dataset, xr.DataArray], dimname: str, chnk_time: xr.DataArray):
    """
    Runs xarr interp on an individual chunk, extrapolating to cover boundary case

    Parameters
    ----------
    xarr
        xarray DataArray or Dataset, object to be interpolated
    dimname
        str, dimension name to interpolate
    chnk_time
        xarray DataArray, time to interpolate to

    Returns
    -------
    Union[xr.Dataset, xr.DataArray]
        Interpolated xarr object
    """

    if dimname == 'time':
        try:  # dataarray workflow, use 'values' to access the numpy array
            chnk_time = chnk_time.values
        except AttributeError:
            pass
        # use extrapolate for when pings are in the file after the last attitude/navigation time stamp
        ans = xarr.interp(time=chnk_time, method='linear', assume_sorted=True, kwargs={'fill_value': 'extrapolate'})
        return ans
    else:
        raise NotImplementedError('Only "time" currently supported dim name')


def _interp_across_chunks_construct_times(xarr: Union[xr.Dataset, xr.DataArray], new_times: xr.DataArray, dimname: str):
    """
    Takes in the existing xarray dataarray/dataset (xarr) and returns chunk indexes and times that allow for
    interpolating to the desired xarray dataarray/dataset (given as new_times).  This allows us to interp across
    the dask array chunks without worrying about boundary cases between worker blocks.

    Parameters
    ----------
    xarr
        xarray DataArray or Dataset, object to be interpolated
    new_times
        xarray DataArray, times for the array to be interpolated to
    dimname
        dimension name to interpolate

    Returns
    -------
    list
        list of lists, each element is a list containing time indexes for the chunk, ex: [[0,2000], [2000,4000]]
    Union[list, xr.DataArray]
        list or DataArray, each element is the section of new_times that applies to that chunk
    """

    # first go ahead and chunk the array if chunks do not exist
    if not xarr.chunks:
        xarr = xarr.chunk()

    try:
        xarr_chunks = xarr.chunks[0]  # works for xarray DataArray
    except KeyError:
        xarr_chunks = xarr.chunks[dimname]  # works for xarray Dataset

    chnk_end = np.cumsum(np.array(xarr_chunks)) - 1
    chnk_end_time = xarr[dimname][chnk_end].values

    #  this is to ensure that we cover the desired time, extrapolate to cover the min/max desired time
    #  - when we break up the times to interp to (new_times) we want to ensure the last chunk covers all the end times
    chnk_end_time[-1] = new_times[-1] + 1
    try:
        # have to compute here, searchsorted not supported for dask arrays, but it is so much faster (should be sorted)
        endtime_idx = np.searchsorted(new_times.compute(), chnk_end_time)
    except AttributeError:
        # new_times is a numpy array, does not need compute
        endtime_idx = np.searchsorted(new_times, chnk_end_time)

    chnkwise_times = np.split(new_times, endtime_idx)[:-1]  # drop the last, its empty

    # build out the slices
    # add one to get the next entry for each chunk
    slices_endtime_idx = np.insert(chnk_end + 1, 0, 0)
    chnk_idxs = [[slices_endtime_idx[i], slices_endtime_idx[i+1]] for i in range(len(slices_endtime_idx)-1)]

    # only return chunk blocks that have valid times in them
    empty_chunks = np.array([chnkwise_times.index(i) for i in chnkwise_times if i.size == 0])
    for idx in empty_chunks[::-1]:  # go backwards to preserve index in list as we remove elements
        del chnk_idxs[idx]
        del chnkwise_times[idx]
    return chnk_idxs, chnkwise_times


def slice_xarray_by_dim(arr: Union[xr.Dataset, xr.DataArray], dimname: str = 'time', start_time: float = None,
                        end_time: float = None):
    """
    Slice the input xarray dataset/dataarray by provided start_time and end_time. Start/end time do not have to be
    values in the dataarray index to be used, this function will find the nearest times.

    If times provided are outside the array, will return the original array.

    If times are not provided, will return the original array

    Parameters
    ----------
    arr
        xarray Dataarray/Dataset with an index of dimname
    dimname
        str, name of dimension to use with selection/slicing
    start_time
        float, start time of slice
    end_time
        float, end time of slice

    Returns
    -------
    Union[xr.Dataset, xr.DataArray]
        xarray dataarray/dataset sliced to the input start time and end time
    """

    if start_time is None and end_time is None:
        return arr

    if start_time is not None:
        nearest_start = float(arr[dimname].sel(time=start_time, method='nearest'))
    else:
        nearest_start = float(arr[dimname][0])

    if end_time is not None:
        nearest_end = float(arr[dimname].sel(time=end_time, method='nearest'))
    else:
        nearest_end = float(arr[dimname][-1])

    if start_time is not None and end_time is not None:
        if nearest_end == nearest_start:
            if (nearest_end == float(arr[dimname][-1])) or (nearest_end == float(arr[dimname][0])):
                # if this is true, you have start/end times that are outside the scope of the data.  The start/end times will
                #  be equal to either the start of the dataset or the end of the dataset, depending on when they fall
                return None
    rnav = arr.sel(time=slice(nearest_start, nearest_end))
    rnav = rnav.chunk(rnav.sizes)  # do this to get past the unify chunks issue, since you are slicing here, you end up with chunks of different sizes
    return rnav


def interp_across_chunks(xarr: Union[xr.Dataset, xr.DataArray], new_times: xr.DataArray, dimname: str = 'time',
                         daskclient: Client = None):
    """
    Takes in xarr and interpolates to new_times.  Ideally we could use xarray interp_like or interp, but neither
    of these are implemented with support for chunked dask arrays.  Therefore, we have to determine the times of
    each chunk and interpolate individually.  To allow for the case where a value is between chunks or right on
    the boundary, we extend the chunk time to buffer the gap.

    Parameters
    ----------
    xarr
        xarray DataArray or Dataset, object to be interpolated
    new_times
        xarray DataArray, times for the array to be interpolated to
    dimname
        dimension name to interpolate
    daskclient
        dask.distributed.client or None, if running outside of dask cluster

    Returns
    -------
    Union[xr.Dataset, xr.DataArray]
        xarray DataArray or Dataset, interpolated xarr
    """

    if type(xarr) not in [xr.DataArray, xr.Dataset]:
        raise NotImplementedError('Only xarray DataArray and Dataset objects allowed.')
    if len(list(xarr.dims)) > 1:
        raise NotImplementedError('Only one dimensional data is currently supported.')

    # chunking and scattering large arrays takes way too long, we load here to avoid this
    xarr = xarr.load()

    # with heading you have to deal with zero crossing, occassionaly see lines where you end up interpolating heading
    #  from 0 to 360, which gets you something around 180deg.  Take the 360 complement and interp that, return it back
    #  to 0-360 domain after
    needs_reverting = False
    if type(xarr) == xr.DataArray:
        if xarr.name == 'heading':
            needs_reverting = True
            xarr = xr.DataArray(np.float32(np.rad2deg(np.unwrap(np.deg2rad(xarr)))), coords=[xarr.time], dims=['time'])
    else:
        if 'heading' in list(xarr.data_vars.keys()):
            needs_reverting = True
            xarr['heading'] = xr.DataArray(np.float32(np.rad2deg(np.unwrap(np.deg2rad(xarr.heading)))), coords=[xarr.time],
                                           dims=['time'])

    chnk_idxs, chnkwise_times = _interp_across_chunks_construct_times(xarr, new_times, dimname)
    xarrs_chunked = [xarr.isel({dimname: slice(i, j)}).chunk(j-i,) for i, j in chnk_idxs]
    if daskclient is None:
        interp_arrs = []
        for ct, xar in enumerate(xarrs_chunked):
            interp_arrs.append(_interp_across_chunks_xarrayinterp(xar, dimname, chnkwise_times[ct]))
        newarr = xr.concat(interp_arrs, dimname)
    else:
        xarrs_chunked = daskclient.scatter(xarrs_chunked)
        interp_futs = daskclient.map(_interp_across_chunks_xarrayinterp, xarrs_chunked, [dimname] * len(chnkwise_times),
                                     chnkwise_times)
        newarr = daskclient.submit(xr.concat, interp_futs, dimname).result()

    if needs_reverting and type(xarr) == xr.DataArray:
        newarr = newarr % 360
    elif needs_reverting and type(xarr) == xr.Dataset:
        newarr['heading'] = newarr['heading'] % 360

    assert(len(new_times) == len(newarr[dimname])), 'interp_across_chunks: Input/Output shape is not equal'
    return newarr


def clear_data_vars_from_dataset(dataset: Union[list, dict, xr.Dataset], datavars: Union[list, str]):
    """
    Some code to handle dropping data variables from xarray Datasets in different containers.  We use lists of Datasets,
    dicts of Datasets and individual Datasets in different places.  Here we can just pass in whatever, drop the
    variable or list of variables, and get the Dataset back.

    Parameters
    ----------
    dataset
        xarray Dataset, list, or dict of xarray Datasets
    datavars
        str or list, variables we wish to drop from the xarray Dataset

    Returns
    -------
    Union[list, dict, xr.Dataset]
        original Dataset(s) with dropped variables
    """

    if type(datavars) == str:
        datavars = [datavars]

    for datavar in datavars:
        if type(dataset) == dict:  # I frequently maintain a dict of datasets for each sector
            for sec_ident in dataset:
                if datavar in dataset[sec_ident].data_vars:
                    dataset[sec_ident] = dataset[sec_ident].drop_vars(datavar)
        elif type(dataset) == list:  # here if you have lists of Datasets
            for cnt, dset in enumerate(dataset):
                if datavar in dset.data_vars:
                    dataset[cnt] = dataset[cnt].drop_vars(datavar)
        elif type(dataset) == xr.Dataset:
            if datavar in dataset.data_vars:
                dataset = dataset.drop_vars(datavar)
    return dataset


def stack_nan_array(dataarray: xr.DataArray, stack_dims: tuple = ('time', 'beam')):
    """
    To handle NaN values in our input arrays, we flatten and index only the valid values.  This comes into play with
    beamwise arrays that have NaN where there were no beams.

    See reform_nan_array to rebuild the original array

    Parameters
    ----------
    dataarray
        xarray DataArray, array that we need to flatten and index non-NaN values
    stack_dims
        tuple, dims of our input data

    Returns
    -------
    np.array
        indexes of the original data
    xr.DataArray
        xarray DataArray, multiindexed and flattened
    """

    orig_idx = np.where(~np.isnan(dataarray))
    dataarray_stck = dataarray.stack(stck=stack_dims)
    nan_idx = ~np.isnan(dataarray_stck).compute()
    dataarray_stck = dataarray_stck[nan_idx]
    return orig_idx, dataarray_stck


def reform_nan_array(dataarray_stack: xr.DataArray, orig_idx: tuple, orig_shape: tuple, orig_coords: xr.DataArray,
                     orig_dims: tuple):
    """
    To handle NaN values in our input arrays, we flatten and index only the valid values.  Here we rebuild the
    original square shaped arrays we need using one of the original arrays as reference.

    See stack_nan_array.  Run this on the stacked output to get the original dimensions back.

    Parameters
    ----------
    dataarray_stack
        flattened array that we just interpolated
    orig_idx
        2 elements, one for 1st dimension indexes and one for 2nd dimension indexes, see np.where
    orig_shape
        original shape of array before stack_nan_array
    orig_coords
        coordinates from array before stack_nan_array
    orig_dims
        original dims of array before stack_nan_array

    Returns
    -------
    xr.DataArray
        values of arr, filled to be square with NaN values, coordinates of ref_array
    """

    final_arr = np.empty(orig_shape, dtype=dataarray_stack.dtype)
    final_arr[:] = np.nan
    final_arr[orig_idx] = dataarray_stack
    final_arr = xr.DataArray(final_arr, coords=orig_coords, dims=orig_dims)
    return final_arr


def reload_zarr_records(pth: str, skip_dask: bool = False, sort_by: str = None):
    """
    After writing new data to the zarr data store, you need to refresh the xarray Dataset object so that it
    sees the changes.  We do that here by just re-running open_zarr.

    All the keyword arguments set to False are there to correctly read the saved zarr arrays.  Mask_and_scale i've
    yet to configure properly, it will replace values equal to the fill_value attribute with NaN.  Even when
    fill_value is non-zero, it seems to replace zeros with NaN.  Setting it to false prevents this.  You can read
    more here:  http://xarray.pydata.org/en/stable/generated/xarray.open_zarr.html

    If you are running this outside of the normal dask-enabled workflow, self.client will be None and you will not
    have the distributed sync object.  I do this with reading attributes from the zarr datastore where I just need
    to open for a minute to get the attributes.

    Returns
    -------
    pth
        string, path to xarray Dataset stored as zarr datastore
    skip_dask
        if True, skip the dask process synchronizer as you are not running dask distributed
    sort_by
        optional, will sort by the dimension provided, if provided (ex: 'time')
    """

    if os.path.exists(pth):
        sync = zarr.ProcessSynchronizer(pth + '.sync')
        if not skip_dask:
            data = xr.open_zarr(pth, synchronizer=sync,
                                mask_and_scale=False, decode_coords=False, decode_times=False,
                                decode_cf=False, concat_characters=False)
        else:
            data = xr.open_zarr(pth, synchronizer=None,
                                mask_and_scale=False, decode_coords=False, decode_times=False,
                                decode_cf=False, concat_characters=False)
        if sort_by:
            return data.sortby(sort_by)
        else:
            return data
    else:
        print('Unable to reload, no paths found: {}'.format(pth))
        return None


def return_chunk_slices(xarr: xr.Dataset):
    """
    Xarray objects are chunked for easy parallelism.  When we write to zarr stores, chunks become segregated, so when
    operating on xarray objects, it makes sense to do it one chunk at a time sometimes.  Here we return slices so that
    we can only pull one chunk into memory at a time.

    Parameters
    ----------
    xarr
        Dataset object, must be only one dimension currently

    Returns
    -------
    list
        list of slices for the indices of each chunk
    """

    try:
        chunk_dim = list(xarr.chunks.keys())
        if len(chunk_dim) > 1:
            raise NotImplementedError('Only 1 dimensional xarray objects supported at this time')
            return None
    except AttributeError:
        print('Only xarray objects are supported')
        return None

    chunk_dim = chunk_dim[0]
    chunks = list(xarr.chunks.values())[0]
    chunk_size = chunks[0]
    chunk_slices = [slice(i * chunk_size, i * chunk_size + chunk_size) for i in range(len(chunks))]

    # have to correct last slice, as last chunk is equal to the length of the array modulo chunk size
    total_len = xarr.dims[chunk_dim]
    last_chunk_size = xarr.dims[chunk_dim] % chunk_size
    if last_chunk_size:
        chunk_slices[-1] = slice(total_len - last_chunk_size, total_len)
    else:  # last slice fits perfectly, they have no remainder some how
        pass

    return chunk_slices


def _find_gaps_split(datagap_times: list, existing_gap_times: list):
    """
    helper for compare_and_find_gaps.  A function to use in a loop to continue splitting gaps until they no longer
    include any existing gaps

    datagap_times = [[0,5], [30,40], [70, 82], [90,100]]

    existing_gap_times = [[10,15], [35,45], [75,80], [85,95]]

    split_dgtime = [[0, 5], [30, 40], [70, 75], [80, 82], [90, 100]]

    Parameters
    ----------
    datagap_times
        list, list of two element lists (start time, end time) for the gaps found in the new data
    existing_gap_times
        list, list of two element lists (start time, end time) for the gaps found in the existing data

    Returns
    -------
    list
        list of two element lists (start time, end time) for the new data gaps split around the existing data gaps
    """

    split = False
    split_dgtime = []
    for dgtime in datagap_times:
        for existtime in existing_gap_times:
            # datagap contains an existing gap, have to split the datagap
            if (dgtime[0] <= existtime[0] <= dgtime[1]) and (dgtime[0] <= existtime[1] <= dgtime[1]):
                split_dgtime.append([dgtime[0], existtime[0]])
                split_dgtime.append([existtime[1], dgtime[1]])
                split = True
                break
        if not split:
            split_dgtime.append(dgtime)
        else:
            split = False
    return split_dgtime


def compare_and_find_gaps(source_dat: Union[xr.DataArray, xr.Dataset], new_dat: Union[xr.DataArray, xr.Dataset],
                          max_gap_length: float = 1.0, dimname: str = 'time'):
    """
    So far, mostly used with Applanix POSPac SBETs.  Converted SBET would be the new_dat and the existing navigation in
    Kluster would be the source_dat.  You'd be interested to know if there were gaps in the sbet greater than a certain
    length that did not coincide with existing gaps related to stopping/starting logging or something.  Here we find
    gaps in the new_dat of size greater than max_gap_length and trim them to the gaps found in source_dat.

    Parameters
    ----------
    source_dat
        xarray DataArray/Dataset, object with dimname as coord that you want to use as the basis for comparison
    new_dat
        xarray DataArray/Dataset that you want to find the gaps in
    max_gap_length
        maximum acceptable gap
    dimname
        name of the dimension you want to find the gaps in

    Returns
    -------
    np.array
        numpy array, nx2 where n is the number of gaps found
    """

    # gaps in source, if a gap in the new data is within a gap in the source, it is not a gap
    existing_gaps = np.argwhere(source_dat[dimname].diff(dimname).values > max_gap_length)
    existing_gap_times = [[float(source_dat[dimname][gp]), float(source_dat[dimname][gp + 1])] for gp in existing_gaps]

    # look for gaps in the new data
    datagaps = np.argwhere(new_dat[dimname].diff(dimname).values > max_gap_length)
    datagap_times = [[float(new_dat[dimname][gp]), float(new_dat[dimname][gp + 1])] for gp in datagaps]

    # consider postprocessed nav starting too late or ending too early as a gap as well
    if new_dat[dimname].min() > source_dat[dimname].time.min() + max_gap_length:
        datagap_times.insert([float(source_dat[dimname].time.min()), float(new_dat[dimname].min())])
    if new_dat[dimname].max() + max_gap_length < source_dat[dimname].time.max():
        datagap_times.append([float(new_dat[dimname].max()), float(source_dat[dimname].time.max())])

    # first, split all the gaps if they contain existing time gaps, keep going until you no longer find contained gaps
    splitting = True
    while splitting:
        dg_split_gaps = _find_gaps_split(datagap_times, existing_gap_times)
        if dg_split_gaps != datagap_times:  # you split
            datagap_times = dg_split_gaps
        else:
            splitting = False

    # next adjust gap boundaries if they overlap with existing gaps
    finalgaps = []
    for dgtime in datagap_times:
        for existtime in existing_gap_times:
            # datagap is fully within an existing gap in the source data, just dont include it
            if (existtime[0] <= dgtime[0] <= existtime[1]) and (existtime[0] <= dgtime[1] <= existtime[1]):
                continue
            # partially covered
            if existtime[0] < dgtime[0] < existtime[1]:
                dgtime[0] = existtime[1]
            elif existtime[0] < dgtime[1] < existtime[1]:
                dgtime[1] = existtime[0]
        finalgaps.append(dgtime)

    return np.array(finalgaps)


def get_beamwise_interpolation(pingtime: xr.DataArray, additional: xr.DataArray, interp_this: xr.DataArray):
    """
    Given ping time and beamwise time addition (delay), return a 2d interpolated version of the provided 1d Dataarray.

    We want this to be efficient, so first we stack and get the unique times to interpolate.  Retain the index of where
    these unique times go in the original array

    Then reform the array, populating the indices with the interpolated values.  Should be faster than brute force
    interpolating all of the data, especially since we expect a lot of duplication (delay values are the same within
    a sector)

    Parameters
    ----------
    pingtime
        1dim array of timestamps representing time of ping
    additional
        2dim (time/beam) array of timestamps representing additional delay
    interp_this
        1dim DataArray (time) that we want to interpolate to pingtime + additional

    Returns
    -------
    xr.DataArray
        2dim array, the interpolated array at pingtime + additional
    """

    beam_tstmp = pingtime + additional
    rx_tstmp_idx, rx_tstmp_stck = stack_nan_array(beam_tstmp, stack_dims=('time', 'beam'))
    unique_rx_times, inv_idx = np.unique(rx_tstmp_stck.values, return_inverse=True)
    rx_interptimes = xr.DataArray(unique_rx_times, coords=[unique_rx_times], dims=['time']).chunk()

    interpolated_flattened = interp_across_chunks(interp_this, rx_interptimes.compute())
    reformed_interpolated = reform_nan_array(interpolated_flattened.isel(time=inv_idx), rx_tstmp_idx, beam_tstmp.shape,
                                             beam_tstmp.coords, beam_tstmp.dims)

    return reformed_interpolated
