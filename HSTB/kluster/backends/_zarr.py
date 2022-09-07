import os
import numpy as np
import xarray as xr
import zarr
import time
from dask.distributed import wait, Client, progress, Future
from typing import Union, Callable, Tuple, Any
from itertools import groupby, count
import shutil
import logging

from HSTB.kluster import kluster_variables
from HSTB.kluster.backends._base import BaseBackend


class ZarrBackend(BaseBackend):
    """
    Backend for writing data to disk, used with fqpr_generation.Fqpr and xarray_conversion.BatchRead.
    """
    def __init__(self, output_folder: str = None):
        super().__init__(output_folder)

    def _get_zarr_path(self, dataset_name: str, sys_id: str = None):
        """
        Get the path to the zarr folder based on the dataset name that we provide.  Ping zarr folders are based on
        the serial number of the system, and that must be provided here as the sys_id
        """

        if self.output_folder is None:
            return None
        if dataset_name == 'ping':
            if not sys_id:
                raise ValueError('Zarr Backend: No system id provided, cannot build ping path')
            return os.path.join(self.output_folder, 'ping_' + sys_id + '.zarr')
        elif dataset_name == 'navigation':
            return os.path.join(self.output_folder, 'navigation.zarr')
        elif dataset_name == 'ppnav':
            return os.path.join(self.output_folder, 'ppnav.zarr')
        elif dataset_name == 'attitude':
            return os.path.join(self.output_folder, 'attitude.zarr')
        else:
            raise ValueError('Zarr Backend: Not a valid dataset name: {}'.format(dataset_name))

    def _get_zarr_indices(self, zarr_path: str, time_array: list, append_dim: str):
        """
        Get the chunk indices (based on the time dimension) using the proivded time arrays
        """
        return get_write_indices_zarr(zarr_path, time_array, append_dim)

    def _get_chunk_sizes(self, dataset_name: str):
        """
        Pull from kluster_variables to get the correct chunk size for each dataset
        """

        if dataset_name == 'ping':
            return kluster_variables.ping_chunks
        elif dataset_name in ['navigation', 'ppnav']:
            return kluster_variables.nav_chunks
        elif dataset_name == 'attitude':
            return kluster_variables.att_chunks
        else:
            raise ValueError('Zarr Backend: Not a valid dataset name: {}'.format(dataset_name))

    def _autodetermine_times(self, data: list, time_array: list = None, append_dim: str = 'time'):
        """
        Get the time arrays for the dataset depending on the dataset type.
        """

        if time_array:
            return time_array
        elif any([isinstance(d, Future) for d in data]):
            raise ValueError('Zarr Backend: cannot autodetermine times from Futures')
        else:
            return [d[append_dim] for d in data]

    def delete(self, dataset_name: str, variable_name: str, sys_id: str = None):
        """
        Delete the provided variable name from the datastore on disk.  var_path will be a directory of chunked files, so
        we use rmtree to remove all files in the var_path directory.
        """
        zarr_path = self._get_zarr_path(dataset_name, sys_id)
        var_path = os.path.join(zarr_path, variable_name)
        if not os.path.exists(var_path):
            self.print('Unable to remove variable {}, path does not exist: {}'.format(variable_name, var_path), logging.ERROR)
        else:
            shutil.rmtree(var_path)

    def write(self, dataset_name: str, data: Union[list, xr.Dataset, Future], time_array: list = None, attributes: dict = None,
              sys_id: str = None, append_dim: str = 'time', skip_dask: bool = False):
        """
        Write the provided data to disk, finding the correct zarr folder using dataset_name.  We need time_array to get
        the correct write indices for the data.  If attributes are provided, we write those as well as xarray Dataset
        attributes.
        """

        if not isinstance(data, list):
            data = [data]
        if attributes is None:
            attributes = {}
        time_array = self._autodetermine_times(data, time_array, append_dim)
        zarr_path = self._get_zarr_path(dataset_name, sys_id)
        data_indices, final_size, push_forward = self._get_zarr_indices(zarr_path, time_array, append_dim)
        chunks = self._get_chunk_sizes(dataset_name)
        fpths = distrib_zarr_write(zarr_path, data, attributes, chunks, data_indices, final_size, push_forward, self.client,
                                   skip_dask=skip_dask, show_progress=self.show_progress,
                                   write_in_parallel=self.parallel_write)
        return zarr_path, fpths

    def write_attributes(self, dataset_name: str, attributes: dict, sys_id: str = None):
        """
        If the data is written to disk, we write the attributes to the zarr store as attributes of the dataset_name record.
        """

        zarr_path = self._get_zarr_path(dataset_name, sys_id)
        if zarr_path is not None:
            zarr_write_attributes(zarr_path, attributes)
        else:
            self.debug_print('Writing attributes is disabled for in-memory processing', logging.INFO)

    def remove_attribute(self, dataset_name: str, attribute: str, sys_id: str = None):
        """
        Remove the attribute matching name provided in the dataset_name_sys_id folder
        """
        zarr_path = self._get_zarr_path(dataset_name, sys_id)
        if zarr_path is not None:
            zarr_remove_attribute(zarr_path, attribute)
        else:
            self.debug_print('Removing attributes is disabled for in-memory processing', logging.INFO)


def _get_indices_dataset_notexist(input_time_arrays):
    """
    Build a list of [start,end] indices that match the input_time_arrays, starting at zero.

    Parameters
    ----------
    input_time_arrays
        list of 1d xarray dataarrays or numpy arrays for the input time values

    Returns
    -------
    list
        list of [start,end] indexes for the indices of input_time_arrays
    """

    running_total = 0
    write_indices = []
    for input_time in input_time_arrays:
        write_indices.append([0 + running_total, len(input_time) + running_total])
        running_total += len(input_time)
    return write_indices


def _get_indices_dataset_exists(input_time_arrays: list, zarr_time: zarr.Array):
    """
    I am so sorry for whomever finds this.  I had this 'great' idea a while ago to concatenate all the multibeam
    lines into daily datasets.  Overall this has been a great thing, except for the sorting.  We have to figure out
    how to assemble daily datasets from lines applied in any order imaginable, with overlap and all kinds of things.
    This function should provide the indices that allow this to happen.

    Recommend examining the test_backend tests if you want to understand this a bit more

    build the indices for where the input_time_arrays fit within the existing zarr_time.  We have three ways to proceed
    within this function:
    1. input time arrays are entirely within the existing zarr_time, we build a numpy array of indices that describe
    where the input_time_arrays will overwrite the zarr_time
    2. input time arrays are entirely outside the existing zarr_time, we just build a 2 element list describing the
    start and end index to append the data to zarr_time
    3 input time arrays are before and might overlap existing data, we build a 2 element list starting with zero
    describing the start and end index and return a push_forward value, letting us know how much the zarr data
    needs to be pushed forward.  If there is overlap, the last index is a numpy array of indices.
    4 input time arrays are after and might overlap existing data, we build a 2 element list starting with the
    index of the start of overlap.  If there is overlap, the last index is a numpy array of indices.

    Parameters
    ----------
    input_time_arrays
        list of 1d xarray dataarrays or numpy arrays for the input time values
    zarr_time
        zarr array 1d for the existing time values saved to disk

    Returns
    -------
    list
        list of either [start,end] indexes or numpy arrays for the indices of input_time_arrays in zarr_time
    list
        list of [index of push, total amount to push] for each push
    int
        how many values need to be inserted to make room for this new data at the beginning of the zarr rootgroup
    """

    running_total = 0
    push_forward = []
    total_push = 0
    write_indices = []
    # time arrays must be in order in case you have to do the 'partly in datastore' workaround
    input_time_arrays.sort(key=lambda x: x[0])
    min_zarr_time = zarr_time[0]
    max_zarr_time = zarr_time[-1]
    zarr_time_len = len(zarr_time)
    for input_time in input_time_arrays:  # for each chunk of data that we are wanting to write, look at the times to see where it fits
        input_time_len = len(input_time)
        input_is_in_zarr = np.isin(input_time, zarr_time)  # where is there overlap with existing data
        if isinstance(input_time, xr.DataArray):
            input_time = input_time.values
        if input_is_in_zarr.any():  # this input array is at least partly in this datastore already
            if not input_is_in_zarr.all():  # this input array is only partly in this datastore
                starter_indices = np.full_like(input_time, -1)  # -1 for values that are outside the existing values
                inside_indices = search_not_sorted(zarr_time, input_time[input_is_in_zarr])  # get the indices for overwriting where there is overlap
                starter_indices[input_is_in_zarr] = inside_indices
                count_outside = len(starter_indices) - len(inside_indices)  # the number of values that do not overlap
                if starter_indices[-1] == -1:  # this input_time contains times after the existing values
                    max_inside_index = inside_indices[-1]
                    # now add in a range starting with the last index for all values outside the zarr time range
                    starter_indices[~input_is_in_zarr] = np.arange(max_inside_index + 1, max_inside_index + count_outside + 1)
                    if input_time[-1] < max_zarr_time:  # data partially overlaps and is after existing data, but not at the end of the existing dataset
                        push_forward.append([max_inside_index + 1 + total_push, count_outside])
                    else:
                        running_total += count_outside
                    write_indices.append(starter_indices + total_push)
                elif starter_indices[0] == -1:  # this input_time contains times before the existing values
                    if input_time[0] < min_zarr_time:
                        starter_indices = np.arange(input_time_len)
                        push_forward.append([total_push, count_outside])
                    else:
                        min_inside_index = inside_indices[0]
                        starter_indices = np.arange(input_time_len) + min_inside_index
                        push_forward.append([min_inside_index + total_push, count_outside])
                    write_indices.append(starter_indices + total_push)
                    total_push += count_outside
                else:
                    raise NotImplementedError('_get_indices_dataset_exists: Found a gap in the overlap between the data provided and the existing dataset on disk')
            else:
                # input data is entirely within the existing zarr data, the input_time is going to be sorted, but the zarr
                # time will be in the order of lines received and saved to disk.  Have to get indices of input_time in zarr_time
                write_indices.append(search_not_sorted(zarr_time, input_time) + total_push)
        else:  # zarr datastore exists, but this data is not in it.  Append to the existing datastore
            if input_time[0] < min_zarr_time:  # data is before existing data, have to push existing data up
                write_indices.append([total_push, input_time_len + total_push])
                push_forward.append([total_push, input_time_len])
                total_push += input_time_len
            elif input_time[0] > max_zarr_time:  # data is after existing data, just tack it on
                write_indices.append([zarr_time_len + running_total + total_push, zarr_time_len + input_time_len + running_total + total_push])
                running_total += input_time_len
            else:   # data is in between existing data, but there is no overlap
                next_value_index = np.where(zarr_time - input_time[0] > 0)[0][0]
                write_indices.append([next_value_index + total_push, next_value_index + input_time_len + total_push])
                push_forward.append([next_value_index + total_push, input_time_len])
                total_push += input_time_len
    return write_indices, push_forward, total_push


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
    list
        list of [index of push, total amount to push] for each push
    """

    zarr_time = np.array([])
    mintimes = [float(i.min()) for i in input_time_arrays]
    if not (np.diff(mintimes) > 0).all():  # input arrays are not time sorted
        raise NotImplementedError('get_write_indices_zarr: input arrays are out of order in time')
    push_forward = []
    total_push = 0
    if os.path.exists(output_pth):
        rootgroup = zarr.open(output_pth, mode='r')  # only opens if the path exists
        zarr_time = rootgroup[index_dim]
        write_indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
    else:  # datastore doesn't exist, we just return the write indices equal to the shape of the input arrays
        write_indices = _get_indices_dataset_notexist(input_time_arrays)
    final_size = np.max([write_indices[-1][-1], len(zarr_time)]) + total_push
    return write_indices, final_size, push_forward


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


def retry_call(callabl: Callable, args=None, kwargs=None, exceptions: Tuple[Any, ...] = (),
               retries: int = 200, wait: float = 0.1):
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
            print('WARNING: starting zarr_write with an empty rootgroup, writing to disk not supported')
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
            # if not attrs:
            #     attrs = None
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

    def remove_attribute(self, attr: str):
        if attr in self.rootgroup.attrs:
            self.rootgroup.attrs.pop(attr)

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

    def _push_existing_data_forward(self, variable_name: str, dims_of_arrays: dict, timaxis: int, push_forward: list,
                                    starting_size: int, max_push_amount: int = 50000):
        """
        If the user is trying to write data that comes before the existing data in the time dimension, we need to push
        the existing data up to make room, after resizing to the total size.  This allows us to then write the new data
        prior to the existing data.

        If the amount of data that we need to push forward is greater (in the time dimension) than the max_push_amount,
        we push the data forward in chunks starting at the end.  This helps with datasets on disk that on trying to load
        entirely into memory we exceed the total system memory.

        Recommend examining the test_backend tests if you want to understand this a bit more

        Parameters
        ----------
        variable_name
            variable name in the zarr rootgroup
        dims_of_arrays
            where keys are array names and values list of dims/shape.  Example: 'beampointingangle': [['time', 'sector', 'beam'], (5000, 3, 400)]
        timaxis
            index of the time dimension
        push_forward
            list of [index of push, total amount to push] for each push
        starting_size
            size of the array on disk before we resized to make room
        max_push_amount
            maximum size of the chunk of data that will be moved.  If the total amount of data that needs to be moved
            is too large, it could cause memory errors.  This amount limits the amount of data read at a time.
        """

        total_push = 0
        for push_idx, push_amount in push_forward:
            push_amount_chunked = []
            push_amount_working = starting_size - push_idx + total_push
            while push_amount_working:
                if push_amount_working > max_push_amount:
                    push_amount_chunked.append(max_push_amount)
                    push_amount_working -= max_push_amount
                else:
                    push_amount_chunked.append(push_amount_working)
                    push_amount_working = 0
            push_amount_chunked = push_amount_chunked[::-1]  # we push starting at the end to not overwrite data as we push
            push_chunk_loc = starting_size
            for pushchunk in push_amount_chunked:
                data_range = slice(push_chunk_loc - pushchunk + total_push, push_chunk_loc + total_push)
                data_chunk_idx = tuple(data_range if dims_of_arrays[variable_name][1].index(i) == timaxis else slice(0, i) for i in
                                       dims_of_arrays[variable_name][1])
                final_location_range = slice(push_chunk_loc - pushchunk + push_amount + total_push, push_chunk_loc + push_amount + total_push)
                loc_chunk_idx = tuple(final_location_range if dims_of_arrays[variable_name][1].index(i) == timaxis else slice(0, i) for i in
                                      dims_of_arrays[variable_name][1])
                self.rootgroup[variable_name][loc_chunk_idx] = self.rootgroup[variable_name][data_chunk_idx]
                push_chunk_loc -= pushchunk
            empty_range = slice(push_idx, push_idx + push_amount)
            empty_chunk_idx = tuple(empty_range if dims_of_arrays[variable_name][1].index(i) == timaxis else slice(0, i) for i in
                                    dims_of_arrays[variable_name][1])
            self.rootgroup[variable_name][empty_chunk_idx] = self.rootgroup[variable_name].fill_value
            total_push += push_amount

    def _overwrite_existing_rootgroup(self, xarr_data: np.array, data_loc_copy: Union[list, np.ndarray], var_name: str,
                                      dims_of_arrays: dict, chunksize: tuple, timlength: int, timaxis: int):
        """
        Write this numpy array to zarr, overwriting the existing data or appending to an existing rootgroup array

        Parameters
        ----------
        xarr_data
            numpy array for the variable from the dataset we want to write to zarr
        data_loc_copy
            [start time index, end time index] for xarr, ex: [0,1000] if xarr time dimension is 1000 long
        var_name
            variable name
        dims_of_arrays
            where keys are array names and values list of dims/shape.  Example: 'beampointingangle': [['time', 'sector', 'beam'], (5000, 3, 400)]
        chunksize
            chunk shape used to create the zarr array.  REVISED with Kluster 1.1.1, existing metdata will specify the chunksize
        timlength
            Length of the time dimension for the input xarray Dataset
        timaxis
            index of the time dimension
        """
        # the last write will often be less than the block size.  This is allowed in the zarr store, but we
        #    need to correct the index for it.
        if timlength != data_loc_copy[1] - data_loc_copy[0]:
            data_loc_copy[1] = data_loc_copy[0] + timlength

        # location for new data, assume constant chunksize (as we are doing this outside of this function)
        chunk_time_range = slice(data_loc_copy[0], data_loc_copy[1])
        # use the chunk_time_range for writes unless this variable is a non-time dim array (beam for example)
        array_dims = dims_of_arrays[var_name][1]
        chunk_idx = tuple(chunk_time_range if cnt == timaxis else slice(0, i) for cnt, i in enumerate(array_dims))
        self.rootgroup[var_name][chunk_idx] = zarr.array(xarr_data, shape=dims_of_arrays[var_name][1],
                                                         chunks=self.rootgroup[var_name].chunks)

    def _write_existing_rootgroup(self, xarr: xr.Dataset, data_loc_copy: Union[list, np.ndarray], var_name: str, dims_of_arrays: dict,
                                  chunksize: tuple, timlength: int, timaxis: int, startingshp: tuple, push_forward: list):
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
            chunk shape used to create the zarr array.  REVISED with Kluster 1.1.1, existing metdata will specify the chunksize
        timlength
            Length of the time dimension for the input xarray Dataset
        timaxis
            index of the time dimension
        startingshp
            desired shape for the rootgroup array, might be modified later for total beams if necessary.  if finalsize
            is None (the case when this is not the first write in a set of distributed writes) this is still returned but not used.
        push_forward
            list of [index of push, total amount to push] for each push
        """

        # array to be written
        xarr_data = xarr[var_name].values
        if startingshp is not None:
            startingshp = self._write_adjust_max_beams(startingshp)
            starting_size = self.rootgroup[var_name].shape[0]
            self.rootgroup[var_name].resize(startingshp)
            if push_forward is not None:
                self._push_existing_data_forward(var_name, dims_of_arrays, timaxis, push_forward, starting_size)

        if isinstance(data_loc_copy, list):  # [start index, end index]
            self._overwrite_existing_rootgroup(xarr_data, data_loc_copy, var_name, dims_of_arrays, chunksize,
                                               timlength, timaxis)
        else:  # np.array([4,5,6,1,2,3,8,9...]), indices of the new data, might not be sorted
            # sort lowers the chance that we end up with a gap in our data locations
            sorted_order = data_loc_copy.argsort()
            xarr_data = xarr_data[sorted_order]
            data_loc_copy = data_loc_copy[sorted_order]
            contiguous_chunks = [list(g) for k, g in groupby(data_loc_copy, key=lambda i, j=count(): i - next(j))]
            idx_start = 0
            for chnk in contiguous_chunks:
                chnkdata = xarr_data[idx_start:idx_start + len(chnk)]
                chnkidx = [chnk[0], chnk[-1]]
                chnkdims = dims_of_arrays.copy()
                chnkdims_size = list(chnkdims[var_name][1])
                chnkdims_size[0] = len(chnk)
                timlength = len(chnk)
                chnkdims[var_name][1] = tuple(chnkdims_size)
                if 'time' in chnkdims:
                    chnktime_size = list(chnkdims['time'][1])
                    chnktime_size[0] = len(chnk)
                    chnkdims['time'][1] = tuple(chnktime_size)
                self._overwrite_existing_rootgroup(chnkdata, chnkidx, var_name, chnkdims, chunksize, timlength, timaxis)
                idx_start += len(chnk)

            # below works great except for when self.rootgroup[var_name] is a massive array, on the order of shape=(1000000,400,1)
            #  then this method explodes your memory.  Instead, have to find contiguous lists and use the list method
            # zarr_mask = np.zeros_like(self.rootgroup[var_name], dtype=bool)
            # zarr_mask[data_loc_copy] = True
            # # seems to require me to ravel first, examples only show setting with integer, not sure what is going on here
            # self.rootgroup[var_name].set_mask_selection(zarr_mask, xarr_data.ravel())

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

        if self.zarr_path:
            sync = zarr.ProcessSynchronizer(self.zarr_path + '.sync')
        newarr = self.rootgroup.create_dataset(var_name, shape=dims_of_arrays[var_name][1], chunks=chunksize,
                                               dtype=xarr[var_name].dtype, synchronizer=sync,
                                               fill_value=self._get_arr_nodatavalue(xarr[var_name].dtype))
        newarr[:] = xarr[var_name].values
        newarr.resize(startingshp)

    def write_to_zarr(self, xarr: xr.Dataset, attrs: dict, dataloc: Union[list, np.ndarray], finalsize: int = None,
                      push_forward: list = None):
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
        push_forward
            list of [index of push, total amount to push] for each push
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
                                                   timaxis, startingshp, push_forward)
                else:
                    self._write_existing_rootgroup(xarr, data_loc_copy, var, dims_of_arrays, chunksize, timlength,
                                                   timaxis, None, None)
            else:
                self._write_new_dataset_rootgroup(xarr, var, dims_of_arrays, chunksize, startingshp)

            # _ARRAY_DIMENSIONS is used by xarray for connecting dimensions with zarr arrays
            self.rootgroup[var].attrs['_ARRAY_DIMENSIONS'] = dims_of_arrays[var][0]
        return self.zarr_path


def zarr_write(zarr_path: str, xarr: xr.Dataset, attrs: dict, desired_chunk_shape: dict, dataloc: Union[list, np.ndarray],
               append_dim: str = 'time', finalsize: int = None, push_forward: list = None):
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
    push_forward
        list of [index of push, total amount to push] for each push

    Returns
    -------
    str
        path to zarr data store
    """

    zw = ZarrWrite(zarr_path, desired_chunk_shape, append_dim=append_dim)
    zarr_path = retry_call(zw.write_to_zarr, (xarr, attrs, dataloc), {'finalsize': finalsize, 'push_forward': push_forward},
                           exceptions=(PermissionError,))
    return zarr_path


def distrib_zarr_write(zarr_path: str, xarrays: list, attributes: dict, chunk_sizes: dict, data_locs: list,
                       finalsize: int, push_forward: list, client: Client, append_dim: str = 'time',
                       write_in_parallel: bool = False, skip_dask: bool = False, show_progress: bool = True):
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
    push_forward
        list of [index of push, total amount to push] for each push
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
                        append_dim=append_dim, finalsize=finalsize, push_forward=push_forward)]
            else:
                futs.append([zarr_write(zarr_path, xarrays[cnt], None, chunk_sizes, data_locs[cnt],
                             append_dim=append_dim)])
    else:
        futs = [client.submit(zarr_write, zarr_path, xarrays[0], attributes, chunk_sizes, data_locs[0],
                              append_dim=append_dim, finalsize=finalsize, push_forward=push_forward)]
        #  I no longer show progress for the disk write, I find it creates too much stdout.  I just have a general
        #    progress bar for each operation.
        # if show_progress:
        #     progress(futs, multi=False)
        wait(futs)
        if len(xarrays) > 1:
            for i in range(len(xarrays) - 1):
                futs.append(client.submit(zarr_write, zarr_path, xarrays[i + 1], None, chunk_sizes,
                                          data_locs[i + 1], append_dim=append_dim))
                if not write_in_parallel:  # wait on each future, write one data chunk at a time
                    wait(futs)
            if write_in_parallel:  # don't wait on the futures until you append all of them
                # if show_progress:
                #     progress(futs, multi=False)
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


def zarr_remove_attribute(zarr_path: str, attr: str):
    """
    Remove the attribute matching the provided key from the datastore on disk
    
    Parameters
    ----------
    zarr_path
        path to zarr data store
    attr
        attribute key that you want to remove
    """
    zw = ZarrWrite(zarr_path)
    zw.remove_attribute(attr)


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
