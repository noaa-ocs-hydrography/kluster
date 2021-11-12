import numpy as np
import xarray as xr
from copy import deepcopy
from typing import Union

import matplotlib.path as mpl_path
from pyproj import CRS, Transformer

from HSTB.kluster.xarray_helpers import slice_xarray_by_dim
from HSTB.kluster.modules.georeference import polygon_to_geohashes
from HSTB.kluster import kluster_variables


class FqprSubset:
    """
    Tools for building a subset, or a smaller piece of the larger Fqpr instance.

    fqpr = fully qualified ping record, the term for the datastore in kluster

    Processed fqpr_generation.Fqpr instance is passed in as argument
    """

    def __init__(self, fqpr):
        """

        Parameters
        ----------
        fqpr
            Fqpr instance to export from
        """
        self.fqpr = fqpr
        self.backup_fqpr = {}
        self.subset_mintime = 0
        self.subset_maxtime = 0
        self.subset_lines = []

        self.ping_filter = None

    @property
    def is_subset(self):
        if self.backup_fqpr == {}:
            return False
        else:
            return True

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
            mintime = np.min([rp.time.values[0] for rp in self.fqpr.multibeam.raw_ping])
        if maxtime is None and mintime is not None:
            maxtime = np.max([rp.time.values[-1] for rp in self.fqpr.multibeam.raw_ping])
        if mintime is None and maxtime is None:
            raise ValueError('subset_by_time: either mintime or maxtime must be provided to subset by time')

        if self.backup_fqpr != {}:
            self.restore_subset()
        self.subset_mintime = mintime
        self.subset_maxtime = maxtime
        self.subset_lines = []
        self.backup_fqpr['raw_ping'] = [ping.copy() for ping in self.fqpr.multibeam.raw_ping]
        self.backup_fqpr['raw_att'] = self.fqpr.multibeam.raw_att.copy()

        slice_raw_ping = []
        for ra in self.fqpr.multibeam.raw_ping:
            slice_ra = slice_xarray_by_dim(ra, dimname='time', start_time=mintime, end_time=maxtime)
            slice_raw_ping.append(slice_ra)
        self.fqpr.multibeam.raw_ping = slice_raw_ping
        self.fqpr.multibeam.raw_att = slice_xarray_by_dim(self.fqpr.multibeam.raw_att, dimname='time', start_time=mintime, end_time=maxtime)

    def subset_by_lines(self, line_names: Union[str, list]):
        """
        Use the logged start time, end time of the multibeam files to exclude data from all other lines.  The result is
        the data only for the lines provided, concatenated into a single dataset.

        To return to the full original dataset, use restore_subset

        Parameters
        ----------
        line_names
            multibeam file names that you want to include in the subset datasets, all other lines are excluded
        """

        if self.backup_fqpr != {}:
            self.restore_subset()
        self.subset_mintime = 0
        self.subset_maxtime = 0
        self.backup_fqpr['raw_ping'] = [ping.copy() for ping in self.fqpr.multibeam.raw_ping]
        self.backup_fqpr['raw_att'] = self.fqpr.multibeam.raw_att.copy()

        mfiles = self.fqpr.return_line_dict(line_names=line_names)
        # ensure files are sorted by line start time, so the resultant dataset is also sorted when you concatenate
        mfiles = dict(sorted(mfiles.items(), key=lambda tme: tme[1][0]))
        self.subset_lines = list(mfiles.keys())
        original_lines = list(self.fqpr.multibeam.raw_ping[0].multibeam_files.keys())
        slice_raw_ping = []
        for ra in self.fqpr.multibeam.raw_ping:
            final_ra = None
            for linename in self.subset_lines:
                starttime, endtime = mfiles[linename]
                slice_ra = slice_xarray_by_dim(ra, dimname='time', start_time=starttime, end_time=endtime)
                if final_ra:
                    final_ra = xr.concat([final_ra, slice_ra], dim='time')
                else:
                    final_ra = slice_ra
            slice_raw_ping.append(final_ra)
        self.fqpr.multibeam.raw_ping = slice_raw_ping
        # ensure the multibeam files that we say are in this dataset match the subset of files
        [self.fqpr.multibeam.raw_ping[0].multibeam_files.pop(mfil) for mfil in original_lines if mfil not in mfiles.keys()]

        final_att = None
        for linename in self.subset_lines:
            starttime, endtime = mfiles[linename]
            slice_nav = slice_xarray_by_dim(self.fqpr.multibeam.raw_att, dimname='time', start_time=starttime, end_time=endtime)
            if final_att:
                final_att = xr.concat([final_att, slice_nav], dim='time')
            else:
                final_att = slice_nav
        self.fqpr.multibeam.raw_att = final_att

    def restore_subset(self):
        """
        Restores the original data if subset_by_time has been run.
        """

        if self.backup_fqpr != {}:
            self.fqpr.multibeam.raw_ping = self.backup_fqpr['raw_ping']
            self.fqpr.multibeam.raw_att = self.backup_fqpr['raw_att']
            self.backup_fqpr = {}
            self.subset_maxtime = 0
            self.subset_mintime = 0
            self.subset_lines = []
        else:
            self.fqpr.logger.error('restore_subset: no subset found to restore from')
            raise ValueError('restore_subset: no subset found to restore from')

    def redo_subset(self):
        """
        Subset by the existing subset times, used after reload to get the subset back
        """
        self.backup_fqpr = {}
        if self.subset_mintime and self.subset_maxtime:
            self.subset_by_time(self.subset_mintime, self.subset_maxtime)
        elif self.subset_lines:
            self.subset_by_lines(self.subset_lines)

    def subset_variables(self, variable_selection: list, ping_times: Union[np.array, float, tuple] = None,
                         skip_subset_by_time: bool = False, filter_by_detection: bool = False):
        """
        Take specific variable names and return just those variables in a new xarray dataset.  If you provide ping_times,
        either as the minmax of a range or individual times, it will return the variables just within those times.

        Parameters
        ----------
        variable_selection
            variable names you want from the fqpr dataset
        ping_times
            time to select the dataset by, can either be an array of times (will use the min/max of the array to subset),
            a float for a single time, or a tuple of (min time, max time).  If None, will use the min/max of the dataset
        skip_subset_by_time
            if True, will not run the subset by time method
        filter_by_detection
            if True, will filter the dataset by the detection info flag = 2 (rejected by multibeam system)

        Returns
        -------
        xr.Dataset
            Dataset with the
        """

        if filter_by_detection and 'detectioninfo' not in variable_selection:
            variable_selection.append('detectioninfo')
        if not skip_subset_by_time:
            if ping_times is None:
                ping_times = (np.min([rp.time.values[0] for rp in self.fqpr.multibeam.raw_ping]),
                              np.max([rp.time.values[-1] for rp in self.fqpr.multibeam.raw_ping]))

            if isinstance(ping_times, float):
                min_time, max_time = ping_times, ping_times
            elif isinstance(ping_times, tuple):
                min_time, max_time = ping_times
            else:
                min_time, max_time = float(np.min(ping_times)), float(np.max(ping_times))

            self.subset_by_time(min_time, max_time)

        dataset_variables = {}
        maxbeams = 0
        times = np.concatenate([rp.time.values for rp in self.fqpr.multibeam.raw_ping]).flatten()
        systems = np.concatenate([[rp.system_identifier] * rp.time.shape[0] for rp in self.fqpr.multibeam.raw_ping]).flatten().astype(np.int32)
        for var in variable_selection:
            if self.fqpr.multibeam.raw_ping[0][var].ndim == 2:
                if self.fqpr.multibeam.raw_ping[0][var].dims == ('time', 'beam'):
                    dataset_variables[var] = (['time', 'beam'], np.concatenate([rp[var] for rp in self.fqpr.multibeam.raw_ping]))
                    newmaxbeams = self.fqpr.multibeam.raw_ping[0][var].shape[1]
                    if maxbeams and maxbeams != newmaxbeams:
                        raise ValueError('Found multiple max beam number values for the different ping datasets, {} and {}, beam shapes must match'.format(maxbeams, newmaxbeams))
                    else:
                        maxbeams = newmaxbeams
                else:
                    raise ValueError('Only time and beam dimensions are suppoted, found {} for {}'.format(self.fqpr.multibeam.raw_ping[0][var].dims, var))
            elif self.fqpr.multibeam.raw_ping[0][var].ndim == 1:
                if self.fqpr.multibeam.raw_ping[0][var].dims == ('time',):
                    dataset_variables[var] = (['time'], np.concatenate([rp[var] for rp in self.fqpr.multibeam.raw_ping]))
                else:
                    raise ValueError('Only time dimension is suppoted, found {} for {}'.format(self.fqpr.multibeam.raw_ping[0][var].dims, var))
            else:
                raise ValueError('Only 2 and 1 dimension variables are supported, {}} is {} dim'.format(var, self.fqpr.multibeam.raw_ping[0][var].ndim))

        dataset_variables['system_identifier'] = (['time'], systems)
        if maxbeams:  # when variables are a mix of time time/beam dimensions
            coords = {'time': times, 'beam': np.arange(maxbeams)}
        else:  # when variables are just time dimension
            coords = {'time': times}
        dset = xr.Dataset(dataset_variables, coords, attrs=deepcopy(self.fqpr.multibeam.raw_ping[0].attrs))
        dset = dset.sortby('time')

        if filter_by_detection:
            dset = filter_subset_by_detection(dset)
        if not skip_subset_by_time:
            self.restore_subset()

        return dset

    def subset_variables_by_line(self, variable_selection: list, line_names: Union[str, list] = None, ping_times: tuple = None,
                                 filter_by_detection: bool = False):
        """
        Apply subset_variables to get the data split up into lines for the variable_selection provided

        Parameters
        ----------
        variable_selection
            variable names you want from the fqpr dataset
        line_names
            if provided, only returns data for the line(s), otherwise, returns data for all lines
        ping_times
            time to select the dataset by, must be a tuple of (min time, max time) in utc seconds.  If None, will use
            the full min/max time of the dataset
        filter_by_detection
            if True, will filter the dataset by the detection info flag = 2 (rejected by multibeam system)

        Returns
        -------
        dict
            dict of {linename: xr.Dataset} for each line name in the dataset (or just for the line names provided
            if you provide line names)
        """

        mfiles = self.fqpr.return_line_dict(line_names=line_names, ping_times=ping_times)

        return_data = {}
        for linename in mfiles.keys():
            starttime, endtime = mfiles[linename]
            dset = self.subset_variables(variable_selection, ping_times=(starttime, endtime), skip_subset_by_time=False,
                                         filter_by_detection=filter_by_detection)
            return_data[linename] = dset
        return return_data

    def _soundings_by_poly(self, geo_polygon: np.ndarray, proj_polygon: np.ndarray, variable_selection: tuple):
        """
        Return soundings and sounding attributes that are within the box formed by the provided coordinates.

        Parameters
        ----------
        geo_polygon
            (N, 2) array of points that make up the selection polygon, (x, y) in Fqpr CRS
        proj_polygon
            (N, 2) array of points that make up the selection polygon, (longitude, latitude) in Fqpr CRS
        variable_selection
            list of the variables that you want to return for the soundings in the polygon

        Returns
        -------
        list
            list of numpy arrays for each variable in variable selection
        """

        data_vars = [[] for _ in variable_selection]
        self.ping_filter = []
        polypath = mpl_path.Path(proj_polygon)
        for rpcnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
            if 'z' not in rp:
                continue
            insidedata, intersectdata = filter_subset_by_polygon(rp, geo_polygon)
            base_filter = np.zeros(rp.x.shape[0] * rp.x.shape[1], dtype=bool)
            if insidedata or intersectdata:
                if insidedata:
                    for mline, mdata in insidedata.items():
                        linemask, startidx, endidx, starttime, endtime = mdata
                        slice_pd = slice_xarray_by_dim(rp, dimname='time', start_time=starttime, end_time=endtime)
                        base_filter[startidx:endidx][linemask] = True
                        stacked_slice = slice_pd.stack({'sounding': ('time', 'beam')})
                        for cnt, dvarname in enumerate(variable_selection):
                            if dvarname == 'head':
                                data_vars[cnt].append(np.full(stacked_slice.beampointingangle[linemask].shape, rpcnt, dtype=np.int8))
                            else:
                                data_vars[cnt].append(stacked_slice[dvarname][linemask].values)
                if intersectdata:
                    for mline, mdata in intersectdata.items():
                        linemask, startidx, endidx, starttime, endtime = mdata
                        # only brute force check those points that are in intersecting geohash regions
                        slice_pd = slice_xarray_by_dim(rp, dimname='time', start_time=starttime, end_time=endtime)
                        xintersect, yintersect = np.ravel(slice_pd.x), np.ravel(slice_pd.y)
                        filt = polypath.contains_points(np.c_[xintersect[linemask], yintersect[linemask]])
                        base_filter[startidx:endidx][linemask] = filt
                        stacked_slice = slice_pd.stack({'sounding': ('time', 'beam')})
                        for cnt, dvarname in enumerate(variable_selection):
                            if dvarname == 'head':
                                data_vars[cnt].append(np.full(stacked_slice.beampointingangle[linemask][filt].shape, rpcnt, dtype=np.int8))
                            else:
                                data_vars[cnt].append(stacked_slice[dvarname][linemask][filt].values)
            self.ping_filter.append(base_filter)
        return data_vars

    def _build_polygons(self, polygon: np.ndarray, geographic: bool):
        """
        Build separate geographic/projected polygon coordinates from the provided polygon coordinates

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon
        geographic
            If True, the coordinates provided are geographic (latitude/longitude)

        Returns
        -------
        polygon
            (N, 2) array of points that make up the selection polygon in geographic coordinates
        polygon
            (N, 2) array of points that make up the selection polygon in projected coordinates in the Fqpr instance coordinate system
        """
        if isinstance(polygon, list):
            polygon = np.array(polygon)

        if not geographic:
            proj_polygon = polygon
            trans = Transformer.from_crs(CRS.from_epsg(self.fqpr.multibeam.raw_ping[0].horizontal_crs),
                                         CRS.from_epsg(kluster_variables.epsg_wgs84), always_xy=True)
            polyx, polyy = trans.transform(polygon[:, 0], polygon[:, 1])
            geo_polygon = np.c_[polyx, polyy]
        else:
            geo_polygon = polygon
            trans = Transformer.from_crs(CRS.from_epsg(kluster_variables.epsg_wgs84),
                                         CRS.from_epsg(self.fqpr.multibeam.raw_ping[0].horizontal_crs), always_xy=True)
            polyx, polyy = trans.transform(polygon[:, 0], polygon[:, 1])
            proj_polygon = np.c_[polyx, polyy]
        return geo_polygon, proj_polygon

    def return_soundings_in_polygon(self, polygon: np.ndarray, geographic: bool = True,
                                    variable_selection: tuple = ('head', 'x', 'y', 'z', 'tvu', 'detectioninfo', 'time', 'beam')):
        """
        Using provided coordinates (in either horizontal_crs projected or geographic coordinates), return the soundings
        and sounding attributes for all soundings within the coordinates.

        Using this method sets the ping_filter attribute so that you can now use the set_variable_by_filter and
        get_variable_by_filter methods to get other variables or set data within the polygon selection.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon
        geographic
            If True, the coordinates provided are geographic (latitude/longitude)
        variable_selection
            list of the variables that you want to return for the soundings in the polygon

        Returns
        -------
        list
            list of numpy arrays for each variable in variable selection
        """

        not_valid_variable = [vr for vr in variable_selection if vr not in kluster_variables.subset_variable_selection]
        if not_valid_variable:
            raise NotImplementedError('These variables are not currently implemented within return_soundings_in_polygon, see '
                                      'set_filter_by_polygon and get_variable_by_filter to use these variables: {}'.format(not_valid_variable))
        if 'horizontal_crs' not in self.fqpr.multibeam.raw_ping[0].attrs or 'z' not in self.fqpr.multibeam.raw_ping[0].variables.keys():
            raise ValueError('Georeferencing has not been run yet, you must georeference before you can get soundings')

        geo_polygon, proj_polygon = self._build_polygons(polygon, geographic)
        data_vars = self._soundings_by_poly(geo_polygon, proj_polygon, variable_selection)

        if len(data_vars[0]) > 1:
            data_vars = [np.concatenate(x) for x in data_vars]
        elif len(data_vars[0]) == 1:
            data_vars = [x[0] for x in data_vars]
        else:
            data_vars = [None for x in data_vars]
        return data_vars

    def set_filter_by_polygon(self, polygon: np.ndarray, geographic: bool = True):
        """
        Using this method sets the ping_filter attribute so that you can now use the set_variable_by_filter and
        get_variable_by_filter methods to get other variables or set data within the polygon selection.

        This is an alternative to return_soundings_in_polygon that you can use if you want to set the filter without
        loading/returning a lot of data.

        Parameters
        ----------
        polygon
            (N, 2) array of points that make up the selection polygon
        geographic
            If True, the coordinates provided are geographic (latitude/longitude)
        """

        if 'horizontal_crs' not in self.fqpr.multibeam.raw_ping[0].attrs or 'z' not in self.fqpr.multibeam.raw_ping[0].variables.keys():
            raise ValueError('Georeferencing has not been run yet, you must georeference before you can get soundings')

        geo_polygon, proj_polygon = self._build_polygons(polygon, geographic)

        self.ping_filter = []
        polypath = mpl_path.Path(proj_polygon)
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
            insidedata, intersectdata = filter_subset_by_polygon(rp, geo_polygon)
            base_filter = np.zeros(rp.x.shape[0] * rp.x.shape[1], dtype=bool)
            if insidedata or intersectdata:
                if insidedata:
                    for mline, mdata in insidedata.items():
                        linemask, startidx, endidx, starttime, endtime = mdata
                        base_filter[startidx:endidx] = linemask
                if intersectdata:
                    for mline, mdata in intersectdata.items():
                        linemask, startidx, endidx, starttime, endtime = mdata
                        # only brute force check those points that are in intersecting geohash regions
                        slice_pd = slice_xarray_by_dim(rp, dimname='time', start_time=starttime, end_time=endtime)
                        xintersect, yintersect = np.ravel(slice_pd.x), np.ravel(slice_pd.y)
                        filt = polypath.contains_points(np.c_[xintersect[linemask], yintersect[linemask]])
                        base_filter[startidx:endidx][linemask] = filt
            self.ping_filter.append(base_filter)

    def set_variable_by_filter(self, variable_name: str, new_data: Union[np.array, list, float, int, str], selected_index: list = None):
        """
        ping_filter is set upon selecting points in 2d/3d in Kluster.  See return_soundings_in_polygon.  Here we can take
        those points and set one of the variables with new data.  Optionally, you can include a selected_index that is a list
        of flattened indices to points in the ping_filter that you want to super-select.  See kluster_main.set_pointsview_points_status

        new data are set in memory and saved to disk

        Parameters
        ----------
        variable_name
            name of the variable to set, i.e. 'detectioninfo'
        new_data
            new data to set to the soundings selected by ping_filter for this variable.  Generally used for setting a new
            sounding flag in detectioninfo, where all selected soundings would have new_data = 1 or 2
        selected_index
            super_selection of the ping_filter selection, done in points_view currently when selecting with the mouse
        """

        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
            ping_filter = self.fqpr.subset.ping_filter[cnt]
            data_var = rp[variable_name]
            if selected_index:
                try:
                    rp_points_idx = selected_index[cnt]
                except:  # no selected soundings, happens for second head when no selected soundings found for second head
                    continue
                point_idx = np.unravel_index(np.where(ping_filter)[0][rp_points_idx], data_var.shape)
            else:
                point_idx = np.unravel_index(np.where(ping_filter)[0], data_var.shape)
            if not point_idx[0].any():  # no selected soundings, happens for first head in dual head when no selected soundings found for first head
                continue
            unique_time_vals, utime_index = np.unique(point_idx[0], return_inverse=True)
            rp_detect = data_var.isel(time=unique_time_vals).load()
            rp_detect_vals = rp_detect.values
            if isinstance(new_data, list) and len(new_data) == len(self.fqpr.multibeam.raw_ping):
                # new data is indexed by head
                rp_detect_vals[utime_index, point_idx[1]] = new_data[cnt]
            else:  # new data is a simple replacement, is identical between heads
                rp_detect_vals[utime_index, point_idx[1]] = new_data

            rp_detect[:] = rp_detect_vals
            self.fqpr.write('ping', [rp_detect.to_dataset()], time_array=[rp_detect.time], sys_id=rp.system_identifier,
                            skip_dask=True)

    def get_variable_by_filter(self, variable_name: str, selected_index: list = None):
        """
        ping_filter is set upon selecting points in 2d/3d in Kluster.  See return_soundings_in_polygon.  Here we can take
        those points and set one of the variables with new data.  Optionally, you can include a selected_index that is a list
        of flattened indices to points in the ping_filter that you want to super-select.  See kluster_main.set_pointsview_points_status

        new data are set in memory and saved to disk

        Parameters
        ----------
        variable_name
            name of the variable to set, i.e. 'detectioninfo'
        selected_index
            super_selection of the ping_filter selection, done in points_view currently when selecting with the mouse
        """

        datablock = []
        raw_att = self.fqpr.multibeam.raw_att
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
            ping_filter = self.fqpr.subset.ping_filter[cnt]
            # must have a 2d variable to determine the correct shape of the index, beampointingangle should always exist in converted kluster data
            data_var_2d = rp['beampointingangle']
            if selected_index:
                rp_points_idx = selected_index[cnt]
                point_idx = np.unravel_index(np.where(ping_filter)[0][rp_points_idx], data_var_2d.shape)
            else:
                point_idx = np.unravel_index(np.where(ping_filter)[0], data_var_2d.shape)
            unique_time_vals, utime_index = np.unique(point_idx[0], return_inverse=True)
            if variable_name in rp:
                data_var = rp[variable_name]
                subsetdata = data_var.isel(time=unique_time_vals).load()
                subsetdata_vals = subsetdata.values
                try:  # 2d variable
                    datablock.append(subsetdata_vals[utime_index, point_idx[1]])
                except:
                    datablock.append(subsetdata_vals[utime_index])
            elif variable_name in raw_att:
                data_var = raw_att[variable_name]
                unique_times = rp.time[unique_time_vals]
                subsetdata = data_var.sel(time=unique_times, method='nearest')
                subsetdata_vals = subsetdata.values
                datablock.append(subsetdata_vals[utime_index])

        datablock = np.concatenate(datablock)
        return datablock


def filter_subset_by_detection(ping_dataset: xr.Dataset):
    """
    Get only the non-rejected soundings.  Additionally, drop all the NaN values where we did not get a georeferenced
    answer.  Returns the dataset stacked (sounding=(time, beam)) so all variables are one dimensional.  We do
    this to make the filtering work, as it results in a non square array.

    Parameters
    ----------
    ping_dataset
        one of the raw_ping datasets

    Returns
    -------
    xr.Dataset
        1dim stacked ping dataset
    """

    # get to a 1dim space for filtering
    ping_dataset = ping_dataset.stack({'sounding': ('time', 'beam')})

    # first drop all nans, all the georeference variables (xyz) should have NaNs in the same place
    if 'x' in ping_dataset.variables:
        nan_mask = ~np.isnan(ping_dataset['x'])
        ping_dataset = ping_dataset.isel(sounding=nan_mask)
    elif 'y' in ping_dataset.variables:
        nan_mask = ~np.isnan(ping_dataset['y'])
        ping_dataset = ping_dataset.isel(sounding=nan_mask)
    elif 'z' in ping_dataset.variables:
        nan_mask = ~np.isnan(ping_dataset['z'])
        ping_dataset = ping_dataset.isel(sounding=nan_mask)
    else:  # no georeferenced data found
        print('Warning: Unable to filter by sounding flag, no georeferenced data found')
        pass

    # rejected soundings are where detectioninfo=2
    dinfo = ping_dataset.detectioninfo
    valid_detections = dinfo != 2
    ping_dataset = ping_dataset.isel(sounding=valid_detections)
    ping_dataset = ping_dataset.drop_vars('detectioninfo')
    return ping_dataset


def filter_subset_by_polygon(ping_dataset: xr.Dataset, polygon: np.array):
    """
    Given the provided polygon coordinates, return the part of the ping dataset that is completely within
    the polygon and the part of the dataset that intersects with the polygon

    Parameters
    ----------
    ping_dataset
        one of the multibeam.raw_ping datasets, containing the ping variables
    polygon
        coordinates of a polygon ex: np.array([[lon1, lat1], [lon2, lat2], ...]), first and last coordinate
        must be the same

    Returns
    -------
    xr.Dataset
        1dim flattened bool mask for soundings in a geohash that is completely within the polygon
    xr.Dataset
        1dim flattened bool mask for soundings in a geohash that intersects with the polygon
    """

    if 'geohash' in ping_dataset.variables:
        if 'geohashes' in ping_dataset.attrs:
            inside_mask_lines = {}
            intersect_mask_lines = {}
            gprecision = int(ping_dataset.geohash.dtype.str[2:])  # ex: dtype='|S7', precision=7
            innerhash, intersecthash = polygon_to_geohashes(polygon, precision=gprecision)
            for mline, mhashes in ping_dataset.attrs['geohashes'].items():
                linestart, lineend = ping_dataset.attrs['multibeam_files'][mline]
                mhashes = [x.encode() for x in mhashes]
                inside_geohash = [x for x in innerhash if x in mhashes]
                intersect_geohash = [x for x in intersecthash if x in mhashes and x not in inside_geohash]
                if inside_geohash or intersect_geohash:
                    slice_pd = slice_xarray_by_dim(ping_dataset, dimname='time', start_time=linestart, end_time=lineend)
                    ghash = np.ravel(slice_pd.geohash)
                    filt_start = int(np.where(ping_dataset.time == slice_pd.time[0])[0]) * ping_dataset.geohash.shape[1]
                    filt_end = filt_start + ghash.shape[0]
                    if inside_geohash:
                        linemask = np.in1d(ghash, inside_geohash)
                        inside_mask_lines[mline] = [linemask, filt_start, filt_end, linestart, lineend]
                    if intersect_geohash:
                        linemask = np.in1d(ghash, intersect_geohash)
                        intersect_mask_lines[mline] = [linemask, filt_start, filt_end, linestart, lineend]
            return inside_mask_lines, intersect_mask_lines
        else:  # treat dataset as if all the data needs to be brute force checked, i.e. all data intersects with polygon
            print('Warning: Unable to filter by polygon, cannot find the "geohashes" attribute in the ping record')
            return None, None
    else:
        print('Warning: Unable to filter by polygon, geohash variable not found')
        return None, None
