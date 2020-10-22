import numpy as np
from dask.distributed import Client
import matplotlib.pyplot as plt
import datetime
from scipy.interpolate import griddata
from scipy.spatial import cKDTree

from HSTB.kluster.numba_helpers import hist2d_numba_seq, _hist2d_add


class BaseSurface:
    """
    First try at a basic surfacing class, implements single resolution gridding using numpy/scipy methods.  Requires
    1d data either provided in the init or via an npz file saved using this class.
    """

    def __init__(self, x=None, y=None, z=None, unc=None, crs=None, resolution=1, from_file=None):
        self.x = x
        self.y = y
        self.z = z
        self.unc = unc
        self.crs = crs
        self.resolution = resolution
        self.output_path = 'in_mem_{}'.format(datetime.datetime.utcnow().strftime('%H%M%S'))

        self.max_x = None
        self.min_x = None
        self.max_y = None
        self.min_y = None
        self.max_z = None
        self.min_z = None
        self.max_unc = None
        self.min_unc = None

        self.x_range = None
        self.y_range = None
        self.ranges = None
        self.node_x_loc = None
        self.node_y_loc = None

        self.cell_count = None
        self.basegrid = None
        self.surf = None
        self.surf_unc = None

        self.layernames = ['depth', 'density', 'uncertainty']
        self.layer_lookup = {'depth': 'surf', 'density': 'cell_count', 'uncertainty': 'surf_unc'}

        self.init_tests(from_file)

    def init_tests(self, from_file):
        """
        Validate arguments passed in the init.  xyz data must be one dimensional and of the same length.  If data
        contains NaN values, filter all variables to the array with the most NaNs.  Do the same filtering if variables
        provided are of different lengths.

        Data provided from a npz file must work in the load method.
        """

        nans = None
        if (self.x is not None) and (self.y is not None) and (self.z is not None):
            ndims = np.array([self.x.ndim, self.y.ndim, self.z.ndim])
            nans = [np.count_nonzero(np.isnan(var)) for var in [self.x, self.y, self.z]]
            if (len(np.unique(ndims)) > 1) or (np.unique(ndims)[0] > 1):
                raise ValueError('Found multiple dimensions in provided xyz data')

            shps = np.array([self.x.shape[0], self.y.shape[0], self.z.shape[0]])
            if len(np.unique(shps)) > 1:
                raise ValueError('Found different lengths on provided xyz data')
        elif from_file is not None:
            try:
                self.load(from_file)
            except:
                raise IOError('Unable to load from {}'.format(from_file))
        else:
            raise ValueError('User must provide either x,y,z data or from_file to read from.')

        if np.any(nans):
            print('Filtering nans from dataset...')
            shortest_idx = ~np.isnan([self.x, self.y, self.z, self.unc][np.argmax(nans)])
            self.x = self.x[shortest_idx]
            self.y = self.y[shortest_idx]
            self.z = self.z[shortest_idx]
            self.unc = self.unc[shortest_idx]

    def save(self, fpth: str):
        """
        Save the class attributes to numpy NpzFile

        Parameters
        ----------
        fpth
            str, file path to where you want to save your surface data
        """

        # no longer save all the class attributes to file, too much data
        # np.savez(fpth, **vars(self))

        # final step here, finalize the density layer.  We don't want a bunch of zeros in the array where there is
        #   no data.  We want NaNs instead so that plotting tools understand there is no data there
        if self.cell_count is not None:
            self.cell_count[self.cell_count == 0] = np.nan

        self.output_path = fpth
        if self.surf is not None and self.cell_count is not None:  # density and depth layers required for saving surface
            data = {'x_range': self.x_range, 'y_range': self.y_range, 'node_x_loc': self.node_x_loc, 'crs': self.crs,
                    'node_y_loc': self.node_y_loc, 'resolution': self.resolution, 'cell_count': self.cell_count,
                    'surf': self.surf, 'output_path': self.output_path}
            if self.surf_unc is not None:
                data['surf_unc'] = self.surf_unc
            np.savez(fpth, **data)
        else:
            print('Unable to save surface {}, either depth or density layers not found.'.format(fpth))

    def load(self, fpth: str):
        """
        Load the class attributes from numpy NpzFile.  Non np.array objects get wrapped in array, cast to float in
        that instance.

        Parameters
        ----------
        fpth
            str, file path from where you want to load surface data
        """

        dat = np.load(fpth, allow_pickle=True)
        kys = list(dat.keys())
        for k in kys:
            if k == 'output_path':
                self.__setattr__(k, str(dat[k]))
            elif not dat[k].shape:
                self.__setattr__(k, float(dat[k]))
            else:
                self.__setattr__(k, dat[k])

    def get_layer_by_name(self, layername: str):
        """
        Get the layer by the provided name

        Parameters
        ----------
        layername
            name of layer to access, ex: 'depth'

        Returns
        -------
        np.ndarray
            (x,y) for the provided layer name
        """

        if layername in list(self.layer_lookup.keys()):
            return self.__getattribute__(self.layer_lookup[layername])
        else:
            print('get_layer_by_name: Unable to find surface layer {}'.format(layername))
            return None

    def return_layer_names(self):
        """
        Return a list of layer names based on what layers exist in the BaseSurface instance.

        Returns
        -------
        list
            list of str surface layer names (ex: ['depth', 'density', 'uncertainty']
        """

        existing_layernames = []
        for lyrname, lyr in self.layer_lookup.items():
            if self.__getattribute__(lyr) is not None:
                existing_layernames.append(lyrname)
        return existing_layernames

    def compute_extents(self):
        """
        Use the 1d x,y,z,u arrays to build out the extents for each variable.  Important for building out the grid
        later.
        """

        self.max_x = float(np.max(self.x))  # cast to float if inputs are xarray Dataarray
        self.min_x = float(np.min(self.x))
        self.max_y = float(np.max(self.y))
        self.min_y = float(np.min(self.y))
        self.max_z = float(np.max(self.z))
        self.min_z = float(np.min(self.z))
        if self.unc is not None:
            self.max_unc = float(np.max(self.unc))
            self.min_unc = float(np.min(self.unc))

    def construct_base_grid(self):
        """
        Use the extents to build the basegrid, the mesh grid built from the min/max northing/easting coordinates.
        """

        self.compute_extents()

        rounded_min_x, rounded_max_x = int(self.min_x), int(np.ceil(self.max_x))
        rounded_min_y, rounded_max_y = int(self.min_y), int(np.ceil(self.max_y))

        x_diff = int(rounded_max_x - rounded_min_x)
        x_spaces = int(np.ceil(x_diff / self.resolution)) + 1
        y_diff = int(rounded_max_y - rounded_min_y)
        y_spaces = int(np.ceil(y_diff / self.resolution)) + 1

        self.x_range = np.linspace(rounded_min_x, rounded_max_x, x_spaces)
        self.y_range = np.linspace(rounded_min_y, rounded_max_y, y_spaces)
        self.ranges = np.array([[rounded_min_x, rounded_max_x], [rounded_min_y, rounded_max_y]])

        self.node_x_loc = self.x_range[:-1] + np.diff(self.x_range)/2
        self.node_y_loc = self.y_range[:-1] + np.diff(self.y_range)/2
        self.basegrid = np.mgrid[rounded_min_x:rounded_max_x:self.resolution, rounded_min_y:rounded_max_y:self.resolution]

    def build_histogram(self, client: Client = None):
        """
        Use numpy histogram2d to build out the counts for each cell.  Important if we go to filter out cells that have
        insufficient density (soundings per cell)

        Parameters
        ----------
        client
            optional dask client, if provided will map to cluster
        """

        if self.x_range is None:
            self.construct_base_grid()

        # numpy histogram2d is slow as hell
        # self.cell_count, xedges, yedges = np.histogram2d(self.x, self.y, bins=(self.x_range, self.y_range))

        bins = np.array([len(self.x_range) - 1, len(self.y_range) - 1])
        if client is not None:
            # first index of chunks is the chunks in the 1st dim
            strt = 0
            chnks = []
            for c in self.x.chunks[0]:
                chnks.append([strt, strt + c])
                strt += c
            bin_futs = client.scatter([bins] * len(chnks))
            range_futs = client.scatter([self.ranges] * len(chnks))
            x_futs = client.scatter([self.x[c[0]:c[1]].values for c in chnks])
            y_futs = client.scatter([self.y[c[0]:c[1]].values for c in chnks])

            rslt = client.map(hist2d_numba_seq, x_futs, y_futs, bin_futs, range_futs)
            summed_rslt = client.submit(_hist2d_add, rslt)
            self.cell_count = summed_rslt.result()
        else:
            try:
                self.cell_count = hist2d_numba_seq(self.x.values, self.y.values, bins, self.ranges)
            except AttributeError:  # numpy workflow
                self.cell_count = hist2d_numba_seq(self.x, self.y, bins, self.ranges)

    def calculate_grid_indices(self, x: np.array, y: np.array, only_nearest: bool = False, dist_scaling: float = 0.25):
        """
        With provided x and y values, return the grid cells those values fall under.  Numpy digitize returns 0 when
        the value is lower than the lowest bin and len(bin) when it is higher than the highest bin.  We return -1 to
        get the correct indices

        If only_nearest, use cKDTree to find the nearest points to each grid node location within self.resolution times
        the provided dist_scaling float.

        Parameters
        ----------
        x
            numpy array, x values to be binned
        y
            numpy array, y values to be binned
        only_nearest
            bool, if true, returns only those points that fall within resolution/4 distance of the grid node
        dist_scaling
            float, cKDTree query uses a distance_upper_bound of self.resolution times this parameter

        Returns
        -------
        np.ndarray
            2d array [x, y] containing grid indices for each provided x y pair.  If both are -1, the pair falls outside
            of the grid
        """

        if self.surf is None:
            raise ValueError('Must run build_surfaces first')

        d = None
        if only_nearest:
            node_pts = np.stack(np.meshgrid(self.node_x_loc, self.node_y_loc), -1).reshape(-1, 2)
            raw_pts = np.c_[x, y]
            tree = cKDTree(node_pts)
            d, i = tree.query(raw_pts, distance_upper_bound=self.resolution * dist_scaling)

        digitized_x = np.digitize(x, self.x_range)
        digitized_y = np.digitize(y, self.y_range)

        # use zero as a nodatavalue to indicate that the provided soundings are outside the grid
        digitized_y[np.where(digitized_x == 0)] = 0
        digitized_y[np.where(digitized_x == len(self.x_range))] = 0
        digitized_y[np.where(digitized_y == len(self.y_range))] = 0

        digitized_x[np.where(digitized_y == 0)] = 0
        digitized_x[np.where(digitized_y == len(self.x_range))] = 0
        digitized_x[np.where(digitized_x == len(self.y_range))] = 0

        if d is not None:
            out_of_bounds = np.where(np.isinf(d))[0]
            if out_of_bounds.size > 0:
                digitized_x[out_of_bounds] = 0
                digitized_y[out_of_bounds] = 0

        digitized_x = digitized_x - 1
        digitized_y = digitized_y - 1

        return digitized_x, digitized_y

    def surf_scipy_griddata(self, arr: np.array, method: str = 'linear', count_msk: int = 0):
        """
        Use scipy griddata to interpolate the data according to the given method.  If a count_msk is provided,
        replace cell values with nan if the total soundings per cell is less than count_msk.

        Looking at what these methods actually do:

        - nearest = super fast, pretty self explanatory, returns nearest to given points
        - linear = delaunay triangulation, build triangles where no other point is inside the circumcircle of the
                   triangle, this helps to ensure no small sliver triangles are formed.  Then just find the triangles
                   that fit the input points and interpolate
        - cubic = triangulate and then construct bivariate interpolating polynomial for each triangle, see
                  Clough-Tocher scheme

        Parameters
        ----------
        arr
            numpy array, array to interp according to basegrid.  Probably one of self.z, self.unc
        method
            str, one of ['linear', 'nearest', 'cubic']
        count_msk
            int, threshold for minimum number of soundings per cell

        Returns
        -------
        np.ndarray
            numpy ndarray (x,y) for interpolated depth grid
        """

        if count_msk and self.cell_count is None:
            raise ValueError('Must run build_histogram first and provide a minimum number of soundings per cell')
        try:
            surf = griddata(np.c_[self.x.values, self.y.values], arr.values, (self.basegrid[0], self.basegrid[1]), method=method)
        except AttributeError:  # numpy workflow
            surf = griddata(np.c_[self.x, self.y], arr, (self.basegrid[0], self.basegrid[1]), method=method)

        if count_msk:
            surf[self.cell_count < count_msk] = np.nan
        return surf

    def build_surfaces(self, method: str = 'linear', count_msk: int = 1):
        """
        Build out depth and uncertainty surfaces if uncertainty is provided

        Parameters
        ----------
        method
            str, one of ['linear', 'nearest', 'cubic']
        count_msk
            int, required number of soundings per node
        """

        print('Building depth surface...')
        self.surf = self.surf_scipy_griddata(self.z, method=method, count_msk=count_msk)
        if self.unc is not None:
            print('Building uncertainty surface...')
            self.surf_unc = self.surf_scipy_griddata(self.unc, count_msk=count_msk)

    def return_surf_xyz(self):
        """
        Return the xyz grid values as well as an index for the valid nodes in the surface

        Returns
        -------
        surf_x
            numpy array, 1d x locations for the grid nodes
        surf_y
            numpy array, 1d y locations for the grid nodes
        surf_z
            numpy 2d array, 2d grid depth values
        valid_nodes
            numpy 2d array, boolean mask for valid nodes with depth
        """

        if self.surf is None:
            raise ValueError('No surface found, please run a gridding mode first')
        valid_nodes = ~np.isnan(self.surf)
        surf_z = self.surf
        surf_x = self.node_x_loc
        surf_y = self.node_y_loc
        return surf_x, surf_y, surf_z, valid_nodes

    def plot_var(self, varname):
        """
        With given variable name, build matplotlib imshow for x/y/varname

        Parameters
        ----------
        varname: str, one of ['density', 'depth', 'uncertainty']
        """

        if varname.lower() == 'density':
            if self.cell_count is None:
                raise ValueError('Must run build_histogram first and provide a minimum number of soundings per cell')
            # currently create a copy because we want to plot with NaNs but we want to retain zeros in the data
            dat = np.ma.masked_where(self.cell_count == 0, self.cell_count, copy=True)
        elif varname.lower() == 'depth':
            dat = self.surf
        elif varname.lower() == 'uncertainty':
            dat = self.surf_unc

        titl = 'Surface {} (mean: {})'.format(varname.lower(), np.round(np.nanmean(dat), 3))
        extents = [np.min(self.node_x_loc), np.max(self.node_x_loc), np.min(self.node_y_loc), np.max(self.node_y_loc)]

        fig = plt.figure()
        plt.subplot(111)
        plt.imshow(dat.T, origin='lower', extent=extents)
        plt.xlabel('X grid locs')
        plt.ylabel('Y grid locs')
        plt.title(titl)
        plt.colorbar()
        plt.show()
