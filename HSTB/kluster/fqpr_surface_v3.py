import os
import numpy as np
import dask.array as da
import xarray as xr
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib
from mpl_toolkits.axes_grid1 import make_axes_locatable
from typing import Union
import pickle


def is_power_of_two(n: Union[int, float]):
    """
    Return True if number is a power of two, supports n>1 and n<1.

    Parameters
    ----------
    n
        number to check, can be float or int

    Returns
    -------
    bool
        number is power of two
    """

    if n > 1:
        if n != int(n):
            return False
        n = int(n)
        return (n != 0) and (n & (n - 1) == 0)
    elif n == 1:
        return True
    elif n > 0:
        return is_power_of_two(1/n)
    else:
        return False


def create_folder(output_directory: str, fldrname: str):
    """
    Generate a new folder with folder name fldrname in output_directory.  Will create output_directory if it does
    not exist.  If fldrname exists, will generate a folder with a time tag next to it instead.  Will always
    create a folder this way.

    Parameters
    ----------
    output_directory
        path to containing folder
    fldrname
        name of new folder to create

    Returns
    -------
    str
        path to the created folder
    """

    os.makedirs(output_directory, exist_ok=True)
    tstmp = datetime.now().strftime('%Y%m%d_%H%M%S')
    try:
        fldr_path = os.path.join(output_directory, fldrname)
        os.mkdir(fldr_path)
    except FileExistsError:
        fldr_path = os.path.join(output_directory, fldrname + '_{}'.format(tstmp))
        os.mkdir(fldr_path)
    return fldr_path


class QuadManager:
    """
    Parent manager of the QuadTree and the points that we are adding to the tree (self.data).  We keep these two things
    separate so that we can pickle the whole tree and not have to pull out the points from each quad to save the points.
    This way no iteration of the tree is necessary to get all the points.

    We record the xyz values for each quad that contains points in node_data.  Each quad that contributes to node_data
    (i.e. has points) has a quad_index, which is an index to the node_data list.

    This is the starting point for bulding a grid.  Use create() to build a new quadtree, supports points in numpy
    structured array, dask Arrays, and xarray Dataset formats.

    """
    def __init__(self):
        self.tree = None  # QuadTree instance
        self.data = None  # point data,
        self.node_data = []  # quad data, list of lists (until finalized into dask array for saving)

        self.sources = {}  # dict of container name, list of multibeam files
        self.coordinate_system = None  # epsg code
        self.vertical_reference = None  # string identifier for the vertical reference, one of 'ellipse' 'waterline'

        self.layernames = ['depth', 'horizontal_uncertainty', 'vertical_uncertainty']
        self.layer_lookup = {'depth': 'z', 'vertical_uncertainty': 'tvu', 'horizontal_uncertainty': 'thu'}

    def _convert_dataset(self):
        """
        We currently convert xarray Dataset input into a numpy structured array.  Xarry Datasets appear to be rather
        slow in testing, I believe because they do some stuff under the hood with matching coordinates when you do
        basic operations.  Also, you can't do any fancy indexing with xarray Dataset, at best you can use slice with isel.

        For all these reasons, we just convert to numpy.
        """
        allowed_vars = ['x', 'y', 'z', 'tvu', 'thu']
        dtyp = [(varname, self.data[varname].dtype) for varname in allowed_vars if varname in self.data]
        empty_struct = np.empty(len(self.data['x']), dtype=dtyp)
        for varname, vartype in dtyp:
            empty_struct[varname] = self.data[varname].values
        self.data = empty_struct

    def validate_input_data(self):
        """
        If parent is None (i.e. this is the entry point to the quad and the data is just now being examined) we ensure
        that it is a valid xarray/numpy structured array/dask array
        """
        if type(self.data) in [np.ndarray, da.Array]:
            if not self.data.dtype.names:
                raise ValueError('QuadTree: numpy array provided for data, but no names were found, array must be a structured array')
            if 'x' not in self.data.dtype.names or 'y' not in self.data.dtype.names:
                raise ValueError('QuadTree: numpy structured array provided for data, but "x" or "y" not found in variable names')
        elif type(self.data) == xr.Dataset:
            if 'x' not in self.data:
                raise ValueError('QuadTree: xarray Dataset provided for data, but "x" or "y" not found in variable names')
            if len(self.data.dims) > 1:
                raise ValueError('QuadTree: xarray Dataset provided for data, but found multiple dimensions, must be one dimensional: {}'.format(self.data.dims))
            self._convert_dataset()  # internally we just convert xarray dataset to numpy for ease of use
        else:
            raise ValueError('QuadTree: numpy structured array or dask array with "x" and "y" as variable must be provided')

    def update_metadata(self, container_name: Union[str, list] = None, multibeam_file_list: list = None, coordinate_system: str = None,
                        vertical_reference: str = None):
        """
        Update the quadtree metadata for the new data

        Parameters
        ----------
        container_name
            the folder name of the converted data, equivalent to splitting the output_path variable in the kluster
            dataset
        multibeam_file_list
            list of multibeam files that exist in the data to add to the grid
        coordinate_system
            epsg (or proj4 string) for the coordinate system of the data.  Proj4 only shows up when there is no valid
            epsg
        vertical_reference
            vertical reference of the data
        """

        if container_name:
            if isinstance(container_name, str):  # only one converted data instance provided
                container_name = [container_name]
            if isinstance(multibeam_file_list[0], str):  # only one converted data instance provided
                multibeam_file_list = [multibeam_file_list]
            for contname, mfiles in zip(container_name, multibeam_file_list):
                if mfiles:
                    if contname in self.sources and (set(mfiles) & set(self.sources[contname])):
                        raise ValueError('QuadManager: Found some of these new lines in the existing lines metadata for container: {}'.format(contname))
                    elif contname in self.sources:
                        self.sources[contname].extend(mfiles)
                    else:
                        self.sources[contname] = mfiles
                else:
                    self.sources[contname] = ['unknown']
        if self.coordinate_system and (self.coordinate_system != coordinate_system):
            raise ValueError('QuadManager: Found existing coordinate system {}, new coordinate system {} must match'.format(self.coordinate_system,
                                                                                                                            coordinate_system))
        if self.vertical_reference and (self.vertical_reference != vertical_reference):
            raise ValueError('QuadManager: Found existing vertical reference {}, new vertical reference {} must match'.format(self.vertical_reference,
                                                                                                                              vertical_reference))
        self.coordinate_system = coordinate_system
        self.vertical_reference = vertical_reference

    def finalize_data(self):
        """
        node_data starts out as list of lists, for fast appending when each quad is generated.  We finalize to dask for
        saving, to use the dask save methods and chunking.

        data is either a dask Array already or a numpy structured array.  Convert to dask for the same reasons, if that
        is needed.
        """
        if isinstance(self.node_data, list):
            struct_data = np.empty(len(self.node_data), dtype=self.data.dtype)
            datavals = np.array(self.node_data)
            for cnt, varname in enumerate(self.data.dtype.names):
                struct_data[varname] = datavals[:, cnt]
            self.node_data = da.from_array(struct_data)
        if isinstance(self.data, np.ndarray):
            self.data = da.from_array(self.data)

    def update_statistics(self):
        """
        After generating the surface, we add the layer statistics to the settings of the root quad
        """
        self.finalize_data()
        if 'z' in self.data.dtype.names:
            self.tree.settings['min_depth'] = np.min(self.node_data['z']).compute()
            self.tree.settings['max_depth'] = np.max(self.node_data['z']).compute()
        if 'tvu' in self.data.dtype.names:
            self.tree.settings['min_tvu'] = np.min(self.node_data['tvu']).compute()
            self.tree.settings['max_tvu'] = np.max(self.node_data['tvu']).compute()
        if 'thu' in self.data.dtype.names:
            self.tree.settings['min_thu'] = np.min(self.node_data['thu']).compute()
            self.tree.settings['max_thu'] = np.max(self.node_data['thu']).compute()

    def create(self, data: Union[xr.Dataset, da.Array, np.ndarray], container_name: Union[str, list] = None,
               multibeam_file_list: list = None, coordinate_system: str = None,
               vertical_reference: str = None, **kwargs):
        """
        Create a new QuadTree instance from the provided soundings

        Parameters
        ----------
        data
            Sounding data from Kluster.  Should contain at least 'x', 'y', 'z' variable names/data
        container_name
            the folder name of the converted data, equivalent to splitting the output_path variable in the kluster
            dataset
        multibeam_file_list
            list of multibeam files that exist in the data to add to the grid
        coordinate_system
            epsg (or proj4 string) for the coordinate system of the data.  Proj4 only shows up when there is no valid
            epsg
        vertical_reference
            vertical reference of the data
        kwargs
            Pass on to QuadTree
        """

        if isinstance(data, (da.Array, xr.Dataset)):
            data = data.compute()
        self.data = data
        self.validate_input_data()
        self.update_metadata(container_name, multibeam_file_list, coordinate_system, vertical_reference)
        self.tree = QuadTree(self, **kwargs)
        self.update_statistics()

    def save(self, folderpath, data_method='numpy', tree_method='pickle'):
        """
        Save the QuadTree to one of the tree_method formats and the point data to the data_method format.

        Parameters
        ----------
        folderpath
            output folder that will hold the saved data
        data_method
            method used to save the point data and quad data
        tree_method
            method used to save the tree and metadata

        Returns
        -------
        str
            Folder path to the gridded data save folder if you successfully saved
        """
        if self.node_data is not []:
            folderpath = create_folder(folderpath, 'grid')
            self.finalize_data()

            if tree_method == 'pickle':
                self._save_tree_pickle(folderpath)
            else:
                raise ValueError('QuadManager: tree saving method not supported: {}'.format(tree_method))

            if data_method == 'numpy':
                self._save_numpy(folderpath)
            else:
                raise ValueError('QuadManager: data saving method not supported: {}'.format(data_method))
            return folderpath
        else:
            return None

    def load(self, outputfolder, data_method='numpy', tree_method='pickle'):
        """
        Load the QuadTree from one of the tree_method formats and the point data/quad data from the data_method format.

        Parameters
        ----------
        outputfolder
            output folder that will hold the saved data
        data_method
            method used to save the point data and quad data
        tree_method
            method used to save the tree and metadata
        """
        if tree_method == 'pickle':
            self._load_tree_pickle(outputfolder)
        else:
            raise ValueError('QuadManager: tree loading method not supported: {}'.format(tree_method))

        if data_method == 'numpy':
            self._load_numpy(outputfolder)
        else:
            raise ValueError('QuadManager: data loading method not supported: {}'.format(data_method))

    def export(self, output_path: str, export_format: str = 'csv'):
        """
        Export the node data to one of the supported formats

        Parameters
        ----------
        output_path
            folder to contain the exported data
        export_format
            format option, one of 'csv'
        """

        if export_format == 'csv':
            self._export_csv(output_path)

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

        if self.node_data is not []:
            if layername in list(self.layer_lookup.keys()):
                return self.node_data[self.layer_lookup[layername]]
            else:
                print('get_layer_by_name: Unable to find {} in node data'.format(layername))
                return None
        else:
            print('get_layer_by_name: Unable to find node data for quad tree')
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
        if self.node_data is not []:
            for lyrname, lyr in self.layer_lookup.items():
                if self.node_data[lyr].any():
                    existing_layernames.append(lyrname)
        return existing_layernames

    def return_extents(self):
        """
        Return the extents of the whole tree, which is equivalent to the extents of the root quad

        Returns
        -------
        list
            [[minx, miny], [maxx, maxy]]
        """

        return [qm.tree.mins, qm.tree.maxs]

    def plot_surface(self, varname, tree=None, ax=None, cmap=None, norm=None):
        """
        With given variable name, build matplotlib imshow for x/y/varname

        Parameters
        ----------
        varname: str, one of ['depth', 'vertical_uncertainty', 'horizontal_uncertainty']
        """

        if tree is None:
            tree = self.tree
            norm = matplotlib.colors.Normalize(vmin=self.tree.settings['min_depth'],
                                               vmax=self.tree.settings['max_depth'])
            cmap = matplotlib.cm.rainbow
            varname = self.layer_lookup[varname]

        if ax is None:
            ax = plt.subplots(figsize=[11, 7], dpi=150)[1]

        if tree.quad_index != -1:
            sizes = [tree.maxs[0] - tree.mins[0], tree.maxs[1] - tree.mins[1]]
            quad_val = self.node_data[varname][tree.quad_index].compute()
            rect = matplotlib.patches.Rectangle(tree.mins, *sizes, zorder=2, alpha=0.5, lw=1,
                                                ec='red', fc=cmap(norm(quad_val)))
            ax.add_patch(rect)

        for child in tree.children:
            self.plot_surface(varname, child, ax, cmap, norm)

        if tree == self.tree:
            xsize = tree.maxs[0] - tree.mins[0]
            ysize = tree.maxs[1] - tree.mins[1]
            ax.set_ylim(tree.mins[1] - ysize / 10, tree.maxs[1] + ysize / 10)
            ax.set_xlim(tree.mins[0] - xsize / 10, tree.maxs[0] + xsize / 10)
            divider = make_axes_locatable(ax)
            cax = divider.append_axes('right', size='5%', pad=0.05)
            plt.gcf().colorbar(matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap), cax=cax, orientation='vertical',
                               label='Depth (+down, meters)')

        return ax

    def _export_csv(self, folderpath: str):
        """
        Export the node data to csv

        Parameters
        ----------
        folderpath
            folder to contain the exported data
        """

        data = self.node_data.compute()
        output_file = os.path.join(folderpath, 'csvwrite.csv')
        if os.path.exists(output_file):
            tstmp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(folderpath, 'csvwrite_{}.csv'.format(tstmp))

        # gdal expects sorted data for XYZ format, either 'x' or 'y' have to be sorted
        sortidx = np.argsort(data['x'])
        np.savetxt(output_file, np.stack([data[var][sortidx] for var in data.dtype.names], axis=1),
                   fmt=['%.3f' for var in data.dtype.names], delimiter=' ', comments='',
                   header=' '.join([nm for nm in data.dtype.names]))

    def _save_tree_pickle(self, folderpath):
        """
        Pickle the QuadManager and QuadTree objects.  Leave out the point data and the node_data, as those are going to
        be saved separately, in a chunked format

        We clear out the manager reference (refers to self) as we don't need to pickle that, repopulate it on load
        instead

        Parameters
        ----------
        folderpath
            output folder to contain the pickled save data
        """

        if not os.path.exists(folderpath):
            raise EnvironmentError('Unable to save pickle file to {}, does not exist'.format(folderpath))
        self.tree.manager = None

        picklefile = open(os.path.join(folderpath, "grid.pickle"), "wb")
        data = {}
        for ky, val in self.__dict__.items():
            if ky not in ['data', 'node_data']:
                data[ky] = val
        pickle.dump(data, picklefile, -1)

    def _load_tree_pickle(self, folderpath):
        """
        Load the pickled objects.  Set the manager reference to this object.

        Parameters
        ----------
        folderpath
            output folder that contains the pickled save data
        """
        pickle_file = os.path.join(folderpath, 'grid.pickle')
        if os.path.exists(pickle_file):
            pf = open(pickle_file, "rb")
            self.__dict__.update(pickle.load(pf))
        else:
            raise EnvironmentError('Unable to load pickle file {}, does not exist'.format(pickle_file))
        self.tree.manager = self

    def _save_numpy(self, folderpath: str):
        """
        Save the data to file, where each chunk of the dask array is saved as a numpy file

        Parameters
        ----------
        folderpath
            output folder to save the data to

        Returns
        -------
        folderpath
            output folder to contain the saved numpy data
        """

        if not os.path.exists(folderpath):
            raise EnvironmentError('Unable to save numpy data to {}, does not exist'.format(folderpath))
        da.to_npy_stack(folderpath + '/data', self.data)
        da.to_npy_stack(folderpath + '/node_data', self.node_data)

    def _load_numpy(self, folderpath: str):
        """
        Load the data from stacked numpy files

        Parameters
        ----------
        folderpath
            output folder containing the saved data

        Returns
        -------
        folderpath
            output folder to contain the saved numpy data
        """

        if not os.path.exists(folderpath + '/data'):
            raise EnvironmentError('Unable to load numpy data, {} does not exist'.format(folderpath + '/data'))
        self.data = da.from_npy_stack(folderpath + '/data')
        self.node_data = da.from_npy_stack(folderpath + '/node_data')


class QuadTree:
    """
    Adapted from https://github.com/GliderToolsCommunity/GliderTools/blob/master/glidertools/mapping.py

    Recursively splits data into quadrants

    Object oriented quadtree can access children recursively

    Ultimately, we want to:

    - save huge datasets in a way that allows for lazy loading by location
    - save indices that allow you to update the grid when the soundings change
    - allow for constraining ultimate grid sizes to powers of two
    - allow for utm and geographic coordinate systems
    - implement mean/CUBE algorithms for cell depth

    - write locks on quads, file lock?  one master lock for now?
    """

    def __init__(self, manager, mins=None, maxs=None, max_points_per_quad=5,
                 max_grid_size=128, min_grid_size=1, location=[], index=[], parent=None):
        self.parent = parent  # parent quad instance, None if this is the top quad
        self.location = location
        self.tree_depth = len(location)
        self.manager = manager
        if manager is None:
            manager = self.root.manager

        # can't save boolean to json/zarr attribute, need to encode as a diff type, this kind of sucks but leaving it for now
        self.is_leaf = False   # is the end of a quad split, contains the data has no children
        self.quad_index = -1

        if not index and not parent:  # first run through with data gets here
            self._validate_inputs(min_grid_size, max_grid_size, max_points_per_quad)
            data = manager.data
            self.index = np.arange(data['x'].shape[0]).tolist()
            self.settings = {'max_points_per_quad': max_points_per_quad, 'max_grid_size': max_grid_size,
                             'min_grid_size': min_grid_size, 'min_depth': 0, 'max_depth': 0,
                             'min_tvu': 0, 'max_tvu': 0, 'min_thu': 0, 'max_thu': 0,
                             'number_of_points': len(self.index)}
        elif not index:  # get here when you intialize empty quad to then load()
            data = None
            self.index = []
        else:  # initialize child with index
            data = manager.data[index]
            self.index = index

        if self.index:
            xval = data['x']
            yval = data['y']
        else:
            xval = None
            yval = None

        if mins is None and maxs is None:
            if self.index:
                self.mins = [np.min(xval).astype(xval.dtype), (np.min(yval).astype(yval.dtype))]
                self.maxs = [np.max(xval).astype(xval.dtype), np.max(yval).astype(yval.dtype)]
                self._align_toplevel_grid(max_grid_size)
                self.mins = [self.mins[0].astype(xval.dtype), self.mins[1].astype(yval.dtype)]
                self.maxs = [self.maxs[0].astype(xval.dtype), self.maxs[1].astype(yval.dtype)]
            else:  # get here when you intialize empty quad to then load()
                self.mins = [0, 0]
                self.maxs = [0, 0]
        else:
            self.mins = mins
            self.maxs = maxs

        self.children = []

        should_divide = False
        if self.index:
            top_left_idx, top_right_idx, bottom_left_idx, bottom_right_idx, xmin, xmax, ymin, ymax, xmid, ymid = self._build_quadrant_indices(xval, yval)
            should_divide = self._build_split_check(len(top_left_idx), len(top_right_idx), len(bottom_left_idx), len(bottom_right_idx),
                                                    max_grid_size, min_grid_size, max_points_per_quad)
            if should_divide:
                props = dict(max_points_per_quad=max_points_per_quad, min_grid_size=min_grid_size, max_grid_size=max_grid_size, parent=self)
                self.children.append(QuadTree(None, [xmin, ymid], [xmid, ymax], index=top_left_idx, location=location + [0], **props))
                self.children.append(QuadTree(None, [xmid, ymid], [xmax, ymax], index=top_right_idx, location=location + [1], **props))
                self.children.append(QuadTree(None, [xmin, ymin], [xmid, ymid], index=bottom_left_idx, location=location + [2], **props))
                self.children.append(QuadTree(None, [xmid, ymin], [xmax, ymid], index=bottom_right_idx, location=location + [3], **props))
                self.index = []

        if not should_divide:
            self.is_leaf = True
            if self.index and 'z' in data.dtype.names:
                quad_depth = manager.data['z'][self.index].mean().astype(manager.data['z'].dtype)
                quad_node_x = (self.mins[0] + (self.maxs[0] - self.mins[0]) / 2).astype(self.mins[0].dtype)
                quad_node_y = (self.mins[1] + (self.maxs[1] - self.mins[1]) / 2).astype(self.mins[1].dtype)
                datachunk = [quad_node_x, quad_node_y, quad_depth]
                if 'tvu' in data.dtype.names:
                    datachunk.append(manager.data['tvu'][self.index].mean().astype(manager.data['tvu'].dtype))
                if 'thu' in data.dtype.names:
                    datachunk.append(manager.data['thu'][self.index].mean().astype(manager.data['thu'].dtype))
                manager.node_data.append(datachunk)
                self.quad_index = np.int32(len(manager.node_data) - 1)

    def __getitem__(self, args, silent=False):
        """
        Go through the quadtree and locate the quadtree at the provided index, see self.loc
        """

        args = np.array(args, ndmin=1)
        if any(args > 3):
            raise UserWarning("A quadtree only has 4 possible children, provided locations: {}".format(args))
        quadtree = self
        passed = []
        for depth in args:
            if (len(quadtree.children) > 0) | (not silent):
                quadtree = quadtree.children[depth]
                passed += [depth]
            else:
                return None

        return quadtree

    def __repr__(self):
        return "<{} : {}>".format(str(self.__class__)[1:-1], str(self.location))

    def __str__(self):
        location = str(self.location)[1:-1]
        location = location if location != "" else "[] - base QuadTree has no location"

        # boundaries and spacing to make it pretty
        left, top = self.mins
        right, bot = self.maxs
        wspace = " " * len("{:.2f}".format(top))
        strtspace = " " * (15 - max(0, (len("{:.2f}".format(top)) - 6)))

        # text output (what youll see when you print the object)
        about_tree = "\n".join(
            [
                "",
                "QuadTree object",
                "===============",
                "  location:         {}".format(location),
                "  tree depth:       {}".format(len(self.location)),
                "  n_points:         {}".format(len(self.index)),
                "  boundaries:       {:.2f}".format(top),
                "{}{:.2f}{}{:.2f}".format(strtspace, left, wspace, right),
                "                    {:.2f}".format(bot),
                "  children_points:  {}".format(str([len(c.index) for c in self.children])),
            ]
        )
        return about_tree

    def _validate_inputs(self, min_grid_size, max_grid_size, max_points_per_quad):
        if not is_power_of_two(min_grid_size):
            raise ValueError('QuadTree: Only supports min_grid_size that is power of two, received {}'.format(min_grid_size))
        if not is_power_of_two(max_grid_size):
            raise ValueError('QuadTree: Only supports max_grid_size that is power of two, received {}'.format(max_grid_size))
        if (not isinstance(max_points_per_quad, int)) or (max_points_per_quad <= 0):
            raise ValueError('QuadTree: max points per quad must be a positive integer, received {}'.format(max_points_per_quad))

    def _align_toplevel_grid(self, max_grid_size: float):
        """
        So that our grids will line up nicely with each other (as long as they use max_grid_size that are divisable
        by a similar number) we adjust the max/min of the top level grid to an even multiple of max_grid_size
        """
        # align origin with multiple of max_grid_size
        # double_max_grid = max_grid_size * 4
        double_max_grid = 512  # use 512 across surfaces so that they all generally align with each other
        self.mins[0] -= self.mins[0] % double_max_grid
        self.mins[1] -= self.mins[1] % double_max_grid

        # extend the grid to make it square and an even multiple of the max grid size
        max_range = max(self.maxs[0] - self.mins[0], self.maxs[1] - self.mins[1])
        maxadjust = max_range % double_max_grid
        if maxadjust:
            max_range += (double_max_grid - maxadjust)
        self.maxs[0] = self.mins[0] + max_range
        self.maxs[1] = self.mins[1] + max_range

    def _build_quadrant_indices(self, xval: Union[np.ndarray, da.Array], yval: Union[np.ndarray, da.Array]):
        """
        Determine the data indices that split the data into four quadrants
        Parameters
        ----------
        xval
            x coordinate for all points
        yval
            y coordinate for all points

        Returns
        -------
        np.array
            data indices that correspond to points in the top left quadrant
        np.array
            data indices that correspond to points in the top right quadrant
        np.array
            data indices that correspond to points in the bottom left quadrant
        np.array
            data indices that correspond to points in the bottom right quadrant
        float
            minimum x value of the input points
        float
            maximum x value of the input points
        float
            minimum y value of the input points
        float
            maximum y value of the input points
        float
            x midpoint value of the input points
        float
            y midpoint value of the input points
        """

        xmin, ymin = self.mins
        xmax, ymax = self.maxs
        xmid = (0.5 * (xmin + xmax)).astype(xmin.dtype)
        ymid = (0.5 * (ymin + ymax)).astype(ymin.dtype)

        # split the data into four quadrants
        xval_lessthan = xval <= xmid
        xval_greaterthan = xval >= xmid
        yval_lessthan = yval <= ymid
        yval_greaterthan = yval >= ymid

        idx = np.array(self.index)
        index_q0 = idx[xval_lessthan & yval_greaterthan].tolist()  # top left
        index_q1 = idx[xval_greaterthan & yval_greaterthan].tolist()  # top left
        index_q2 = idx[xval_lessthan & yval_lessthan].tolist()  # top left
        index_q3 = idx[xval_greaterthan & yval_lessthan].tolist()  # top left

        return index_q0, index_q1, index_q2, index_q3, xmin, xmax, ymin, ymax, xmid, ymid

    def _build_split_check(self, q0_size: int, q1_size: int, q2_size: int, q3_size: int, max_grid_size, min_grid_size, max_points_per_quad):
        """
        Builds a check to determine whether or not this quadrant should be divided.  Uses:

        point_check - points in the quad must not exceed the provided maximum allowable points
        max_size_check - quad size must not exceed the provided maximum allowable grid size
        min_size_check - quad size (after splitting) must not end up less than minimum allowable grid size
        too_few_points_check - if you know that splitting will lead to less than allowable max points, dont split
        empty_quad_check - if there are three quadrants that are empty, split so that you don't end up with big
            quads that are mostly empty

        Parameters
        ----------
        q0_size
            size of points that belong to the top left quadrant
        q1_size
            size of points that belong to the top right quadrant
        q2_size
            size of points that belong to the bottom left quadrant
        q3_size
            size of points that belong to the bottom right quadrant

        Returns
        -------
        bool
            if True, split this quad into 4 quadrants
        """
        n_points = len(self.index)
        sizes = [self.maxs[0] - self.mins[0], self.maxs[1] - self.mins[1]]

        point_check = n_points > max_points_per_quad
        max_size_check = sizes[0] > max_grid_size
        min_size_check = sizes[0] / 2 >= min_grid_size

        too_few_points_check = True
        empty_quad_check = False
        if n_points <= max_points_per_quad * 4:  # only do these checks if there are just a few points, they are costly
            too_few_points_quads = [q0_size >= max_points_per_quad or q0_size == 0,
                                    q1_size >= max_points_per_quad or q1_size == 0,
                                    q2_size >= max_points_per_quad or q2_size == 0,
                                    q3_size >= max_points_per_quad or q3_size == 0]
            too_few_points_check = np.count_nonzero(too_few_points_quads) == 4
            if n_points <= max_points_per_quad:
                empty_quads = [q0_size == 0, q1_size == 0, q2_size == 0, q3_size == 0]
                empty_quad_check = np.count_nonzero(empty_quads) == 3
                too_few_points_check = True  # hotwire this, we always split when there are three empty quadrants and we are greater than min resolution

        if (point_check or max_size_check or empty_quad_check) and min_size_check and too_few_points_check:
            return True
        return False

    def _traverse_tree(self):
        """
        iterate through the quadtree
        """
        if not self.children:
            yield self
        for child in self.children:
            yield from child._traverse_tree()

    def loc(self, *args: list, silent=False):
        """
        Get a child quad by index

        self.loc(0,1,2) returns the bottom left (2) of the top right (1) of the top left (0) child quad of self

        Parameters
        ----------
        args
            list of the quad indices to use to locate a quad
        silent
            if True, will return None if the index does not exist.  Otherwise raises IndexError

        Returns
        -------
        QuadTree
            QuadTree instance at that location
        """

        return self.__getitem__(args, silent=silent)

    def query_xy(self, x: float, y: float):
        """
        Given the provided x/y value, find the leaf that contains the point.  The point does not have to be an actual
        point in the quadtree, will find the leaf that theoretically would contain it.

        Returns None if point is out of bounds

        search_qtree = qtree.query_xy(538999, 5292700)
        search_qtree.is_leaf
        Out[10]: True

        Parameters
        ----------
        x
            x value of point to search for
        y
            y value of point to search for

        Returns
        -------
        QuadTree
            leaf node that contains the given point
        """
        xmid = 0.5 * (self.mins[0] + self.maxs[0])
        ymid = 0.5 * (self.mins[1] + self.maxs[1])
        idx = np.where([(x < xmid) & (y > ymid), (x >= xmid) & (y > ymid), (x < xmid) & (y <= ymid), (x >= xmid) & (y <= ymid)])[0].tolist()
        self = self.loc(*idx, silent=True)
        while not self.is_leaf:
            self = self.query_xy(x, y)

        return self

    def get_leaves(self):
        """
        Get a list of all leaves from the quadtree

        Returns
        -------
        list
            list of QuadTrees that are leaves
        """
        return list(set(list(self._traverse_tree())))

    def get_leaves_attr(self, attr: str):
        """
        Get the attribute corresponding to attr from all leaves

        Parameters
        ----------
        attr
            str name of an attribute

        Returns
        -------
        list
            list of attribute values
        """

        return [getattr(q, attr) for q in self.leaves]

    def draw_tree(self, ax: plt.Axes = None, tree_depth: int = None, exclude_empty: bool = False,
                  line_width: int = 1, edge_color='red', plot_nodes: bool = False, plot_points: bool = False):
        """
        Recursively plot an x/y box drawing of the qtree.

        Parameters
        ----------
        ax
            matplotlib subplot, provide if you want to draw on an existing plot
        tree_depth
            optional, if provided will only plot the tree from this level down
        exclude_empty
            optional, if provided will only plot the leaves that contain points
        line_width
            line width in the outline of the Rect object plotted for each quad
        edge_color
            color of the outline of the Rect object plotted for each quad
        plot_nodes
            if True, will plot the node of each quad
        plot_points
            if True, will plot all the points given to the QuadTree
        """

        manager = self.root.manager
        manager.finalize_data()

        root_quad = self.root
        norm = matplotlib.colors.Normalize(vmin=root_quad.settings['min_depth'], vmax=root_quad.settings['max_depth'])
        cmap = matplotlib.cm.rainbow

        if ax is None:
            ax = plt.subplots(figsize=[11, 7], dpi=150)[1]

        if tree_depth is None or tree_depth == 0:
            if exclude_empty and not self.index:
                pass
            else:
                sizes = [self.maxs[0] - self.mins[0], self.maxs[1] - self.mins[1]]
                if self.quad_index != -1:
                    quad_z = manager.node_data['z'][self.quad_index].compute()
                    rect = matplotlib.patches.Rectangle(self.mins, *sizes, zorder=2, alpha=0.5, lw=line_width, ec=edge_color, fc=cmap(norm(quad_z)))
                    if plot_nodes:
                        quad_x = manager.node_data['x'][self.quad_index].compute()
                        quad_y = manager.node_data['y'][self.quad_index].compute()
                        ax.scatter(quad_x, quad_y, s=5)
                    if plot_points:
                        ax.scatter(manager.data['x'][self.index].compute(),
                                   manager.data['y'][self.index].compute(), s=2)
                else:  # no depth for the quad
                    rect = matplotlib.patches.Rectangle(self.mins, *sizes, zorder=2, alpha=1, lw=line_width, ec=edge_color, fc='None')
                ax.add_patch(rect)

        if tree_depth is None:
            for child in self.children:
                child.draw_tree(ax, tree_depth=None, exclude_empty=exclude_empty, line_width=line_width, edge_color=edge_color, plot_points=plot_points, plot_nodes=plot_nodes)
        elif tree_depth > 0:
            for child in self.children:
                child.draw_tree(ax, tree_depth=tree_depth - 1, exclude_empty=exclude_empty, line_width=line_width, edge_color=edge_color, plot_points=plot_points, plot_nodes=plot_nodes)

        if (self.tree_depth == 0) or (tree_depth is None and self.tree_depth == 0):
            xsize = self.maxs[0] - self.mins[0]
            ysize = self.maxs[1] - self.mins[1]
            ax.set_ylim(self.mins[1] - ysize / 10, self.maxs[1] + ysize / 10)
            ax.set_xlim(self.mins[0] - xsize / 10, self.maxs[0] + xsize / 10)
            divider = make_axes_locatable(ax)
            cax = divider.append_axes('right', size='5%', pad=0.05)
            plt.gcf().colorbar(matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap), cax=cax, orientation='vertical', label='Depth (+down, meters)')

        return ax

    @property
    def root(self):
        """
        Return the root of the tree

        Returns
        -------
        QuadTree
            root quadtree for the tree
        """

        parent = self
        for _ in self.location:
            parent = parent.parent
        return parent

    @property
    def siblings(self):
        """
        Return a list of siblings for this QuadTree, returns None if this QuadTree has no parent (top level)

        Returns
        -------
        list
            list of QuadTree instances for the siblings of this QuadTree
        """

        if self.parent is None:
            return None

        siblings = self.parent.children.copy()
        siblings.remove(self)
        return siblings

    def _get_border_children(self, quad, location):
        """Returns all T/L/R/B boundaries as defined by bound_location"""
        bounds = [[2, 3], [0, 2], [0, 1], [1, 3]]
        bound_location = bounds[location]
        if not quad.is_leaf:
            for i in bound_location:
                yield from self._get_border_children(quad.children[i], location)
        else:
            yield quad

    @property
    def neighbours(self):
        """
        Return a list of all neighbors for this QuadTree (orthogonal)

        Returns
        -------
        list
            list of QuadTrees that are orthogonally adjacent to this QuadTree
        """

        neighbours = []
        root = self.root
        if self == root:
            return neighbours

        ########################
        # IMMEDIATELY ADJACENT #
        sizes = [self.maxs[0] - self.mins[0], self.maxs[1] - self.mins[1]]
        coords = [(self.mins[0] + sizes[0] / 2, self.maxs[1] + sizes[1] / 2,),
                  (self.maxs[0] + sizes[0] / 2, self.mins[1] + sizes[1] / 2,),
                  (self.mins[0] + sizes[0] / 2, self.mins[1] - sizes[1] / 2,),
                  (self.maxs[0] - sizes[0] / 2, self.mins[1] + sizes[1] / 2,),]
        # loop through top, right, bottom, left
        for i in range(4):
            x, y = coords[i]
            query_quad = root.query_xy(x, y)
            if query_quad is not None:
                same_size_idx = query_quad.location[: self.tree_depth]
                same_size_quad = root[same_size_idx]
                neighbours += list(self._get_border_children(same_size_quad, i))

        #############
        # DIAGONALS #
        root_sizes = [root.maxs[0] - root.mins[0], root.maxs[1] - root.mins[1]]
        xs, ys = (root_sizes / 2 ** root.max_tree_depth) / 2
        neighbours += [
            root.query_xy(self.mins[0] - xs, self.mins[1] - ys),  # TL
            root.query_xy(self.maxs[0] + xs, self.mins[1] - ys),  # TR
            root.query_xy(self.mins[0] - xs, self.maxs[1] + ys),  # BL
            root.query_xy(self.maxs[0] + xs, self.maxs[1] + ys),  # BR
        ]

        unique_neighbours = list(set(neighbours))
        try:
            unique_neighbours.remove(self)
        except ValueError:
            pass

        return unique_neighbours

    @property
    def max_tree_depth(self):
        """
        Return the max depth of this QuadTree tree

        Returns
        -------
        int
            max depth of tree (a 7 means there are 7 levels to the tree)
        """

        depths = np.array([leaf.tree_depth for leaf in self.leaves])

        return depths.max()

    @property
    def leaves(self):
        """
        Return a list of all leaves for the tree, leaves being QuadTrees with no children

        Returns
        -------
        list
            list of QuadTrees
        """

        return self.get_leaves()

    @property
    def leaves_with_data(self):
        """
        Return a list of all leaves for the tree that have data

        Returns
        -------
        list
            list of QuadTrees
        """

        return [lv for lv in self.get_leaves() if lv.index]


if __name__ == '__main__':
    from time import perf_counter
    from HSTB.kluster.fqpr_convenience import *

    fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040", skip_dask=True)

    # x = np.random.uniform(538900, 539300, 1000).astype(np.float32)
    # y = np.random.uniform(5292800, 5293300, 1000).astype(np.float32)
    # z = np.random.uniform(30, 35, 1000).astype(np.float32)
    #
    # test_data_arr = np.stack([x, y, z], axis=1)
    # test_data_arr = test_data_arr.ravel().view(dtype=[('x', 'f4'), ('y', 'f4'), ('z', 'f4')])

    raw_ping = fq.multibeam.raw_ping[0]
    dataset = raw_ping.drop_vars([nms for nms in raw_ping.variables if nms not in ['x', 'y', 'z', 'tvu', 'thu']])
    dataset = dataset.isel(time=slice(0, 50)).stack({'sounding': ('time', 'beam')})
    fq.close()

    def timethisthing(userfunc, args, kwargs, msg):
        st = perf_counter()
        ret = userfunc(*args, **kwargs)
        end = perf_counter()
        print(msg.format(end-st))
        return ret

    # qm = QuadManager()
    # timethisthing(qm.create, [data_arr], {'max_points_per_quad': 5}, 'Numpy build time: {}')
    # timethisthing(qm.save, [r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040'], {}, 'Numpy save time: {}')
    # timethisthing(qm.tree.draw_tree, [], {'plot_points': False}, 'Numpy draw time: {}')
    #
    # qm = QuadManager()
    # timethisthing(qm.create, [data_dask], {'max_points_per_quad': 5}, 'Dask build time: {}')
    # timethisthing(qm.save, [r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040'], {}, 'Dask save time: {}')
    # timethisthing(qm.tree.draw_tree, [], {'plot_points': False}, 'Dask draw time: {}')

    qm = QuadManager()
    coordsys = dataset.xyz_crs
    vertref = dataset.vertical_reference
    containername = os.path.split(dataset.output_path)[1]
    multibeamlist = list(dataset.multibeam_files.keys())

    timethisthing(qm.create, [dataset],
                  {'container_name': containername, 'multibeam_file_list': multibeamlist,
                   'coordinate_system': coordsys, 'vertical_reference': vertref, 'min_grid_size': 0.5, 'max_grid_size': 32},
                  'Dataset build time: {}')
    fldrpath = timethisthing(qm.save, [r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040'], {}, 'Dataset save time: {}')
    timethisthing(qm.export, [r'C:\collab\dasktest\data_dir\outputtest\tj_patch_test_2040'], {}, 'Dataset export time: {}')
    timethisthing(qm.load, [fldrpath], {}, 'Dataset load time: {}')
    timethisthing(qm.plot_surface, ['depth'], {}, 'Dataset draw time: {}')

    plt.show(block=True)
