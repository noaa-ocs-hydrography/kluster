import matplotlib
from mpl_toolkits.axes_grid1 import make_axes_locatable
from typing import Union

from HSTB.kluster.surface_helpers import *


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

    def __init__(self, data: Union[np.ndarray, xr.Dataset], mins=None, maxs=None, max_points_per_quad=5,
                 max_grid_size=128, min_grid_size=0.5, location=[], index=[], parent=None):
        self.parent = parent  # parent quad instance, None if this is the top quad
        self.data = QuadData(data)
        if self.parent is None:  # first run through make sure the input data to Root is of the right type
            self.data.validate_input_data()
        emptyquad = self.data.is_empty()

        # can't save boolean to json/zarr attribute, need to encode as a diff type, this kind of sucks but leaving it for now
        self.is_leaf = False   # is the end of a quad split, contains the data has no children
        self.quad_depths = []
        self.quad_depth = 0

        self.max_grid_size = max_grid_size
        self.min_grid_size = min_grid_size
        self.max_points_per_quad = max_points_per_quad

        if mins is None and maxs is None:
            if not emptyquad:
                xval = self.data.getvalues('x')
                yval = self.data.getvalues('y')
                self.mins = [np.min(xval), np.min(yval)]
                self.maxs = [np.max(xval), np.max(yval)]
                self._align_toplevel_grid()
            else:  # get here when you intialize empty quad to then load()
                self.mins = [0, 0]
                self.maxs = [0, 0]
        else:
            self.mins = mins
            self.maxs = maxs

        if not index:
            if not emptyquad:
                self.index = np.arange(self.data()['x'].shape[0]).tolist()
            else:  # get here when you intialize empty quad to then load()
                self.index = []
        else:
            self.index = index

        self.sizes = [self.maxs[0] - self.mins[0], self.maxs[1] - self.mins[1]]
        self.n_points = self.data()['x'].shape[0]

        self.location = location
        self.tree_depth = len(location)

        self.children = []

        if not emptyquad:
            index_q0, index_q1, index_q2, index_q3, xmin, xmax, ymin, ymax, xmid, ymid = self._build_quadrant_indices()
            top_left_data = self.data.mask_data(index_q0)
            top_right_data = self.data.mask_data(index_q1)
            bottom_left_data = self.data.mask_data(index_q2)
            bottom_right_data = self.data.mask_data(index_q3)

            should_divide = self._build_split_check(len(top_left_data['x']), len(top_right_data['x']), len(bottom_left_data['x']), len(bottom_right_data['x']))
        else:
            should_divide = False

        if should_divide:
            props = dict(max_points_per_quad=max_points_per_quad, min_grid_size=min_grid_size, max_grid_size=max_grid_size, parent=self)
            self.children.append(QuadTree(top_left_data, [xmin, ymid], [xmid, ymax], index=np.array(self.index)[index_q0].tolist(), location=location + [0], **props))
            self.children.append(QuadTree(top_right_data, [xmid, ymid], [xmax, ymax], index=np.array(self.index)[index_q1].tolist(), location=location + [1], **props))
            self.children.append(QuadTree(bottom_left_data, [xmin, ymin], [xmid, ymid], index=np.array(self.index)[index_q2].tolist(), location=location + [2], **props))
            self.children.append(QuadTree(bottom_right_data, [xmid, ymin], [xmax, ymid], index=np.array(self.index)[index_q3].tolist(), location=location + [3], **props))
            self.index = []
            self.data = None
        else:
            self.is_leaf = True
            if not self.data.is_empty():
                if self.data.check_data_names('z'):
                    self.quad_depth = float(self.data.getvalues('z').mean())
                    self.root.quad_depths.append(self.quad_depth)

    def save(self, path, storage=StorePickles):
        handle = storage.save(self, path)
        storage.clear_children(handle)
        for i, child in enumerate(self.children):
            child_handle = storage.child_path(handle, storage.child_names[i])
            child.save(child_handle, storage=storage)

    @classmethod
    def load(cls, path, storage=StorePickles):
        loaded_quad = cls(np.zeros((1,), dtype=[('x', 'f4'), ('y', 'f4'), ('z', 'f4')]))  # initialize a new object
        handle = storage.load(loaded_quad, path)
        if storage.has_children(handle):
            for i in storage.child_names:
                child = cls.load(storage.child_path(handle, i), storage=storage)
                child.parent = loaded_quad
                loaded_quad.children.append(child)
        return loaded_quad

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
                "  n_points:         {}".format(self.n_points),
                "  boundaries:       {:.2f}".format(top),
                "{}{:.2f}{}{:.2f}".format(strtspace, left, wspace, right),
                "                    {:.2f}".format(bot),
                "  children_points:  {}".format(str([c.n_points for c in self.children])),
            ]
        )
        return about_tree

    def _align_toplevel_grid(self):
        """
        So that our grids will line up nicely with each other (as long as they use max_grid_size that are divisable
        by a similar number) we adjust the max/min of the top level grid to an even multiple of max_grid_size
        """
        # align origin with multiple of max_grid_size
        double_max_grid = self.max_grid_size * 4
        self.mins[0] -= self.mins[0] % double_max_grid
        self.mins[1] -= self.mins[1] % double_max_grid

        # extend the grid to make it square and an even multiple of the max grid size
        max_range = max(self.maxs[0] - self.mins[0], self.maxs[1] - self.mins[1])
        maxadjust = max_range % double_max_grid
        if maxadjust:
            max_range += (double_max_grid - maxadjust)
        self.maxs[0] = self.mins[0] + max_range
        self.maxs[1] = self.mins[1] + max_range

    def _build_quadrant_indices(self):
        """
        Determine the data indices that split the data into four quadrants

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
        xmid = 0.5 * (xmin + xmax)
        ymid = 0.5 * (ymin + ymax)

        # split the data into four quadrants
        xval = self.data.getvalues('x')
        yval = self.data.getvalues('y')
        xval_lessthan = xval <= xmid
        xval_greaterthan = xval >= xmid
        yval_lessthan = yval <= ymid
        yval_greaterthan = yval >= ymid

        index_q0 = xval_lessthan & yval_greaterthan  # top left
        index_q1 = xval_greaterthan & yval_greaterthan  # top right
        index_q2 = xval_lessthan & yval_lessthan  # bottom left
        index_q3 = xval_greaterthan & yval_lessthan  # bottom right

        return index_q0, index_q1, index_q2, index_q3, xmin, xmax, ymin, ymax, xmid, ymid

    def _build_split_check(self, q0_size: int, q1_size: int, q2_size: int, q3_size: int):
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

        point_check = self.n_points > self.max_points_per_quad
        max_size_check = self.sizes[0] > self.max_grid_size
        min_size_check = self.sizes[0] / 2 >= self.min_grid_size

        too_few_points_check = True
        empty_quad_check = False
        if self.n_points <= self.max_points_per_quad * 4:  # only do these checks if there are just a few points, they are costly
            too_few_points_quads = [q0_size >= self.max_points_per_quad or q0_size == 0,
                                    q1_size >= self.max_points_per_quad or q1_size == 0,
                                    q2_size >= self.max_points_per_quad or q2_size == 0,
                                    q3_size >= self.max_points_per_quad or q3_size == 0]
            too_few_points_check = np.count_nonzero(too_few_points_quads) == 4
            if self.n_points <= self.max_points_per_quad:
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

    def draw_tree(self, ax=None, tree_depth=None, exclude_empty=False, color='red'):
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
        color
            color of the boxes
        """

        root = self.root
        mindepth = np.min(root.quad_depths)
        maxdepth = np.max(root.quad_depths)
        norm = matplotlib.colors.Normalize(vmin=mindepth, vmax=maxdepth)
        cmap = matplotlib.cm.rainbow

        if ax is None:
            ax = plt.subplots(figsize=[11, 7], dpi=150)[1]

        if tree_depth is None or tree_depth == 0:
            if exclude_empty and (self.data is None or self.data.is_empty()):
                pass
            else:
                if self.quad_depth:
                    rect = matplotlib.patches.Rectangle(self.mins, *self.sizes, zorder=2, alpha=0.5, lw=1, ec=color, fc=cmap(norm(self.quad_depth)))
                    ax.scatter(self.data.getvalues('x'), self.data.getvalues('y'), s=2)
                else:  # no depth for the quad
                    rect = matplotlib.patches.Rectangle(self.mins, *self.sizes, zorder=2, alpha=1, lw=1, ec=color, fc='None')
                ax.add_patch(rect)

        if tree_depth is None:
            for child in self.children:
                child.draw_tree(ax, tree_depth=None, exclude_empty=exclude_empty, color=color)
        elif tree_depth > 0:
            for child in self.children:
                child.draw_tree(ax, tree_depth=tree_depth - 1, exclude_empty=exclude_empty, color=color)

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
        coords = [(self.mins[0] + self.sizes[0] / 2, self.maxs[1] + self.sizes[1] / 2,),
                  (self.maxs[0] + self.sizes[0] / 2, self.mins[1] + self.sizes[1] / 2,),
                  (self.mins[0] + self.sizes[0] / 2, self.mins[1] - self.sizes[1] / 2,),
                  (self.maxs[0] - self.sizes[0] / 2, self.mins[1] + self.sizes[1] / 2,),]
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
        xs, ys = (root.sizes / 2 ** root.max_depth) / 2
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
    def max_depth(self):
        """
        Return the max depth of this QuadTree tree

        Returns
        -------
        int
            max depth of tree (a 7 means there are 7 levels to the tree)
        """

        leaves = self.get_leaves()
        depths = np.array([leaf.tree_depth for leaf in leaves])

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


if __name__ == '__main__':
    from HSTB.kluster.fqpr_convenience import *
    from time import perf_counter

    fq = reload_data(r"C:\collab\dasktest\data_dir\outputtest\EM2040_Fairweather_SmallFile")

    x = fq.multibeam.raw_ping[0].x.values.ravel()[:10000].astype(np.float32)
    y = fq.multibeam.raw_ping[0].y.values.ravel()[:10000].astype(np.float32)
    z = fq.multibeam.raw_ping[0].z.values.ravel()[:10000].astype(np.float32)

    fq.close()

    # x = np.random.uniform(538900, 539300, 1000).astype(np.float32)
    # y = np.random.uniform(5292800, 5293300, 1000).astype(np.float32)
    # z = np.random.uniform(30, 35, 1000).astype(np.float32)

    data_arr = np.stack([x, y, z], axis=1)
    data_arr = data_arr.ravel().view(dtype=[('x', 'f4'), ('y', 'f4'), ('z', 'f4')])
    data_dset = xr.Dataset({'x': (xr.DataArray(x)), 'y': (xr.DataArray(y)), 'z': (xr.DataArray(z))})
    assert (data_dset['x'] == data_arr['x']).all()

    save_path_numpy = "c:\\temp\\quadtree_numpy"
    save_path_hdf5 = "c:\\temp\\quadtree_hdf5"
    save_path_zarr = "c:\\temp\\quadtree_zarr"
    save_path_netcdf = "c:\\temp\\quadtree_netcdf"
    save_path_json = "c:\\temp\\quadtree_json"

    for pth in [save_path_netcdf, save_path_zarr, save_path_hdf5, save_path_numpy, save_path_json]:
        try:
            os.makedirs(pth)
        except FileExistsError:
            pass

    def timethisthing(userfunc, args, kwargs, msg):
        st = perf_counter()
        ret = userfunc(*args, **kwargs)
        end = perf_counter()
        print(msg.format(end-st))
        return ret

    qtree_numpy = timethisthing(QuadTree, [data_arr], {'max_points_per_quad': 5}, 'Numpy build time: {}')
    # qtree_xarray = timethisthing(QuadTree, [data_dset], {'max_points_per_quad': 5}, 'Xarray build time: {}')

    print('*********************************')

    timethisthing(qtree_numpy.save, [save_path_json], {'storage': StoreJson}, 'Json save time: {}')
    qtree_json = timethisthing(QuadTree.load, [save_path_json], {'storage': StoreJson}, 'Json load time: {}')
    timethisthing(qtree_json.draw_tree, [], {'exclude_empty': False, 'color': 'red'}, 'Pickles draw time: {}')

    print('*********************************')

    # timethisthing(qtree_numpy.save, [save_path_numpy], {}, 'Pickles save time: {}')
    # qtree_pickles = timethisthing(QuadTree.load, [save_path_numpy], {}, 'Pickles load time: {}')
    # timethisthing(qtree_pickles.draw_tree, [], {'exclude_empty': False, 'color': 'red'}, 'Pickles draw time: {}')
    #
    print('*********************************')
    #
    # timethisthing(qtree_numpy.save, [save_path_hdf5 + "\\test_hdf.h5"], {'storage': StoreHDF5}, 'HDF5 save time: {}')
    # qtree_hdf = timethisthing(QuadTree.load, [save_path_hdf5 + "\\test_hdf.h5"], {'storage': StoreHDF5}, 'HDF5 load time: {}')
    # timethisthing(qtree_hdf.draw_tree, [], {'exclude_empty': False, 'color': 'red'}, 'HDF5 draw time: {}')

    print('*********************************')

    # timethisthing(qtree_xarray.save, [save_path_zarr], {'storage': StoreZarr}, 'xarray->zarr save time: {}')
    # qtree_zarr = timethisthing(QuadTree.load, [save_path_zarr], {'storage': StoreZarr}, 'xarray->zarr load time: {}')
    # timethisthing(qtree_zarr.draw_tree, [], {'exclude_empty': False, 'color': 'red'}, 'xarray->zarr draw time: {}')
    #
    print('*********************************')

    # timethisthing(qtree_xarray.save, [save_path_netcdf], {'storage': StoreNetcdf}, 'xarray->netcdf save time: {}')
    # qtree_netcdf = timethisthing(QuadTree.load, [save_path_netcdf], {'storage': StoreNetcdf}, 'xarray->netcdf load time: {}')
    # timethisthing(qtree_netcdf.draw_tree, [], {'exclude_empty': False, 'color': 'red'}, 'xarray->netcdf draw time: {}')

    plt.show(block=True)
