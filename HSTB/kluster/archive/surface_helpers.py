import os
import shutil
import pickle
from abc import ABC, abstractmethod
from dask import array as da

import numpy as np
import xarray as xr
import h5py
import json


class QuadData:
    def __init__(self, data):
        self.data = data
        self.dtype = type(self.data)
        self.isnumpy = self.dtype is np.ndarray
        self.isdask = self.dtype is da.Array

    def __call__(self):
        return self.data

    def getvalues(self, varname):
        if self.isnumpy:
            return self.data[varname]
        elif self.isdask:
            self.data = self.data.compute()
            self.isnumpy = True
            return self.data[varname]
        else:
            return self.data[varname].values

    def validate_input_data(self):
        """
        If parent is None (i.e. this is the entry point to the quad and the data is just now being examined) we ensure
        that it is a valid xarray/numpy structured array
        """
        if self.dtype in [np.ndarray, da.Array]:
            if not self.data.dtype.names:
                raise ValueError('QuadTree: numpy array provided for data, but no names were found, array must be a structured array')
            if 'x' not in self.data.dtype.names or 'y' not in self.data.dtype.names:
                raise ValueError('QuadTree: numpy structured array provided for data, but "x" or "y" not found in variable names')
        elif self.dtype is xr.Dataset:
            if 'x' not in list(self.data.variables.keys()) or 'y' not in list(self.data.variables.keys()):
                raise ValueError('QuadTree: xarray Dataset provided for data, but "x" or "y" not found in variable names')
        else:
            raise ValueError('QuadTree: xarray Dataset or numpy structured array with "x" and "y" as variable must be provided')

    def check_data_names(self, variable_name: str):
        """
        Check if the provided variable name is in the data, works for numpy structured arrays and xarray Datasets

        Parameters
        ----------
        variable_name
            string identifier for the variable name we want to check
        """

        if self.isnumpy or self.isdask:
            return variable_name in self.data.dtype.names
        else:
            return variable_name in list(self.data.variables.keys())

    def return_variable_names(self):
        try:
            if self.isnumpy or self.isdask:
                return list(self.data.dtype.names)
            else:
                return list(self.data.variables.keys())
        except AttributeError:
            return None

    def mask_data(self, bool_mask: np.ndarray):
        if self.isnumpy:
            return self.data[bool_mask]
        elif self.isdask:
            self.data = self.data.compute()
            self.isnumpy = True
            return self.data[bool_mask]
        else:
            dimname = list(self.data.dims.keys())[0]
            return self.data.where(xr.DataArray(bool_mask, dims=dimname), drop=True)

    def is_empty(self):
        return self.data['x'].shape[0] == 0


def save_pickle(quad, path):
    """
    Given a directory as 'path' this function will save an attr.pickle file of all the attributes of the given object.
    The data attribute and parent/children attributes will not be stored as they are more complex and not a straight pickle.dump

    pickle is good at saving the whole data structure but is not as reliably portable across python versions
    json is safe across python versions but can't serialize some more complex types.

    So, pickle is easier to experiment with code and when the format is stable then convert to a save_json function which may need
    more custom code to store all the objects (like quad.mins, quad.xlim)

    Parameters
    ----------
    quad
        QuadTree instance to store the top level data from.  Children/parent/data not stored
    path
        Directory to make the pickle file in

    Returns
    -------
    None
    """

    os.makedirs(path, exist_ok=True)
    picklefile = open(os.path.join(path, "attr.pickle"), "wb")
    d = quad.__dict__.copy()
    # don't save the object pointers to children and parent as they will be rebuilt on load
    d.pop("children")
    d.pop("parent")
    # don't save is_leaf, set on init
    d.pop('is_leaf')
    # don't save the data array, it will be stored as a lazy zarr array or something else.
    d.pop("data")
    pickle.dump(d, picklefile, 4)


def load_pickle(quad, path):
    """Load the pickled metadata from the pickle file and update the quad's data"""
    picklefile = open(os.path.join(path, "attr.pickle"), "rb")
    quad.__dict__.update(pickle.load(picklefile))


def save_json(quad, path):
    """
    JSON can't store numpy arrays, so we convert to list and include a key specifying which arrays need conversion
    on load.

    Parameters
    ----------
    quad
        QuadTree instance to store the top level data from.  Children/parent/data not stored
    path
        Directory to make the json file in

    Returns
    -------
    None
    """

    os.makedirs(path, exist_ok=True)
    outputfile = open(os.path.join(path, "attr.json"), "w")
    d = quad.__dict__.copy()
    # don't save the object pointers to children and parent as they will be rebuilt on load
    d.pop("children")
    d.pop("parent")
    # don't save is_leaf, set on init
    d.pop('is_leaf')
    # don't save the data array, it will be stored as a lazy zarr array or something else.
    d.pop("data")
    json.dump(d, outputfile)


def load_json(quad, path):
    """
    restore metadata for a quad from disk

    Parameters
    ----------
    quad
        QuadTree instance to store the top level data from.  Children/parent/data not stored
    path
        Directory to make the json file in
    Returns
    -------
    None
    """

    outputfile = open(os.path.join(path, "attr.json"), "r")
    data = json.load(outputfile)
    quad.__dict__.update(data)


def save_zarr(quad, path):
    """
    Save the quadtree data to zarr storage

    Parameters
    ----------
    quad
        QuadTree instance to store the top level data from.  Children/parent/data not stored
    path
        Directory to make the json file in
    """

    os.makedirs(path, exist_ok=True)
    if quad.data is None:
        # leave a mark that data was none
        dataset = xr.Dataset({})
    else:
        if not isinstance(quad.data(), xr.Dataset):
            raise NotImplementedError('Only xarray datasets support save_zarr, found {}'.format(type(quad.data())))
        dataset = quad.data()
    d = quad.__dict__.copy()
    # don't save the object pointers to children and parent as they will be rebuilt on load
    d.pop("children")
    d.pop("parent")
    # don't save the data array, it will be stored as the main dataset here.
    d.pop("data")
    # don't save is_leaf, set on init
    d.pop('is_leaf')
    dataset.attrs = d
    dataset.to_zarr(os.path.join(path, "data.zarr"))


def load_zarr(quad, path):
    zarrpath = os.path.join(path, "data.zarr")
    if os.path.exists(zarrpath):
        dataset = xr.open_zarr(zarrpath)
        quad.__dict__.update(dataset.attrs)
        dataset.attrs = {}
        if not dataset.sizes:  # this is an empty dataset we generated when data was None
            quad.data = None
        else:
            quad.data = QuadData(dataset)
    else:
        raise FileNotFoundError(f'Unable to find zarr store at {zarrpath}')


def save_netcdf(quad, path):
    os.makedirs(path, exist_ok=True)
    if quad.data is None:
        # leave a mark that data was none
        dataset = xr.Dataset({})
    else:
        if not isinstance(quad.data(), xr.Dataset):
            raise NotImplementedError('Only xarray datasets support save_netcdf, found {}'.format(type(quad.data())))
        dataset = quad.data()
    d = quad.__dict__.copy()
    # don't save the object pointers to children and parent as they will be rebuilt on load
    d.pop("children")
    d.pop("parent")
    # don't save the data array, it will be stored as the main dataset here.
    d.pop("data")
    # don't save is_leaf, set on init
    d.pop('is_leaf')
    dataset.attrs = d
    dataset.to_netcdf(os.path.join(path, "data.nc"), format='NETCDF4')


def load_netcdf(quad, path):
    netcdfpath = os.path.join(path, "data.nc")
    if os.path.exists(netcdfpath):
        dataset = xr.open_dataset(netcdfpath, engine='netcdf4', chunks={})
        for ky, val in dataset.attrs.items():
            if isinstance(val, np.ndarray):
                dataset.attrs[ky] = val.tolist()
            elif isinstance(val, np.int32) and ky == 'location':
                dataset.attrs[ky] = [int(val)]
        quad.__dict__.update(dataset.attrs)
        dataset.attrs = {}
        if not dataset.sizes:  # this is an empty dataset we generated when data was None
            quad.data = None
        else:
            quad.data = QuadData(dataset)
    else:
        raise FileNotFoundError(f'Unable to find zarr store at {netcdfpath}')


def save_numpy(quad, path):
    """ Saves the QuadTree.data to a numpy file

    Parameters
    ----------
    quad
        QuadTree instance to store the top level data from.  Children/parent/data not stored
    path
        Directory to make the json file in
    Returns
    -------
    None

    """
    os.makedirs(path, exist_ok=True)
    # numpy will write a None to disk but not load it back.  If data is None then leave a special file so we know the data was truly none (not missed)
    if quad.data is None:
        # leave a mark that data was none
        outputfile = open(os.path.join(path, "data.none.npy"), "wb")
    else:
        outputfile = open(os.path.join(path, "data.npy"), "wb")
        if isinstance(quad.data(), da.Array):
            np.save(outputfile, quad.data().compute())
        else:
            np.save(outputfile, quad.data())


def load_numpy(quad, path):
    """ Load the QuadTree.data from disk

    Parameters
    ----------
    quad
        QuadTree instance to store the top level data from.  Children/parent/data not stored
    path
        Directory to make the json file in
    Returns
    -------
    None

    """
    try:
        outputfile = open(os.path.join(path, "data.npy"), "rb")
        quad.data = QuadData(da.from_array(np.load(outputfile)))
    except FileNotFoundError as e:
        # if the data didn't read, see if it was originally none, otherwise raise an error.
        if not os.path.exists(os.path.join(path, "data.none.npy")):
            raise e
        else:
            quad.data = None


class StoreQuad(ABC):
    """ This abstract class defines the required interface for storing a QuadTree object.
    Pass a class derived from this base class to the QuadTree.save or QuadTree.load function and it will save or load to disk.
    """
    # these will become the names of the children quads, could change to NW, NE, SW, SE or something more meaningful too.
    child_names = ('0', '1', '2', '3')
    @abstractmethod
    def save(self, quad, path):
        """ Save function is expected to store both the metadata (attributes) of the QuadTree and the data arrays.

        Parameters
        ----------
        quad
            The data object to save
        path
            The location or file object to use for saving data

        Returns
        -------
        The path or file object to be used in future calls to child_path and save or load for children

        """
        raise NotImplementedError()
    @abstractmethod
    def load(self, quad, path):
        """ Load is expected to update an existing QuadTree instance with data from disk.
        Parameters
        ----------
        quad
            The data object to save
        path
            The location or file object to use for loading data

        Returns
        -------
        The path or file object to be used in future calls to child_path and save or load for children

        """
        raise NotImplementedError()
    @abstractmethod
    def child_path(self, path, i):
        """ Each quad has four children and this function will return the path or object to use when calling save or load for this child.

        Parameters
        ----------
        path
            The location or file object to use for saving data
        i
            The child name or number to store.  This is currently expected to be an integer in range(4).

        Returns
        -------
        The path or file object to be used in future calls to child_path and save or load for children

        """
        raise NotImplementedError()
    @classmethod
    def has_children(cls, path):
        """ Determine if the path/object contains children that could be loaded
        Parameters
        ----------
        path
            The location or file object to use for loading data

        Returns
        -------
        Boolean
        """
        raise NotImplementedError()
    @classmethod
    def clear_children(cls, path, names=(0, 1, 2, 3)):
        """ Remove children from the path/file
        Parameters
        ----------
        path
            The location or file object to use for loading/saving data

        Returns
        -------
        None
        """
        raise NotImplementedError()


class StorePickles(StoreQuad):
    """ Store the metadata of a QuadTree as pickles.
    Separates out the QuadTree.data to store as a numpy array to show how to use a second format.
    This would likely be replaced by a zarr or lazy load implementation.
    """

    @classmethod
    def save(cls, quad, path):
        """ Store most attributes to a pickle file and the data to numpy files

        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for saving data

        Returns
        -------
        str
            Same path as what was supplied
        """
        save_pickle(quad, path)
        save_numpy(quad, path)
        return path

    @classmethod
    def load(cls, quad, path):
        """
        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for loading data

        Returns
        -------
        str
            Same path as passed in
        """
        load_pickle(quad, path)
        load_numpy(quad, path)
        return path

    @staticmethod
    def child_path(path, i):
        """ Return the directory name the child should be put in
        Parameters
        ----------
        path
            parent directory str
        i
            which quad name to use

        Returns
        -------
        str
        """
        return os.path.join(path, str(i))

    @classmethod
    def clear_children(cls, path):
        for i in cls.child_names:
            # clear existing child directories so we don't accidentally have old data get mixed in
            child_path = cls.child_path(path, i)
            if os.path.exists(child_path):
                shutil.rmtree(child_path)

    @classmethod
    def has_children(cls, path):
        return os.path.exists(cls.child_path(path, 0))


class StoreJson(StoreQuad):
    """ Store the metadata of a QuadTree as pickles.
    Separates out the QuadTree.data to store as a numpy array to show how to use a second format.
    This would likely be replaced by a zarr or lazy load implementation.
    """

    @classmethod
    def save(cls, quad, path):
        """ Store most attributes to a pickle file and the data to numpy files

        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for saving data

        Returns
        -------
        str
            Same path as what was supplied
        """
        save_json(quad, path)
        save_numpy(quad, path)
        return path

    @classmethod
    def load(cls, quad, path):
        """
        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for loading data

        Returns
        -------
        str
            Same path as passed in
        """
        load_json(quad, path)
        load_numpy(quad, path)
        return path

    @staticmethod
    def child_path(path, i):
        """ Return the directory name the child should be put in
        Parameters
        ----------
        path
            parent directory str
        i
            which quad name to use

        Returns
        -------
        str
        """
        return os.path.join(path, str(i))

    @classmethod
    def clear_children(cls, path):
        for i in cls.child_names:
            # clear existing child directories so we don't accidentally have old data get mixed in
            child_path = cls.child_path(path, i)
            if os.path.exists(child_path):
                shutil.rmtree(child_path)

    @classmethod
    def has_children(cls, path):
        return os.path.exists(cls.child_path(path, 0))


class StoreZarr(StoreQuad):
    """
    Store the metadata of a QuadTree as zarr rootgroup.
    """

    @classmethod
    def save(cls, quad, path):
        """ Store most attributes to a pickle file and the data to numpy files

        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for saving data

        Returns
        -------
        str
            Same path as what was supplied
        """
        save_zarr(quad, path)
        return path

    @classmethod
    def load(cls, quad, path):
        """
        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for loading data

        Returns
        -------
        str
            Same path as passed in
        """
        load_zarr(quad, path)
        return path

    @staticmethod
    def child_path(path, i):
        """ Return the directory name the child should be put in
        Parameters
        ----------
        path
            parent directory str
        i
            which quad name to use

        Returns
        -------
        str
        """
        return os.path.join(path, str(i))

    @classmethod
    def clear_children(cls, path):
        for i in cls.child_names:
            # clear existing child directories so we don't accidentally have old data get mixed in
            child_path = cls.child_path(path, i)
            if os.path.exists(child_path):
                shutil.rmtree(child_path)

    @classmethod
    def has_children(cls, path):
        return os.path.exists(cls.child_path(path, 0))


class StoreNetcdf(StoreQuad):
    """
    Store the metadata of a QuadTree as zarr rootgroup.
    """

    @classmethod
    def save(cls, quad, path):
        """ Store most attributes to a pickle file and the data to numpy files

        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for saving data

        Returns
        -------
        str
            Same path as what was supplied
        """
        save_netcdf(quad, path)
        return path

    @classmethod
    def load(cls, quad, path):
        """
        Parameters
        ----------
        quad
            The data object to save
        path
            The parent directory to use for loading data

        Returns
        -------
        str
            Same path as passed in
        """
        load_netcdf(quad, path)
        return path

    @staticmethod
    def child_path(path, i):
        """ Return the directory name the child should be put in
        Parameters
        ----------
        path
            parent directory str
        i
            which quad name to use

        Returns
        -------
        str
        """
        return os.path.join(path, str(i))

    @classmethod
    def clear_children(cls, path):
        for i in cls.child_names:
            # clear existing child directories so we don't accidentally have old data get mixed in
            child_path = cls.child_path(path, i)
            if os.path.exists(child_path):
                shutil.rmtree(child_path)

    @classmethod
    def has_children(cls, path):
        return os.path.exists(cls.child_path(path, 0))


class StoreHDF5(StoreQuad):
    """ Store the QuadTree in HDF5.
    Saves all standard types as attributes (bool, int, float, str).
    Saves numpy arrays and lists as datasets.
    Note: lists saved to HDF5 will be loaded back as numpy arrays.
    """

    @staticmethod
    def save(quad, hdffile):
        """ Store Quad into a HDF5 file.
        Standard types stored as attributes while lists and numpy arrays are stored as h5py.Datasets

        Parameters
        ----------
        quad
            The data object to save
        hdffile
            Either the str full path to the hdf5 file or an h5py.File objeect to write inside of.

        Returns
        -------
        h5py object
            A h5py group object that can be used to write data into hdf5
        """
        # if hdffile parameter is a string, convert the file path to an h5py.File
        if isinstance(hdffile, str):
            hdffile = h5py.File(hdffile, 'w')
        for key, value in quad.__dict__.items():
            # don't store the parent or children objects that are linked list pointers
            # they will be set by QuadTree.load() as it reads the data recursively
            if key in ('parent', 'children'):
                continue
            # None doesn't seem to work, so don't even store them in hdf5 -- relies on __init__ to make None place holders.
            elif value is None:
                continue
            # Save standard stuff as hdf5 attributes
            elif isinstance(value, (int, float, str)):
                hdffile.attrs[key] = value
            # store lists and numpy arrays as datasets
            elif isinstance(value, (np.ndarray, list, QuadData)):
                if key == 'data':
                    dataset = hdffile.create_dataset(key, data=value(), chunks=True, compression='gzip', compression_opts=9)
                else:
                    try:
                        rec_array = np.core.records.fromarrays([value])  # , dtype=[(name, dtype) for name, dtype in zip(write_keys, write_compound_dtype)])
                        dataset = hdffile.create_dataset(key, data=rec_array, chunks=True, compression='gzip', compression_opts=9)
                    # this exception comes up with empty arrays - ignore it if it was empty (like None is ignored)
                    except IndexError as e:
                        if rec_array.size > 0:  # empty lists cause an exception
                            raise e
            # watch out for other types that aren't being handled.
            else:
                raise TypeError(f"Unexpected type in {key}, {value}")
        return hdffile

    @classmethod
    def load(cls, quad, hdffile):
        """ Store Quad into a HDF5 file

        Parameters
        ----------
        quad
            The data object to load into
        hdffile
            Either the str full path to the hdf5 file or an h5py.File object to write inside of.

        Returns
        -------
        h5py object
            A h5py group object that can be used to write data into hdf5
        """
        # if hdffile parameter is a string, convert the file path to an h5py.File
        if isinstance(hdffile, str):
            hdffile = h5py.File(hdffile, 'r')
        # load simple attributes back into the dictionary.
        # Note that things not in the hdf5 are not replaced, so defaults or new items in QuadTree __init__ will be retained
        for attr_name in hdffile.attrs:
            quad.__dict__[attr_name] = hdffile.attrs[attr_name]
        # Go through the datasets and
        for key in hdffile.keys():
            if isinstance(hdffile[key], h5py.Dataset):
                if key == 'data':
                    try:
                        quad.__dict__[key] = QuadData(da.from_array(np.array(hdffile[key])))
                    except ValueError as e:
                        if np.array(hdffile[key]).size > 0:  # empty lists cause an exception
                            raise e
                else:
                    quad.__dict__[key] = np.array(np.array(hdffile[key]), dtype=hdffile[key].dtype.descr[0][1])
            elif isinstance(hdffile[key], h5py.Group):
                # these should be the children
                if key not in cls.child_names:
                    raise TypeError(f"Found a key with an unexpected group value {key}")
        return hdffile

    @staticmethod
    def child_path(path, i):
        """ Return the h5py group of the child
        Parameters
        ----------
        path
            h5py group object to store under
        i
            which quad name to use

        Returns
        -------
        h5py Group object
        """
        try:
            grp = path[str(i)]
        except KeyError:
            grp = path.create_group(str(i))  # returns the group object inside te hdf5 file
        return grp

    @classmethod
    def clear_children(cls, path):
        # does sphinx/pycharm use the docs from the parent?  See parent class docs
        for i in cls.child_names:
            # clear existing child directories so we don't accidentally have old data get mixed in
            try:
                del path[str(i)]
            except KeyError:
                pass

    @classmethod
    def has_children(cls, path):
        # does sphinx/pycharm use the docs from the parent?  See parent class docs
        return cls.child_names[0] in path.keys()
