import os
import shutil
import json
import numpy as np
import xarray as xr
import zarr
import tempfile

from HSTB.kluster.backends._zarr import _get_indices_dataset_exists, _get_indices_dataset_notexist, \
    _my_xarr_to_zarr_build_arraydimensions, _my_xarr_to_zarr_writeattributes, ZarrWrite, ZarrBackend, search_not_sorted
from HSTB.kluster.xarray_helpers import reload_zarr_records
import unittest


class TestZarr(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.clsFolder = os.path.join(tempfile.gettempdir(), 'TestZarr')
        try:
            os.mkdir(cls.clsFolder)
        except FileExistsError:
            shutil.rmtree(cls.clsFolder)
            os.mkdir(cls.clsFolder)

    def setUp(self) -> None:
        self.zarr_folder = tempfile.mkdtemp(dir=self.clsFolder)
        self.zb = ZarrBackend(self.zarr_folder)
        self.zw = ZarrWrite(self.zarr_folder, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
        self.zw.rootgroup = zarr.group()

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.clsFolder)

    def test_search_not_sorted(self):
        master = np.array([3, 4, 5, 6, 1, 9, 0, 2, 7, 8])
        search = np.array([6, 4, 3, 1, 1])

        final_inds = search_not_sorted(master, search)

        assert (np.array_equal(master[final_inds], search))

    def test_get_write_indices_zarr_create(self):
        # test the easy one first, this is the indices when no data exists, we are writing for the first time
        # indices should be a list of start index, end index for each array in the list
        data_time = np.array([1, 2, 3, 4, 5])
        input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
        indices = _get_indices_dataset_notexist(input_time_arrays)
        assert indices == [[0, 5]]

    def test_get_write_indices_zarr_append(self):
        # let zarr_time represent the time dimension of the data that is on disk for our test
        zarr_time = zarr.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        # now lets test what happens with an append
        # indices are still lists of start/end index
        data_time = np.array([10, 11, 12, 13, 14])
        input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
        indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
        assert indices == [[10, 15]]
        assert push_forward == []
        assert total_push == 0

    def test_get_write_indices_zarr_overwrite(self):
        # let zarr_time represent the time dimension of the data that is on disk for our test
        zarr_time = zarr.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        # now lets test what happens when we want to overwrite
        # indices for overwrite will be an array equal to the data length, to use the zarr set coordinate selection method
        data_time = np.array([4, 5, 6, 7, 8])
        input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
        indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
        assert len(indices) == 1
        assert np.array_equal(indices[0], np.array([4, 5, 6, 7, 8]))
        # pushforward is 0 here as we did not need to move the original data up at all
        assert push_forward == []
        assert total_push == 0

    def test_get_write_indices_zarr_partlycoveredafter(self):
        # let zarr_time represent the time dimension of the data that is on disk for our test
        zarr_time = zarr.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        # now we make sure that when data is partly in the array, we get the correct indices to overwrite and append
        data_time = np.array([7, 8, 9, 10, 11])
        input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
        indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
        assert len(indices) == 1
        assert np.array_equal(indices[0], np.array([7, 8, 9, 10, 11]))
        # pushforward is 0 here as we did not need to move the original data up at all
        assert push_forward == []
        assert total_push == 0

    def test_get_write_indices_zarr_partlycoveredafter_middle(self):
        # let zarr_time represent the time dimension of the data that is on disk for our test
        zarr_time = zarr.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17])
        # now we make sure that when data is partly in the array, we get the correct indices to overwrite and append
        data_time = np.array([7, 8, 9, 10, 11])
        input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
        indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
        assert len(indices) == 1
        assert np.array_equal(indices[0], np.array([7, 8, 9, 10, 11]))
        # pushforward is 2 here as we need to push the original data up two to make room
        assert push_forward == [[10, 2]]
        # no room needed at beginning though
        assert total_push == 0

    def test_get_write_indices_zarr_partlycoveredprior(self):
        # let zarr_time represent the time dimension of the data that is on disk for our test
        zarr_time = zarr.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        # now we make sure that when data is partly in the array, we get the correct indices to overwrite and append
        data_time = np.array([8, 9, 10, 11, 12])
        input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
        indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
        assert len(indices) == 1
        assert np.array_equal(indices[0], np.array([0, 1, 2, 3, 4]))
        # pushforward is 2 here as we need to push the original data up two to make room
        assert push_forward == [[0, 2]]
        assert total_push == 2

    def test_get_write_indices_zarr_outoforder(self):
        # now check to make sure this all works when the already written data is out of time order
        zarr_time = zarr.array([5, 6, 7, 8, 9, 0, 1, 2, 3, 4])
        data_time = np.array([4, 5, 6, 7, 8])
        input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
        indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
        assert len(indices) == 1
        assert np.array_equal(indices[0], np.array([9, 0, 1, 2, 3]))
        assert push_forward == []
        assert total_push == 0

    def test_xarr_to_zarr_writeattributes(self):
        rootgroup = zarr.group()
        test_attrs = {'test': 1}
        _my_xarr_to_zarr_writeattributes(rootgroup, test_attrs)
        assert rootgroup.attrs['test'] == 1

        # numpy is saved as list
        test_attrs = {'test2': np.array([1, 2, 3])}
        _my_xarr_to_zarr_writeattributes(rootgroup, test_attrs)
        assert rootgroup.attrs['test2'] == [1, 2, 3]

        # new values are appended to existing lists
        test_attrs = {'test2': [4]}
        _my_xarr_to_zarr_writeattributes(rootgroup, test_attrs)
        assert rootgroup.attrs['test2'] == [1, 2, 3, 4]

        # new dict is saved like normal
        test_attrs = {'test3': {'a': 1}}
        _my_xarr_to_zarr_writeattributes(rootgroup, test_attrs)
        assert rootgroup.attrs['test3'] == {'a': 1}

        # keys that exist will be updated with the new data
        test_attrs = {'test3': {'a': 3, 'b': 4}}
        _my_xarr_to_zarr_writeattributes(rootgroup, test_attrs)
        assert rootgroup.attrs['test3'] == {'a': 3, 'b': 4}

        # lets you have some pretty complex attributes
        test_attrs = {'test4': {'line1': ['name', 123, 456], 'line2': ['nametwo', 345, 457]}}
        _my_xarr_to_zarr_writeattributes(rootgroup, test_attrs)
        assert rootgroup.attrs['test4'] == {'line1': ['name', 123, 456], 'line2': ['nametwo', 345, 457]}

    def test_build_arraydimensions(self):
        ping_time = np.arange(100)
        data_arr = np.arange(100)
        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': ping_time})
        dims = _my_xarr_to_zarr_build_arraydimensions(dataset)
        assert dims == {'data': [('time',), (100,), None], 'data2': [('time',), (100,), None],
                        'time': [('time',), (100,), None]}

    def test_zarr_write_create(self):
        # simulated write to disk
        #  this is for the first write, where we have to use zarr to create the dataset
        data_arr = np.arange(100)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, {'testthis': 123}, dataloc=indices[0], finalsize=(100, 400))

        assert np.array_equal(self.zw.rootgroup['data'], data_arr)
        assert np.array_equal(self.zw.rootgroup['data2'], data_arr)
        assert np.array_equal(self.zw.rootgroup['time'], data_arr)
        assert self.zw.rootgroup.attrs == {'testthis': 123}

    def test_zarr_write_append(self):
        #  this is for the first write, where we have to use zarr to create the dataset
        data_arr = np.arange(10)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(10, 400))

        data_arr2 = np.array([10, 11, 12, 13, 14])
        indices, push_forward, total_push = _get_indices_dataset_exists([data_arr2], self.zw.rootgroup['time'])

        dataset = xr.Dataset({'data': (['time'], data_arr2), 'data2': (['time'], data_arr2)},
                             coords={'time': data_arr2})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(15, 400))

        assert np.array_equal(self.zw.rootgroup['data'], np.concatenate([data_arr, data_arr2]))
        assert np.array_equal(self.zw.rootgroup['data2'], np.concatenate([data_arr, data_arr2]))
        assert np.array_equal(self.zw.rootgroup['time'], np.concatenate([data_arr, data_arr2]))

    def test_zarr_write_overwrite(self):
        # overwrite existing data with this new dataset since the times overlap
        data_arr = np.arange(10)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(10, 400))

        data_arr2 = np.array([3, 4, 5, 6, 7])
        new_data = np.array([999, 999, 999, 999, 999])
        indices, push_forward, total_push = _get_indices_dataset_exists([data_arr2], self.zw.rootgroup['time'])

        dataset = xr.Dataset({'data': (['time'], new_data), 'data2': (['time'], new_data)}, coords={'time': data_arr2})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(10, 400))

        expected_answer = np.array([0, 1, 2, 999, 999, 999, 999, 999, 8, 9])
        assert np.array_equal(self.zw.rootgroup['data'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['data2'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['time'], data_arr)

    def test_zarr_write_prior_overlap(self):
        # for when data being written is both partly within existing data and prior to existing data
        data_arr = np.arange(20, 40, 1)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(20, 400))

        data_arr2 = [np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]),
                     np.array([20, 21, 22, 23, 24, 25, 26, 27, 28, 29])]
        indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, self.zw.rootgroup['time'])

        for cnt, arr in enumerate(data_arr2):
            if cnt == 0:
                fsize = (30, 400)
            else:
                fsize = None
            dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
            self.zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

        expected_answer = np.array(
            [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
             37, 38, 39])
        assert np.array_equal(self.zw.rootgroup['data'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['data2'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['time'], expected_answer)

    def test_zarr_write_prior(self):
        # for when data being written is prior to existing data
        data_arr = np.arange(30, 50, 1)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(20, 400))

        data_arr2 = [np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]),
                     np.array([20, 21, 22, 23, 24, 25, 26, 27, 28, 29])]
        indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, self.zw.rootgroup['time'])

        for cnt, arr in enumerate(data_arr2):
            if cnt == 0:
                fsize = (40, 400)
            else:
                fsize = None
            dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
            self.zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

        expected_answer = np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                                    30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49])
        assert np.array_equal(self.zw.rootgroup['data'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['data2'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['time'], expected_answer)

    def test_zarr_write_prior_bigone(self):
        # for when data being written is prior to existing data
        data_arr = np.arange(150000, 255000)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(105000, 400))

        data_arr2 = [np.arange(150000)]
        indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, self.zw.rootgroup['time'])

        dataset2 = xr.Dataset({'data': (['time'], data_arr2[0]), 'data2': (['time'], data_arr2[0])},
                              coords={'time': data_arr2[0]})
        self.zw.write_to_zarr(dataset2, None, dataloc=indices[0], finalsize=(255000, 400), push_forward=push_forward)

        assert np.array_equal(self.zw.rootgroup['data'], np.arange(255000))
        assert np.array_equal(self.zw.rootgroup['data2'], np.arange(255000))
        assert np.array_equal(self.zw.rootgroup['time'], np.arange(255000))

    def test_zarr_write_prior_multiplepushes(self):
        # for when data being written is prior to existing data and is in pieces
        data_arr = np.concatenate([np.arange(100000, 150000), np.arange(200000, 255000)])
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(105000, 400))

        data_arr2 = [np.arange(100000), np.arange(150000, 200000)]
        indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, self.zw.rootgroup['time'])

        for cnt, arr in enumerate(data_arr2):
            if cnt == 0:
                fsize = (255000, 400)
            else:
                fsize = None
            dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
            self.zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

        assert np.array_equal(self.zw.rootgroup['data'], np.arange(255000))
        assert np.array_equal(self.zw.rootgroup['data2'], np.arange(255000))
        assert np.array_equal(self.zw.rootgroup['time'], np.arange(255000))

    def test_zarr_write_later_overlap(self):
        # for when data being written is both partly within existing data and later than existing data
        data_arr = np.arange(20, 40, 1)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(20, 400))

        data_arr2 = [np.array([30, 31, 32, 33, 34, 35, 36, 37, 38, 39]),
                     np.array([40, 41, 42, 43, 44, 45, 46, 47, 48, 49])]
        indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, self.zw.rootgroup['time'])

        for cnt, arr in enumerate(data_arr2):
            if cnt == 0:
                fsize = (30, 400)
            else:
                fsize = None
            dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
            self.zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

        expected_answer = np.array(
            [20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
             47, 48, 49])
        assert np.array_equal(self.zw.rootgroup['data'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['data2'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['time'], expected_answer)

    def test_zarr_write_later(self):
        # for when data being written is after existing data
        data_arr = np.arange(30, 50, 1)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(20, 400))

        data_arr2 = [np.array([50, 51, 52, 53, 54, 55, 56, 57, 58, 59]),
                     np.array([60, 61, 62, 63, 64, 65, 66, 67, 68, 69])]
        indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, self.zw.rootgroup['time'])

        for cnt, arr in enumerate(data_arr2):
            if cnt == 0:
                fsize = (40, 400)
            else:
                fsize = None
            dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
            self.zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

        expected_answer = np.array([30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                                    50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69])
        assert np.array_equal(self.zw.rootgroup['data'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['data2'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['time'], expected_answer)

    def test_zarr_write_inbetween(self):
        # for when data is written inbetween existing data without overlap
        data_arr = np.arange(10)
        indices = _get_indices_dataset_notexist([data_arr])
        dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(10, 400))

        data_arr2 = np.array([20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
        indices, push_forward, total_push = _get_indices_dataset_exists([data_arr2], self.zw.rootgroup['time'])
        dataset2 = xr.Dataset({'data': (['time'], data_arr2), 'data2': (['time'], data_arr2)},
                              coords={'time': data_arr2})
        self.zw.write_to_zarr(dataset2, None, dataloc=indices[0], finalsize=(20, 400), push_forward=push_forward)

        data_arr3 = np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        indices, push_forward, total_push = _get_indices_dataset_exists([data_arr3], self.zw.rootgroup['time'])
        dataset3 = xr.Dataset({'data': (['time'], data_arr3), 'data2': (['time'], data_arr3)},
                              coords={'time': data_arr3})
        self.zw.write_to_zarr(dataset3, None, dataloc=indices[0], finalsize=(30, 400), push_forward=push_forward)

        expected_answer = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
        assert np.array_equal(self.zw.rootgroup['data'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['data2'], expected_answer)
        assert np.array_equal(self.zw.rootgroup['time'], expected_answer)

    def test_zarr_write_merge(self):
        # merge is for when we have an existing rootgroup, but the new dataset has a variable that is not in the rootgroup
        data_arr = np.arange(10)
        indices = _get_indices_dataset_notexist([data_arr])

        dataset = xr.Dataset({'data': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(10, 400))

        indices, push_forward, total_push = _get_indices_dataset_exists([data_arr], self.zw.rootgroup['time'])

        dataset = xr.Dataset({'data2': (['time'], data_arr)}, coords={'time': data_arr})
        self.zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=(10, 400))

        assert np.array_equal(self.zw.rootgroup['data'], data_arr)
        assert np.array_equal(self.zw.rootgroup['data2'], data_arr)
        assert np.array_equal(self.zw.rootgroup['time'], data_arr)

    def _return_basic_datasets(self, start: int, end: int, override_beam_number: int = 400):
        dataset_name = 'ping'
        sysid = '123'
        datasets = []
        dataset_time_arrays = []
        attributes = {'test_attribute': 'abc'}
        for i in range(start, end):
            data_arr = np.arange(i * 10, (i * 10) + 10)
            data2_arr = np.random.uniform(-1, 1, (10, override_beam_number))
            beam_arr = np.arange(override_beam_number)
            dataset = xr.Dataset({'counter': (['time'], data_arr.copy()), 'beampointingangle': (['time', 'beam'], data2_arr)},
                                 coords={'time': data_arr.copy(), 'beam': beam_arr})
            datasets.append(dataset)
            dataset_time_arrays.append(data_arr.copy())
        return dataset_name, datasets, dataset_time_arrays, attributes, sysid

    def test_zarr_backend_newdata(self):
        # write actual data to disk in the following tests.  This test illustrates writing data to a new data store
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 3)
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(30))
        assert np.array_equal(xdataset.time.values, np.arange(30))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        assert np.array_equal(xdataset.beampointingangle.values,
                              np.concatenate([d.beampointingangle for d in datasets]))
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_overwrite(self):
        # write new data to disk
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 4)
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        # now build data inside the existing data
        dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 3)
        zarr_path, _ = self.zb.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(40))
        assert np.array_equal(xdataset.time.values, np.arange(40))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate(
            [datasets[0].beampointingangle, newdatasets[0].beampointingangle, newdatasets[1].beampointingangle,
             datasets[3].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_partial_insert_drop(self):
        # write new data to disk, leave a gap at the last to simulate overlap where data does not exist
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 1)
        newtime = datasets[0].time.values
        newtime[-1] = 10
        dataset_time_arrays = [newtime]
        datasets = [xr.Dataset({'counter': (['time'], datasets[0].counter.data), 'beampointingangle': (['time', 'beam'], datasets[0].beampointingangle.data)},
                               coords={'time': newtime, 'beam': datasets[0].beam.data})]
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

        # now build data inside the existing data
        dataset_name2, datasets2, dataset_time_arrays2, attributes2, sysid2 = self._return_basic_datasets(1, 2)
        newtime2 = datasets2[0].time.values
        newtime2[0] = 9
        dataset_time_arrays2 = [newtime2]
        datasets2 = [xr.Dataset({'counter': (['time'], datasets2[0].counter.data), 'beampointingangle': (['time', 'beam'], datasets2[0].beampointingangle.data)},
                                coords={'time': newtime2, 'beam': datasets2[0].beam.data})]
        zarr_path, _ = self.zb.write(dataset_name2, datasets2, dataset_time_arrays2, attributes2, skip_dask=True, sys_id=sysid2)

        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        # with existing counter values of array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        #  the new write with values array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]) will have the first value dropped as
        #  the time for that value overlaps the existing data, and is a new time, which is not allowed
        expected_counter = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        answer = xdataset.counter.values
        assert np.array_equal(answer, expected_counter)

        # with existing counter values of array([ 0,  1,  2,  3,  4,  5,  6,  7,  8, 10])
        #  the new write with values array([ 9, 11, 12, 13, 14, 15, 16, 17, 18, 19]) will have the first value dropped as
        #  the time for that value overlaps the existing data, and is a new time, which is not allowed
        expected_time = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        assert np.array_equal(xdataset.time.values, expected_time)

        assert np.array_equal(xdataset.beam.values, np.arange(400))

        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_partial_insert_drop_prior(self):
        # write new data to disk, leave a gap at the last to simulate overlap where data does not exist
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 2)
        newtime = datasets[0].time.values
        newtime[0] = 9
        dataset_time_arrays = [newtime]
        datasets = [xr.Dataset({'counter': (['time'], datasets[0].counter.data),
                                'beampointingangle': (['time', 'beam'], datasets[0].beampointingangle.data)},
                               coords={'time': newtime, 'beam': datasets[0].beam.data})]
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

        # now build data inside the existing data
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 1)
        newtime = datasets[0].time.values
        newtime[-1] = 10
        dataset_time_arrays = [newtime]
        datasets = [xr.Dataset({'counter': (['time'], datasets[0].counter.data),
                                'beampointingangle': (['time', 'beam'], datasets[0].beampointingangle.data)},
                               coords={'time': newtime, 'beam': datasets[0].beam.data})]
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        expected_counter = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        answer = xdataset.counter.values
        assert np.array_equal(answer, expected_counter)

        expected_time = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        assert np.array_equal(xdataset.time.values, expected_time)

        assert np.array_equal(xdataset.beam.values, np.arange(400))

        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_partial_before(self):
        # write new data to disk
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 7)
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        # now build data partially before and inside the existing dataset
        dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(2, 4)
        zarr_path, _ = self.zb.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(20, 70))
        assert np.array_equal(xdataset.time.values, np.arange(20, 70))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate(
            [newdatasets[0].beampointingangle, newdatasets[1].beampointingangle, datasets[1].beampointingangle,
             datasets[2].beampointingangle, datasets[3].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_partial_after(self):
        # write new data to disk
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 7)
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        # now build data partially after and inside the existing dataset
        dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(6, 8)
        zarr_path, _ = self.zb.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(30, 80))
        assert np.array_equal(xdataset.time.values, np.arange(30, 80))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate(
            [datasets[0].beampointingangle, datasets[1].beampointingangle, datasets[2].beampointingangle,
             newdatasets[0].beampointingangle, newdatasets[1].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_fully_before(self):
        # write new data to disk

        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 7)
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        # now build data fully before the existing data
        dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 3)
        zarr_path, _ = self.zb.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(10, 70))
        assert np.array_equal(xdataset.time.values, np.arange(10, 70))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate([newdatasets[0].beampointingangle, newdatasets[1].beampointingangle,
                                        datasets[0].beampointingangle, datasets[1].beampointingangle,
                                        datasets[2].beampointingangle,
                                        datasets[3].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_fully_after(self):
        # write new data to disk
        dataset_name, datasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 7)
        zarr_path, _ = self.zb.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        # now build data fully before the existing data
        dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(7, 9)
        zarr_path, _ = self.zb.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(30, 90))
        assert np.array_equal(xdataset.time.values, np.arange(30, 90))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate(
            [datasets[0].beampointingangle, datasets[1].beampointingangle, datasets[2].beampointingangle,
             datasets[3].beampointingangle, newdatasets[0].beampointingangle, newdatasets[1].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_newdata_inside(self):
        # write new data to disk
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 1)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # write next data to disk, with a gap between it and existing data
        dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(2, 3)
        zarr_path, _ = self.zb.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write inbetween
        dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 2)
        zarr_path, _ = self.zb.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(30))
        assert np.array_equal(xdataset.time.values, np.arange(30))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
                                        thirddatasets[0].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_inbetween_and_overlap_and_after(self):
        # Test complex array layouts.
        # Times 0 - 9 will be written first and never overlapped
        # Times 20 - 39 will be written and later have overlap from 30-39
        # Times 50 - 89 will be written and later have overlap from 60-69 and 80-89
        # Then a group of times 10 - 19, 30 - 49 and 60-80 will be written
        # write new data to disk
        dataset_name, d1, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 2)
        zarr_path, _ = self.zb.write(dataset_name, d1, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # write next data to disk, with a gap between it and existing data
        dataset_name, d2, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 5)
        zarr_path, _ = self.zb.write(dataset_name, d2, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # write next data to disk, with a gap between it and existing data
        dataset_name, d3, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(6, 8)
        zarr_path, _ = self.zb.write(dataset_name, d3, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        # write next data to disk, with a gap between it and existing data
        dataset_name, d4, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(9, 10)
        zarr_path, _ = self.zb.write(dataset_name, d4, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        input_datasets, input_times = [], []
        for start, stop in [(0, 2),  # prepend + overlap + duplicates the times from the first group (goes first for the sort of inputs requirement)
                            (1, 2),  # just overlap
                            (2, 3),  # now write in between
                            (4, 6),  # overlap + in between
                            (9, 11),  # overlap + end
                            ]:  # overlap plus at the end
            dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(start, stop)
            consolidated_data = xr.concat(seconddatasets, dim='time')
            consolidated_time_arrays = np.concatenate(dataset_time_arrays)
            input_datasets.append(consolidated_data)
            input_times.append(consolidated_time_arrays)
        zarr_path, _ = self.zb.write(dataset_name, input_datasets, input_times, attributes, skip_dask=True,
                                     sys_id=sysid)

        xdataset = reload_zarr_records(zarr_path, skip_dask=True)
        # left the range from 80-89 empty
        assert np.array_equal(xdataset.counter.values, np.concatenate([np.arange(80), np.arange(90, 110)]))
        assert np.array_equal(xdataset.time.values, np.concatenate([np.arange(80), np.arange(90, 110)]))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        # the angles are random numbers so we have to be right.
        # The angle stored would be the last data sent to the zarr write function
        expectedangle = np.concatenate([input_datasets[0].beampointingangle.values[:10],  # (0, 2) but the second half is overwritten by the next input
                                        input_datasets[1].beampointingangle.values[:10],  # (1, 2)
                                        input_datasets[2].beampointingangle.values[:10],  # (2, 3)
                                        d2[0].beampointingangle.values[:10],  # (3, 5) from original setup
                                        input_datasets[3].beampointingangle.values[:20],  # (4, 6)
                                        d3[0].beampointingangle.values[:10],  # (6, 8) from original setup
                                        d3[1].beampointingangle.values[:10],  # (6, 8) from original setup
                                        input_datasets[4].beampointingangle.values[:20],  # (9, 10) from original setup
                                        ])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'


    def test_zarr_backend_alternating(self):
        # write new data to disk
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 1)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # write next data to disk, with a gap between it and existing data
        dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(2, 3)
        zarr_path, _ = self.zb.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write inbetween
        dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 2)
        zarr_path, _ = self.zb.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # write new data at the end
        dataset_name, fourthdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 4)
        zarr_path, _ = self.zb.write(dataset_name, fourthdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(40))
        assert np.array_equal(xdataset.time.values, np.arange(40))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
                                        thirddatasets[0].beampointingangle, fourthdatasets[0].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_write_backwards(self):
        # write new data to disk
        dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(2, 3)
        zarr_path, _ = self.zb.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write data prior
        dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 2)
        zarr_path, _ = self.zb.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write prior to that entry
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 1)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(30))
        assert np.array_equal(xdataset.time.values, np.arange(30))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate(
            [firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
             thirddatasets[0].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_multiple_concatenations(self):
        # write new data to disk
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 1)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write data after
        dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 2)
        zarr_path, _ = self.zb.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write after that entry
        dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(2, 3)
        zarr_path, _ = self.zb.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(30))
        assert np.array_equal(xdataset.time.values, np.arange(30))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate(
            [firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
             thirddatasets[0].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_overlap_inside(self):
        # write new data to disk
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 2)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write data after
        dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 4)
        zarr_path, _ = self.zb.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write prior to that entry that overlaps the first
        dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 3)
        zarr_path, _ = self.zb.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(40))
        assert np.array_equal(xdataset.time.values, np.arange(40))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
                                        seconddatasets[1].beampointingangle, thirddatasets[0].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_multiple_overlap_inside(self):
        # write new data to disk
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 2)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write data after
        dataset_name, fourthdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(4, 6)
        zarr_path, _ = self.zb.write(dataset_name, fourthdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write prior to that entry that overlaps the second
        dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(3, 5)
        zarr_path, _ = self.zb.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write prior to that entry that overlaps the first
        dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(1, 3)
        zarr_path, _ = self.zb.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)
        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(60))
        assert np.array_equal(xdataset.time.values, np.arange(60))
        assert np.array_equal(xdataset.beam.values, np.arange(400))
        expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
                                        seconddatasets[1].beampointingangle, thirddatasets[0].beampointingangle,
                                        thirddatasets[1].beampointingangle, fourthdatasets[1].beampointingangle])
        assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_expanding_beams(self):
        # write new data to disk
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 2)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        # now write data with a larger beam dimension to ensure that it expands to accomodate
        dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(2, 4, override_beam_number=512)
        zarr_path, _ = self.zb.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True,
                                     sys_id=sysid)

        xdataset = reload_zarr_records(zarr_path, skip_dask=True)

        assert np.array_equal(xdataset.counter.values, np.arange(40))
        assert np.array_equal(xdataset.time.values, np.arange(40))
        assert np.array_equal(xdataset.beam.values, np.arange(512))
        assert xdataset.beampointingangle.shape == (40, 512)

        answer = np.concatenate([np.concatenate([firstdatasets[0].beampointingangle.values, np.full((10, 112), np.nan)], axis=1),
                                 np.concatenate([firstdatasets[1].beampointingangle.values, np.full((10, 112), np.nan)], axis=1),
                                 thirddatasets[0].beampointingangle.values, thirddatasets[1].beampointingangle.values], axis=0)
        assert np.array_equal(xdataset.beampointingangle.values[~np.isnan(xdataset.beampointingangle.values)],
                              answer[~np.isnan(answer)])
        assert xdataset.beampointingangle.shape == answer.shape
        assert xdataset.attrs['test_attribute'] == 'abc'

    def test_zarr_backend_delete(self):
        # write new data to disk
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 2)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
        # writing will create a folder for each variable
        assert os.path.exists(os.path.join(zarr_path, 'beampointingangle'))
        # deleting the variable will delete the folder
        self.zb.delete(dataset_name, 'beampointingangle', sysid)
        assert not os.path.exists(os.path.join(zarr_path, 'beampointingangle'))

    def test_zarr_read_write_attributes(self):
        dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = self._return_basic_datasets(0, 2)
        zarr_path, _ = self.zb.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
        attrs = os.path.join(zarr_path, '.zattrs')
        with open(attrs, 'r') as attrsfile:
            data_on_disk = json.loads(attrsfile.read())
        assert data_on_disk == attributes
        # now we write a new attribute
        self.zb.write_attributes(dataset_name, {'test1': 1}, sysid)
        with open(attrs, 'r') as attrsfile:
            data_on_disk = json.loads(attrsfile.read())
        assert data_on_disk['test1'] == 1
        # now delete the attribute, you should be back to the original attribution
        self.zb.remove_attribute(dataset_name, 'test1', sysid)
        with open(attrs, 'r') as attrsfile:
            data_on_disk = json.loads(attrsfile.read())
        assert data_on_disk == attributes
