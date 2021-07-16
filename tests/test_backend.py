import os, shutil

from HSTB.kluster.backends._zarr import *
from HSTB.kluster.backends._zarr import _get_indices_dataset_exists, _get_indices_dataset_notexist, \
    _my_xarr_to_zarr_build_arraydimensions, _my_xarr_to_zarr_writeattributes
from HSTB.kluster.xarray_helpers import reload_zarr_records


def get_testzarr_paths():
    new_zarr_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data', 'zarrtest')
    if os.path.exists(new_zarr_folder):
        shutil.rmtree(new_zarr_folder)
    os.makedirs(new_zarr_folder, exist_ok=True)
    return new_zarr_folder


def cleanup_after_tests():
    newzarrfolder = get_testzarr_paths()
    shutil.rmtree(newzarrfolder)


def test_search_not_sorted():
    master = np.array([3, 4, 5, 6, 1, 9, 0, 2, 7, 8])
    search = np.array([6, 4, 3, 1, 1])

    final_inds = search_not_sorted(master, search)

    assert (np.array_equal(master[final_inds], search))


def test_get_write_indices_zarr_create():
    # test the easy one first, this is the indices when no data exists, we are writing for the first time
    # indices should be a list of start index, end index for each array in the list
    data_time = np.array([1, 2, 3, 4, 5])
    input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
    indices = _get_indices_dataset_notexist(input_time_arrays)
    assert indices == [[0, 5]]


def test_get_write_indices_zarr_append():
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


def test_get_write_indices_zarr_overwrite():
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


def test_get_write_indices_zarr_partlycoveredafter():
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


def test_get_write_indices_zarr_partlycoveredprior():
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


def test_get_write_indices_zarr_outoforder():
    # now check to make sure this all works when the already written data is out of time order
    zarr_time = zarr.array([5, 6, 7, 8, 9, 0, 1, 2, 3, 4])
    data_time = np.array([4, 5, 6, 7, 8])
    input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
    indices, push_forward, total_push = _get_indices_dataset_exists(input_time_arrays, zarr_time)
    assert len(indices) == 1
    assert np.array_equal(indices[0], np.array([9, 0, 1, 2, 3]))
    assert push_forward == []
    assert total_push == 0


def test_xarr_to_zarr_writeattributes():
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


def test_build_arraydimensions():
    ping_time = np.arange(100)
    data_arr = np.arange(100)
    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': ping_time})
    dims = _my_xarr_to_zarr_build_arraydimensions(dataset)
    assert dims == {'data': [('time',), (100,), None], 'data2': [('time',), (100,), None], 'time': [('time',), (100,), None]}


def test_zarr_write_create():
    # simulated write to disk
    #  this is for the first write, where we have to use zarr to create the dataset
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(100)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, {'testthis': 123}, dataloc=indices[0], finalsize=100)

    assert np.array_equal(zw.rootgroup['data'], data_arr)
    assert np.array_equal(zw.rootgroup['data2'], data_arr)
    assert np.array_equal(zw.rootgroup['time'], data_arr)
    assert zw.rootgroup.attrs == {'testthis': 123}


def test_zarr_write_append():
    #  this is for the first write, where we have to use zarr to create the dataset
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(10)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    data_arr2 = np.array([10, 11, 12, 13, 14])
    indices, push_forward, total_push = _get_indices_dataset_exists([data_arr2], zw.rootgroup['time'])

    dataset = xr.Dataset({'data': (['time'], data_arr2), 'data2': (['time'], data_arr2)}, coords={'time': data_arr2})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=15)

    assert np.array_equal(zw.rootgroup['data'], np.concatenate([data_arr, data_arr2]))
    assert np.array_equal(zw.rootgroup['data2'], np.concatenate([data_arr, data_arr2]))
    assert np.array_equal(zw.rootgroup['time'], np.concatenate([data_arr, data_arr2]))


def test_zarr_write_overwrite():
    # overwrite existing data with this new dataset since the times overlap
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(10)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    data_arr2 = np.array([3, 4, 5, 6, 7])
    new_data = np.array([999, 999, 999, 999, 999])
    indices, push_forward, total_push = _get_indices_dataset_exists([data_arr2], zw.rootgroup['time'])

    dataset = xr.Dataset({'data': (['time'], new_data), 'data2': (['time'], new_data)}, coords={'time': data_arr2})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    expected_answer = np.array([0, 1, 2, 999, 999, 999, 999, 999, 8, 9])
    assert np.array_equal(zw.rootgroup['data'], expected_answer)
    assert np.array_equal(zw.rootgroup['data2'], expected_answer)
    assert np.array_equal(zw.rootgroup['time'], data_arr)


def test_zarr_write_prior_overlap():
    # for when data being written is both partly within existing data and prior to existing data
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(20, 40, 1)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=20)

    data_arr2 = [np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]), np.array([20, 21, 22, 23, 24, 25, 26, 27, 28, 29])]
    indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, zw.rootgroup['time'])

    for cnt, arr in enumerate(data_arr2):
        if cnt == 0:
            fsize = 30
        else:
            fsize = None
        dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
        zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

    expected_answer = np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39])
    assert np.array_equal(zw.rootgroup['data'], expected_answer)
    assert np.array_equal(zw.rootgroup['data2'], expected_answer)
    assert np.array_equal(zw.rootgroup['time'], expected_answer)


def test_zarr_write_prior():
    # for when data being written is prior to existing data
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(30, 50, 1)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=20)

    data_arr2 = [np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]), np.array([20, 21, 22, 23, 24, 25, 26, 27, 28, 29])]
    indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, zw.rootgroup['time'])

    for cnt, arr in enumerate(data_arr2):
        if cnt == 0:
            fsize = 40
        else:
            fsize = None
        dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
        zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

    expected_answer = np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                                30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49])
    assert np.array_equal(zw.rootgroup['data'], expected_answer)
    assert np.array_equal(zw.rootgroup['data2'], expected_answer)
    assert np.array_equal(zw.rootgroup['time'], expected_answer)


def test_zarr_write_prior_bigone():
    # for when data being written is prior to existing data
    zw = ZarrWrite(None, desired_chunk_shape={'time': (1000,), 'data2': (1000,), 'data': (1000,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(15000, 20000)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=5000)

    data_arr2 = [np.arange(15000)]
    indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, zw.rootgroup['time'])

    dataset2 = xr.Dataset({'data': (['time'], data_arr2[0]), 'data2': (['time'], data_arr2[0])}, coords={'time': data_arr2[0]})
    zw.write_to_zarr(dataset2, None, dataloc=indices[0], finalsize=20000, push_forward=push_forward)

    assert np.array_equal(zw.rootgroup['data'], np.arange(20000))
    assert np.array_equal(zw.rootgroup['data2'], np.arange(20000))
    assert np.array_equal(zw.rootgroup['time'], np.arange(20000))


def test_zarr_write_later_overlap():
    # for when data being written is both partly within existing data and later than existing data
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(20, 40, 1)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=20)

    data_arr2 = [np.array([30, 31, 32, 33, 34, 35, 36, 37, 38, 39]), np.array([40, 41, 42, 43, 44, 45, 46, 47, 48, 49])]
    indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, zw.rootgroup['time'])

    for cnt, arr in enumerate(data_arr2):
        if cnt == 0:
            fsize = 30
        else:
            fsize = None
        dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
        zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

    expected_answer = np.array([20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49])
    assert np.array_equal(zw.rootgroup['data'], expected_answer)
    assert np.array_equal(zw.rootgroup['data2'], expected_answer)
    assert np.array_equal(zw.rootgroup['time'], expected_answer)


def test_zarr_write_later():
    # for when data being written is after existing data
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(30, 50, 1)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=20)

    data_arr2 = [np.array([50, 51, 52, 53, 54, 55, 56, 57, 58, 59]), np.array([60, 61, 62, 63, 64, 65, 66, 67, 68, 69])]
    indices, push_forward, total_push = _get_indices_dataset_exists(data_arr2, zw.rootgroup['time'])

    for cnt, arr in enumerate(data_arr2):
        if cnt == 0:
            fsize = 40
        else:
            fsize = None
        dataset2 = xr.Dataset({'data': (['time'], arr), 'data2': (['time'], arr)}, coords={'time': arr})
        zw.write_to_zarr(dataset2, None, dataloc=indices[cnt], finalsize=fsize, push_forward=push_forward)

    expected_answer = np.array([30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                                50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69])
    assert np.array_equal(zw.rootgroup['data'], expected_answer)
    assert np.array_equal(zw.rootgroup['data2'], expected_answer)
    assert np.array_equal(zw.rootgroup['time'], expected_answer)


def test_zarr_write_inbetween():
    # for when data is written inbetween existing data without overlap
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(10)
    indices = _get_indices_dataset_notexist([data_arr])
    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    data_arr2 = np.array([20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
    indices, push_forward, total_push = _get_indices_dataset_exists([data_arr2], zw.rootgroup['time'])
    dataset2 = xr.Dataset({'data': (['time'], data_arr2), 'data2': (['time'], data_arr2)}, coords={'time': data_arr2})
    zw.write_to_zarr(dataset2, None, dataloc=indices[0], finalsize=20, push_forward=push_forward)

    data_arr3 = np.array([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
    indices, push_forward, total_push = _get_indices_dataset_exists([data_arr3], zw.rootgroup['time'])
    dataset3 = xr.Dataset({'data': (['time'], data_arr3), 'data2': (['time'], data_arr3)}, coords={'time': data_arr3})
    zw.write_to_zarr(dataset3, None, dataloc=indices[0], finalsize=30, push_forward=push_forward)

    expected_answer = np.array([0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
    assert np.array_equal(zw.rootgroup['data'], expected_answer)
    assert np.array_equal(zw.rootgroup['data2'], expected_answer)
    assert np.array_equal(zw.rootgroup['time'], expected_answer)


def test_zarr_write_merge():
    # merge is for when we have an existing rootgroup, but the new dataset has a variable that is not in the rootgroup
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(10)
    indices = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    indices, push_forward, total_push = _get_indices_dataset_exists([data_arr], zw.rootgroup['time'])

    dataset = xr.Dataset({'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    assert np.array_equal(zw.rootgroup['data'], data_arr)
    assert np.array_equal(zw.rootgroup['data2'], data_arr)
    assert np.array_equal(zw.rootgroup['time'], data_arr)


def _return_basic_datasets(start: int, end: int):
    dataset_name = 'ping'
    sysid = '123'
    datasets = []
    dataset_time_arrays = []
    attributes = {'test_attribute': 'abc'}
    for i in range(start, end):
        data_arr = np.arange(i * 10, (i * 10) + 10)
        data2_arr = np.random.uniform(-1, 1, (10, 400))
        beam_arr = np.arange(400)
        dataset = xr.Dataset({'counter': (['time'], data_arr), 'beampointingangle': (['time', 'beam'], data2_arr)},
                             coords={'time': data_arr, 'beam': beam_arr})
        datasets.append(dataset)
        dataset_time_arrays.append(data_arr)
    return dataset_name, datasets, dataset_time_arrays, attributes, sysid


def test_zarr_backend_newdata():
    # write actual data to disk in the following tests.  This test illustrates writing data to a new data store
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, datasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 3)
    zarr_path, _ = zw.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(30))
    assert np.array_equal(xdataset.time.values, np.arange(30))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    assert np.array_equal(xdataset.beampointingangle.values, np.concatenate([d.beampointingangle for d in datasets]))
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_overwrite():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, datasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 4)
    zarr_path, _ = zw.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    # now build data inside the existing data
    dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 3)
    zarr_path, _ = zw.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(40))
    assert np.array_equal(xdataset.time.values, np.arange(40))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([datasets[0].beampointingangle, newdatasets[0].beampointingangle, newdatasets[1].beampointingangle, datasets[3].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_partial_before():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, datasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(3, 7)
    zarr_path, _ = zw.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    # now build data partially before and inside the existing dataset
    dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(2, 4)
    zarr_path, _ = zw.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(20, 70))
    assert np.array_equal(xdataset.time.values, np.arange(20, 70))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([newdatasets[0].beampointingangle, newdatasets[1].beampointingangle, datasets[1].beampointingangle, datasets[2].beampointingangle, datasets[3].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_partial_after():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, datasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(3, 7)
    zarr_path, _ = zw.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    # now build data partially after and inside the existing dataset
    dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(6, 8)
    zarr_path, _ = zw.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(30, 80))
    assert np.array_equal(xdataset.time.values, np.arange(30, 80))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([datasets[0].beampointingangle, datasets[1].beampointingangle, datasets[2].beampointingangle, newdatasets[0].beampointingangle, newdatasets[1].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_fully_before():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, datasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(3, 7)
    zarr_path, _ = zw.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    # now build data fully before the existing data
    dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 3)
    zarr_path, _ = zw.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(10, 70))
    assert np.array_equal(xdataset.time.values, np.arange(10, 70))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([newdatasets[0].beampointingangle, newdatasets[1].beampointingangle,
                                    datasets[0].beampointingangle, datasets[1].beampointingangle, datasets[2].beampointingangle,
                                    datasets[3].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_fully_after():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, datasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(3, 7)
    zarr_path, _ = zw.write(dataset_name, datasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    # now build data fully before the existing data
    dataset_name, newdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(7, 9)
    zarr_path, _ = zw.write(dataset_name, newdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(30, 90))
    assert np.array_equal(xdataset.time.values, np.arange(30, 90))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([datasets[0].beampointingangle, datasets[1].beampointingangle, datasets[2].beampointingangle,
                                    datasets[3].beampointingangle, newdatasets[0].beampointingangle, newdatasets[1].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_newdata_inside():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 1)
    zarr_path, _ = zw.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # write next data to disk, with a gap between it and existing data
    dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(2, 3)
    zarr_path, _ = zw.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write inbetween
    dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 2)
    zarr_path, _ = zw.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(30))
    assert np.array_equal(xdataset.time.values, np.arange(30))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle, thirddatasets[0].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_alternating():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 1)
    zarr_path, _ = zw.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # write next data to disk, with a gap between it and existing data
    dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(2, 3)
    zarr_path, _ = zw.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write inbetween
    dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 2)
    zarr_path, _ = zw.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # write new data at the end
    dataset_name, fourthdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(3, 4)
    zarr_path, _ = zw.write(dataset_name, fourthdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(40))
    assert np.array_equal(xdataset.time.values, np.arange(40))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
                                    thirddatasets[0].beampointingangle, fourthdatasets[0].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_write_backwards():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(2, 3)
    zarr_path, _ = zw.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write data prior
    dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 2)
    zarr_path, _ = zw.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write prior to that entry
    dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 1)
    zarr_path, _ = zw.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(30))
    assert np.array_equal(xdataset.time.values, np.arange(30))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate(
        [firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle, thirddatasets[0].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_multiple_concatenations():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 1)
    zarr_path, _ = zw.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write data after
    dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 2)
    zarr_path, _ = zw.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write after that entry
    dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(2, 3)
    zarr_path, _ = zw.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(30))
    assert np.array_equal(xdataset.time.values, np.arange(30))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate(
        [firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle, thirddatasets[0].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_overlap_inside():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 2)
    zarr_path, _ = zw.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write data after
    dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(3, 4)
    zarr_path, _ = zw.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write prior to that entry that overlaps the first
    dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 3)
    zarr_path, _ = zw.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(40))
    assert np.array_equal(xdataset.time.values, np.arange(40))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
                                    seconddatasets[1].beampointingangle, thirddatasets[0].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()


def test_zarr_backend_multiple_overlap_inside():
    # write new data to disk
    zarr_folder = get_testzarr_paths()
    zw = ZarrBackend(zarr_folder)
    dataset_name, firstdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(0, 2)
    zarr_path, _ = zw.write(dataset_name, firstdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write data after
    dataset_name, fourthdatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(4, 6)
    zarr_path, _ = zw.write(dataset_name, fourthdatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write prior to that entry that overlaps the second
    dataset_name, thirddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(3, 5)
    zarr_path, _ = zw.write(dataset_name, thirddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)

    # now write prior to that entry that overlaps the first
    dataset_name, seconddatasets, dataset_time_arrays, attributes, sysid = _return_basic_datasets(1, 3)
    zarr_path, _ = zw.write(dataset_name, seconddatasets, dataset_time_arrays, attributes, skip_dask=True, sys_id=sysid)
    xdataset = reload_zarr_records(zarr_path, skip_dask=True)

    assert np.array_equal(xdataset.counter.values, np.arange(60))
    assert np.array_equal(xdataset.time.values, np.arange(60))
    assert np.array_equal(xdataset.beam.values, np.arange(400))
    expectedangle = np.concatenate([firstdatasets[0].beampointingangle, seconddatasets[0].beampointingangle,
                                    seconddatasets[1].beampointingangle, thirddatasets[0].beampointingangle,
                                    thirddatasets[1].beampointingangle, fourthdatasets[1].beampointingangle])
    assert np.array_equal(xdataset.beampointingangle.values, expectedangle)
    assert xdataset.attrs['test_attribute'] == 'abc'
    cleanup_after_tests()
