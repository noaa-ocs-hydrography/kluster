from HSTB.kluster.backends._zarr import *
from HSTB.kluster.backends._zarr import _get_indices_dataset_exists, _get_indices_dataset_notexist, \
    _my_xarr_to_zarr_build_arraydimensions, _my_xarr_to_zarr_writeattributes


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
    indices, running_total = _get_indices_dataset_notexist(input_time_arrays)
    assert indices == [[0, 5]]
    assert running_total == 5


def test_get_write_indices_zarr_append():
    # let zarr_time represent the time dimension of the data that is on disk for our test
    zarr_time = zarr.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    # now lets test what happens with an append
    # indices are still lists of start/end index
    data_time = np.array([10, 11, 12, 13, 14])
    input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
    indices, running_total = _get_indices_dataset_exists(input_time_arrays, zarr_time)
    assert indices == [[10, 15]]
    assert running_total == 5


def test_get_write_indices_zarr_overwrite():
    # let zarr_time represent the time dimension of the data that is on disk for our test
    zarr_time = zarr.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    # now lets test what happens when we want to overwrite
    # indices for overwrite will be an array equal to the data length, to use the zarr set coordinate selection method
    data_time = np.array([4, 5, 6, 7, 8])
    input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
    indices, running_total = _get_indices_dataset_exists(input_time_arrays, zarr_time)
    assert len(indices) == 1
    assert np.array_equal(indices[0], np.array([4, 5, 6, 7, 8]))
    # running_total is 0 here as we did not add any new values, the size remains the same
    assert running_total == 0


def test_get_write_indices_zarr_partlycovered():
    # let zarr_time represent the time dimension of the data that is on disk for our test
    zarr_time = zarr.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    # now we make sure that when data is partly in the array, we get a not implemented
    data_time = np.array([7, 8, 9, 10, 11])
    input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
    try:
        indices, running_total = _get_indices_dataset_exists(input_time_arrays, zarr_time)
        assert False
    except NotImplementedError:
        assert True


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


def test_get_write_indices_zarr_outoforder():
    # now check to make sure this all works when the already written data is out of time order
    zarr_time = zarr.array([5, 6, 7, 8, 9, 0, 1, 2, 3, 4])
    data_time = np.array([4, 5, 6, 7, 8])
    input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
    indices, running_total = _get_indices_dataset_exists(input_time_arrays, zarr_time)
    assert len(indices) == 1
    assert np.array_equal(indices[0], np.array([9, 0, 1, 2, 3]))
    assert running_total == 0


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
    indices, running_total = _get_indices_dataset_notexist([data_arr])

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
    indices, running_total = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    data_arr2 = np.array([10, 11, 12, 13, 14])
    indices, running_total = _get_indices_dataset_exists([data_arr2], zw.rootgroup['time'])

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
    indices, running_total = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    data_arr2 = np.array([3, 4, 5, 6, 7])
    new_data = np.array([999, 999, 999, 999, 999])
    indices, running_total = _get_indices_dataset_exists([data_arr2], zw.rootgroup['time'])

    dataset = xr.Dataset({'data': (['time'], new_data), 'data2': (['time'], new_data)}, coords={'time': data_arr2})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    expected_answer = np.array([0, 1, 2, 999, 999, 999, 999, 999, 8, 9])
    assert np.array_equal(zw.rootgroup['data'], expected_answer)
    assert np.array_equal(zw.rootgroup['data2'], expected_answer)
    assert np.array_equal(zw.rootgroup['time'], data_arr)


def test_zarr_write_merge():
    # merge is for when we have an existing rootgroup, but the new dataset has a variable that is not in the rootgroup
    zw = ZarrWrite(None, desired_chunk_shape={'time': (10,), 'data2': (10,), 'data': (10,)})
    zw.rootgroup = zarr.group()

    data_arr = np.arange(10)
    indices, running_total = _get_indices_dataset_notexist([data_arr])

    dataset = xr.Dataset({'data': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    indices, running_total = _get_indices_dataset_exists([data_arr], zw.rootgroup['time'])

    dataset = xr.Dataset({'data2': (['time'], data_arr)}, coords={'time': data_arr})
    zw.write_to_zarr(dataset, None, dataloc=indices[0], finalsize=10)

    assert np.array_equal(zw.rootgroup['data'], data_arr)
    assert np.array_equal(zw.rootgroup['data2'], data_arr)
    assert np.array_equal(zw.rootgroup['time'], data_arr)
