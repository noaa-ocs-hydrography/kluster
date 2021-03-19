from HSTB.kluster.xarray_helpers import *
from HSTB.kluster.xarray_helpers import _get_indices_dataset_exists, _get_indices_dataset_notexist, \
    _my_xarr_to_zarr_build_arraydimensions, _my_xarr_to_zarr_writeattributes


def test_search_not_sorted():
    master = np.array([3, 4, 5, 6, 1, 9, 0, 2, 7, 8])
    search = np.array([6, 4, 3, 1, 1])

    final_inds = search_not_sorted(master, search)

    assert (np.array_equal(master[final_inds], search))


def test_compare_and_find_gaps():
    # looking for gaps in one compared to the other
    source_time = np.arange(10)
    # introduce a 2 sec gap in compare time (between 4 and 6)
    compare_time = np.concatenate([np.arange(5), np.arange(6,11)])

    source = xr.DataArray(np.random.rand(10), coords={'time': source_time}, dims=['time'])
    compare = xr.DataArray(np.random.rand(10), coords={'time': compare_time}, dims=['time'])

    # with max gap of 2, should find no gaps
    chk = compare_and_find_gaps(source, compare, max_gap_length=2, dimname='time')
    assert not chk.any()
    # with max gap of 1, should find one gap, and the return is the index of the start/end of gap
    chk = compare_and_find_gaps(source, compare, max_gap_length=1, dimname='time')
    assert np.array_equal(chk, np.array([[4, 6]]))


def test_get_beamwise_interpolation():
    # ping time plus delay equals beam time at ping
    ping_time = xr.DataArray(np.arange(10), coords={'time': np.arange(10)}, dims=['time'])
    delay = xr.DataArray(np.ones((10, 400)), coords={'time': np.arange(10), 'beam': np.arange(400)}, dims=['time', 'beam'])
    # array that we have good times for, but we want the values at beam time
    interp_this = xr.DataArray(np.array([400, 500]), coords={'time': np.array([2.5, 3.8])}, dims=['time'])

    # answer is now the interp values at beam time
    answer = get_beamwise_interpolation(ping_time, delay, interp_this)

    assert answer.shape == (10, 400)

    assert np.round(answer.isel(time=0).values[0], 3) == 284.615
    assert np.all(answer.isel(time=0).values[0] == answer.isel(time=0).values)

    assert np.round(answer.isel(time=9).values[0], 3) == 976.923
    assert np.all(answer.isel(time=9).values[0] == answer.isel(time=9).values)


def test_return_chunk_slices():
    ping_time = np.arange(100)
    data_arr = np.arange(100)

    dset = xr.Dataset({'data': (['time'], data_arr)}, coords={'time': ping_time})
    dset = dset.chunk({'time': 10})

    chnkslices = return_chunk_slices(dset)
    assert len(chnkslices) == 10
    assert chnkslices[0] == slice(0, 10, None)
    assert chnkslices[-1] == slice(90, 100, None)


def test_stack_and_reform_nan_array():
    # build an array with nans
    data = np.full((10, 400), 1.5)
    data[1, 50] = np.nan
    data[2, 75] = np.nan
    data[3, 150] = np.nan
    data_array = xr.DataArray(data, coords={'time': np.arange(10), 'beam': np.arange(400)}, dims=['time', 'beam'])
    # flatten the array and remove the nans by stacking the time and beam dimensions to get a 1dim result
    original_index, stacked_data = stack_nan_array(data_array)

    # size of the stacked data should be the original size minus the number of nans
    assert original_index[0].shape == (3997,)
    assert original_index[1].shape == (3997,)
    assert stacked_data.shape == (3997,)
    assert np.all(stacked_data == stacked_data[0])

    # we do the stack/reform thing so that we can operate on the data with functions that do not play well with nan
    # we can run a routine on the stacked result, and rebuild the original shape with the nans afterwards
    # multibeam data with varying beams or with beams with no data will show as nan in the time/beam arrays

    # get back to the original array by building an array of nans equal in shape to the original array and repopulate
    #  with the original data
    orig_array = reform_nan_array(stacked_data, original_index, data_array.shape, data_array.coords, data_array.dims)
    assert np.isnan(orig_array[1, 50])
    assert np.isnan(orig_array[2, 75])
    assert np.isnan(orig_array[3, 150])


def test_clear_data_vars_from_dataset():
    ping_time = np.arange(100)
    data_arr = np.arange(100)
    datasets = [xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': ping_time}),
                xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': ping_time}),
                xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': ping_time})]
    new_datasets = clear_data_vars_from_dataset(datasets, 'data2')
    assert(isinstance(new_datasets, list))
    assert np.all(['data2' not in d for d in new_datasets])

    new_dataset = clear_data_vars_from_dataset(datasets[0], 'data2')
    assert (isinstance(new_dataset, xr.Dataset))
    assert 'data2' not in new_dataset


def test_interp_across_chunks():
    data_arr = np.arange(100)
    ping_time = np.arange(100)
    test_data = xr.Dataset({'data': (['time'], data_arr)}, coords={'time': ping_time})

    # we chunk the test data set in chunks of 10
    test_data = test_data.chunk(10)

    # the new data is the time that we want to interpolate the testdata to
    # pick times that are going to fall near or inbetween chunks
    # standard xarray interp would fail for chunked datasets
    new_data_arr = np.array([8.5, 9.5, 10.5])
    new_ping_time = np.array([8.5, 9.5, 10.5])
    new_times = xr.DataArray(new_data_arr, coords={'time': new_ping_time}, dims=['time'])

    # since we used interp_across_chunks and not xarray.interp, this succeeded
    interp_data = interp_across_chunks(test_data, new_times, dimname='time')
    assert isinstance(interp_data, xr.Dataset)
    assert interp_data['data'][0] == 8.5
    assert interp_data['data'][1] == 9.5
    assert interp_data['data'][2] == 10.5


def test_slice_xarray_by_dim():
    data_arr = np.arange(100)
    test_data = xr.Dataset({'data': (['time'], data_arr)}, coords={'time': data_arr})

    # this method lets you slice by dim values that are not in the actual data
    start_time = 28.7
    end_time = 29.4
    ans = slice_xarray_by_dim(test_data, dimname='time', start_time=start_time, end_time=end_time)
    assert ans['data'].values == 29

    # can also slice with numbers beyond the data range, will clip to the limits
    start_time = 98.2
    end_time = 104
    ans = slice_xarray_by_dim(test_data, dimname='time', start_time=start_time, end_time=end_time)
    assert ans['data'].shape == (2,)
    assert ans['data'].values[0] == 98
    assert ans['data'].values[1] == 99


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


def test_get_write_indices_zarr_outoforder():
    # now check to make sure this all works when the already written data is out of time order
    zarr_time = zarr.array([5, 6, 7, 8, 9, 0, 1, 2, 3, 4])
    data_time = np.array([4, 5, 6, 7, 8])
    input_time_arrays = [xr.DataArray(data_time, coords={'time': data_time}, dims=['time'])]
    indices, running_total = _get_indices_dataset_exists(input_time_arrays, zarr_time)
    assert len(indices) == 1
    assert np.array_equal(indices[0], np.array([9, 0, 1, 2, 3]))
    assert running_total == 0


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


def test_combine_arrays_to_dataset():
    dataarr = np.arange(5)
    arr = xr.DataArray(dataarr, coords={'time': dataarr}, dims=['time'])
    dset = combine_arrays_to_dataset([arr, arr, arr], ['arr1', 'arr2', 'arr3'])
    assert list(dset.keys()) == ['arr1', 'arr2', 'arr3']


def test_combine_xr_attributes():
    # test the translating functionality of the kluster attribute combine
    tst1 = xr.Dataset()
    tst1.attrs = {'install': json.dumps({'raw_file_name': ['testthis.all'], 'survey_identifier': 'h12345'}),
                  'system_serial_number': np.array([123]), 'secondary_system_serial_number': np.array([124])}
    tst2 = xr.Dataset()
    tst2.attrs = {'install': json.dumps({'raw_file_name': ['testthis2.all'], 'survey_identifier': 'h12335'}),
                  'system_serial_number': np.array([123]), 'secondary_system_serial_number': np.array([124])}
    tst3 = xr.Dataset()
    tst3.attrs = {'install': json.dumps({'raw_file_name': ['testthis3.all'], 'survey_identifier': 'h12345'}),
                  'system_serial_number': np.array([123]), 'secondary_system_serial_number': np.array([124])}
    newattrs = combine_xr_attributes([tst1, tst2, tst3])

    compare_attrs = {'install': '{}',  # we popped the filename, surveyidentifier attributes and rebuilt them as attributes
                     'system_serial_number': [123],  # can contain multiple serial numbers from multiple systems
                     'secondary_system_serial_number': [124],
                     'multibeam_files': ['testthis.all', 'testthis2.all', 'testthis3.all'],  # list of all files across attributes
                     'survey_number': ['h12335', 'h12345']}  # unique survey identifiers found
    assert newattrs == compare_attrs


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
