import unittest
import numpy as np
import xarray as xr
import json

from pytest import approx

from HSTB.kluster.xarray_helpers import compare_and_find_gaps, get_beamwise_interpolation, return_chunk_slices, \
    stack_nan_array, reform_nan_array, clear_data_vars_from_dataset, interp_across_chunks, slice_xarray_by_dim, \
    combine_arrays_to_dataset, combine_xr_attributes
try:  # when running from pycharm console
    from hstb_kluster.tests.test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr, load_dataset
except ImportError:  # relative import as tests directory can vary in location depending on how kluster is installed
    from .test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr, load_dataset


class TestXArrayHelper(unittest.TestCase):

    def test_compare_and_find_gaps(self):
        # looking for gaps in one compared to the other
        source_time = np.arange(10)
        # introduce a 2 sec gap in compare time (between 4 and 6)
        compare_time = np.concatenate([np.arange(5), np.arange(6, 11)])

        source = xr.DataArray(np.random.rand(10), coords={'time': source_time}, dims=['time'])
        compare = xr.DataArray(np.random.rand(10), coords={'time': compare_time}, dims=['time'])

        # with max gap of 2, should find no gaps
        chk = compare_and_find_gaps(source, compare, max_gap_length=2, dimname='time')
        assert not chk.any()

        # with max gap of 1, should find one gap, and the return is the index of the start/end of gap
        chk = compare_and_find_gaps(source, compare, max_gap_length=1, dimname='time')
        assert np.array_equal(chk, np.array([[4, 6]]))

    def test_get_beamwise_interpolation(self):
        # ping time plus delay equals beam time at ping
        ping_time = xr.DataArray(np.arange(10), coords={'time': np.arange(10)}, dims=['time'])
        delay = xr.DataArray(np.ones((10, 400)), coords={'time': np.arange(10), 'beam': np.arange(400)},
                             dims=['time', 'beam'])
        # array that we have good times for, but we want the values at beam time
        interp_this = xr.DataArray(np.array([400, 500]), coords={'time': np.array([2.5, 3.8])}, dims=['time'])

        # answer is now the interp values at beam time
        answer = get_beamwise_interpolation(ping_time, delay, interp_this)

        assert answer.shape == (10, 400)

        assert np.round(answer.isel(time=0).values[0], 3) == 284.615
        assert np.all(answer.isel(time=0).values[0] == answer.isel(time=0).values)

        assert np.round(answer.isel(time=9).values[0], 3) == 976.923
        assert np.all(answer.isel(time=9).values[0] == answer.isel(time=9).values)

    def test_return_chunk_slices(self):
        dset = xr.Dataset({'data': (['time'], np.arange(100))}, coords={'time': np.arange(100)}).chunk({'time': 10})
        chnkslices = return_chunk_slices(dset)
        assert len(chnkslices) == 10
        assert chnkslices[0] == slice(0, 10, None)
        assert chnkslices[-1] == slice(90, 100, None)

    def test_stack_and_reform_nan_array(self):
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
        orig_array = reform_nan_array(stacked_data, original_index, data_array.shape, data_array.coords,
                                      data_array.dims)
        assert np.isnan(orig_array[1, 50])
        assert np.isnan(orig_array[2, 75])
        assert np.isnan(orig_array[3, 150])

    def test_clear_data_vars_from_dataset(self):
        data_arr = np.arange(100)
        datasets = [xr.Dataset({'data': (['time'], data_arr), 'data2': (['time'], data_arr)}, coords={'time': np.arange(100)}) * 3]
        new_datasets = clear_data_vars_from_dataset(datasets, 'data2')
        assert (isinstance(new_datasets, list))
        assert np.all(['data2' not in d for d in new_datasets])

        new_dataset = clear_data_vars_from_dataset(datasets[0], 'data2')
        assert (isinstance(new_dataset, xr.Dataset))
        assert 'data2' not in new_dataset

    def test_interp_across_chunks(self):
        # we chunk the test data set in chunks of 10
        test_data = xr.Dataset({'data': (['time'], np.arange(100))}, coords={'time': np.arange(100)}).chunk(10)

        # the new data is the time that we want to interpolate the testdata to
        # pick times that are going to fall near or inbetween chunks
        # standard xarray interp would fail for chunked datasets
        new_times = xr.DataArray(np.array([8.5, 9.5, 10.5]), coords={'time': np.array([8.5, 9.5, 10.5])}, dims=['time'])

        # since we used interp_across_chunks and not xarray.interp, this succeeded
        interp_data = interp_across_chunks(test_data, new_times, dimname='time')
        assert isinstance(interp_data, xr.Dataset)
        assert interp_data['data'][0] == 8.5
        assert interp_data['data'][1] == 9.5
        assert interp_data['data'][2] == 10.5

    def test_slice_xarray_by_dim(self):
        data_arr = np.arange(100)
        test_data = xr.Dataset({'data': (['time'], data_arr)}, coords={'time': data_arr})

        # this method lets you slice by dim values that are not in the actual data
        ans = slice_xarray_by_dim(test_data, dimname='time', start_time=28.7, end_time=29.4)
        assert ans['data'].values == 29

        # can also slice with numbers beyond the data range, will clip to the limits
        ans = slice_xarray_by_dim(test_data, dimname='time', start_time=98.2, end_time=104)
        assert ans['data'].shape == (2,)
        assert ans['data'].values[0] == 98
        assert ans['data'].values[1] == 99

    def test_combine_arrays_to_dataset(self):
        dataarr = np.arange(5)
        arr = xr.DataArray(dataarr, coords={'time': dataarr}, dims=['time'])
        dset = combine_arrays_to_dataset([arr, arr, arr], ['arr1', 'arr2', 'arr3'])
        assert list(dset.keys()) == ['arr1', 'arr2', 'arr3']

    def test_combine_xr_attributes(self):
        # test the translating functionality of the kluster attribute combine
        tst1 = xr.Dataset(attrs = {'install': json.dumps({'raw_file_name': ['testthis.all'], 'survey_identifier': 'h12345'}),
                      'system_serial_number': np.array([123]), 'secondary_system_serial_number': np.array([124])})
        tst2 = xr.Dataset(attrs = {'install': json.dumps({'raw_file_name': ['testthis2.all'], 'survey_identifier': 'h12335'}),
                      'system_serial_number': np.array([123]), 'secondary_system_serial_number': np.array([124])})
        tst3 = xr.Dataset(attrs = {'install': json.dumps({'raw_file_name': ['testthis3.all'], 'survey_identifier': 'h12345'}),
                      'system_serial_number': np.array([123]), 'secondary_system_serial_number': np.array([124])})
        newattrs = combine_xr_attributes([tst1, tst2, tst3])

        compare_attrs = {'install': '{}',
                         # we popped the filename, surveyidentifier attributes and rebuilt them as attributes
                         'system_serial_number': [123],  # can contain multiple serial numbers from multiple systems
                         'secondary_system_serial_number': [124],
                         'multibeam_files': ['testthis.all', 'testthis2.all', 'testthis3.all'],
                         # list of all files across attributes
                         'survey_number': ['h12335', 'h12345']}  # unique survey identifiers found
        assert newattrs == compare_attrs


    def test_interp_across_chunks(self):
        synth = load_dataset(RealFqpr(), skip_dask=False)
        # 11 attitude values, chunking by 4 gives us three chunks
        # att.chunks
        # Out[10]: Frozen(SortedKeysDict({'time': (4, 4, 3)}))
        att = synth.raw_att.chunk(4)
        times_interp_to = xr.DataArray(np.array([1495563084.455, 1495563084.490, 1495563084.975]), dims={'time'},
                                       coords={'time': np.array([1495563084.455, 1495563084.490, 1495563084.975])})
        dask_interp_att = interp_across_chunks(att, times_interp_to, dimname='time', daskclient=synth.client)
        interp_att = interp_across_chunks(att, times_interp_to, dimname='time')

        expected_att = xr.Dataset(
            {'heading': (['time'], np.array([307.8539977551496, 307.90348427192055, 308.6139892100822])),
             'heave': (['time'], np.array([0.009999999776482582, 0.009608692733222632, -0.009999999776482582])),
             'roll': (['time'], np.array([0.4400004684929343, 0.07410809820512047, -4.433999538421631])),
             'pitch': (['time'], np.array([-0.5, -0.5178477924436871, -0.3760000467300415]))},
            coords={'time': np.array([1495563084.455, 1495563084.49, 1495563084.975])})

        # make sure the dask/non-dask methods line up
        assert dask_interp_att.time.values == approx(interp_att.time.values, 0.001)
        assert dask_interp_att.heading.values == approx(interp_att.heading.values, 0.001)
        assert dask_interp_att.heave.values == approx(interp_att.heave.values, 0.001)
        assert dask_interp_att.pitch.values == approx(interp_att.pitch.values, 0.001)
        assert dask_interp_att['roll'].values == approx(interp_att['roll'].values, 0.001)

        # make sure the values line up with what we would expect
        assert dask_interp_att.time.values == approx(expected_att.time.values, 0.001)
        assert dask_interp_att.heading.values == approx(expected_att.heading.values, 0.001)
        assert dask_interp_att.heave.values == approx(expected_att.heave.values, 0.001)
        assert dask_interp_att.pitch.values == approx(expected_att.pitch.values, 0.001)
        assert dask_interp_att['roll'].values == approx(expected_att['roll'].values, 0.001)

        print('Passed: interp_across_chunks')