import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import logging

import os
print('HELLO')
print(os.path.exists(r'/home/travis/build/noaa-ocs-hydrography/kluster/HSTB/__init__.py'))
print(os.listdir(r'/home/travis/build/noaa-ocs-hydrography/kluster/HSTB'))
print(os.listdir(r'/home/travis/build/noaa-ocs-hydrography/kluster/HSTB/drivers'))


from HSTB.kluster import fqpr_generation, xarray_conversion
from HSTB.kluster.tests.test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr
from HSTB.kluster.xarray_helpers import interp_across_chunks
from HSTB.kluster.fqpr_surface import BaseSurface


def test_get_orientation_vectors(dset='realdualhead'):
    """
    Automated test of fqpr_generation get_orientation_vectors

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'

    """
    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_tx_per_sec = np.array([[0.6136555921172974, -0.7895255928982701, 0.008726535498373935],
                                        [0.6236867410029571, -0.7816427427545967, 0.007033618996063356]])
        expected_rx_per_sector_first_beam = [np.array([0.7834063124935767, 0.6195440420293157, -0.04939361832457566]),
                                             np.array([0.7835067418224286, 0.6194772694894388, -0.048632274311522526]),
                                             np.array([0.786992303627918, 0.616656032517831, -0.019453832264897088]),
                                             np.array([0.7869914430005911, 0.6166568778070365, -0.019461852355958494]),
                                             np.array([0.7869506383086127, 0.6166968512681303, -0.019841534760212512]),
                                             np.array([0.7869495956077067, 0.6166978700669955, -0.01985122232251618])]
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_tx_per_sec = [np.array([[-0.8173967230596009, -0.5756459946918305, -0.022232663846213512]]),
                               np.array([[-0.8173967230596009, -0.5756459946918305, -0.022232663846213512]]),
                               np.array([[-0.818098137098556, -0.5749317404941526, -0.013000579640495315]]),
                               np.array([[-0.818098137098556, -0.5749317404941526, -0.013000579640495315]])]
        expected_rx_per_sector_first_beam = [np.array([0.5707206057741382, -0.8178136286558314, 0.07388380848347877]),
                                             np.array([0.5707251090313201, -0.8178104859878021, 0.07388380848347877]),
                                             np.array([0.5752302545527056, -0.8157217016726686, -0.060896177270015645]),
                                             np.array([0.5752302545527056, -0.8157217016726686, -0.060896177270015645])]
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    # dump_data/delete_futs set the workflow to either keeping everything in memory after completion (False) or writing
    #     data to disk (both are True).  Could probably condense these arguments to one argument in the future.
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)

    # arrays of computed vectors
    secs = fq.return_sector_ids()
    tstmp = list(fq.intermediate_dat[secs[0]]['orientation'].keys())[0]
    # since we kept data in memory, we can now get the result of get_orientation_vectors using result()
    loaded_data = [fq.intermediate_dat[s]['orientation'][tstmp][0][0] for s in fq.return_sector_ids()]

    # we examine the tx vector for each sector (not beam based) and the rx vector for each sector's first beam (rx
    #     vectors change per beam, as attitude changes based on beam traveltime)
    txvecdata = [ld[0].values for ld in loaded_data]
    rxvecdata = [ld[1].values[0][0] for ld in loaded_data]

    # check for the expected tx orientation vectors
    if dset != 'realdualhead':
        expected_tx_vector = [expected_tx_per_sec] * len(synth.raw_ping)
    else:
        # can't simply expect each sector to have the same tx vector, two tx's in dual head sonar...gotta write it all out
        expected_tx_vector = expected_tx_per_sec
    assert np.array_equal(expected_tx_vector, txvecdata)

    # check for the expected rx orientation vectors
    assert np.array_equal(expected_rx_per_sector_first_beam, rxvecdata)

    print('Passed: get_orientation_vectors')


def test_build_beam_pointing_vector(dset='realdualhead'):
    """
    Automated test of fqpr_generation build_beam_pointing_vector

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'

    """
    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_ba = [4.697702770857325, 4.701442008565216, 4.707855899184765, 4.718268102552111, 1.5583690576936515,
                       1.5526471079672053]
        expected_bda = [1.209080718248996, 1.209595123811155, 0.6905748987012734, 0.6905384444662406,
                        -0.6950466227093792, -0.6951468071954617]
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_ba = np.array([4.722867225444818, 4.714469418825591, 4.7409403464076965, 4.725275412543552])
        expected_bda = np.array([1.2056359109924677, 1.2049044014648185, 0.5250013986289013, 0.5239366227760862])
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)

    secs = fq.return_sector_ids()
    tstmp = list(fq.intermediate_dat[secs[0]]['bpv'].keys())[0]
    loaded_data = [fq.intermediate_dat[s]['bpv'][tstmp][0][0] for s in fq.return_sector_ids()]

    ba_data = [ld[0].isel(time=0).values[0] for ld in loaded_data]
    bda_data = [ld[1].isel(time=0).values[0] for ld in loaded_data]

    # beam azimuth check
    assert np.array_equal(ba_data, expected_ba)

    # beam depression angle check
    assert np.array_equal(bda_data, expected_bda)

    print('Passed: build_beam_pointing_vector')


def test_sv_correct(dset='realdualhead'):
    """
    Automated test of fqpr_generation sv_correct

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'

   """

    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_x = np.array([-3.42, -2.515, -0.336, 0.436, 0.939, 1.372], dtype=np.float32)
        expected_y = np.array([-232.884, -229.799, -74.056, -74.079, 75.626, 75.647], dtype=np.float32)
        expected_z = np.array([91.12, 89.728, 90.258, 90.285, 91.246, 91.279], dtype=np.float32)
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_x = np.array([1.086, 0.692, 0.738, 0.567], dtype=np.float32)
        expected_y = np.array([-60.159, -59.925,  -9.306,  -9.283], dtype=np.float32)
        expected_z = np.array([18.365, 18.307, 18.853, 18.866], dtype=np.float32)
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)
    fq.sv_correct(dump_data=False, delete_futs=False)

    secs = fq.return_sector_ids()
    tstmp = list(fq.intermediate_dat[secs[0]]['sv_corr'].keys())[0]
    loaded_data = [fq.intermediate_dat[s]['sv_corr'][tstmp][0][0] for s in fq.return_sector_ids()]

    x_data = np.array([ld[0].isel(time=0).values[0] for ld in loaded_data], dtype=np.float32)
    y_data = np.array([ld[1].isel(time=0).values[0] for ld in loaded_data], dtype=np.float32)
    z_data = np.array([ld[2].isel(time=0).values[0] for ld in loaded_data], dtype=np.float32)

    # beam azimuth check
    assert np.array_equal(x_data, expected_x)

    # two way travel time check
    assert np.array_equal(y_data, expected_y)

    # beam depression angle check
    assert np.array_equal(z_data, expected_z)

    print('Passed: sv_correct')


def test_georef_xyz(dset='realdualhead'):
    """
    Automated test of fqpr_generation sv_correct

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'

    """

    vert_ref = 'waterline'
    datum = 'NAD83'

    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_x = np.array([539016.6, 539017.75, 539110.75, 539110.1, 539200.75, 539200.44], dtype=np.float32)
        expected_y = np.array([5292789.0, 5292792.0, 5292917.0, 5292917.0, 5293036.5, 5293036.5], dtype=np.float32)
        expected_z = np.array([91.7700, 90.378, 90.908, 90.935, 91.896, 91.929], dtype=np.float32)
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
        expected_x = np.array([492984.53, 492984.56, 492942.66, 492942.72], dtype=np.float32)
        expected_y = np.array([3365068.5, 3365069.0, 3365097.2, 3365097.5], dtype=np.float32)
        expected_z = np.array([22.147, 22.089, 22.684, 22.697], dtype=np.float32)
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)
    fq.sv_correct(dump_data=False, delete_futs=False)
    fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
    fq.georef_xyz(dump_data=False, delete_futs=False)

    secs = fq.return_sector_ids()
    tstmp = list(fq.intermediate_dat[secs[0]]['xyz'].keys())[0]
    loaded_xyz_data = [fq.intermediate_dat[s]['xyz'][tstmp][0][0] for s in fq.return_sector_ids()]

    x_data = np.array([ld[0].isel(time=0).values[0] for ld in loaded_xyz_data], dtype=np.float32)
    y_data = np.array([ld[1].isel(time=0).values[0] for ld in loaded_xyz_data], dtype=np.float32)
    z_data = np.array([ld[2].isel(time=0).values[0] for ld in loaded_xyz_data], dtype=np.float32)

    # beam azimuth check
    assert np.array_equal(x_data, expected_x)

    # two way travel time check
    assert np.array_equal(y_data, expected_y)

    # beam depression angle check
    assert np.array_equal(z_data, expected_z)

    print('Passed: georef_xyz')


def test_interp_across_chunks():
    synth = load_dataset(RealFqpr(), skip_dask=False)
    # 11 attitude values, chunking by 4 gives us three chunks
    # att.chunks
    # Out[10]: Frozen(SortedKeysDict({'time': (4, 4, 3)}))
    att = synth.raw_att.chunk(4)
    times_interp_to = xr.DataArray(np.array([1495563084.455, 1495563084.490, 1495563084.975]), dims={'time'},
                                   coords={'time': np.array([1495563084.455, 1495563084.490, 1495563084.975])})
    dask_interp_att = interp_across_chunks(att, times_interp_to, dimname='time', daskclient=synth.client)
    interp_att = interp_across_chunks(att, times_interp_to, dimname='time')

    expected_att = xr.Dataset({'heading': (['time'], np.array([307.8539977551496, 307.90348427192055, 308.6139892100822])),
                               'heave': (['time'], np.array([0.009999999776482582, 0.009608692733222632, -0.009999999776482582])),
                               'roll': (['time'], np.array([0.4400004684929343, 0.07410809820512047, -4.433999538421631])),
                               'pitch': (['time'], np.array([-0.5, -0.5178477924436871, -0.3760000467300415]))},
                              coords={'time': np.array([1495563084.455, 1495563084.49, 1495563084.975])})

    # make sure the dask/non-dask methods line up
    assert np.all(dask_interp_att.time == interp_att.time).compute()
    assert np.all(dask_interp_att.heading == interp_att.heading).compute()
    assert np.all(dask_interp_att.heave == interp_att.heave).compute()
    assert np.all(dask_interp_att.pitch == interp_att.pitch).compute()
    assert np.all(dask_interp_att['roll'] == interp_att['roll']).compute()

    # make sure the values line up with what we would expect
    assert np.all(dask_interp_att.time == expected_att.time).compute()
    assert np.all(dask_interp_att.heading == expected_att.heading).compute()
    assert np.all(dask_interp_att.heave == expected_att.heave).compute()
    assert np.all(dask_interp_att.pitch == expected_att.pitch).compute()
    assert np.all(dask_interp_att['roll'] == expected_att['roll']).compute()

    print('Passed: interp_across_chunks')


def test_reform_2d(dset='real'):
    """
    Automated test of fqpr_generation reform_2d_vars_across_sectors_at_time

    Will run using the 'real' dataset or 'realdualhead' included in the test_datasets file.

    No current support for the synthetic dataset, need to look at adding that in.  I've yet to find a reason to do so
    though, now that I have the real pings.

    Parameters
    ----------
    dset: str, specify which dataset you want to use, one of 'real' and 'realdualhead'

    """
    if dset == 'real':
        synth = load_dataset(RealFqpr())
        expected_sector_ids = np.array([[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                                         2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                                         2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                                         2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                                         2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                                         2, 2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                         4, 4, 4, 4]])
        expected_reformed_angles = [72.05999755859375, 71.94999694824219, 71.83999633789062, 71.72999572753906,
                                    71.61000061035156, 71.5, 71.37999725341797, 71.15999603271484, 71.04000091552734,
                                    70.91999816894531, 70.79999542236328, 70.68000030517578, 70.54999542236328,
                                    70.1500015258789, 70.0199966430664, 69.88999938964844, 69.7699966430664,
                                    69.62999725341797, 69.5, 69.3699951171875, 69.22999572753906, 69.0999984741211,
                                    68.95999908447266, 68.81999969482422, 68.68000030517578, 68.54000091552734,
                                    68.38999938964844, 68.25, 68.0999984741211, 67.94999694824219, 67.79999542236328,
                                    67.54000091552734, 67.38999938964844, 67.22999572753906, 67.08000183105469,
                                    66.90999603271484, 66.75, 66.58999633789062, 66.31999969482422, 66.15999603271484,
                                    65.98999786376953, 65.80999755859375, 65.63999938964844, 65.45999908447266,
                                    65.18000030517578, 65.0, 64.81999969482422, 64.63999938964844, 64.04000091552734,
                                    63.849998474121094, 63.65999984741211, 63.459999084472656, 63.27000045776367,
                                    63.06999969482422, 62.8599967956543, 62.65999984741211, 62.44999694824219,
                                    62.23999786376953, 62.029998779296875, 61.80999755859375, 61.599998474121094,
                                    61.369998931884766, 61.14999771118164, 60.91999816894531, 60.689998626708984,
                                    60.459999084472656, 60.21999740600586, 59.97999954223633, 59.73999786376953,
                                    59.48999786376953, 59.25, 58.98999786376953, 58.73999786376953, 58.47999954223633,
                                    58.209999084472656, 57.939998626708984, 57.56999969482422, 57.29999923706055,
                                    57.02000045776367, 56.73999786376953, 56.44999694824219, 56.15999984741211,
                                    55.86000061035156, 55.44999694824219, 55.14999771118164, 54.84000015258789,
                                    54.529998779296875, 54.209999084472656, 53.88999938964844, 53.55999755859375,
                                    53.119998931884766, 52.779998779296875, 52.439998626708984, 52.099998474121094,
                                    51.73999786376953, 51.38999938964844, 51.02000045776367, 50.55999755859375,
                                    50.18000030517578, 49.79999923706055, 49.41999816894531, 49.029998779296875,
                                    48.62999725341797, 48.22999954223633, 47.709999084472656, 47.29999923706055,
                                    46.87999725341797, 46.45000076293945, 46.02000045776367, 45.56999969482422,
                                    45.12999725341797, 44.66999816894531, 44.099998474121094, 43.63999938964844,
                                    43.15999984741211, 42.68000030517578, 42.189998626708984, 41.689998626708984,
                                    41.189998626708984, 40.68000030517578, 40.15999984741211, 39.529998779296875,
                                    38.98999786376953, 38.45000076293945, 37.89999771118164, 37.34000015258789,
                                    36.779998779296875, 36.20000076293945, 35.619998931884766, 35.029998779296875,
                                    34.31999969482422, 33.709999084472656, 33.09000015258789, 32.46999740600586,
                                    31.829999923706055, 31.189998626708984, 30.53999900817871, 29.8799991607666,
                                    29.219999313354492, 28.53999900817871, 27.85999870300293, 27.049999237060547,
                                    26.349998474121094, 25.639999389648438, 24.93000030517578, 24.19999885559082,
                                    23.469999313354492, 22.729999542236328, 21.979999542236328, 21.219999313354492,
                                    20.459999084472656, 19.689998626708984, 18.90999984741211, 18.119998931884766,
                                    17.329999923706055, 16.529998779296875, 15.729999542236328, 14.809999465942383,
                                    13.989999771118164, 13.170000076293945, 12.34000015258789, 11.50999927520752,
                                    10.670000076293945, 9.829999923706055, 8.989999771118164, 8.139999389648438,
                                    7.289999961853027, 6.429999828338623, 5.579999923706055, 4.71999979019165,
                                    3.859999895095825, 2.990000009536743, 2.129999876022339, 1.2699999809265137,
                                    0.3999999761581421, -0.4599999785423279, -1.3299999237060547, -2.190000057220459,
                                    -3.049999952316284, -3.9099998474121094, -4.769999980926514, -5.619999885559082,
                                    -6.46999979019165, -7.319999694824219, -8.170000076293945, -9.010000228881836,
                                    -9.84000015258789, -10.679999351501465, -11.5, -12.329999923706055,
                                    -13.029999732971191, -13.84000015258789, -14.649999618530273, -15.449999809265137,
                                    -16.239999771118164, -17.029998779296875, -17.809999465942383, -18.579999923706055,
                                    -19.34000015258789, -20.100000381469727, -20.850000381469727, -21.59000015258789,
                                    -22.31999969482422, -23.049999237060547, -23.76999855041504, -24.35999870300293,
                                    -25.06999969482422, -25.760000228881836, -26.439998626708984, -27.119998931884766,
                                    -27.78999900817871, -28.44999885559082, -29.099998474121094, -29.739999771118164,
                                    -30.369998931884766, -31.0, -31.510000228881836, -32.11000061035156,
                                    -32.709999084472656, -33.30999755859375, -33.88999938964844, -34.46999740600586,
                                    -35.029998779296875, -35.59000015258789, -36.13999938964844, -36.689998626708984,
                                    -37.119998931884766, -37.64999771118164, -38.16999816894531, -38.68000030517578,
                                    -39.18000030517578, -39.68000030517578, -40.16999816894531, -40.64999771118164,
                                    -41.12999725341797, -41.48999786376953, -41.95000076293945, -42.39999771118164,
                                    -42.849998474121094, -43.290000915527344, -43.72999954223633, -44.14999771118164,
                                    -44.56999969482422, -44.87999725341797, -45.28999710083008, -45.689998626708984,
                                    -46.09000015258789, -46.47999954223633, -46.86000061035156, -47.23999786376953,
                                    -47.619998931884766, -47.87999725341797, -48.25, -48.599998474121094,
                                    -48.959999084472656, -49.29999923706055, -49.64999771118164, -49.97999954223633,
                                    -50.20000076293945, -50.529998779296875, -50.849998474121094, -51.16999816894531,
                                    -51.47999954223633, -51.78999710083008, -52.099998474121094, -52.39999771118164,
                                    -52.57999801635742, -52.869998931884766, -53.15999984741211, -53.439998626708984,
                                    -53.71999740600586, -54.0, -54.27000045776367, -54.53999710083008,
                                    -54.69999694824219, -54.959999084472656, -55.21999740600586, -55.46999740600586,
                                    -55.71999740600586, -55.96999740600586, -56.209999084472656, -56.44999694824219,
                                    -56.689998626708984, -56.91999816894531, -57.14999771118164, -57.37999725341797,
                                    -57.599998474121094, -57.81999969482422, -58.03999710083008, -58.2599983215332,
                                    -58.46999740600586, -58.68000030517578, -58.88999938964844, -59.09000015258789,
                                    -59.29999923706055, -59.5, -59.689998626708984, -59.88999938964844,
                                    -60.07999801635742, -60.27000045776367, -60.459999084472656, -60.63999938964844,
                                    -60.81999969482422, -61.0099983215332, -61.18000030517578, -60.94999694824219,
                                    -61.119998931884766, -61.29999923706055, -61.46999740600586, -61.62999725341797,
                                    -61.79999923706055, -61.959999084472656, -62.12999725341797, -62.28999710083008,
                                    -62.439998626708984, -62.599998474121094, -62.65999984741211, -62.80999755859375,
                                    -62.959999084472656, -63.1099967956543, -63.2599983215332, -63.39999771118164,
                                    -63.54999923706055, -63.689998626708984, -63.82999801635742, -63.96999740600586,
                                    -64.01000213623047, -64.1500015258789, -64.27999877929688, -64.41999816894531,
                                    -64.54999542236328, -64.68000030517578, -64.80999755859375, -64.93999481201172,
                                    -65.05999755859375, -65.18999481201172, -65.30999755859375, -65.33000183105469,
                                    -65.45999908447266, -65.58000183105469, -65.69999694824219, -65.80999755859375,
                                    -65.93000030517578, -66.04000091552734, -66.15999603271484, -66.2699966430664,
                                    -66.37999725341797, -66.48999786376953, -66.5999984741211, -66.70999908447266,
                                    -66.81999969482422, -66.93000030517578, -67.02999877929688, -67.13999938964844,
                                    -67.23999786376953, -67.33999633789062, -67.43999481201172, -67.54000091552734,
                                    -67.63999938964844, -67.73999786376953, -67.83999633789062, -67.93000030517578,
                                    -68.02999877929688, -68.1199951171875, -68.22000122070312, -68.30999755859375,
                                    -68.4000015258789, -68.5, -68.58999633789062, -68.66999816894531,
                                    -68.76000213623047, -68.8499984741211, -68.93999481201172, -69.02999877929688,
                                    -68.83000183105469, -68.91999816894531, -69.0, -69.08000183105469, -69.16999816894531,
                                    -69.25, -69.33000183105469, -69.40999603271484, -69.48999786376953, -69.56999969482422,
                                    -69.6500015258789, -69.72000122070312, -69.79999542236328, -69.87999725341797,
                                    -69.94999694824219, -70.02999877929688, -70.0999984741211, -70.08000183105469,
                                    -70.1500015258789, -70.22000122070312, -70.29000091552734, -70.3699951171875,
                                    -70.43999481201172, -70.51000213623047, -70.58000183105469, -70.63999938964844]
    elif dset == 'realdualhead':
        synth = load_dataset(RealDualheadFqpr())
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    test_time = fq.return_unique_times_across_sectors()

    if dset == 'real':
        data, secs, times = fq.reform_2d_vars_across_sectors_at_time(['beampointingangle'], test_time[0])
        assert len(times) == 1
        assert times[0] == test_time[0]
        assert (secs[0] == expected_sector_ids).all()
        assert (data[0] == expected_reformed_angles).all()
    elif dset == 'realdualhead':
        # should only be one time here
        data, secs, times = fq.reform_2d_vars_across_sectors_at_time(['beampointingangle'], test_time)
        assert len(test_time) == 1
        assert len(np.unique(times)) == 1

        # secs here should be uniform for each returned ping
        ping_sectors = np.unique(secs[0, :, :], axis=1)[:, 0]
        assert ping_sectors.size == 4

        for cnt, pingsec in enumerate(ping_sectors):
            assert (data[0][cnt][:] == fq.multibeam.raw_ping[pingsec].beampointingangle.values).all()


def test_basesurface():
    testz = np.array([1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2., 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0,
                      3.1, 3.2, 3.3, 3.4, 3.5, 3.6])
    testx = np.array([1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5])
    testy = np.array([1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5])
    testbs = BaseSurface(testx, testy, testz, resolution=1)
    testbs.construct_base_grid()
    testbs.build_histogram()
    testbs.build_surfaces(method='linear', count_msk=1)

    expected_surf = np.array([[1.2, 1.3, 1.4, 1.5],
                              [1.7, 1.8, 1.9, 2.0],
                              [2.2, 2.3, 2.4, 2.5],
                              [2.7, 2.8, 2.9, 3.0]])

    assert np.array_equal(testbs.surf, expected_surf)

    print('Passed: basesurface')


def build_georef_correct_comparison(dset='realdual', vert_ref='waterline', datum='NAD83'):
    """
   Generate mine/kongsberg xyz88 data set from the test dataset.

   Will run using the 'realdualhead' dataset included in this file or a small synthetic test dataset with meaningless
   numbers that I've just come up with.

   Parameters
   ----------
   dset: str, specify which dataset you want to use
   vert_ref: str, vertical reference, one of ['waterline', 'vessel', 'ellipse']
   datum: str, datum identifier, anything recognized by pyproj CRS

   """

    if dset == 'real':
        synth_dat = RealFqpr()
        synth = load_dataset(synth_dat)
    elif dset == 'realdual':
        synth_dat = RealDualheadFqpr()
        synth = load_dataset(synth_dat)
    else:
        raise NotImplementedError('mode not recognized')

    fq = fqpr_generation.Fqpr(synth)
    fq.logger = logging.getLogger()
    fq.logger.setLevel(logging.INFO)
    fq.read_from_source()
    fq.get_orientation_vectors(dump_data=False, delete_futs=False, initial_interp=False)
    fq.get_beam_pointing_vectors(dump_data=False, delete_futs=False)
    fq.sv_correct(dump_data=False, delete_futs=False)
    fq.construct_crs(datum=datum, projected=True, vert_ref=vert_ref)
    fq.georef_xyz(dump_data=False, delete_futs=False)

    secs = fq.return_sector_ids()
    tstmp = list(fq.intermediate_dat[secs[0]]['xyz'].keys())[0]

    loaded_xyz_data = [fq.intermediate_dat[s]['xyz'][tstmp][0][0].result() for s in fq.return_sector_ids()]
    loaded_sv_data = [fq.intermediate_dat[s]['sv_corr'][tstmp][0][0].result() for s in fq.return_sector_ids()]
    loaded_ang_data = [np.rad2deg(fq.intermediate_dat[s]['bpv'][tstmp][0][0].result()[1]) for s in
                       fq.return_sector_ids()]

    fq.intermediate_dat = {}

    if dset == 'realdual':
        loaded_data = [[loaded_sv_data[i][0].values[0], loaded_sv_data[i][1].values[0], loaded_xyz_data[i][2].values[0],
                        loaded_ang_data[i].values[0]] for i in range(int(len(loaded_xyz_data)))]

        # apply waterline, z lever arm and z phase center offsets to get at the actual waterline rel value
        depth_wline_addtl = [-float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z_1'][tstmp]),
                             -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_port_z_1'][tstmp]),
                             -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z_1'][tstmp]),
                             -float(fq.multibeam.xyzrph['waterline'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z'][tstmp]) +
                             float(fq.multibeam.xyzrph['tx_stbd_z_1'][tstmp])]

        # kongsberg angles are rel horiz, here is what I came up with to get vert rel angles (to match kluster)
        xyz_88_corrangle = [90 - np.array(synth_dat.xyz88_corrangle[0]), 90 - np.array(synth_dat.xyz88_corrangle[1]),
                            np.array(synth_dat.xyz88_corrangle[2]) - 90, np.array(synth_dat.xyz88_corrangle[3]) - 90]
        xyz88_data = [[np.array(synth_dat.xyz88_alongtrack[i]), np.array(synth_dat.xyz88_acrosstrack[i]),
                       np.array(synth_dat.xyz88_depth[i]) + depth_wline_addtl[i],
                       xyz_88_corrangle[i]] for i in range(int(len(synth_dat.xyz88_depth)))]

    elif dset == 'real':
        loaded_data = []
        for tme in [0, 1]:
            for secs in [[0, 2, 4], [1, 3, 5]]:
                dpth = np.concatenate([loaded_xyz_data[secs[0]][2].values[tme][~np.isnan(loaded_xyz_data[secs[0]][2].values[tme])],
                                       loaded_xyz_data[secs[1]][2].values[tme][~np.isnan(loaded_xyz_data[secs[1]][2].values[tme])],
                                       loaded_xyz_data[secs[2]][2].values[tme][~np.isnan(loaded_xyz_data[secs[2]][2].values[tme])]])
                along = np.concatenate([loaded_sv_data[secs[0]][0].values[tme][~np.isnan(loaded_sv_data[secs[0]][0].values[tme])],
                                        loaded_sv_data[secs[1]][0].values[tme][~np.isnan(loaded_sv_data[secs[1]][0].values[tme])],
                                        loaded_sv_data[secs[2]][0].values[tme][~np.isnan(loaded_sv_data[secs[2]][0].values[tme])]])
                across = np.concatenate([loaded_sv_data[secs[0]][1].values[tme][~np.isnan(loaded_sv_data[secs[0]][1].values[tme])],
                                         loaded_sv_data[secs[1]][1].values[tme][~np.isnan(loaded_sv_data[secs[1]][1].values[tme])],
                                         loaded_sv_data[secs[2]][1].values[tme][~np.isnan(loaded_sv_data[secs[2]][1].values[tme])]])
                angle = np.concatenate([loaded_ang_data[secs[0]].values[tme][~np.isnan(loaded_ang_data[secs[0]].values[tme])],
                                        loaded_ang_data[secs[1]].values[tme][~np.isnan(loaded_ang_data[secs[1]].values[tme])],
                                        loaded_ang_data[secs[2]].values[tme][~np.isnan(loaded_ang_data[secs[2]].values[tme])]])
                loaded_data.append([along, across, dpth, angle])

        # in the future, include sec index to get the additional phase center offsets included here
        depth_wline_addtl = -float(fq.multibeam.xyzrph['waterline'][tstmp]) + float(fq.multibeam.xyzrph['tx_z'][tstmp])

        # kongsberg angles are rel horiz, here is what I came up with to get vert rel angles (to match kluster)
        xyz_88_corrangle = []
        for ang in synth_dat.xyz88_corrangle:
            ang = 90 - np.array(ang)
            ang[np.argmin(ang):] = ang[np.argmin(ang):] * -1
            xyz_88_corrangle.append(ang)

        xyz88_data = [[np.array(synth_dat.xyz88_alongtrack[i]), np.array(synth_dat.xyz88_acrosstrack[i]),
                       np.array(synth_dat.xyz88_depth[i]) + depth_wline_addtl, xyz_88_corrangle[i]] for i in
                      range(int(len(synth_dat.xyz88_depth)))]

    else:
        raise NotImplementedError('only real and realdual are currently implemented')

    return loaded_data, xyz88_data


def build_kongs_comparison_plots(dset='realdual', vert_ref='waterline', datum='NAD83'):
    """
    Use the build_georef_correct_comparison function to get kongsberg and my created values from the test_dataset
    and build some comparison plots.

    Parameters
    ----------
    dset: string identifier, identifies which of the test_datasets to use
    vert_ref: str, vertical reference, one of ['waterline', 'vessel', 'ellipse']
    datum: str, datum identifier, anything recognized by pyproj CRS

    Returns
    -------
    plots: list, each element of the list is a tuple of the figure and all the subplots associated with that ping

    """
    mine, kongsberg = build_georef_correct_comparison(dset=dset, vert_ref=vert_ref, datum=datum)

    plots = []

    if dset == 'realdual':
        for cnt, idxs in enumerate([[0, 2], [1, 3]]):
            print('Generating Ping {} plot'.format(cnt + 1))

            fig, (z_plt, x_plt, y_plt, ang_plt) = plt.subplots(4)

            fig.suptitle('Ping {}'.format(cnt + 1))
            z_plt.set_title('depth compare')
            x_plt.set_title('along compare')
            y_plt.set_title('across compare')
            ang_plt.set_title('angle compare')

            z_plt.plot(np.concatenate([mine[idxs[0]][2], mine[idxs[1]][2]]), c='b')
            z_plt.plot(np.concatenate([kongsberg[idxs[0]][2], kongsberg[idxs[1]][2]]), c='r')
            x_plt.plot(np.concatenate([mine[idxs[0]][0], mine[idxs[1]][0]]), c='b')
            x_plt.plot(np.concatenate([kongsberg[idxs[0]][0], kongsberg[idxs[1]][0]]), c='r')
            y_plt.plot(np.concatenate([mine[idxs[0]][1], mine[idxs[1]][1]]), c='b')
            y_plt.plot(np.concatenate([kongsberg[idxs[0]][1], kongsberg[idxs[1]][1]]), c='r')
            ang_plt.plot(np.concatenate([mine[idxs[0]][3], mine[idxs[1]][3]]), c='b')
            ang_plt.plot(np.concatenate([kongsberg[idxs[0]][3], kongsberg[idxs[1]][3]]), c='r')
            plots.append([fig, z_plt, x_plt, y_plt, ang_plt])
    else:
        for i in range(len(mine)):
            print('Generating Ping {} plot'.format(i + 1))

            fig, (z_plt, x_plt, y_plt, ang_plt) = plt.subplots(4)

            fig.suptitle('Ping {}'.format(i + 1))
            z_plt.set_title('depth compare')
            x_plt.set_title('along compare')
            y_plt.set_title('across compare')
            ang_plt.set_title('angle compare')

            z_plt.plot(mine[i][2], c='b')
            z_plt.plot(kongsberg[i][2], c='r')
            x_plt.plot(mine[i][0], c='b')
            x_plt.plot(kongsberg[i][0], c='r')
            y_plt.plot(mine[i][1], c='b')
            y_plt.plot(kongsberg[i][1], c='r')
            ang_plt.plot(mine[i][3], c='b')
            ang_plt.plot(kongsberg[i][3], c='r')
            plots.append([fig, z_plt, x_plt, y_plt, ang_plt])

    return plots


def load_dataset(dset=None, skip_dask=True):
    """
    Returns the 'real' dataset constructed using one of the synth data classes.  If None, uses SyntheticFqpr with some
    dummy values.  Otherwise, expects one of RealFqpr, RealDualheadFqpr, SyntheticFqpr, etc.  Builds the
    xarray_conversion BatchRead class using the dataset data.

    Parameters
    ----------
    dset: optional, if None will use SyntheticFqpr with zeroed values, otherwise one of RealFqpr, RealDualheadFqpr,
           SyntheticFqpr, etc classes.
    skip_dask

    Returns
    -------
    kongs_dat: xarray_conversion BatchRead object

    """
    if dset is None:
        dset = SyntheticFqpr(synth_time=0, synth_heave=0, synth_roll=0, synth_pitch=0, synth_yaw=0,
                             synth_tx_mountroll=0, synth_tx_mountpitch=0, synth_tx_mountyaw=0, synth_rx_mountroll=0,
                             synth_rx_mountpitch=0, synth_rx_mountyaw=0, secs=('999_0_290000', '999_0_300000'))

    kongs_dat = xarray_conversion.BatchRead('', skip_dask=skip_dask)
    kongs_dat.logger = logging.getLogger()
    kongs_dat.logger.setLevel(logging.INFO)
    kongs_dat.xyzrph = dset.xyzrph
    kongs_dat.raw_ping = dset.raw_ping
    kongs_dat.raw_att = dset.raw_att
    kongs_dat.raw_nav = dset.raw_nav
    return kongs_dat
