import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import logging


from HSTB.kluster import fqpr_generation, xarray_conversion
from HSTB.kluster.test_datasets import RealFqpr, RealDualheadFqpr, SyntheticFqpr
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
        depth_wline_addtl = [-float(fq.source_dat.xyzrph['waterline'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_port_z'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_port_z_1'][tstmp]),
                             -float(fq.source_dat.xyzrph['waterline'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_port_z'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_port_z_1'][tstmp]),
                             -float(fq.source_dat.xyzrph['waterline'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_stbd_z'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_stbd_z_1'][tstmp]),
                             -float(fq.source_dat.xyzrph['waterline'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_stbd_z'][tstmp]) +
                             float(fq.source_dat.xyzrph['tx_stbd_z_1'][tstmp])]

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
        depth_wline_addtl = -float(fq.source_dat.xyzrph['waterline'][tstmp]) + float(fq.source_dat.xyzrph['tx_z'][tstmp])

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
