from HSTB.kluster.fqpr_drivers import sequential_read_multibeam
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from scipy.interpolate import interp1d
import glob
import pandas as pd
import os
import datetime as dt
from HSTB.drivers import kmall
import sys

'''
Functions to evaluate backscatter quality on a set of raw data (.all or .kmall lines). 
'''

# evaluate_raw_backscatter runs bad ping, bad sounding, and sector detection on a given line. Bad ping and sounding
# algorithms are same as used in Iskaffe (with some parameters changes)
def evaluate_raw_backscatter_for_file(fle):
    # This function works for both kmall and all files
    data = sequential_read_multibeam(fle)

    # Note that kluster currently does not include the reflectivity1 datagram. This is a workaround using the kmall driver
    # until I can fix this issue in the sequential_read_multibeam function. I suspect this is making the process take quite a bit longer
    # for .kmall files.
    expected_num_beams = 400 #True for 712. Should probably add a sonar-dependent function here.
    if fle.split('.')[-1] == 'kmall':
        km = kmall.kmall(fle)
        reflectivity1 = []
        num_sdg_max = []
        num_sdg_valid = []
        while not km.eof:
            km.decode_datagram()
            if km.datagram_ident != 'MRZ':
                km.skip_datagram()
            else:
                km.read_datagram()
                ref1_ping = km.datagram_data['sounding']['reflectivity1_dB']
                num_sdg_max_ping = km.datagram_data['rxInfo']['numSoundingsMaxMain']
                num_sdg_valid_ping = km.datagram_data['rxInfo']['numSoundingsValidMain']
                if num_sdg_valid_ping == 0:
                    ref1_ping = [0.0] * len(reflectivity1[-1])
                    reflectivity1.append(ref1_ping)
                    num_ping = len(reflectivity1)
                    print('Warning: Ping ' + str(num_ping) + ' likely corrupt. Contains no valid soundings')
                else:
                    reflectivity1.append(ref1_ping)
                num_sdg_max.append(num_sdg_max_ping)
                num_sdg_valid.append(num_sdg_valid_ping)
        reflectivity = np.array(reflectivity1)
    else:
        reflectivity = data['ping']['reflectivity']

    # Identifying Bad Beams
    detection_info = data['ping']['detectioninfo']  # detction info : re - accepted, 2 = rejected, 1 = phase detection, 0 = amplitude detection.
    detection_info = detection_info.astype(float)
    # reflectivity = data['ping']['reflectivity']
    bad_beams = detection_info[detection_info == 2].size
    bad_beam_arr = detection_info
    bad_beam_arr[bad_beam_arr != 2] = np.nan
    total_beams = detection_info.size
    bad_beam_percentage = bad_beams / total_beams * 100
    # Bad Ping Detection
    threshold = 0.2  # between 0 and 1

    npings = detection_info.shape[0]
    nbeams = detection_info.shape[1]
    bad_ping1_inds = []
    bad_ping2_inds = []
    sector_border_inds = []
    for n in range(npings):
        detection_info_ping = detection_info[n]
        if len(detection_info_ping[detection_info_ping == 2]) / len(detection_info_ping) > threshold:
            bad_ping1_inds.append(n)

    # Identifying Bad Pings by two methods

    window_len = 10  # length of window
    gate_len = 5  # how many pings back from the test ping is the window.
    drop_threshold = -3  # threshold dB to be considered drop
    ratio_drop_threshold = 0.5  # threshold ratio of beams that dropped for an entire ping to be considered bad.
    ikeep = np.zeros(npings)
    nbeams_drop = np.zeros(npings)
    nbeams_drop[:] = np.nan
    ikeep[:window_len + gate_len] = 1
    loop_range = np.arange(window_len + gate_len, npings, 1)
    for n in loop_range:
        iwindow = np.arange(-window_len, 0) + n - gate_len
        iwindow = iwindow[ikeep[iwindow] == 1]
        if np.sum(iwindow) != 0:
            ref_window_mean = np.mean(reflectivity[iwindow], axis=0)
        else:
            break
        diff_to_window = reflectivity[n] - ref_window_mean
        nbeams_drop[n] = len(diff_to_window[diff_to_window <= drop_threshold])

        if nbeams_drop[n] / nbeams > ratio_drop_threshold:
            ikeep[n] = 0
            bad_ping2_inds.append(n)
        else:
            ikeep[n] = 1

    # Sector Boundary Detection
    sector_threshold = 1  # dB threshold for sector to sector jump

    # Averaging reflectivity
    reflectivity_smoothed = np.zeros(reflectivity.shape)
    sector_smoother_length = 100
    if npings < sector_smoother_length:
        print('Line shorter than ' + str(
            sector_smoother_length) + ' pings. Reducing smoother length to line length for sector detection.')
        sector_smoother_length = npings
    convolve_array = np.ones(sector_smoother_length) / sector_smoother_length
    for row in range(reflectivity.shape[1]):
        reflectivity_smoothed[:, row] = np.convolve(reflectivity[:, row], convolve_array, mode='same')

    sectors = data['ping']['txsector_beam']
    ref_diff = np.zeros((sectors.shape[0], len(set(sectors[0])) - 1))
    ref_diff[:, :] = np.nan
    sector_boundaries = np.zeros(
        (sectors.shape[0], len(set(sectors.reshape(sectors.shape[0] * sectors.shape[1]))) - 1))  # Thi
    sector_boundaries[:, :] = np.nan
    for n in range(sectors.shape[0]):
        #Overlooking sector boundaries from first 3 and last 3 beams. Not generally "real" sectors and can cause problems.
        sectors_for_detection = np.concatenate((sectors[n][3]*np.ones(3), sectors[n][3:-3], sectors[n][3]*np.ones(3)))
        sector_boundaries_ping = np.argwhere(np.diff(sectors_for_detection) == 1)
        if sector_boundaries_ping.shape[0] == sector_boundaries.shape[1]: #If the number of sector boundaries in a given ping equals the overall expected number in the line.
            sector_boundaries[n] = sector_boundaries_ping.reshape(sector_boundaries_ping.shape[0])
            # sector_boundaries = np.array((190, 210)).reshape(2,1)
            ref_diff_ping = reflectivity_smoothed[n, sector_boundaries_ping + 1] - reflectivity_smoothed[
                n, sector_boundaries_ping - 1]
            # mean_diff = np.mean((ref_diff_ping[sector_boundaries], ref_diff_ping[sector_boundaries-1], ref_diff_ping[sector_boundaries+1]), axis=0)
            # ref_diff_boundary = ref_diff_ping[sector_boundaries_ping]
            ref_diff[n] = ref_diff_ping.reshape(sector_boundaries_ping.shape[0])

    ref_diff_mean = np.nanmean(np.abs(ref_diff[sector_smoother_length:-sector_smoother_length]), axis=0)
    sector_boundaries_approx = np.nanmean(sector_boundaries, axis=0)
    if False in np.isnan(sector_boundaries_approx):
        sector_boundaries_approx = sector_boundaries_approx.astype(int)
    else:
        sector_boundaries_approx = []
    sectors_detected = (ref_diff_mean > sector_threshold) | (ref_diff_mean < -sector_threshold)
    if len(sector_boundaries_approx) == sector_boundaries.shape[1]:
        sector_border_inds = sector_boundaries_approx[sectors_detected]

    results = {'bad_beam_arr': bad_beam_arr, 'bad_beam_percentage': bad_beam_percentage,
               'bad_ping1_inds': bad_ping1_inds,
               'bad_ping2_inds': bad_ping2_inds, 'reflectivity': reflectivity, 'npings': npings,
               'sector_border_inds': sector_border_inds}
    return results

def make_plot_for_line(fle, results, combined_results, results_dir):
    reflectivity = results['reflectivity']
    nbeams = results['bad_beam_arr'].size
    bad_beam_arr = results['bad_beam_arr']
    npings = results['npings']
    bad_beam_percentage = results['bad_beam_percentage']
    bad_ping1_inds = results['bad_ping1_inds']
    bad_ping2_inds = results['bad_ping2_inds']
    sector_border_inds = results['sector_border_inds']
    bad_ping_inds = np.concatenate((bad_ping1_inds, bad_ping2_inds))
    bad_ping_inds = list(set(bad_ping_inds))
    combined_results[fle] = [npings, len(bad_ping_inds) / npings * 100, nbeams, bad_beam_percentage,
                             len(sector_border_inds)]
    fig, ax = plt.subplots(1, 2, figsize=(20, 20))

    fig.suptitle(
        fle.split('\\')[-1] + ' Backscatter Quality: \n' + str(round(bad_beam_percentage, 2)) + '% Bad Soundings, '
        + str(round(len(bad_ping_inds) / npings * 100, 2)) + '% Bad Pings')
    im = ax[0].imshow(reflectivity, aspect='auto', vmin=-70, vmax=10, cmap='gray')
    fig.colorbar(im, orientation='vertical')
    # aspect = ax[0].get_aspect()
    ax[1].imshow(reflectivity, aspect='auto', vmin=-70, vmax=10, cmap='gray')
    ax[0].grid(False)
    ax[1].grid(False)

    plt_label = 'Bad Pings, 10% of Soundings bad'
    for bad_ping1_ind in bad_ping1_inds:
        ax[1].plot([0, reflectivity.shape[1]], [bad_ping1_ind, bad_ping1_ind], 'm', label=plt_label)
        plt_label = "_nolegend_"

    plt_label = 'Bad Pings, by drop threshold'
    for bad_ping2_ind in bad_ping2_inds:
        ax[1].plot([0, reflectivity.shape[1]], [bad_ping2_ind, bad_ping2_ind], 'r', label=plt_label)
        plt_label = "_nolegend_"

    plt_label = 'Detected Sector Boundary'
    for sector_border in sector_border_inds:
        ax[1].plot([sector_border, sector_border], [0, reflectivity.shape[0]], 'y', label=plt_label)
        plt_label = "_nolegend_"

    cmap_blue = plt.get_cmap('rainbow')
    cmap_blue.set_over('blue')
    # bad_beam_arr = np.nan_to_num(bad_beam_arr)
    # bad_beam_arr[bad_beam_arr == 2] = np.nan
    ax[1].imshow(bad_beam_arr, aspect='auto', interpolation='none', vmin=0, vmax=1, cmap=cmap_blue,
                 label='Bad Soundings')
    handles, labels = ax[1].get_legend_handles_labels()
    point = Line2D([0], [0], label='Bad Soundings', marker='s', markersize=10, markerfacecolor='blue', linestyle='')
    handles.extend([point])
    ax[1].legend(handles=handles)
    # ax[0].colorbar()
    ax[0].set_xlabel('Beam Number')
    ax[1].set_xlabel('Beam Number')
    ax[0].set_ylabel('Ping Number')
    fig_fle = results_dir + '\\Results_Images\\' + fle.split('\\')[-1].split('.')[0] + '_bs_eval.png'
    fig.savefig(fig_fle)
    plt.close('all')
    return combined_results

def find_files(fle_dir, results_dir):
    if os.path.isdir(results_dir + '\\Results_Images') == False:
        os.mkdir(results_dir + '\\Results_Images')
    fles_kmall = glob.glob(fle_dir + '\\**\\*.kmall', recursive=True)
    fles_all = glob.glob(fle_dir + '\\**\\*.all', recursive=True)
    fles = fles_kmall + fles_all

    results_csv = results_dir + '\\raw_backscatter_evaluation.csv'
    if os.path.isfile(results_csv) == True:
        df_existing = pd.read_csv(results_csv, index_col=0)
        df_existing = df_existing.drop('All Lines')
        fles_existing = list(df_existing.index)
        for fle in fles_existing:
            fles.remove(fle)
    else:
        df_existing = pd.DataFrame()
    return fles, results_csv, df_existing

def assemble_results_csv(combined_results, results_csv, df_existing):
    combined_results_df = pd.DataFrame.from_dict(data=combined_results, orient='index')
    combined_results_df = combined_results_df.rename(
        columns={0: 'npings', 1: 'percentage bad pings', 2: 'nbeams', 3: 'percentage bad beams',
                 4: 'detected sector boundaries'})
    combined_results_df = pd.concat([combined_results_df, df_existing])

    bad_ping_totals = combined_results_df.npings * combined_results_df['percentage bad pings'] / 100
    bad_ping_sum = bad_ping_totals.sum()
    total_npings = combined_results_df.npings.sum()
    total_bad_ping_percent = bad_ping_sum / total_npings * 100
    bad_beam_totals = combined_results_df.nbeams * combined_results_df['percentage bad beams'] / 100
    bad_beam_sum = bad_beam_totals.sum()
    total_nbeams = combined_results_df.nbeams.sum()
    total_bad_beam_percent = bad_beam_sum / total_nbeams * 100
    total_lines_with_detected_sectors = (combined_results_df['detected sector boundaries'] != 0).sum()
    total_lines_with_detected_sectors_string = str(total_lines_with_detected_sectors) + ' lines with detected sectors'
    header_dict = {'All Lines': [total_npings, total_bad_ping_percent, total_nbeams, total_bad_beam_percent,
                                 total_lines_with_detected_sectors_string]}

    header_df = pd.DataFrame.from_dict(data=header_dict, orient='index')
    header_df = header_df.rename(
        columns={0: 'npings', 1: 'percentage bad pings', 2: 'nbeams', 3: 'percentage bad beams',
                 4: 'detected sector boundaries'})
    combined_results_df = pd.concat([header_df, combined_results_df])
    combined_results_df.to_csv(results_csv)
def create_bs_evaluation(fle_dir, results_dir):
    print('Started processing: ', dt.datetime.now())
    fles, results_csv, df_existing = find_files(fle_dir, results_dir)
    combined_results = {}
    for q in range(len(fles)):
        fle = fles[q]
        print('Opening ' + fle + ' for evaluation. File ' + str(q + 1) + ' of ' + str(len(fles)))
        results = evaluate_raw_backscatter_for_file(fle)
        combined_results = make_plot_for_line(fle, results, combined_results, results_dir)
    assemble_results_csv(combined_results, results_csv, df_existing)
    print('Completed processing: ', dt.datetime.now())


if __name__ == "__main__":
    # Example of running bs_evaluation on a directory with .kmall data.

    # fle = r"C:\Users\samuel.umfress\Documents\HSTB\TJ\Backscatter_Calibration\100m depth\0005_20240605_164755_S222_EM712.kmall"
    # data = sequential_read_multibeam(fle)

    fle_dir = r'C:\Users\samuel.umfress\Documents\HSTB\TJ\Backscatter_Calibration\100m depth\Test'
    # fle_dir = r'C:\Users\samuel.umfress\Documents\HSTB\TJ\Backscatter_Calibration\600m depth\Test'
    create_bs_evaluation(fle_dir, fle_dir)

    print('Finished processing: ', dt.datetime.now())

