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

'''
In development. Currently set up to be run as a script. 

Functions are tools for backscatter quality analysis and calibration. 

evaluate_raw_backscatter(fle) assess a .kmall or .all file and returns the number and index of the bad pings, 
bad soundings, and detected sectors.

create_raw_bs_evaluation(fle_dir) assess the contents of a directory using the evaluate_raw_backscatter_function, 
creates a plot for each line, and compiles the results into a .csv.
'''


#evaluate_raw_backscatter runs bad ping, bad sounding, and sector detection on a given line. Bad ping and sounding
#algorithms are same as used in Iskaffe (with some parameters changes)
def evaluate_raw_backscatter(fle):
    #This function works for both kmall and all files
    data = sequential_read_multibeam(fle)

    # Note that kluster currently does not include the reflectivity1 datagram. This is a workaround using the kmall driver
    # until I can fix this issue in the sequential_read_multibeam function. I suspect this is making the process take quite a bit longer
    # for .kmall files.
    if fle.split('.')[-1] == 'kmall':
        km = kmall.kmall(fle)
        reflectivity1 = []
        while not km.eof:
            km.decode_datagram()
            if km.datagram_ident != 'MRZ':
                km.skip_datagram()
            else:
                km.read_datagram()
                ref1_ping = km.datagram_data['sounding']['reflectivity1_dB']
                reflectivity1.append(ref1_ping)

        reflectivity = np.array(reflectivity1)
    else:
        reflectivity = data['ping']['reflectivity']

# Identifying Bad Beams
    detection_info = data['ping']['detectioninfo']     # detction info : re - accepted, 2 = rejected, 1 = phase detection, 0 = amplitude detection.
    detection_info = detection_info.astype(float)
    # reflectivity = data['ping']['reflectivity']
    bad_beams = detection_info[detection_info == 2].size
    bad_beam_arr = detection_info
    bad_beam_arr[bad_beam_arr != 2] = np.nan
    total_beams = detection_info.size
    bad_beam_percentage = bad_beams/total_beams*100
    # Bad Ping Detection
    threshold = 0.2  # between 0 and 1

    npings = detection_info.shape[0]
    nbeams = detection_info.shape[1]
    bad_ping1_inds = []
    bad_ping2_inds = []
    sector_border_inds = []
    for n in range(npings):
        detection_info_ping = detection_info[n]
        if len(detection_info_ping[detection_info_ping == 2])/len(detection_info_ping) > threshold:
            bad_ping1_inds.append(n)

#Identifying Bad Pings by two methods

    window_len = 10 #length of window
    gate_len = 5 #how many pings back from the test ping is the window.
    drop_threshold = -3 #threshold dB to be considered drop
    ratio_drop_threshold = 0.5 #threshold ratio of beams that dropped for an entire ping to be considered bad.
    ikeep = np.zeros(npings)
    nbeams_drop = np.zeros(npings)
    nbeams_drop[:] = np.nan
    ikeep[:window_len+gate_len] = 1
    loop_range = np.arange(window_len+gate_len,npings,1)
    for n in loop_range:
        iwindow = np.arange(-window_len,0) + n - gate_len
        iwindow = iwindow[ikeep[iwindow] == 1]
        if np.sum(iwindow) != 0:
            ref_window_mean = np.mean(reflectivity[iwindow], axis=0)
        else:
            break
        diff_to_window = reflectivity[n] - ref_window_mean
        nbeams_drop[n] = len(diff_to_window[diff_to_window <= drop_threshold])

        if nbeams_drop[n]/nbeams > ratio_drop_threshold:
            ikeep[n] = 0
            bad_ping2_inds.append(n)
        else:
            ikeep[n] = 1

#Sector Boundary Detection
    sector_threshold = 1 #dB threshold for sector to sector jump

    #Averaging reflectivity
    reflectivity_smoothed = np.zeros(reflectivity.shape)
    sector_smoother_length = 100
    if npings < sector_smoother_length:
        print('Line shorter than ' + str(sector_smoother_length) + ' pings. Reducing smoother length to line length for sector detection.')
        sector_smoother_length = npings
    convolve_array = np.ones(sector_smoother_length)/sector_smoother_length
    for row in range(reflectivity.shape[1]):
        reflectivity_smoothed[:, row] = np.convolve(reflectivity[:, row], convolve_array, mode='same')

    sectors = data['ping']['txsector_beam']
    ref_diff = np.zeros((sectors.shape[0], len(set(sectors[0]))-1))
    ref_diff[:,:] = np.nan
    sector_boundaries = np.zeros((sectors.shape[0], len(set(sectors.reshape(sectors.shape[0]*sectors.shape[1]))) - 1)) #Thi
    sector_boundaries[:,:] = np.nan
    for n in range(sectors.shape[0]):
        sector_boundaries_ping = np.argwhere(np.diff(sectors[n]) == 1)
        if sector_boundaries_ping.shape[0] == sector_boundaries.shape[1]:
            sector_boundaries[n] = sector_boundaries_ping.reshape(sector_boundaries_ping.shape[0])
            # sector_boundaries = np.array((190, 210)).reshape(2,1)
            ref_diff_ping = reflectivity_smoothed[n, sector_boundaries_ping+1]-reflectivity_smoothed[n, sector_boundaries_ping-1]
            # mean_diff = np.mean((ref_diff_ping[sector_boundaries], ref_diff_ping[sector_boundaries-1], ref_diff_ping[sector_boundaries+1]), axis=0)
            # ref_diff_boundary = ref_diff_ping[sector_boundaries_ping]
            ref_diff[n] = ref_diff_ping.reshape(sector_boundaries_ping.shape[0])

    ref_diff_mean = np.nanmean(np.abs(ref_diff[sector_smoother_length:-sector_smoother_length]), axis=0)
    sector_boundaries_approx = np.nanmean(sector_boundaries, axis=0).astype(int)
    sectors_detected = (ref_diff_mean > sector_threshold) | (ref_diff_mean < -sector_threshold)
    sector_border_inds = sector_boundaries_approx[sectors_detected]





    results = {'bad_beam_arr': bad_beam_arr, 'bad_beam_percentage': bad_beam_percentage, 'bad_ping1_inds': bad_ping1_inds,
               'bad_ping2_inds': bad_ping2_inds, 'reflectivity': reflectivity, 'npings': npings, 'sector_border_inds': sector_border_inds}
    return results

def create_raw_bs_evaluation(fle_dir):
    print('Started processing: ', dt.datetime.now())
    if os.path.isdir(fle_dir+'\\Results') == False:
        os.mkdir(fle_dir+'\\Results')
    fles_kmall = glob.glob(fle_dir+'\\**\\*.kmall', recursive=True)
    fles_all = glob.glob(fle_dir+'\\**\\*.all', recursive=True)
    fles = fles_kmall + fles_all

    results_csv = fle_dir+'\\raw_backscatter_evaluation.csv'
    if os.path.isfile(results_csv) == True:
        df_existing = pd.read_csv(results_csv, index_col=0)
        df_existing = df_existing.drop('All Lines')
        fles_existing = list(df_existing.index)
        for fle in fles_existing:
            fles.remove(fle)
    else:
        df_existing = pd.DataFrame()


    combined_results = {}
    # results['FileNames'] = fles
    for q in range(len(fles)):
        fle = fles[q]
        print('Opening ' + fle + ' for evaluation. File ' + str(q+1) + ' of ' +  str(len(fles)))
        results = evaluate_raw_backscatter(fle)
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
        combined_results[fle] = [npings, len(bad_ping_inds)/npings*100, nbeams, bad_beam_percentage, len(sector_border_inds)]
        fig, ax = plt.subplots(1,2, figsize=(20,20))

        fig.suptitle(fle.split('\\')[-1] + ' Backscatter Quality: \n'+ str(round(bad_beam_percentage,2)) + '% Bad Soundings, '
                     + str(round(len(bad_ping_inds)/npings*100,2)) + '% Bad Pings' )
        im = ax[0].imshow(reflectivity, aspect='auto', vmin=-70, vmax=10, cmap='gray')
        fig.colorbar(im, orientation='vertical')
        # aspect = ax[0].get_aspect()
        ax[1].imshow(reflectivity, aspect='auto', vmin=-70, vmax=10, cmap='gray')

        plt_label = 'Bad Pings, 10% of Soundings bad'
        for bad_ping1_ind in bad_ping1_inds:
            ax[1].plot([0,reflectivity.shape[1]],[bad_ping1_ind, bad_ping1_ind], 'm', label=plt_label)
            plt_label = "_nolegend_"

        plt_label = 'Bad Pings, by drop threshold'
        for bad_ping2_ind in bad_ping2_inds:
            ax[1].plot([0,reflectivity.shape[1]],[bad_ping2_ind, bad_ping2_ind], 'r', label=plt_label)
            plt_label = "_nolegend_"

        plt_label = 'Detected Sector Boundary'
        for sector_border in sector_border_inds:
            ax[1].plot([sector_border, sector_border], [0, reflectivity.shape[0]], 'y', label=plt_label)
            plt_label = "_nolegend_"

        cmap_rainbow = plt.get_cmap('rainbow')
        cmap_rainbow.set_over('blue')
        # bad_beam_arr = np.nan_to_num(bad_beam_arr)
        # bad_beam_arr[bad_beam_arr == 2] = np.nan
        ax[1].imshow(bad_beam_arr, aspect='auto', interpolation='none', vmin=0, vmax=1, cmap=cmap_rainbow, label='Bad Soundings')
        handles, labels = ax[1].get_legend_handles_labels()
        point = Line2D([0], [0], label='Bad Soundings', marker='s', markersize=10, markerfacecolor='blue', linestyle='')
        handles.extend([point])
        ax[1].legend(handles=handles)
        # ax[0].colorbar()
        ax[0].set_xlabel('Beam Number')
        ax[1].set_xlabel('Beam Number')
        ax[0].set_ylabel('Ping Number')
        fig_fle = fle_dir + '\\Results\\' + fle.split('\\')[-1].split('.')[0] + '_bs_eval.png'
        fig.savefig(fig_fle)
        plt.close('all')


    combined_results_df = pd.DataFrame.from_dict(data=combined_results, orient='index')
    combined_results_df = combined_results_df.rename(columns={0: 'npings', 1: 'percentage bad pings', 2: 'nbeams', 3: 'percentage bad beams', 4: 'detected sector boundaries'})
    combined_results_df = pd.concat([combined_results_df, df_existing])

    bad_ping_totals = combined_results_df.npings * combined_results_df['percentage bad pings'] / 100
    bad_ping_sum = bad_ping_totals.sum()
    total_npings = combined_results_df.npings.sum()
    total_bad_ping_percent = bad_ping_sum/total_npings*100
    bad_beam_totals = combined_results_df.nbeams * combined_results_df['percentage bad beams'] / 100
    bad_beam_sum = bad_beam_totals.sum()
    total_nbeams = combined_results_df.nbeams.sum()
    total_bad_beam_percent = bad_beam_sum/total_nbeams*100
    total_lines_with_detected_sectors = (combined_results_df['detected sector boundaries'] != 0).sum()
    total_lines_with_detected_sectors_string = str(total_lines_with_detected_sectors) + ' lines with detected sectors'
    header_dict = {'All Lines': [total_npings, total_bad_ping_percent, total_nbeams, total_bad_beam_percent, total_lines_with_detected_sectors_string]}

    header_df = pd.DataFrame.from_dict(data=header_dict, orient='index')
    header_df = header_df.rename(columns={0: 'npings', 1: 'percentage bad pings', 2: 'nbeams', 3: 'percentage bad beams', 4: 'detected sector boundaries'})
    combined_results_df = pd.concat([header_df, combined_results_df])

    combined_results_df.to_csv(results_csv)

    print('Completed processing: ', dt.datetime.now())
    # return combined_results
# with open(results_csv, 'wb') as csv_file:
#     writer = csv.writer(csv_file)
#     for key, value in


# Calibration

# Next up: Get this thing to run on a whole directory and sort out modes. The dict is key.
# Also experience with removing the 2*nnic from the backscatter line. Maybe mess around with EAC again (tile angle?)

#Processing Steps Summary
#KM = Kongsberg Maritime, TVG = Time Varying Gain, NNIC = Near Nadir Insidence Correction, LC = Lambertian Correciton
# EAC = Ensonified area correction, TL = Transmission Loss, CA = Crossover Angle

#Stage 1:   Remove KM Special TVG from reflectivity 2
#           TVG_kmall = NNIC + LC - EAC + 2TL
#Stage 2:   Add back -EAC and +2TL
#           If NNIC in TVG is proper sign.
#           BS = Ref1 - TVG - EAC + 2TL
#           If NNIC in TVG is flipped...
#           BS = Ref1 - TVG - EAC + 2TL - 2*NNIC
#Stage 3:   Bin everything into increments of equal angle. Store this as Sb_noTVG and use it for later corrections
#Stage 4:   Fit a new NNIC + LC curve to the dataset, based on the actual observed CA.
#           Resulting data should be flattened with an improved TVG
#Stage 5:   Determine a single average dB of the flattened BS.
#Stage 6:   On a sector-by-sector, ping-by-ping basis, determine the difference between the average and the measured
#           intensity value for a given angle. Then average along each angle bin.
#Stage 7:   Assemble into a calibration file for PU Upload.
def prepare_backscatter_for_file(fle):
    data = sequential_read_multibeam(fle)
    mode = data['runtime_params']['runtime_settings'][0]['Depth setting']
    # data_dict[fle] = [{'mode': mode}, {'data': data}]
    approx_sv_mean = 1500
    reflectivity2 = data['ping']['reflectivity']
    # reflectivity = reflectivity2

    #Adding Hacky Reflectivity1 Reader in here
    km = kmall.kmall(fle)
    reflectivity1 = []
    bs_oblique = []
    bs_normal = []
    seabedimage_snippets = []
    seabedimage_start = []
    seabedimage_numsamples = []
    seabedimage_centersample = []

    while not km.eof:
        km.decode_datagram()
        if km.datagram_ident != 'MRZ':
            km.skip_datagram()
        else:
            km.read_datagram()
            ref1_ping = km.datagram_data['sounding']['reflectivity1_dB']
            bs_oblique_ping = km.datagram_data['rxInfo']['BSoblique_dB']
            bs_normal_ping = km.datagram_data['rxInfo']['BSnormal_dB']
            seabedimage_snippets_ping = km.datagram_data['SIsample_desidB'] #snippets
            seabedimage_start_ping = km.datagram_data['sounding']['SIstartRange_samples']
            seabedimage_numsamples_ping = km.datagram_data['sounding']['SInumSamples']
            seabedimage_centersample_ping = km.datagram_data['sounding']['SIcentreSample']

            reflectivity1.append(ref1_ping)
            bs_oblique.append(bs_oblique_ping)
            bs_normal.append(bs_normal_ping)
            seabedimage_snippets.append(seabedimage_snippets_ping)
            seabedimage_start.append(seabedimage_start_ping)
            seabedimage_numsamples.append(seabedimage_numsamples_ping)
            seabedimage_centersample.append(seabedimage_centersample_ping)

    #Seabed Image Averaging
    num_pings = len(bs_oblique)
    num_beams = len(seabedimage_numsamples[0])
    seabedimage = np.zeros((num_pings, num_beams))
    seabedimage[:] = np.nan
    seabedimage_center = np.zeros((num_pings, num_beams))
    seabedimage_center[:] = np.nan
    for p in range(num_pings):
        for b in range(num_beams):
            snippet_beam = seabedimage_snippets[p][seabedimage_start[p][b]:seabedimage_start[p][b]+seabedimage_numsamples[p][b]]
            snippet_beam = np.array(snippet_beam)/10
            reflectivity_sb_beam = 10*np.log10(1/seabedimage_numsamples[p][b]*np.sum(10**(snippet_beam/10)))
            seabedimage[p,b] = reflectivity_sb_beam
            seabedimage_center[p,b] = seabedimage_snippets[p][seabedimage_centersample[p][b]]



    data['ping']['reflectivity1'] = reflectivity1

    reflectivity = reflectivity1

    beampointingangle = data['ping']['beampointingangle']
    beam_angle_bins = np.arange(-80,81,1)*(-1)
    num_pings = np.shape(reflectivity)[0]
    tvg = data['ping']['tvg']
    twtt = data['ping']['traveltime']
    time_tx = data['ping']['time']
    time_tx = time_tx[:, None]*np.ones(twtt.shape)
    time_rx = twtt + time_tx
    roll = data['attitude']['roll']
    pitch = data['attitude']['pitch']
    time = data['attitude']['time']
    roll_rx = np.interp(time_rx, time, roll)
    beam_angle = roll_rx + beampointingangle

    tiltangle = data['ping']['tiltangle']
    pitch_tx = np.interp(time_tx, time, pitch)
    tx_angle = pitch_tx + tiltangle

    r = twtt*approx_sv_mean/2
    r_0 = np.min(r, axis=1)
    r_0_array = np.ones(np.shape(r))
    for q in range(len(r_0)):
        r_0_array[q] = r_0_array[q]*r_0[q]

    sectors = data['ping']['txsector_beam']
    frequencies = data['ping']['frequency']
    f_nominals = [12000, 30000, 100000, 200000, 300000, 400000] #EM124, EM304, EM712, EM2040 200 kHz, EM2040 300 kHz, EM2040 400 kHz
    f_nominal = f_nominals[np.argmin(np.abs(f_nominals - np.mean(frequencies)))] #finds the closest value to the mean of all freqnecies across the sectors.
    # f_nominal = 100000 #100 kHz for the EM712. Should add routine to check sonar type and automatically id nominal freq


    tau = data['ping']['pulselength']*0.375 #effective pulse length datagram not included in Kluster. Using this shortcut to get the Tau_eff
    omega_tx_nom = float(data['installation_params']['installation_settings'][0]['transducer_1_sounding_size_deg'])
    omega_rx_nom = float(data['installation_params']['installation_settings'][0]['transducer_2_sounding_size_deg'])

    #For TX should it be rx beam pointing angle or tx tile angle?
    # Is it possible that the pulse is to short for the depth and the entire swath is in the A_PL regime?
    omega_tx = omega_tx_nom*f_nominal/(frequencies*np.cos(tx_angle*3.14/180))*3.14/180
    omega_rx = omega_rx_nom*f_nominal/(frequencies*np.cos(beam_angle*3.14/180))*3.14/180

    absorption = data['ping']['absorption']

    a_bl = omega_rx*omega_tx*(r**2)
    a_pl = approx_sv_mean*tau*omega_tx*r/(2*np.sqrt(1-(r_0_array/r)**2))
    eac = 5*np.log10((a_bl**(-2)+a_pl**(-2))**(-1))

    #this is user selected, according to the kmall datagram guide.
    # In the WCD runtimes.Would be preferred to pull this out of the kmall datagrams.
    # Datagram format says its just Xlog(R) where X is user selected. I'm going out on a limb
    spreadingloss_realtime = 30*np.log10(r)+30
    spreadingloss = 40*np.log10(r)
    tl2 = spreadingloss+2*r*absorption/1000


    #Removing Realtime TVG
    # tvg_rt = 30*np.log10(r)+2*r*absorption/1000+10*np.log10(omega_rx*omega_tx*r**2/np.cos(beam_angle*3.14/180))
    # tvg_rt = 30 * np.log10(r) + 2 * r * absorption / 1000 + 10 * np.log10(omega_rx * omega_tx/np.cos(beam_angle*3.14/180))
    tvg_rt = 30 * np.log10(r) + 2*r*absorption/1000
    #NOT CLEAR WHAT SHOULD BE HAPPENING HERE

    # tvg_rt = 30*np.log10(r)+2*r*absorption/1000
    tvg_improved = 40*np.log10(r)+2*r*absorption/1000-eac



    #NNIC & Lambertian Corrections
    #NNIC
    nnic_pings = np.ones(np.shape(r))
    nnic_pings[:,:] = np.nan
    lc_pings = np.ones(np.shape(r))
    lc_pings[:,:] = np.nan
    crossover_angle_runtime = float(data['runtime_params']['runtime_settings'][0]['Normal incidence corr.']) #added this datagram

    reflectivity_improved = reflectivity - tvg + tvg_improved

    # bs_o_minus_bs_n_list = []
    bs_n_list = []
    bs_o_list = []
    for p in range(num_pings):
        bs_o_1 = reflectivity_improved[p][np.argwhere(beam_angle[p] < crossover_angle_runtime)[0][0]]
        bs_o_2 = reflectivity_improved[p][np.argwhere(beam_angle[p] < -crossover_angle_runtime)[0][0]]
        bs_o = (bs_o_1 + bs_o_2)/2
        bs_o_list.append(bs_o)
        bs_n = reflectivity_improved[p][np.argwhere(r[p] == r_0[p])[0][0]]
        bs_n_list.append(bs_n)
        # bs_o_minus_bs_n_list.append(-(bs_o - bs_n)) #bs_o - bs_n is half of measured value because reflectivity presumably has a NNIC applied twice...

    bs_n = np.mean(bs_n_list)
    bs_o = np.mean(bs_o_list)
    bs_o_minus_bs_n = -(bs_o - bs_n)

    for p in range(num_pings):
        for m in range(len(r[p])):
            if r[p][m] <= r_0[p]:
                nnic_pings[p,m] = bs_o_minus_bs_n
                lc_pings[p,m] = 0
            elif r[p][m] <= (r_0[p]*(np.cos(crossover_angle_runtime*3.14/180)**(-1))):
                # nnic_pings[p,m] = (bs_o_minus_bs_n)*(1-np.sqrt((r[p][m]-r_0[p])/(r_0[p]*np.cos(crossover_angle_runtime*3.14/180)**(-1)-r_0[p])))
                # nnic_pings[p, m] = (bs_o_minus_bs_n) * (1 - np.sqrt((r_0[p]*(np.cos(beam_angle[p][m]*3.14/180)**(-1) - 1)) / (r_0[p] * np.cos(crossover_angle_runtime * 3.14 / 180) ** (-1) - r_0[p])))
                nnic_pings[p, m] = (bs_o_minus_bs_n) * (1 - np.sqrt((np.cos(beam_angle[p][m]*3.14/180)**(-1) - 1) / (np.cos(crossover_angle_runtime * 3.14/180) ** (-1) - 1))) #From eqn 5.6 in Miguel's paper. swapped out R term to be in terms of beam angle (seafloor relative). This also makes R_0 disappear. Results is that NNIC is linear and not jittery due to noise in R.
                # lc_pings[p,m] = 20*np.log10(r_0[p]/r[p][m])
                lc_pings[p,m] = 20*np.log10(np.cos(beam_angle[p][m]*3.14/180)) #again, expressed in terms of beam_angle (seafloor relative).
            elif r[p][m] > (r_0[p]*(np.cos(crossover_angle_runtime*3.14/180)**(-1))):
                nnic_pings[p,m] = 0
                lc_pings[p,m] = 20*np.log10(np.cos(beam_angle[p][m]*3.14/180))



    # If NNIC in TVG is flipped...
    # TVGkmall should be TVG = NNIC + LC - EAC + 2TL
    # If sign is flipped: TVG = -NNIC + LC - EAC + 2TL
    # Then BS = Ref1 - NNIC - LC + EAC - 2TL - EAC + 2TL is actually
    # BS = Ref1 + NNIC - LC + EAC - 2TL - EAC + 2TL - 2*NNIC
    # BS = Ref1 - TVG - EAC + 2TL - 2*NNIC
    # backscatter = reflectivity - tvg - eac + tl2 + 2*nnic_pings
    # The above line makes for a coherent picture, but I don't think it actually makes a difference for the cal.

    # backscatter = reflectivity - tvg - eac + tl2

    reflectivity_final = reflectivity_improved - nnic_pings - lc_pings

    reflectivity_binned = np.empty((num_pings, len(beam_angle_bins)))
    reflectivity_binned[:,:] = np.nan
    sectors_binned = np.empty((num_pings, len(beam_angle_bins)))
    sectors_binned[:,:] = np.nan
    r_binned = np.empty((num_pings, len(beam_angle_bins)))
    r_binned[:,:] = np.nan

    for n in range(num_pings):
        if frequencies[n][0] != frequencies[0][0]:
            sectors[n] = sectors[n] + 3*np.ones(len(sectors[n]))

        beam_angle_ping = beampointingangle[n]
        reflectivity_ping = reflectivity_final[n]
        bin_ind1 = np.nonzero(np.nanmax(beam_angle_ping) > beam_angle_bins)[0][1]
        bin_ind2 = np.nonzero(np.nanmin(beam_angle_ping) > beam_angle_bins)[0][0]
        reflectivity_binned[n, bin_ind1:bin_ind2] = interp1d(beam_angle_ping, reflectivity_ping)(beam_angle_bins[bin_ind1:bin_ind2])
        sectors_binned[n, bin_ind1:bin_ind2] = np.round(interp1d(beam_angle_ping, sectors[n])(beam_angle_bins[bin_ind1:bin_ind2]))
        r_binned[n, bin_ind1:bin_ind2] = interp1d(beam_angle_ping, r[n])(beam_angle_bins[bin_ind1:bin_ind2])

    r_binned_mean = np.mean(r_binned, axis=0)
    reflectivity_mean = np.mean(reflectivity_binned, axis=0)
    sector_list = np.unique(sectors_binned)
    sector_list = sector_list[np.isnan(sector_list)==False]
    num_sectors = int(np.nanmax(sector_list)+1)
    num_pings = np.shape(reflectivity_binned)[0]
    num_bins = np.shape(reflectivity_binned)[1]
    reflectivity_sectorized = np.empty((num_sectors, num_pings, num_bins))
    reflectivity_sectorized[:, :, :] = np.nan

    for sector in range(num_sectors):
            for ping in range(num_pings):
                for bin in range(num_bins):
                    if sectors_binned[ping, bin] == sector:
                        reflectivity_sectorized[sector, ping, bin] = reflectivity_binned[ping, bin]
                    else:
                        reflectivity_sectorized[sector, ping, bin] = np.nan

    results = {'mode': mode,
               'reflectivity_binned': reflectivity_binned,
               'reflectivity_sectorized': reflectivity_sectorized,
               'sectors_binned': sectors_binned,
               'r_binned_mean': r_binned_mean,
               'beam_angle_bins': beam_angle_bins}

    return data, results


def package_backscatter_from_dir(fle_dir):
    fles = glob.glob(fle_dir+'\\*.kmall')
    print(str(len(fles)) + ' files to process.')
    data_dict = {}
    results_dict = {}

    for fle in fles:
        print('Processing ' + fle)
        data, results = prepare_backscatter_for_file(fle)
        data_dict[fle] = data
        results_dict[fle] = results

    return data_dict, results_dict


def backscatter_correction(results_dict):
    fles = list(results_dict.keys())
    modes = []
    bs_scalar_means = []
    beam_angle_bins = results_dict[fles[0]]['beam_angle_bins']

    #Determining all the modes in the directory
    for fle in fles:
        mode = results_dict[fle]['mode']
        bs_scalar_means.append(np.nanmean(results_dict[fle]['reflectivity_binned']))
        modes.append(mode)
    modes = list(set(modes))

    bs_scalar_mean = np.mean(bs_scalar_means)
    #populating a dictionary with key equal to mode and value equal to associated files
    mode_dict = {}
    for mode in modes:
        fle_mode = []
        for fle in fles:
            if results_dict[fle]['mode'] == mode:
                fle_mode.append(fle)
        mode_dict[mode] = {'files': fle_mode}

    for mode in modes:
        fles = mode_dict[mode]['files']
        if len(fles) != 2:
            print('Found '+ str(len(fles)) + ' files. Did not find 2 files for ' + mode + ' mode. Cannot complete calibration. Check directory.')

        bs_1 = results_dict[fles[0]]['reflectivity_sectorized']
        bs_2 = results_dict[fles[1]]['reflectivity_sectorized']
        bs_1_mean = np.nanmean(bs_1, axis=1)
        bs_2_mean = np.nanmean(bs_2, axis=1)
        bs_mean = (bs_1_mean+bs_2_mean)/2
        bs_mean_corr = bs_mean #corrected mean backscatter for a given mode as a function of transducer relative angle
        bs_corr = bs_mean_corr - bs_scalar_mean
        mode_dict[mode]['bs_corr'] = bs_corr
        mode_dict[mode]['beam_angle_bins'] = beam_angle_bins

    return mode_dict

def make_calib_file(mode_dict, nadir_mask_angle):
    port_sector_angles = np.flipud(np.arange(30,81,1))
    center_sector_angles = np.flipud(np.arange(-50,51,1))
    stbd_sector_angles = np.flipud(np.arange(-80,-29,1))


    calib_angles = {'Very shallow': [np.flipud(np.arange(30,81,1)), np.flipud(np.arange(-50,51,1)), np.flipud(np.arange(-80,-29,1))],
              'Shallow': [np.flipud(np.arange(30,81,1)), np.flipud(np.arange(-50,51,1)), np.flipud(np.arange(-80,-29,1))],
              'Medium': [np.flipud(np.arange(30,81,1)), np.flipud(np.arange(-50,51,1)), np.flipud(np.arange(-80,-29,1))],
              'Deep': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)), np.flipud(np.arange(-80, -29, 1))],
              'Very deep': [np.flipud(np.arange(20, 71, 1)), np.flipud(np.arange(-40, 41, 1)), np.flipud(np.arange(-70, -19, 1))],
              'Extra deep': [np.flipud(np.arange(20, 61, 1)), np.flipud(np.arange(-40, 41, 1)), np.flipud(np.arange(-60, -19, 1))]
              }

    modes = mode_dict.keys()

    calib_file_dict = {}

    for mode in modes:
        calib_vals = mode_dict[mode]['bs_corr']*(-1) #Flipping per JHC advice
        sector_num = calib_vals.shape[0]
        calib_file_dict[mode] = {}

        for sector in range(sector_num):
            angle_bins = mode_dict[mode]['beam_angle_bins']
            bs_corr = mode_dict[mode]['bs_corr'][sector]
            bs_corr = bs_corr.round(2)
            calib_angles_sector = calib_angles[mode][int(np.remainder(sector,sector_num/2))]
            # bs_corr_sector = np.interp(calib_angles_sector, angle_bins, bs_corr)
            bs_corr_sector = np.ones(len(calib_angles_sector))
            bs_corr_sector[:] = np.nan
            for n in range(len(calib_angles_sector)):
                bs_corr_sector[n] = bs_corr[np.argwhere(angle_bins == calib_angles_sector[n])]
            if True in np.isnan(bs_corr_sector):
                ind1 = np.argwhere(np.isnan(bs_corr_sector) == False)[0][0]
                ind2 = np.argwhere(np.isnan(bs_corr_sector) == False)[-1][0]
                bs_corr_sector[:ind1] = bs_corr_sector[ind1+1]
                bs_corr_sector[ind2+1:] = bs_corr_sector[ind2]
            if 0 in calib_angles_sector:
                ind1 = np.argwhere(calib_angles_sector == nadir_mask_angle)[0][0]
                ind2 = np.argwhere(calib_angles_sector == -nadir_mask_angle)[0][0]
                bs_corr_sector[ind1:ind2] = np.interp(calib_angles_sector[ind1:ind2],
                                                      [calib_angles_sector[ind2], calib_angles_sector[ind1]],
                                                      [bs_corr_sector[ind2], bs_corr_sector[ind1]])

            calib_file_dict[mode][sector] = {'calib_angles': calib_angles_sector, 'bs_corr': bs_corr_sector}

    return calib_file_dict

def lc_nnic_improved(results_dict, crossover_angle):
    fles = list(results_dict.keys())
    beam_angle_bins = results_dict[fles[0]][5]['beam_angle_bins']
    backscatter_mean_arr = np.empty((len(fles), len(beam_angle_bins)))
    backscatter_mean_arr[:, :] = np.nan
    r_binned_mean_arr = np.empty((len(fles), len(beam_angle_bins)))
    r_binned_mean_arr[:, :] = np.nan
    for n in range(len(fles)):
        fle = fles[n]
        backscatter_binned = results_dict[fle][1]['backscatter_binned']
        backscatter_mean_fle = np.nanmean(backscatter_binned, axis=0)
        backscatter_mean_arr[n,:] = backscatter_mean_fle
        r_binned_mean_fle = results_dict[fle][4]['r_binned_mean']
        r_binned_mean_arr[n,:] = r_binned_mean_fle
    backscatter_mean = np.nanmean(backscatter_mean_arr, axis=0)
    r_mean = np.nanmean(r_binned_mean_arr, axis=0)

    r = r_mean
    nnic = np.empty(len(r))
    nnic[:] = np.nan
    lc = np.empty(len(r))
    lc[:] = np.nan
    # Flat seafloor assumption
    r_0 = np.nanmin(r)
    bs_o_1 = backscatter_mean[np.argwhere(beam_angle_bins < crossover_angle)[0][0]]
    bs_o_2 = backscatter_mean[np.argwhere(beam_angle_bins < -crossover_angle)[0][0]]
    bs_o = np.mean((bs_o_1, bs_o_2))
    bs_n = backscatter_mean[np.argwhere(r == r_0)][0][0]

    for m in range(len(r)):
        if r[m] <= r_0:
            nnic[m] = -(bs_o - bs_n)
            lc[m] = 0
        elif r[m] <= (r_0 * (np.cos(crossover_angle * 3.14 / 180) ** (-1))):
            nnic[m] = -(bs_o - bs_n) * (1 - np.sqrt((r[m] - r_0) / (r_0 * np.cos(crossover_angle * 3.14 / 180) ** (
                -1) - r_0)))  # negative sign is not there in paper.
            lc[m] = 20 * np.log10(r_0 / r[m])
        elif r[m] > (r_0 * (np.cos(crossover_angle * 3.14 / 180) ** (-1))):
            nnic[m] = 0
            lc[m] = 20 * np.log10(r_0 / r[m])

    lc_nnic = lc + nnic
    bs_scalar_mean = np.nanmean(lc_nnic+bs_n)
    #This constructs a single lc_nnic as a function of angle. Is this legit?
    #This means there is no compensation for roll. Is the lc_nnic transducer relative or seafloor relative. Is the roll sufficiently small that it doesn't matter?

    return lc_nnic, bs_scalar_mean

def plot_curves(mode_dict):
    modes = list(mode_dict.keys())
    plt.ioff()
    fig, ax = plt.subplots(len(modes),1)
    for n in range(len(modes)):
        mode = modes[n]
        bs_corr = mode_dict[mode]['bs_corr']
        beam_angle_bins = mode_dict[mode]['beam_angle_bins']
        for m in range(np.shape(bs_corr)[0]):
            ax[n].plot(-beam_angle_bins, bs_corr[m,:]) #Beam angle bins are negative because kongsberg indexes backwards. Port side is positive, stbd side is negative.
    plt.show()

def plot_calib_file(calib_file_dict):
    modes = list(calib_file_dict.keys())
    plt.ioff()
    fig, ax = plt.subplots(len(modes),1)
    for n in range(len(modes)):
        mode = modes[n]
        sectors = calib_file_dict[mode].keys()
        for sector in sectors:
            ax[n].plot(calib_file_dict[mode][sector]['calib_angles'], calib_file_dict[mode][sector]['bs_corr'])
    plt.show()

if __name__ == "__main__":

    #Example of running bs_evaluation on a directory with .kmall data.
    # fle_dir = r'D:\Backscatter\OPR-O392-FA-23\H13776'
    # create_raw_bs_evaluation(fle_dir)

    fle = r"D:\Fairweather\FA_2023_Backscatter_Calibration\100m Data Test 1\MBES\0002_20230404_101731.kmall"
    # data, results = prepare_backscatter_for_file(fle)
    fle_dir = r'D:\Fairweather\FA_2023_Backscatter_Calibration\100m Data Test 1\MBES\Test'
    data_dict, results_dict = package_backscatter_from_dir(fle_dir)
    mode_dir = backscatter_correction(results_dict)

    print('Complete')
