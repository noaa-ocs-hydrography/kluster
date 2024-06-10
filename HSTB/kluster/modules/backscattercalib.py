import datetime as dt
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
import pickle


# Calibration

# Next up: Get this thing to run on a whole directory and sort out modes. The dict is key.
# Also experience with removing the 2*nnic from the backscatter line. Maybe mess around with EAC again (tile angle?)

# Processing Steps Summary
# KM = Kongsberg Maritime, TVG = Time Varying Gain, NNIC = Near Nadir Insidence Correction, LC = Lambertian Correciton
# EAC = Ensonified area correction, TL = Transmission Loss, CA = Crossover Angle

# Stage 1:   Remove KM Special TVG from reflectivity 2
#           TVG_kmall = NNIC + LC - EAC + 2TL
# Stage 2:   Add back -EAC and +2TL
#           If NNIC in TVG is proper sign.
#           BS = Ref1 - TVG - EAC + 2TL
#           If NNIC in TVG is flipped...
#           BS = Ref1 - TVG - EAC + 2TL - 2*NNIC
# Stage 3:   Bin everything into increments of equal angle. Store this as Sb_noTVG and use it for later corrections
# Stage 4:   Fit a new NNIC + LC curve to the dataset, based on the actual observed CA.
#           Resulting data should be flattened with an improved TVG
# Stage 5:   Determine a single average dB of the flattened BS.
# Stage 6:   On a sector-by-sector, ping-by-ping basis, determine the difference between the average and the measured
#           intensity value for a given angle. Then average along each angle bin.
# Stage 7:   Assemble into a calibration file for PU Upload.

def package_backscatter_from_dir(fle_dir):
    fles = glob.glob(fle_dir + '\\*.kmall')
    print(str(len(fles)) + ' files to process.')
    data_dict = {}
    results_dict = {}

    for fle in fles:
        print('Processing ' + fle)
        data, results = prepare_backscatter_for_file(fle)
        data_dict[fle] = data
        results_dict[fle] = results

    return data_dict, results_dict

def prepare_backscatter_for_file(fle):
    data = sequential_read_multibeam(fle)
    mode = data['runtime_params']['runtime_settings'][0]['Depth setting']
    # data_dict[fle] = [{'mode': mode}, {'data': data}]
    approx_sv_mean = 1500
    reflectivity2 = data['ping']['reflectivity']
    # reflectivity = reflectivity2

    #7JUN2024 Need to add routine to check for corrupt pings and remove them

    # Adding Hacky Reflectivity1 Reader in here
    data = read_reflectivity1(fle, data)
    reflectivity = data['ping']['reflectivity1']

    beampointingangle = data['ping']['beampointingangle']
    beam_angle_bins = np.arange(-80, 81, 1) * (-1)
    num_pings = np.shape(reflectivity)[0]
    tvg = data['ping']['tvg']
    twtt = data['ping']['traveltime']
    time_tx = data['ping']['time']
    time_tx = time_tx[:, None] * np.ones(twtt.shape)
    time_rx = twtt + time_tx
    roll = data['attitude']['roll']
    pitch = data['attitude']['pitch']
    time = data['attitude']['time']
    roll_rx = np.interp(time_rx, time, roll)
    beam_angle = roll_rx + beampointingangle

    tiltangle = data['ping']['tiltangle']
    pitch_tx = np.interp(time_tx, time, pitch)
    tx_angle = pitch_tx + tiltangle

    r = twtt * approx_sv_mean / 2
    r_0 = []
    for p in range(len(r)):
        r_ping = r[p]
        r_0_ping = np.min(r_ping[r_ping > 10])
        r_0.append(r_0_ping)
    r_0 = np.array(r_0)

    r_0 = np.min(r, axis=1)
    r_0_array = np.ones(np.shape(r))
    for q in range(len(r_0)):
        r_0_array[q] = r_0_array[q] * r_0[q]

    sectors = data['ping']['txsector_beam']
    frequencies = data['ping']['frequency']
    f_nominals = [12000, 30000, 100000, 200000, 300000,
                  400000]  # EM124, EM304, EM712, EM2040 200 kHz, EM2040 300 kHz, EM2040 400 kHz
    f_nominal = f_nominals[np.argmin(np.abs(f_nominals - np.mean(
        frequencies)))]  # finds the closest value to the mean of all freqnecies across the sectors.
    # f_nominal = 100000 #100 kHz for the EM712. Should add routine to check sonar type and automatically id nominal freq

    tau = data['ping'][
              'pulselength'] * 0.375  # effective pulse length datagram not included in Kluster. Using this shortcut to get the Tau_eff
    omega_tx_nom = float(data['installation_params']['installation_settings'][0]['transducer_1_sounding_size_deg'])
    omega_rx_nom = float(data['installation_params']['installation_settings'][0]['transducer_2_sounding_size_deg'])

    # For TX should it be rx beam pointing angle or tx tile angle?
    # Is it possible that the pulse is to short for the depth and the entire swath is in the A_PL regime?
    omega_tx = omega_tx_nom * f_nominal / (frequencies * np.cos(tx_angle * 3.14 / 180)) * 3.14 / 180
    omega_rx = omega_rx_nom * f_nominal / (frequencies * np.cos(beam_angle * 3.14 / 180)) * 3.14 / 180

    absorption = data['ping']['absorption']

    a_bl = omega_rx * omega_tx * (r ** 2)
    a_pl = approx_sv_mean * tau * omega_tx * r / (2 * np.sqrt(1 - (r_0_array / r) ** 2))
    eac = 5 * np.log10((a_bl ** (-2) + a_pl ** (-2)) ** (-1))

    # this is user selected, according to the kmall datagram guide.
    # In the WCD runtimes.Would be preferred to pull this out of the kmall datagrams.
    # Datagram format says its just Xlog(R) where X is user selected. I'm going out on a limb
    spreadingloss_realtime = 30 * np.log10(r) + 30
    spreadingloss = 40 * np.log10(r)
    tl2 = spreadingloss + 2 * r * absorption / 1000

    # Removing Realtime TVG
    # tvg_rt = 30*np.log10(r)+2*r*absorption/1000+10*np.log10(omega_rx*omega_tx*r**2/np.cos(beam_angle*3.14/180))
    # tvg_rt = 30 * np.log10(r) + 2 * r * absorption / 1000 + 10 * np.log10(omega_rx * omega_tx/np.cos(beam_angle*3.14/180))
    tvg_rt = 30 * np.log10(r) + 2 * r * absorption / 1000
    # NOT CLEAR WHAT SHOULD BE HAPPENING HERE

    # tvg_rt = 30*np.log10(r)+2*r*absorption/1000
    tvg_improved = 40 * np.log10(r) + 2 * r * absorption / 1000 - eac

    # NNIC & Lambertian Corrections
    # NNIC
    nnic_pings = np.ones(np.shape(r))
    nnic_pings[:, :] = np.nan
    lc_pings = np.ones(np.shape(r))
    lc_pings[:, :] = np.nan
    crossover_angle_runtime = float(
        data['runtime_params']['runtime_settings'][0]['Normal incidence corr.'])  # added this datagram

    reflectivity_improved = reflectivity - tvg + tvg_improved

    # bs_o_minus_bs_n_list = []
    bs_n_list = []
    bs_o_list = []
    for p in range(num_pings):
        bs_o_1 = reflectivity_improved[p][np.argwhere(beam_angle[p] < crossover_angle_runtime)[0][0]]
        bs_o_2 = reflectivity_improved[p][np.argwhere(beam_angle[p] < -crossover_angle_runtime)[0][0]]
        bs_o = (bs_o_1 + bs_o_2) / 2
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
                nnic_pings[p, m] = bs_o_minus_bs_n
                lc_pings[p, m] = 0
            elif r[p][m] <= (r_0[p] * (np.cos(crossover_angle_runtime * 3.14 / 180) ** (-1))):
                # nnic_pings[p,m] = (bs_o_minus_bs_n)*(1-np.sqrt((r[p][m]-r_0[p])/(r_0[p]*np.cos(crossover_angle_runtime*3.14/180)**(-1)-r_0[p])))
                # nnic_pings[p, m] = (bs_o_minus_bs_n) * (1 - np.sqrt((r_0[p]*(np.cos(beam_angle[p][m]*3.14/180)**(-1) - 1)) / (r_0[p] * np.cos(crossover_angle_runtime * 3.14 / 180) ** (-1) - r_0[p])))
                nnic_pings[p, m] = (bs_o_minus_bs_n) * (1 - np.sqrt(
                    (np.cos(beam_angle[p][m] * 3.14 / 180) ** (-1) - 1) / (
                                np.cos(crossover_angle_runtime * 3.14 / 180) ** (
                            -1) - 1)))  # From eqn 5.6 in Miguel's paper. swapped out R term to be in terms of beam angle (seafloor relative). This also makes R_0 disappear. Results is that NNIC is linear and not jittery due to noise in R.
                # lc_pings[p,m] = 20*np.log10(r_0[p]/r[p][m])
                lc_pings[p, m] = 20 * np.log10(np.cos(
                    beam_angle[p][m] * 3.14 / 180))  # again, expressed in terms of beam_angle (seafloor relative).
            elif r[p][m] > (r_0[p] * (np.cos(crossover_angle_runtime * 3.14 / 180) ** (-1))):
                nnic_pings[p, m] = 0
                lc_pings[p, m] = 20 * np.log10(np.cos(beam_angle[p][m] * 3.14 / 180))

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
    reflectivity_binned[:, :] = np.nan
    sectors_binned = np.empty((num_pings, len(beam_angle_bins)))
    sectors_binned[:, :] = np.nan
    r_binned = np.empty((num_pings, len(beam_angle_bins)))
    r_binned[:, :] = np.nan

    for n in range(num_pings):
        if frequencies[n][0] != frequencies[0][0]:
            sectors[n] = sectors[n] + 3 * np.ones(len(sectors[n]))

        beam_angle_ping = beampointingangle[n]
        reflectivity_ping = reflectivity_final[n]
        bin_ind1 = np.nonzero(np.nanmax(beam_angle_ping) > beam_angle_bins)[0][1]
        bin_ind2 = np.nonzero(np.nanmin(beam_angle_ping) > beam_angle_bins)[0][0]
        reflectivity_binned[n, bin_ind1:bin_ind2] = interp1d(beam_angle_ping, reflectivity_ping)(
            beam_angle_bins[bin_ind1:bin_ind2])
        sectors_binned[n, bin_ind1:bin_ind2] = np.round(
            interp1d(beam_angle_ping, sectors[n])(beam_angle_bins[bin_ind1:bin_ind2]))
        r_binned[n, bin_ind1:bin_ind2] = interp1d(beam_angle_ping, r[n])(beam_angle_bins[bin_ind1:bin_ind2])

    r_binned_mean = np.mean(r_binned, axis=0)
    reflectivity_mean = np.mean(reflectivity_binned, axis=0)
    sector_list = np.unique(sectors_binned)
    sector_list = sector_list[np.isnan(sector_list) == False]
    num_sectors = int(np.nanmax(sector_list) + 1)
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


def read_reflectivity1(fle, data):
    km = kmall.kmall(fle)
    reflectivity1 = []
    bs_oblique = []
    bs_normal = []
    # seabedimage_snippets = []
    # seabedimage_start = []
    # seabedimage_numsamples = []
    # seabedimage_centersample = []
    expected_num_beams = 400 #True for 712
    while not km.eof:
        km.decode_datagram()
        if km.datagram_ident != 'MRZ':
            km.skip_datagram()
        else:
            km.read_datagram()
            ref1_ping = km.datagram_data['sounding']['reflectivity1_dB']
            bs_oblique_ping = km.datagram_data['rxInfo']['BSoblique_dB']
            bs_normal_ping = km.datagram_data['rxInfo']['BSnormal_dB']
            # seabedimage_snippets_ping = km.datagram_data['SIsample_desidB']  # snippets
            # seabedimage_start_ping = km.datagram_data['sounding']['SIstartRange_samples']
            # seabedimage_numsamples_ping = km.datagram_data['sounding']['SInumSamples']
            # seabedimage_centersample_ping = km.datagram_data['sounding']['SIcentreSample']

            if len(ref1_ping) != expected_num_beams:
                ref1_ping = ref1_ping + [np.nan] * (expected_num_beams - len(ref1_ping))  # some pings have 256 pings, possibly due to bug in SIS. This pads it to make sure total beam number is same for each ping in whole array
                print('Warning: Expected ' + str(expected_num_beams) + ' beams. Ping only has ' + str(
                    len(ref1_ping)) + ' beams.')

            reflectivity1.append(ref1_ping)
            bs_oblique.append(bs_oblique_ping)
            bs_normal.append(bs_normal_ping)
            # seabedimage_snippets.append(seabedimage_snippets_ping)
            # seabedimage_start.append(seabedimage_start_ping)
            # seabedimage_numsamples.append(seabedimage_numsamples_ping)
            # seabedimage_centersample.append(seabedimage_centersample_ping)

    # Seabed Image Averaging
    # num_pings = len(bs_oblique)
    # num_beams = len(seabedimage_numsamples[0])
    # seabedimage = np.zeros((num_pings, num_beams))
    # seabedimage[:] = np.nan
    # seabedimage_center = np.zeros((num_pings, num_beams))
    # seabedimage_center[:] = np.nan
    # for p in range(num_pings):
    #     for b in range(num_beams):
    #         snippet_beam = seabedimage_snippets[p][
    #                        seabedimage_start[p][b]:seabedimage_start[p][b] + seabedimage_numsamples[p][b]]
    #         snippet_beam = np.array(snippet_beam) / 10
    #         reflectivity_sb_beam = 10 * np.log10(1 / seabedimage_numsamples[p][b] * np.sum(10 ** (snippet_beam / 10)))
    #         seabedimage[p, b] = reflectivity_sb_beam
    #         seabedimage_center[p, b] = seabedimage_snippets[p][seabedimage_centersample[p][b]]

    data['ping']['reflectivity1'] = reflectivity1
    return data
def backscatter_correction(results_dict):
    fles = list(results_dict.keys())
    modes = []
    bs_scalar_means = []
    beam_angle_bins = results_dict[fles[0]]['beam_angle_bins']

    # Determining all the modes in the directory
    for fle in fles:
        mode = results_dict[fle]['mode']
        # if mode == 'Medium':
        #     bs_scalar_means.append(np.nanmean(results_dict[fle]['reflectivity_binned']))
        modes.append(mode)
    modes = list(set(modes))

    # bs_scalar_mean = np.mean(bs_scalar_means)
    # bs_scalar_mean, medium mode 100m, -5.87 dB

    # populating a dictionary with key equal to mode and value equal to associated files
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
            print('Found ' + str(
                len(fles)) + ' files. Did not find 2 files for ' + mode + ' mode. Cannot complete calibration. Check directory.')

        bs_1 = results_dict[fles[0]]['reflectivity_sectorized']
        bs_2 = results_dict[fles[1]]['reflectivity_sectorized']
        bs_1_mean = np.nanmean(bs_1, axis=1)
        bs_2_mean = np.nanmean(bs_2, axis=1)
        bs_mean = (bs_1_mean + bs_2_mean) / 2
        bs_mean_corr = bs_mean  # corrected mean backscatter for a given mode as a function of transducer relative angle
        bs_scalar_mean = np.nanmean(
            bs_mean_corr)  # subtracting the average of the overall BS, to center each beam pattern at zero.
        bs_corr = bs_mean_corr  # This is effectively where the "sign" of the bs_cal is set. It's opposite of expectation, confirmed with JHC testing.
        mode_dict[mode]['bs_corr'] = bs_corr
        mode_dict[mode]['beam_angle_bins'] = beam_angle_bins

    return mode_dict

def make_calib_file(mode_dict, nadir_mask_angle_pos, nadir_mask_angle_neg):
    port_sector_angles = np.flipud(np.arange(30, 81, 1))
    center_sector_angles = np.flipud(np.arange(-50, 51, 1))
    stbd_sector_angles = np.flipud(np.arange(-80, -29, 1))

    calib_angles = {'Very shallow': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                                     np.flipud(np.arange(-80, -29, 1))],
                    'Shallow': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                                np.flipud(np.arange(-80, -29, 1))],
                    'Medium': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                               np.flipud(np.arange(-80, -29, 1))],
                    'Deep': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                             np.flipud(np.arange(-80, -29, 1))],
                    'Very deep': [np.flipud(np.arange(20, 71, 1)), np.flipud(np.arange(-40, 41, 1)),
                                  np.flipud(np.arange(-70, -19, 1))],
                    'Extra deep': [np.flipud(np.arange(20, 61, 1)), np.flipud(np.arange(-40, 41, 1)),
                                   np.flipud(np.arange(-60, -19, 1))]
                    }

    modes = mode_dict.keys()

    calib_file_dict = {}

    for mode in modes:
        # calib_vals = mode_dict[mode]['bs_corr']*(-1) #Flipping per JHC advice
        sector_num = mode_dict[mode]['bs_corr'].shape[0]
        calib_file_dict[mode] = {}
        mode_bs_scalar_mean = []
        for sector in range(sector_num):
            angle_bins = mode_dict[mode]['beam_angle_bins']
            bs_corr = mode_dict[mode]['bs_corr'][sector]
            bs_corr = bs_corr.round(2)
            if sector_num == 6:
                calib_angles_sector = calib_angles[mode][int(np.remainder(sector, sector_num / 2))]
            elif sector_num == 3:
                calib_angles_sector = calib_angles[mode][sector]
            else:
                print(
                    'Number of sectors is not equal to 3 or 6. Maybe you are using a 304? Or some other system? Either way it is not supported at this time')
            # bs_corr_sector = np.interp(calib_angles_sector, angle_bins, bs_corr)
            bs_corr_sector = np.ones(len(calib_angles_sector))
            bs_corr_sector[:] = np.nan
            for n in range(len(calib_angles_sector)):
                bs_corr_sector[n] = bs_corr[np.argwhere(angle_bins == calib_angles_sector[n])]
            if True in np.isnan(bs_corr_sector) and False in np.isnan(bs_corr_sector):
                ind1 = np.argwhere(np.isnan(bs_corr_sector) == False)[0][0]
                ind2 = np.argwhere(np.isnan(bs_corr_sector) == False)[-1][0]
                bs_corr_sector[:ind1] = bs_corr_sector[ind1]
                bs_corr_sector[ind2:] = bs_corr_sector[ind2]
            if 0 in calib_angles_sector:
                ind1 = np.argwhere(calib_angles_sector == nadir_mask_angle_pos)[0][0]
                ind2 = np.argwhere(calib_angles_sector == nadir_mask_angle_neg)[0][0]
                bs_corr_sector[ind1:ind2] = np.round(np.interp(calib_angles_sector[ind1:ind2],
                                                               [calib_angles_sector[ind2], calib_angles_sector[ind1]],
                                                               [bs_corr_sector[ind2], bs_corr_sector[ind1]]), 2)

            calib_file_dict[mode][sector] = {'calib_angles': calib_angles_sector, 'bs_corr': bs_corr_sector}
            mode_bs_scalar_mean.append(np.nanmean(calib_file_dict[mode][sector]['bs_corr']))

        mode_bs_scalar_mean = np.mean(
            mode_bs_scalar_mean)  # Centering each mode at zero, after masking out nadir spike.
        for sector in range(sector_num):
            calib_file_dict[mode][sector]['bs_corr'] = np.round(
                calib_file_dict[mode][sector]['bs_corr'] - mode_bs_scalar_mean, 2)

    return calib_file_dict


def plot_curves(mode_dict):
    modes = list(mode_dict.keys())
    plt.ioff()
    fig, ax = plt.subplots(len(modes), 1)
    for n in range(len(modes)):
        mode = modes[n]
        bs_corr = mode_dict[mode]['bs_corr']
        beam_angle_bins = mode_dict[mode]['beam_angle_bins']
        for m in range(np.shape(bs_corr)[0]):
            ax[n].plot(-beam_angle_bins, bs_corr[m,
                                         :])  # Beam angle bins are negative because kongsberg indexes backwards. Port side is positive, stbd side is negative.
    plt.show()


def plot_calib_file(calib_file_dict):
    modes = list(calib_file_dict.keys())
    plt.ioff()

    for n in range(len(modes)):
        fig, ax = plt.subplots(1, 1)
        mode = modes[n]
        sectors = calib_file_dict[mode].keys()
        for sector in sectors:
            ax.plot(calib_file_dict[mode][sector]['calib_angles'], calib_file_dict[mode][sector]['bs_corr'],
                    label=sector)
        fig.suptitle(mode)
        ax.legend()
        plt.show()


def read_calibtxtfle(fle):
    # fle = r"D:\Fairweather\FA_2023_Backscatter_Calibration\Calib Files\From Kongsberg\Calib712_70_100.txt"
    data = open(fle, 'r')
    lines = data.readlines()
    bscorr_dict = {}
    for n in range(len(lines)):
        line = lines[n]
        if line[-2] == ' ':  # Dealing with the hanging spaces in some of the files.
            line_list = list(line)
            line_list[-2] = ''
            line = ''.join(line_list)
            line = ''.join(line_list)
        if line[0] == '#' and 'Swath' in line:
            # line.split('Swath')[0]
            mode = line.split('Swath')[0]
            if 'Dual' in line and '1' in line:
                swath = '1'
            if 'Dual' in line and '2' in line:
                swath = '2'
            else:
                swath = ''
            if mode not in list(bscorr_dict.keys()):
                bscorr_dict[mode] = {}
        if line[0] == '#' and 'sector' in line:
            sector = line[:-2]
            if len(swath) != 0:
                sector = sector + '_' + swath
            bscorr_dict[mode][sector] = {'angles': [], 'bscorrs': []}
        if line.count(' ') == 1:
            angle = int(line.split(' ')[0])
            bscorr = float(line.split(' ')[1][:-2])
            bscorr_dict[mode][sector]['angles'].append(angle)
            bscorr_dict[mode][sector]['bscorrs'].append(bscorr)

    modes = list(bscorr_dict.keys())
    plt.ioff()

    for n in range(len(modes)):
        fig, ax = plt.subplots(1, 1)
        mode = modes[n]
        sectors = bscorr_dict[mode].keys()
        for sector in sectors:
            ax.plot(bscorr_dict[mode][sector]['angles'], bscorr_dict[mode][sector]['bscorrs'], label=sector)
            ax.legend()
            fig.suptitle(mode)
        plt.show()


def write_calib_files_to_text(calib_file_dict, text_fle_dir):
    modes = list(calib_file_dict.keys())
    for mode in modes:
        fout = text_fle_dir + r'\\' + mode + '.txt'
        fo = open(fout, 'w')
        sectors = calib_file_dict[mode].keys()
        for sector in sectors:
            fo.write('Sector ' + str(sector) + '\n')
            fo.write(str(len(calib_file_dict[mode][sector]['calib_angles'])) + '\n')
            for n in range(len(calib_file_dict[mode][sector]['calib_angles'])):
                fo.write(str(calib_file_dict[mode][sector]['calib_angles'][n]) + ' ' + str(
                    calib_file_dict[mode][sector]['bs_corr'][n]) + '\n')
        fo.close()


def write_bscalib_files_712(calib_file_dict, text_fle_dir):
    calib_file_sections = ['# Very Shallow - Single Swath', '# Very Shallow - Dual Swath 1',
                           '# Very Shallow - Dual Swath 2',
                           '# Shallow - Single Swath', '# Shallow - Dual Swath 1', '# Shallow - Dual Swath 2',
                           '# Medium - Single Swath', '# Medium - Dual Swath 1', '# Medium - Dual Swath 2',
                           '# Deep - Single Swath', '# Deep - Dual Swath 1', '# Deep - Dual Swath 2',
                           '# Very Deep - Single Swath', '# Very Deep - Dual Swath 1, not used',
                           '# Very Deep - Dual Swath 2, not used',
                           '# Extra Deep - Single Swath', '# Extra Deep - Dual Swath 1, not used',
                           '# Extra Deep - Dual Swath 2, not used']

    modes = list(calib_file_dict.keys())
    fout = text_fle_dir + r'\\' + 'Calib712_generated.txt'
    fo = open(fout, 'w')
    for n in range(len(calib_file_sections)):
        calib_file_section = calib_file_sections[n]
        section_mode = calib_file_section.split('-')[0][2:-1]
        section_mode = section_mode[0] + section_mode[1:].lower()

        if section_mode not in calib_file_dict.keys():
            if 'Single' in calib_file_section:
                section_sectors = [0, 1, 2]
                swath_id = '0'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo, set_zeros=True)
            elif 'Dual Swath 1' in calib_file_section:
                section_sectors = [0, 1, 2]
                swath_id = '1'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo, set_zeros=True)
            elif 'Dual Swath 2' in calib_file_section:
                section_sectors = [3, 4, 5]
                swath_id = '2'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo, set_zeros=True)

        else:
            sectors = calib_file_dict[section_mode].keys()
            if 'Single' in calib_file_section and len(sectors) == 3:
                section_sectors = [0, 1, 2]
                swath_id = '0'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo)
            elif 'Single' in calib_file_section and len(sectors) == 6:
                section_sectors = [0, 1, 2]
                swath_id = '0'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo, set_zeros=True)
            elif 'Dual Swath 1' in calib_file_section and len(sectors) == 6:
                section_sectors = [0, 1, 2]
                swath_id = '1'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo)

            elif 'Dual Swath 2' in calib_file_section and len(sectors) == 6:
                section_sectors = [3, 4, 5]
                swath_id = '2'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo)

            elif 'Dual Swath 1' in calib_file_section and len(sectors) == 3:
                section_sectors = [0, 1, 2]
                swath_id = '1'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo, set_zeros=True)
            elif 'Dual Swath 2' in calib_file_section and len(sectors) == 3:
                section_sectors = [3, 4, 5]
                swath_id = '2'
                write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id,
                                         fo, set_zeros=True)
    fo.close()


def write_calib_mode_section(calib_file_dict, section_mode, calib_file_section, section_sectors, swath_id, fo, set_zeros=False):
    sector_section_names = {'0': '# Port sector', '1': '# Cent sector', '2': '# Stb sector',
                            '3': '# Port sector', '4': '# Cent sector', '5': '# Stb sector'}

    calib_angles = {'Very shallow': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                                     np.flipud(np.arange(-80, -29, 1)),
                                     np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                                     np.flipud(np.arange(-80, -29, 1))],
                    'Shallow': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                                np.flipud(np.arange(-80, -29, 1)),
                                np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                                np.flipud(np.arange(-80, -29, 1))],
                    'Medium': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                               np.flipud(np.arange(-80, -29, 1)),
                               np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                               np.flipud(np.arange(-80, -29, 1))],
                    'Deep': [np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                             np.flipud(np.arange(-80, -29, 1)),
                             np.flipud(np.arange(30, 81, 1)), np.flipud(np.arange(-50, 51, 1)),
                             np.flipud(np.arange(-80, -29, 1))],
                    'Very deep': [np.flipud(np.arange(20, 71, 1)), np.flipud(np.arange(-40, 41, 1)),
                                  np.flipud(np.arange(-70, -19, 1)),
                                  np.flipud(np.arange(20, 71, 1)), np.flipud(np.arange(-40, 41, 1)),
                                  np.flipud(np.arange(-70, -19, 1))],
                    'Extra deep': [np.flipud(np.arange(20, 61, 1)), np.flipud(np.arange(-40, 41, 1)),
                                   np.flipud(np.arange(-60, -19, 1)),
                                   np.flipud(np.arange(20, 61, 1)), np.flipud(np.arange(-40, 41, 1)),
                                   np.flipud(np.arange(-60, -19, 1))]
                    }

    mode_ids = {'Very shallow': '1', 'Shallow': '2', 'Medium': '3', 'Deep': '4', 'Very deep': '5', 'Extra deep': '6'}
    mode_id = mode_ids[section_mode]
    # swath_ids = {'[0, 1, 2]': '0', '[3, 4, 5]': '1'}
    # swath_id = swath_ids[str(section_sectors)]
    sector_len = str(len(section_sectors))
    section_id = mode_id + '   ' + swath_id + '   ' + sector_len
    fo.write(calib_file_section + '\n')
    fo.write(section_id + '\n')

    for m in range(len(section_sectors)):
        sector = section_sectors[m]
        sector_section_name = sector_section_names[str(sector)]
        fo.write(sector_section_name + '\n')
        if set_zeros == True:
            fo.write(str(len(calib_angles[section_mode][sector])) + '\n')
            for n in range(len(calib_angles[section_mode][sector])):
                angle_str = str(calib_angles[section_mode][sector][n])
                bs_corr_str = '0.00'
                fo.write(angle_str + ' ' + bs_corr_str + '\n')

        else:
            fo.write(str(len(calib_file_dict[section_mode][sector]['calib_angles'])) + '\n')
            for n in range(len(calib_file_dict[section_mode][sector]['calib_angles'])):
                angle_str = str(calib_file_dict[section_mode][sector]['calib_angles'][n])
                bs_corr_str = str(calib_file_dict[section_mode][sector]['bs_corr'][n])
                if len(bs_corr_str.split('.')[-1]) == 1:
                    bs_corr_str = bs_corr_str + '0'
                fo.write(angle_str + ' ' + bs_corr_str + '\n')


def plot_pre_calib_post_calib(pre_calib_fle, post_calib_fle, mode):
    fles = [pre_calib_fle, post_calib_fle]
    fig, ax = plt.subplots(len(fles), 1)
    for n in range(len(fles)):
        fle = fles[n]
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
        inds_even = np.arange(0, len(reflectivity), 2)
        inds_odd = np.arange(1, len(reflectivity), 2)
        ax[n].plot(np.mean(reflectivity[inds_even], axis=0), label='First Swath')
        ax[n].plot(np.mean(reflectivity[inds_odd], axis=0), label='Second Swath')
        ax[n].legend()
    ax[0].set_ylabel('dB')
    ax[1].set_ylabel('dB')
    ax[1].set_xlabel('Beam Index')
    ax[1].title.set_text('Post-calibration: ' + fle.split(r'\\')[-1])
    ax[0].title.set_text('Pre-calibration: ' + fle.split(r'\\')[-1])
    ax[1].title.set_text('Post-calibration: ' + fle.split(r'\\')[-1])
    fig.suptitle(mode)
    plt.show()

if __name__ == '__main__':
    print('Started processing: ', dt.datetime.now())
    # fle_dir = r'D:\Fairweather\FA_2023_Backscatter_Calibration\100m Data Test 1\MBES\Calibration'
    # data_dict, results_dict = package_backscatter_from_dir(fle_dir)

    # with open(r'C:\Users\samuel.umfress\Documents\HSTB\Backscatter\Calib_Dev_Dump\data.pickle', 'wb') as f:
    #     # Pickle the 'data' dictionary using the highest protocol available.
    #     pickle.dump(results_dict, f, pickle.HIGHEST_PROTOCOL)

    # with open(r'C:\Users\samuel.umfress\Documents\HSTB\Backscatter\Calib_Dev_Dump\data.pickle', 'rb') as f:
    #     # The protocol version used is detected automatically, so we do not
    #     # have to specify it.
    #     results_dict = pickle.load(f)

    fle_dir = r'C:\Users\samuel.umfress\Documents\HSTB\TJ\Backscatter_Calibration\Calibration'
    data_dict, results_dict = package_backscatter_from_dir(fle_dir)
    mode_dict = backscatter_correction(results_dict)
    calib_file_dict = make_calib_file(mode_dict, 15, -15)
    write_bscalib_files_712(calib_file_dict, r'C:\Users\samuel.umfress\Documents\HSTB\TJ\Backscatter_Calibration\Calib_Results')
    # read_calibtxtfle(r"C:\Users\samuel.umfress\Documents\HSTB\Backscatter\Calib_Dev_Dump\Calib712_generated.txt")


    print('Finished processing: ', dt.datetime.now())