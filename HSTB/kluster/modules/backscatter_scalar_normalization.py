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

fle_dir = r'D:\Fairweather\2024 Launch Backscatter Normalization\Calibration'
fles = glob.glob(fle_dir + r'\\**\\*.kmall', recursive=True)
reflectivity_means = {}
print(str(len(fles))+ ' total files to process.')

vessels = []
modes = []
for fle in fles:
    print('Processing ' + fle)
    vessel = fle.split('\\')[-2]
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
    mode_num = km.datagram_data['pingInfo']['depthMode']
    if type(mode_num) == int: #checking that only one mode was used throughout line.
        mode = km.translate_mode_two_tostring(np.array([km.datagram_data['pingInfo']['depthMode']]))[0]
    else:
        print('Potentially multiple modes detected in a single line?')

    vessels.append(vessel)
    modes.append(mode)

    reflectivity_means[fle] = {'vessel': vessel, 'mode': mode, 'reflectivity_mean': np.mean(reflectivity, axis=0), 'reflectivity': reflectivity}

vessels = list(set(vessels))
modes = list(set(modes))

# reflectivity_means_org = {

# for vessel in vessels:
#     for mode in modes:
#         for fle in fles:
#             reflectivity_means_org[vessel][mode][fle] = reflectivity_means[fle]['reflectivity_mean']
#
#
#             if mode == reflectivity_means[fle]['mode'] and vessel == reflectivity_means[fle]['vessel']:
#                 reflectivity_means_org[vessel] = {mode:{fle: reflectivity_means[fle]['reflectivity_mean']}}
scalar_means = {}
for vessel in vessels:
    scalar_means[vessel] = {}
    for mode in modes:
        scalar_means[vessel][mode] = []

for fle in fles:
        vessel = reflectivity_means[fle]['vessel']
        mode = reflectivity_means[fle]['mode']
        scalar_mean = np.mean(reflectivity_means[fle]['reflectivity_mean'][10:-10]) #trimming off outer 10 beam indicies to reduce a bit of noise.
        if len(scalar_means[vessel][mode]) == 0:
            scalar_means[vessel][mode] = [scalar_mean]
        else:
            scalar_means[vessel][mode].append(scalar_mean)

reference_value = np.mean([scalar_means[vessel]['MEm'] for vessel in vessels])

offsets = {}
for vessel in vessels:
    offsets[vessel] = {}
    for mode in modes:
        offsets[vessel][mode] = np.round(np.mean(scalar_means[vessel][mode]) - reference_value, 2)



        # if reflectivity_means[fle]['mode'] == mode:

            # ax[n].plot(reflectivity_means[fle]['reflectivity_mean'], label=reflectivity_means[fle]['vessel'])




fig, ax = plt.subplots(len(modes), 1)
for fle in fles:
    for n in range(len(modes)):
        mode = modes[n]
        if reflectivity_means[fle]['mode'] == mode:
            ax[n].plot(reflectivity_means[fle]['reflectivity_mean'], label=reflectivity_means[fle]['vessel'])
        ax[n].legend()
        ax[n].title.set_text(mode)





# for fle in fles:
#     vessels.append(reflectivity_means[fle]['vessel'])
# vessels = list(set(vessels))
#
# modes = []
#
# for fle in fles:
#     for vessel in vessels:


    # data = sequential_read_multibeam(fle) #This is making this take way longer and is only sensible if adding reflectivity1 to kluster output later on.
    # if vessel in reflectivity_means.keys():
    #     if mode in reflectivity_means[vessel].keys():
    #         reflectivity_means[vessel][mode][fle] = np.mean(reflectivity, axis=0)
    #     else:
    #         reflectivity_means[vessel] = {mode: {}}
    #         reflectivity_means[vessel][mode] = {fle: np.mean(reflectivity, axis=0)}
    # else:
    #     reflectivity_means[vessel] = {mode: {}}
    #     reflectivity_means[vessel][mode] = {fle: np.mean(reflectivity, axis=0)}


print('this partys over')



