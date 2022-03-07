import numpy as np

from HSTB.kluster.modules.filter import BaseFilter
from HSTB.kluster import kluster_variables


class Filter(BaseFilter):
    def __init__(self, fqpr, selected_index=None):
        super().__init__(fqpr, selected_index)
        self.controls = []

    def _run_algorithm(self):
        print(f'Running reaccept_rejected on {self.fqpr.output_folder}')
        self.new_status = []  # new_status will be a list where each element is a 2d array of new detectioninfo (sounding flag) values
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):  # for each sonar head...
            # much easier to work in 1dimension, can either go to numpy and flatten (this is more mem efficient / faster)
            rp_detect = rp['detectioninfo'].values.flatten()
            # or you can use xarray stack, which will get you the time/beam of each value, which might be useful, but can use up a lot of memory
            # rp_detect = rp['detectioninfo'].stack({'sounding': ('time', 'beam')})

            # where our mask is True, we set to rejected
            rp_detect[rp_detect == kluster_variables.rejected_flag] = kluster_variables.accepted_flag
            # reshape our new detectioninfo (to get back to 2d) and append to our new_status attribute
            self.new_status.append(rp_detect.reshape(rp['detectioninfo'].shape))
        print(f'reaccept_rejected complete')
