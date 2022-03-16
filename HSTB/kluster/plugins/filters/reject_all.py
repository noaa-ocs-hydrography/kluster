import numpy as np

from HSTB.kluster.modules.filter import BaseFilter
from HSTB.kluster import kluster_variables


class Filter(BaseFilter):
    def __init__(self, fqpr, selected_index=None):
        super().__init__(fqpr, selected_index)
        self.controls = []
        self.description = 'Reject all soundings.'

    def _run_algorithm(self):
        print(f'Running reject_all on {self.fqpr.output_folder}')
        self.new_status = []  # new_status will be a list where each element is a 2d array of new detectioninfo (sounding flag) values
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):  # for each sonar head...
            # build a new array of all rejected flags, same size as existing detectioninfo
            rp_detect = np.full(rp['detectioninfo'].shape, kluster_variables.rejected_flag, dtype=rp['detectioninfo'].dtype)
            # new status will be a list of arrays, one for each sonar head.
            self.new_status.append(rp_detect)
        print(f'reject_all complete')
