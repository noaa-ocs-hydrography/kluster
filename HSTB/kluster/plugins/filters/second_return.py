import numpy as np

from HSTB.kluster.modules.filter import BaseFilter
from HSTB.kluster import kluster_variables


class Filter(BaseFilter):
    def __init__(self, fqpr, selected_index=None):
        super().__init__(fqpr, selected_index)
        self.controls = [['float', 'percent_depth', 70.0, {'minimum': 0, 'maximum': 100, 'singleStep': 1}]]
        self.description = 'Reject all soundings that are greater i.e. deeper than (percent_depth / 100) * 2 * nadir depth in the ping.\n' \
                           'Will compute a rolling minimum of the nadir depths to use as nadir depth in the algorithm.\n' \
                           'The closer the percent depth is to 0%, the more aggressive the filter.  70% is a fairly conservative first try.'

    def _run_algorithm(self, percent_depth: float = None):
        print(f'Running second_return ({percent_depth}) on {self.fqpr.output_folder}')
        window_size = 10
        self.new_status = []  # new_status will be a list where each element is a 2d array of new detectioninfo (sounding flag) values
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
            rp_detect = rp['detectioninfo'].values.flatten()
            nadirbeam = int(rp.z.beam.shape[0] / 2) - 1
            filt_nadir = rp.z.isel(beam=nadirbeam).rolling(time=window_size).min()
            filt_nadir[0:8] = filt_nadir[9]
            filter_mask = (rp.z > ((percent_depth / 100) * 2 * filt_nadir)).values.flatten()
            print(f'second_return: rejecting {np.count_nonzero(filter_mask)} out of {filter_mask.size} points')
            # where our mask is True, we set to rejected
            rp_detect[filter_mask] = kluster_variables.rejected_flag
            # reshape our new detectioninfo (to get back to 2d) and append to our new_status attribute
            self.new_status.append(rp_detect.reshape(rp['detectioninfo'].shape))
        print(f'filter_by_angle complete')
