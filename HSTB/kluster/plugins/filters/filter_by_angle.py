import numpy as np

from HSTB.kluster.modules.filter import BaseFilter
from HSTB.kluster import kluster_variables


class Filter(BaseFilter):
    def __init__(self, fqpr, selected_index = None):
        super().__init__(fqpr, selected_index)
        self.controls = [['float', 'min_angle', -45.0, {'minimum': -180, 'maximum': 180, 'singleStep': 0.1}],
                         ['float', 'max_angle', 45.0, {'minimum': -180, 'maximum': 180, 'singleStep': 0.1}]]

    def _run_algorithm(self, min_angle: float = None, max_angle: float = None):
        if min_angle is None and max_angle is None:
            raise ValueError('filter_by_angle: Filter must have either min or max angle set')
        print(f'Running filter_by_angle ({min_angle},{max_angle}) on {self.fqpr.output_folder}')
        self.new_status = []
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):
            rp_detect = rp['detectioninfo'].stack({'sounding': ('time', 'beam')})
            rp_angle = rp['corr_pointing_angle'].stack({'sounding': ('time', 'beam')}).values * (180 / np.pi)
            angle_mask = np.zeros_like(rp_detect, dtype=bool)
            if min_angle:
                angle_mask = np.logical_or(angle_mask, rp_angle < min_angle)
            if max_angle:
                angle_mask = np.logical_or(angle_mask, rp_angle > max_angle)
            rp_detect[angle_mask] = kluster_variables.rejected_flag
            self.new_status.append(rp_detect.unstack().values)
        print(f'filter_by_angle complete')
