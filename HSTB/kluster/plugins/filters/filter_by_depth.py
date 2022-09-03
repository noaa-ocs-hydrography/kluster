import numpy as np

from HSTB.kluster.modules.filter import BaseFilter
from HSTB.kluster import kluster_variables


class Filter(BaseFilter):
    def __init__(self, fqpr, selected_index=None):
        super().__init__(fqpr, selected_index)
        # when running from Kluster GUI, these two controls will pop up after starting the filter to ask for min and max depth
        self.controls = [['float', 'min_depth', 0.0, {'minimum': -100, 'maximum': 99999999, 'singleStep': 0.1}],
                         ['float', 'max_depth', 500.0, {'minimum': -100, 'maximum': 99999999, 'singleStep': 0.1}]]
        self.description = 'Reject all soundings that are greater than maximum depth and less than minimum depth.  Only retain soundings within the given minimum/maximum depth envelope.'

    def _run_algorithm(self, min_depth: float = None, max_depth: float = None):
        # note that min_depth and max_depth match the self.controls name parameters
        if min_depth is None and max_depth is None:
            raise ValueError('filter_by_depth: Filter must have either min or max depth set')
        if min_depth is not None and max_depth is not None and min_depth >= max_depth:
            raise ValueError(f'filter_by_depth: minimum depth {min_depth} cannot be greater than maximum depth {max_depth}')
        if 'z' not in self.fqpr.multibeam.raw_ping[0]:
            raise ValueError(f'filter_by_depth: unable to find georeferenced depths, have you processed this data?')

        print(f'Running filter_by_depth ({min_depth},{max_depth}) on {self.fqpr.output_folder}')
        self.new_status = []  # new_status will be a list where each element is a 2d array of new detectioninfo (sounding flag) values
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):  # for each sonar head...

            # much easier to work in 1dimension, can either go to numpy and flatten (this is more mem efficient / faster)
            rp_detect = rp['detectioninfo'].values.flatten()
            # or you can use xarray stack, which will get you the time/beam of each value, which might be useful, but can use up a lot of memory
            # rp_detect = rp['detectioninfo'].stack({'sounding': ('time', 'beam')})

            # do the same for depth (z)
            rp_depth = rp['z'].values.flatten()
            # now build a blank boolean mask for depth filter
            depth_mask = np.zeros_like(rp_detect, dtype=bool)
            if min_depth:  # set to True where depth is less than minimum depth
                depth_mask = np.logical_or(depth_mask, rp_depth < min_depth)
            if max_depth:  # set to True where depth is greater than maximum depth
                depth_mask = np.logical_or(depth_mask, rp_depth > max_depth)
            # where our mask is True, we set to rejected
            rp_detect[depth_mask] = kluster_variables.rejected_flag
            # reshape our new detectioninfo (to get back to 2d) and append to our new_status attribute
            self.new_status.append(rp_detect.reshape(rp['detectioninfo'].shape))
        print(f'filter_by_depth complete')
