import numpy as np

from HSTB.kluster.modules.filter import BaseFilter
from HSTB.kluster import kluster_variables


class Filter(BaseFilter):
    def __init__(self, fqpr, selected_index=None):
        super().__init__(fqpr, selected_index)
        # when running from Kluster GUI, these two controls will pop up after starting the filter to ask for min and max angle
        self.controls = [['float', 'min_angle', -45.0, {'minimum': -180, 'maximum': 180, 'singleStep': 0.1}],
                         ['float', 'max_angle', 45.0, {'minimum': -180, 'maximum': 180, 'singleStep': 0.1}]]
        self.description = 'Reject all soundings that are greater than maximum beam angle and less than minimum beam angle.  Only retain soundings within the given minimum/maximum beam angle envelope.'

    def _run_algorithm(self, min_angle: float = None, max_angle: float = None):
        # note that min_angle and max_angle match the self.controls name parameters
        if min_angle is None and max_angle is None:
            raise ValueError('filter_by_angle: Filter must have either min or max angle set')
        if min_angle is not None and max_angle is not None and min_angle >= max_angle:
            raise ValueError(f'filter_by_angle: minimum angle {min_angle} cannot be greater than maximum angle {max_angle}')
        if 'corr_pointing_angle' not in self.fqpr.multibeam.raw_ping[0]:
            raise ValueError(f'filter_by_angle: unable to find corrected beam angles, have you processed this data?')

        print(f'Running filter_by_angle ({min_angle},{max_angle}) on {self.fqpr.output_folder}')
        self.new_status = []  # new_status will be a list where each element is a 2d array of new detectioninfo (sounding flag) values
        for cnt, rp in enumerate(self.fqpr.multibeam.raw_ping):  # for each sonar head...

            # much easier to work in 1dimension, can either go to numpy and flatten (this is more mem efficient / faster)
            rp_detect = rp['detectioninfo'].values.flatten()
            # or you can use xarray stack, which will get you the time/beam of each value, which might be useful, but can use up a lot of memory
            # rp_detect = rp['detectioninfo'].stack({'sounding': ('time', 'beam')})

            # do the same for corr_pointing_angle (corrected beam angles), but also convert from radians to degrees
            assert rp.units['corr_pointing_angle'] == 'radians'
            rp_angle = rp['corr_pointing_angle'].values.flatten() * (180 / np.pi)
            # now build a blank boolean mask for corrected beam angle
            angle_mask = np.zeros_like(rp_detect, dtype=bool)
            if min_angle:  # set to True where angle is less than minimum angle
                angle_mask = np.logical_or(angle_mask, rp_angle < min_angle)
            if max_angle:  # set to True where angle is greater than maximum angle
                angle_mask = np.logical_or(angle_mask, rp_angle > max_angle)
            # where our mask is True, we set to rejected
            rp_detect[angle_mask] = kluster_variables.rejected_flag
            # reshape our new detectioninfo (to get back to 2d) and append to our new_status attribute
            self.new_status.append(rp_detect.reshape(rp['detectioninfo'].shape))
        print(f'filter_by_angle complete')
