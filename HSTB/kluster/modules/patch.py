import numpy as np
from copy import deepcopy
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from scipy.signal import firwin, lfilter, freqz
from scipy.optimize import curve_fit

from HSTB.kluster.xarray_helpers import interp_across_chunks
from HSTB.kluster import kluster_variables
from HSTB.kluster.fqpr_vessel import compare_dict_data, only_retain_earliest_entry, split_by_timestamp


class PatchTest:
    """

    """

    def __init__(self):
        self.fqpr = None
        self.azimuth = None
        self.initial_xyzrph = None

        self.mintime = min([rp.time.values[0] for rp in self.fqpr.multibeam.raw_ping])
        self.maxtime = max([rp.time.values[-1] for rp in self.fqpr.multibeam.raw_ping])
        self.xyzrph = None
        self.xyzrph_timestamps = None

    def initialize(self, fqpr, azimuth: float = None, initial_xyzrph: dict = None, mintime: float = None,
                   maxtime: float = None):
        pass


    def clear(self):
        self.fqpr = None
        self.azimuth = None
        self.initial_xyzrph = None
        self.mintime = 0
        self.maxtime = 0
        self.xyzrph_timestamps = None
        self.xyzrph = None

    def _validate_fqpr(self):
        self.xyzrph_timestamps = list(self.fqpr.multibeam.xyzrph['waterline'].keys())
        if len(self.xyzrph_timestamps) > 1:
            splits = split_by_timestamp(self.fqpr.multibeam.xyzrph)





    def _generate_rotated_points(self):
        cos_az = np.cos(np.deg2rad(90 - self.azimuth))
        sin_az = np.sin(np.deg2rad(90 - self.azimuth))

        dset = []
        systems = self.fqpr.multibeam.return_system_time_indexed_array()
        for s_cnt, system in enumerate(systems):
            # for each serial number combination...only one loop here unless you have a dual head system
            ra = self.multibeam.raw_ping[s_cnt]  # raw ping record
            sys_ident = ra.system_identifier
            print('Retrieving points for sonar serial number: {}'.format(sys_ident))
            # for each installation parameters record...
            for applicable_index, timestmp, prefixes in system:
                self.logger.info('using installation params {}'.format(timestmp))
                self.motion_latency = float(self.multibeam.xyzrph['latency'][timestmp])
                self.generate_starter_orientation_vectors(prefixes, timestmp)
                idx_by_chunk = self.return_chunk_indices(applicable_index, pings_per_chunk)


        for s_cnt, system in enumerate(systems):
            ra = self.multibeam.raw_ping[s_cnt]  # raw ping record


        rotx = cos_az * x - sin_az * y
        roty = sin_az * x + cos_az * y