import numpy as np
import xarray as xr


class Tpu:
    def __init__(self):
        self.heave = 0.05  # 1 sigma standard deviation for the heave data (meters)
        self.heave_percent = 0.05  # percentage of the instantaneous heave (percent)
        self.x_offset = 0.2  # 1 sigma standard deviation in your measurement of x lever arm (meters)
        self.y_offset = 0.2  # 1 sigma standard deviation in your measurement of y lever arm (meters)
        self.z_offset = 0.2  # 1 sigma standard deviation in your measurement of z lever arm (meters)
        self.svp = 2.0  # 1 sigma standard deviation in sv profile sensor (meters/second)
        self.surface_sv = 0.5  # 1 sigma standard deviation in surface sv sensor (meters/second)
        self.roll_patch = 0.1  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        self.pitch_patch = 0.1  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        self.heading_patch = 0.5  # 1 sigma standard deviation in your roll angle patch test procedure (degrees)
        self.latency_patch = 0.0  # 1 sigma standard deviation in your latency calculation (seconds)
        self.timing_latency = 0.001  # 1 sigma standard deviation of the timing accuracy of the system (seconds)
        self.dynamic_draft = 0.1  # 1 sigma standard deviation of the dynamic draft measurement (meters)
        self.separation_model = 0.1  # 1 sigma standard deivation in the sep model (tidal, ellipsoidal, etc) (meters)
        self.waterline = 0.02  # 1 sigma standard deviation of the waterline (meters)
        self.vessel_speed = 0.1  # 1 sigma standard deviation of the vessel speed (meters/second)
        self.horizontal_positioning = 5  # 1 sigma standard deviation of the horizontal positioning (meters)

        # sonar config metadata
        self.receiver_alongtrack

        # vectors from sensors necessary for computation
        self.roll = None
        self.pitch = None
        self.heave = None
        self.beam_angles = None
        self.acrosstrack_offset = None
        self.depth_offset = None

        # intermediate data
        self.radius = None

    def populate_from_dict(self, tpu_dict):
        for ky, val in tpu_dict.items():
            self.__setattr__(ky, val)

    def load_from_data(self, roll, pitch, heave, beam_angles, acrosstrack_offset, depth_offset, roll_in_degrees=True,
                       pitch_in_degrees=True, beam_angles_in_degrees=True):
        nms = ['roll', 'pitch', 'heave', 'beam_angles', 'acrosstrack_offset', 'depth_offset']
        lens = []
        for cnt, dataset in enumerate([roll, pitch, heave, beam_angles, acrosstrack_offset, depth_offset]):
            if not isinstance(dataset, (np.array, xr.DataArray)):
                raise ValueError('{} must be either a numpy array or an xarray DataArray'.format(nms[cnt]))
            lens.append(len(dataset))
        if np.unique(lens) > 1:
            arrlens = {x: lens[nms.index(x)] for x in nms}
            raise ValueError('All provided arrays must be the same length: {}'.format(arrlens))

        if roll_in_degrees:
            self.roll = np.deg2rad(self.roll)
        else:
            self.roll = roll
        if pitch_in_degrees:
            self.pitch = np.deg2rad(self.pitch)
        else:
            self.pitch = pitch
        if beam_angles_in_degrees:
            self.beam_angles = np.deg2rad(self.beam_angles)
        else:
            self.beam_angles = beam_angles

        self.pitch = pitch
        self.heave = heave
        self.beam_angles = beam_angles
        self.acrosstrack_offset = acrosstrack_offset
        self.depth_offset = depth_offset

        self.radius = np.hypot(self.depth, self.acrosstrack_offset)


def build_tpu(tpu_dict: dict = None):
    tp = Tpu()
    if tpu_dict is not None:
        tp.populate_from_dict(tpu_dict)

