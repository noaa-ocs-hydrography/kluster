import os, sys
import numpy as np
import json
from collections import OrderedDict
import xarray as xr

from HSTB.kluster.utc_helpers import julian_day_time_to_utctimestamp
from HSTB.kluster.dms import parse_dms_to_dd
from HSTB.kluster.rotations import build_rot_mat
from HSTB.kluster.xarray_helpers import stack_nan_array, reform_nan_array

supported_file_formats = ['.svp']


class SoundSpeedProfile:
    """
    *** DEPRECATED - See run_ray_trace_v2.  This was my old way of raytracing beams by building these static lookuptables
    for every beam from 0 to 90deg at 0.02deg increments.  I thought this would be a faster way of doing it.  But the
    more variation in surface sv you have, the more tables you need, so it explodes the user memory. ***

    Take in a processed sound velocity profile, and generate ray traced offsets using surface sound speed, beam angles
    beam azimuths and two way travel time.

    Will read from input data and generate profile dict for looking up soundspeed at depth, as well as other cast
    metadata
    """

    def __init__(self, raw_profile: str, z_val: float, ss_sound_speed: xr.DataArray, prof_name: str = None,
                 prof_time: float = None, prof_location: list = None, prof_type: str = 'raw_ping'):
        """
        | Profile datastore, can accept inputs from:
        | - prof_type = raw_ping, soundspeed cast comes in from xarray Dataset attribute, used with xarray_conversion
        | - prof_type = caris_svp, soundspeed cast comes in as file path to caris .svp file

        | ex: raw_ping
        | >> fq.source_dat.raw_ping[0].profile_1583312816
        | '[[0.0, 1489.2000732421875], [0.32, 1489.2000732421875], [0.5, 1488.7000732421875], ...
        | >> prof = fq.source_dat.raw_ping[0].profile_1583312816
        | >> z_pos = float(fq.source_dat.xyzrph['waterline']['1583312816']) + float(fq.source_dat.xyzrph['tx_port_z']['1583312816'])
        | >> ssv = fq.source_dat.raw_ping[0].soundspeed
        | >> cst = svcorrect.SoundSpeedProfile(fq.source_dat.raw_ping[0].profile_1583312816, z_pos, ssv, prof_time=1583312816, prof_name='profile_1583312816', prof_type='raw_ping')

        | >> cst.profile
        | {0.0: 1489.2000732421875, 0.32: 1489.2000732421875, 0.5: 1488.7000732421875, ...

        | ex: caris_svp
        | >> z_pos = float(fq.source_dat.xyzrph['waterline']['1583312816']) + float(fq.source_dat.xyzrph['tx_port_z']['1583312816'])
        | >> ssv = fq.source_dat.raw_ping[0].soundspeed
        | >> cst = svcorrect.SoundSpeedProfile(r'C:\\Users\\eyou1\\Downloads\\2016_288_021224.svp', z_pos, ssv, prof_type='caris_svp')
        | >> cst.prof_time
        | 1476411120.0
        | >> cst.prof_name
        | '2016_288_021224.svp'
        | >> cst.prof_location
        | [37.58972222222222, -76.10972222222222]
        | >> cst.profile
        | {1.2: 1505.25, 1.6: 1505.26, 1.81: 1505.27, 2.18: 1505.29, 4.42: 1506.05, 5.38: 1507.08, ...

        Parameters
        ----------
        raw_profile
            json dump string of the profile (for the xarray workflow) or a path to a Caris SVP file (for the caris
            svp workflow)
        z_val
            z position relative to the waterline, positive down
        ss_sound_speed
            DataArray representing surface sound speed
        prof_name
            metadata stored for the cast, identifier for this cast
        prof_time
            UTC time of cast
        prof_location
            cast location, [latitude in degrees, longitude in degrees]
        prof_type
            profile identifier, one of 'raw_ping' or 'caris_svp'
        """

        self.raw_profile = raw_profile
        self.prof_type = prof_type
        self.prof_time = prof_time
        self.prof_name = prof_name
        self.prof_location = prof_location
        self.profile = self.load_from_profile()

        self.z_val = -z_val
        self.ss_sound_speed = ss_sound_speed
        self.corr_profile = None
        self.corr_profile_lkup = None
        self.adjust_for_z()

        self.interpolate_extended_casts()

        self.lkup_across_dist = None
        self.lkup_down_dist = None
        self.dim_raytime = None
        self.dim_angle = None

    def __sizeof__(self):
        """
        Calculate size of the large attributes of the class and return total size in bytes

        Returns
        -------
        int
            total size of large attributes in bytes
        """

        soundspeed_size = sys.getsizeof(np.array(self.ss_sound_speed.values))
        prof_size = sys.getsizeof(self.raw_profile) + sys.getsizeof(self.profile)
        corr_prof_size = sys.getsizeof(self.corr_profile_lkup) + sys.getsizeof(self.corr_profile)
        lkup_size = sys.getsizeof(self.lkup_across_dist) + sys.getsizeof(self.lkup_down_dist)
        dim_size = sys.getsizeof(self.dim_raytime) + sys.getsizeof(self.dim_angle)
        return soundspeed_size + prof_size + corr_prof_size + lkup_size + dim_size

    def load_from_profile(self):
        """
        Uses prof_type to run correct load function, returns cast data

        Returns
        -------
        dict
            keys are depth and values are soundspeed
        """

        if self.prof_type == 'raw_ping':  # profile comes from xarray_conversion raw_ping dataset attribute
            return self.load_from_xarray()
        elif self.prof_type == 'caris_svp':
            svpdata, self.prof_location, self.prof_time, self.prof_name = cast_data_from_file(self.raw_profile)
            if len(svpdata) > 1:
                print('WARNING: Found multiple casts in file {}, only using the first for this object'.format(self.raw_profile))
                svpdata = svpdata[0]
                self.prof_location = self.prof_location[0]
                self.prof_time = self.prof_time[0]
            return svpdata
        else:
            raise ValueError('Unrecognized format: {}'.format(self.prof_type))

    def load_from_xarray(self):
        """
        Xarray dataset from xarray_conversion BatchRead class stores profile in json so it can be serialized.  We want
        a dictionary where we can look up the depth and get a soundspeed.  Convert it here.

        Returns
        -------
        dict
            keys are depth in meters and values are soundspeed in m/s
        """

        if type(self.raw_profile) == str:   # this is how it should come in, as a json string
            self.raw_profile = json.loads(self.raw_profile)

        # insert zero point in profile with matching soundspeed as first actual layer, for when trans is above the
        #   shallowest point in the cast
        self.raw_profile = [['0.0', self.raw_profile[0][1]]] + self.raw_profile

        profdata = {float(k): float(v) for k, v in self.raw_profile}
        return OrderedDict(sorted(profdata.items()))

    def adjust_for_z(self):
        """
        self.profile contains the sound velocity data as seen by the profiler.  We need a table that starts at the depth
        of the sonar relative to the waterline.  Also need to insert a snapback soundspeed layer equal to the data seen
        by the surface sound velocimeter.  This method will generate a list of lookup tables that equal in length to the
        length of the unique surface soundvelocity entries.
        """

        unique_ss = sorted(np.unique(self.ss_sound_speed))
        self.corr_profile_lkup = np.searchsorted(unique_ss, self.ss_sound_speed)
        self.corr_profile = []
        # print('Generating lookup tables for {} unique soundspeed entries...'.format(len(unique_ss)))
        for u in unique_ss:
            newprof = OrderedDict({i - self.z_val: j for (i, j) in self.profile.items() if i - self.z_val >= 0})
            frst_ss = OrderedDict({0: u})
            self.corr_profile.append(OrderedDict(list(frst_ss.items()) + list(newprof.items())))

    def interpolate_extended_casts(self, max_allowable_depth_distance: float = 100.0):
        """
        Take max distance parameter, interpolate layers with depth differences greater than that.  This is super
        important as extending a cast from 100m to 1200m to satisfy Kongsberg might result in a change in tens of m/s
        between layers.  This results in steering angles with a huge change across that boundary.

        Parameters
        ----------
        max_allowable_depth_distance
            max allowable distance in meters between layer entries.
        """

        rslt = []
        for prof in self.corr_profile:
            dpths = np.array(list(prof))
            dif_dpths = np.diff(dpths)
            needs_interp = dif_dpths > max_allowable_depth_distance
            if np.any(needs_interp):
                interp_idx = np.where(needs_interp)
                if len(interp_idx) > 1:
                    raise ValueError(
                        'Found more than one gap in profile greater than {}'.format(max_allowable_depth_distance))
                firstval = dpths[interp_idx[0]][0]
                secval = dpths[interp_idx[0] + 1][0]
                new_dpths = np.round(np.linspace(firstval + max_allowable_depth_distance, secval,
                                                int((secval - firstval) / max_allowable_depth_distance)), 2)
                new_svp = np.interp(new_dpths, [firstval, secval], [prof[firstval], prof[secval]])
                for d, s in zip(new_dpths, new_svp):
                    prof[d] = s
            rslt.append(OrderedDict(sorted(prof.items())))
        self.corr_profile = rslt

    def generate_lookup_table(self, max_pointing_angle: float = 90.0, beam_inc: float = 0.02):
        """
        Compute a lookup table for all possible launch angles to get acrosstrack/alongtrack distance and travel time.
        Build look up table around approximate launch angles, something like .02 deg increments.  When using table, find
        nearest launch angle that applies.  Error should be within unc of the attitude sensor (plus patch uncertainty)
        Table dims look something like 70 * 50 (launch angle * beam increment) by 50 (sound speed layers)
        Table is indexed by time.  Knowing two-way-travel-time, search table for final x, y.
        Table would be from the waterline, when using it in practice, have to offset by the transducer vertical position
        relative to the waterline.

        Parameters
        ----------
        max_pointing_angle
            max angle of the swath
        beam_inc
            beam angle increments you want to generate entries for
        """

        # max_pointing_angle is from boresight, only doing one side as port/stbd are the same
        # print('Generating lookup table to {} deg in {} deg increments'.format(max_pointing_angle, beam_inc))
        starter_angles = np.arange(np.deg2rad(0), np.deg2rad(max_pointing_angle), np.deg2rad(beam_inc), dtype=np.float32)

        lookup_table_angles = np.zeros([len(self.corr_profile), starter_angles.shape[0], len(self.corr_profile[0])], dtype=np.float32)
        cumulative_ray_time = np.zeros([len(self.corr_profile), starter_angles.shape[0], len(self.corr_profile[0])], dtype=np.float32)
        cumulative_across_dist = np.zeros([len(self.corr_profile), starter_angles.shape[0], len(self.corr_profile[0])], dtype=np.float32)
        cumulative_down_dist = np.zeros([len(self.corr_profile), len(self.corr_profile[0])], dtype=np.float32)

        for pcnt, prof in enumerate(self.corr_profile):
            lookup_table_angles[pcnt, :, 0] = starter_angles
            dpths = np.array(list(prof.keys()))
            # print('- Lookup table to depth of {} with {} total layers...'.format(np.max(dpths), len(dpths)))
            for cnt, dpth in enumerate(dpths):
                if cnt != len(dpths) - 1:
                    # ray parameters for all lookup angles
                    difdpth = dpths[cnt + 1] - dpth
                    across_dist = difdpth * np.tan(lookup_table_angles[pcnt, :, cnt])
                    ray_dist = np.sqrt(difdpth**2 + across_dist**2)
                    ray_time = ray_dist / prof[dpth]

                    cumulative_ray_time[pcnt, :, cnt + 1] = cumulative_ray_time[pcnt, :, cnt] + ray_time
                    cumulative_across_dist[pcnt, :, cnt + 1] = cumulative_across_dist[pcnt, :, cnt] + across_dist
                    cumulative_down_dist[pcnt, cnt + 1] = cumulative_down_dist[pcnt, cnt] + difdpth

                    # incidence angles for next layer
                    # use clip to clamp values where beams are reflected, i.e. greater than 1 (90°)
                    #  - this happens with a lot of the extended-to-1200m casts in kongsberg data, these angles should
                    #    not be used, this is mostly to suppress the runtime warning
                    _tmpdat = prof[dpths[cnt + 1]] / prof[dpths[cnt]] * np.sin(lookup_table_angles[pcnt, :, cnt])
                    next_angles = np.arcsin(np.clip(_tmpdat, -1, 1))
                    lookup_table_angles[pcnt, :, cnt + 1] = next_angles
        self.lkup_across_dist = cumulative_across_dist
        self.lkup_down_dist = cumulative_down_dist
        self.dim_raytime = cumulative_ray_time
        self.dim_angle = starter_angles

    def _rotate_2d_to_3d(self, x, y, angle, data_idx):
        """
        Take vector with x/y (2d) components and then rotate it along x axis to get 3d vector.

        In:  angle.values[0]
        Out: -0.6200000047683716
        In:  zerod_x.values[0]
        Out: 0
        In:  zerod_y.values[0]
        Out: 0
        In:  rotmat.values[0]
        Out: array([[ 1.        , -0.        ,  0.        ],
                    [ 0.        ,  0.99994145,  0.01082083],
                    [-0.        , -0.01082083,  0.99994145]])
        (use data_idx values to replicate rotation matrix for each matching time value, have to do this for the
        flattened time/beam arrays we are working with)
        In:  expanded_rotmat = rotmat[data_idx].values
        In:  vec = np.squeeze(np.dstack([x, y, np.zeros_like(x)]))
        In:  vec[0]
        Out: array([239.71445634,  70.29585725,   0.        ])
        In:  rotvec = np.einsum('lij,lj->li', expanded_rotmat, vec)
        In:  rotvec[0]
        Out: array([239.71445634,  70.29174165,  -0.76065954])

        Parameters
        ----------
        x: xarray DataArray, 1d stacked array of acrosstrack offsets with MultiIndex coords of time/beam
        y: xarray DataArray, 1d stacked array of alongtrack offsets with MultiIndex coords of time/beam
        angle: xarray DataArray, 1d array with coordinate 'time', represents the x axis angle change over time
               data_idx
        data_idx: numpy array 1d, with length of stacked array, contains the index of the appropriate rotation matrix
                  to use for that vector.  EX: array([0,0,0,1,1,1]) says to use the first rotmat for the first three
                  vectors and the second rotmat for the last three vectors

        Returns
        -------
        newacross: numpy array 1d, rotated acrosstrack offset
        newdown: numpy array 1d, newly generated down offset, from rotating xy
        newalong: numpy array 1d, rotated alongtrack offset

        """
        zerod_x = xr.DataArray(np.zeros_like(angle), coords={'time': angle.time}, dims=['time'])
        zerod_y = zerod_x
        rotmat = build_rot_mat(angle, zerod_x, zerod_y)
        expanded_rotmat = rotmat[data_idx].values

        vec = np.squeeze(np.dstack([x, y, np.zeros_like(x)]))
        rotvec = np.round(np.einsum('lij,lj->li', expanded_rotmat, vec), 3)  # round to mm

        newacross = rotvec[:, 0]
        newdown = rotvec[:, 1]
        newalong = rotvec[:, 2]

        return newacross, newdown, newalong

    def _transducer_depth_correction(self, transdepth, nearest_across):
        """
        OUTDATED: NOT IN USE.  REPLACED BY ADDING SSV LAYER AND ADJUSTING PROFILE FOR TRANS DEPTH REL WATERLINE

        Take in a floating point number for height of waterline from transducer and generate a time series corrector
        for acrosstrack offsets.

        Parameters
        ----------
        transdepth: float, height of waterline from transducer, positive down (i.e.
                       transdepth=-0.640 means the waterline is 64cm above the transducer)
        nearest_across: numpy array, first dim is beams, second is cumulative across track distance for water column

        Returns
        -------
        interp_across: numpy array, first dim is beams, second is a corrector for across track distance taking into
                       account the transducer position relative to the waterline

        """
        # here we want to use transdepth in our table, so we make it positive
        transdepth = np.abs(transdepth)

        dpths = np.array(list(self.profile.keys()))

        # get the index of the closest depth from transdepth
        srch_vals = np.argmin(np.abs(dpths - transdepth))
        srch_rows = np.arange(len(nearest_across))

        # get val before and after actual value nearest the index
        dpth1 = dpths[srch_vals]
        if transdepth > dpth1:
            offset = 1
        else:
            offset = -1
        dpth2 = dpths[srch_vals + offset]

        # get the interp factor to apply to other arrays
        fac = (dpth1 - transdepth) / (dpth1 - dpth2)

        # apply interp factor to get across value corrector
        interp_across = np.round(nearest_across[srch_rows, srch_vals] - (fac * (nearest_across[srch_rows, srch_vals] -
                                 nearest_across[srch_rows, srch_vals + offset])), 3)

        return interp_across

    def _run_ray_trace(self, dim_angle: np.array, dim_raytime: np.ndarray, lkup_across_dist: np.ndarray,
                      lkup_down_dist: np.ndarray, corr_profile_lkup: np.ndarray,
                      beam_azimuth: xr.DataArray, beam_angle: xr.DataArray, two_way_travel_time: xr.DataArray,
                      subset: np.array = None, offsets: list = None):
        """
        | Sources:
        |    Ray Trace Modeling of Underwater Sound Propagation - Jens M. Hovern
        |    medwin/clay, fundamentals of acoustical oceanography, ch3
        |    Underwater Ray Tracing Tutorial in 2D Axisymmetric Geometry - COMSOL
        |    Barry Gallagher, NOAA hydrographic wizard extraordinaire

        Ray Theory approach - assumes sound propagates along rays normal to wave fronts, refracts when sound speed changes
        in the water.  Layers of differing sound speed are obtained through sound speed profiler.  Here we make linear
        approximation, where we assume sound speed changes linearly between layers.

        When searching by half two-way-travel-time, unlikely to find exact time in table.  Find nearest previous time,
        get the difference in time and multiply by the layer speed to get distance.

        This function supports distributed svcorrect, which must be run without the use of the main SoundSpeedProfile
        class due to limitations with serializing classes and dask.distributed.  Users should either

        To better understand inputs, see fqpr_generation.Fqpr.get_beam_pointing_vectors

        Use the generated lookup table to return across track / along track offsets from given beam angles and travel
        times.  We interpolate the table to get the values at the actual given travel time.  For beam angle, we simply
        search for the nearest value, assuming the table beam angle increments are roughly on par with the accuracy
        of the attitude sensor.

        dims and lookup tables come from the SoundSpeedProfile object.

        Parameters
        ----------
        dim_angle
            numpy 1d array of angles in radians from zero to max swath angle
        dim_raytime
            numpy 3d array of travel times for each layer in cast (num_unique_ssv, num_angles, num_layers)
        lkup_across_dist
            numpy 3d array, lookup table for across distance value (num_unique_ssv, num_angles, num_layers)
        lkup_down_dist
            numpy 2d array, lookup table for down distance value (num_unique_ssv, num_layers)
        corr_profile_lkup
            numpy 1d array, we have a number of lookup tables equal to the unique ssv values found in the data set.  This lookup tells you which ssv table applies to which time.
        beam_angle
            2 dimension array of time/beam angle.  Assumes the second dim contains the actual angles, ex: (time, angle)
        two_way_travel_time
            2 dimension array of time/two_way_travel_time.  Assumes the second dim contains the actual traveltime, ex: (time, twtt)
        beam_azimuth
            2 dimension array of time/beam azimuth.  Assumes the second dim contains the actual beam azimuth data, ex: (time, azimuth)
        subset
            numpy array, if provided subsets the corrected profile lookup
        offsets
            list of float offsets to be added, [alongtrack offset, acrosstrack offset, depth offset]

        Returns
        -------
        list
            [xarray DataArray (time, along track offset in meters), xarray DataArray (time, across track offset in meters),
             xarray DataArray (time, down distance in meters)]
        """

        if lkup_across_dist is None:
            raise ValueError('Generate lookup table first')
        if subset is not None:
            corr_lkup = corr_profile_lkup[subset]
        else:
            corr_lkup = corr_profile_lkup

        # take in (time, beam) dimension data or (beam) dimension data and flatten so we can work on what are likely
        #   jagged arrays with np.nan padding.  Maintain original shape for reshaping after
        # use stack to get a 1d multidimensional index, allows you to do the bool indexing later to remove nan vals
        # - use absvalue of beam_angle_stck for the lookup table, retain original beam_angle to determine port/stbd
        orig_shape = beam_angle.shape
        orig_coords = beam_angle.coords
        orig_dims = beam_angle.dims
        beam_idx, beam_angle_stck = stack_nan_array(beam_angle, stack_dims=('time', 'beam'))
        twtt_idx, twoway_stck = stack_nan_array(two_way_travel_time, stack_dims=('time', 'beam'))
        beamaz_idx, beamaz_stck = stack_nan_array(beam_azimuth, stack_dims=('time', 'beam'))

        arr_lens = [len(beam_angle_stck), len(twoway_stck), len(beamaz_stck)]
        if len(np.unique(arr_lens)) > 1:
            print('Found uneven arrays:')
            print('corr_pointing_angle: {}'.format(arr_lens[0]))
            print('traveltime: {}'.format(arr_lens[1]))
            print('rel_azimuth: {}'.format(arr_lens[2]))
            shortest_idx = np.argmin(np.array(arr_lens))
            shortest = [beam_angle, two_way_travel_time, beam_azimuth][shortest_idx]
            beam_idx = [beam_idx, twtt_idx, beamaz_idx][shortest_idx]
            nan_idx = ~np.isnan(shortest.stack(stck=('time', 'beam'))).compute()
            beam_angle_stck = beam_angle.stack(stck=('time', 'beam'))[nan_idx]
            twoway_stck = two_way_travel_time.stack(stck=('time', 'beam'))[nan_idx]
            beamaz_stck = beam_azimuth.stack(stck=('time', 'beam'))[nan_idx]
        del beam_angle, two_way_travel_time, beam_azimuth

        beam_angle_stck = np.abs(beam_angle_stck)
        oneway_stck = twoway_stck / 2
        del twoway_stck

        # get indexes of nearest beam_angle to the table angle dimension, default is first suitable location (left)
        # TODO: this gets the index after (insertion index), look at getting nearest index
        nearest_angles = np.searchsorted(dim_angle, beam_angle_stck)
        del beam_angle_stck

        # get the arrays according to the nearest angle search
        # lkup gives the right table to use for each run, multiple tables exist for each unique ssv entry
        lkup = corr_lkup[beam_idx[0]]
        try:
            nearest_raytimes = dim_raytime[lkup, nearest_angles, :]
        except:
            print('Found beams with angles greater than lookup table')
            nearest_angles = np.clip(nearest_angles, 0, int(dim_raytime.shape[1]) - 1)
            nearest_raytimes = dim_raytime[lkup, nearest_angles, :]

        nearest_across = lkup_across_dist[lkup, nearest_angles, :]
        nearest_down = lkup_down_dist[lkup]

        # print('Running sv_correct on {} total beams...'.format(nearest_raytimes.shape[0]))

        interp_acrossvals, interp_downvals = _construct_across_down_vals(nearest_raytimes, nearest_across,
                                                                         nearest_down, oneway_stck)
        del nearest_raytimes, nearest_across, nearest_down, oneway_stck

        # here we use the beam azimuth to go from xy sv corrected beams to xyz soundings
        newacross = interp_acrossvals * np.sin(beamaz_stck)
        newalong = interp_acrossvals * np.cos(beamaz_stck)
        del interp_acrossvals

        if offsets:
            try:  # offsets provided as a numpy array
                newacross = newacross + offsets[1].ravel()
                newalong = newalong + offsets[0].ravel()
                interp_downvals = interp_downvals + offsets[2].ravel()
            except:  # offsets provided as float
                newacross = newacross + offsets[1]
                newalong = newalong + offsets[0]
                interp_downvals = interp_downvals + offsets[2]

        reformed_across = reform_nan_array(newacross, beam_idx, orig_shape, orig_coords, orig_dims)
        reformed_downvals = reform_nan_array(interp_downvals, beam_idx, orig_shape, orig_coords, orig_dims)
        reformed_along = reform_nan_array(newalong, beam_idx, orig_shape, orig_coords, orig_dims)
        del newacross, newalong, interp_downvals

        return [np.round(reformed_along, 3), np.round(reformed_across, 3), np.round(reformed_downvals, 3)]

    def run_sv_correct(self, beam_angle: xr.DataArray, two_way_travel_time: xr.DataArray, beam_azimuth: xr.DataArray):
        """
        Convenience function for run_ray_trace on self.  See run_ray_trace for more info.

        Parameters
        ----------
        beam_angle
            2d array of time/beam angle.  Assumes the second dim contains the actual angles, ex: (time, angle)
        two_way_travel_time
            2d array of time/two_way_travel_time.  Assumes the second dim contains the actual traveltime, ex: (time, twtt)
        beam_azimuth
            2d array of time/beam azimuth.  Assumes the second dim contains the actual beam azimuth data, ex: (time, azimuth)
        """

        x, y, z = self._run_ray_trace(self.dim_angle, self.dim_raytime, self.lkup_across_dist, self.lkup_down_dist,
                                      self.corr_profile_lkup, beam_azimuth, beam_angle, two_way_travel_time,
                                      subset=None, offsets=None)
        return x, y, z


def get_sv_files_from_directory(dir_path: str, search_subdirs: bool = True):
    """
    Returns a list of all files that have an extension in the global variable supported_file_formats

    Disable search_subdirs if you want to only search the given folder, not subfolders

    Parameters
    ----------
    dir_path
        string, path to the parent directory containing sv files
    search_subdirs
        bool, if True searches all subfolders as well

    Returns
    -------
    list
        full file paths to all sv files with approved file extension
    """

    svfils = []
    for root, dirs, files in os.walk(dir_path):
        for f in files:
            if os.path.splitext(f)[1] in supported_file_formats:
                svfils.append(os.path.join(root, f))
        if not search_subdirs:
            break
    return svfils


def return_supported_casts_from_list(list_files: list):
    """
    Take in a list of files, return all the valid cast files

    Parameters
    ----------
    list_files
        list of cast file paths

    Returns
    -------
    list
        files from list that have extension in supported_file_formats
    """

    if type(list_files) != list:
        raise TypeError('Function expects a list of sv files to be provided.')
    return [x for x in list_files if os.path.splitext(x)[1] in supported_file_formats]


def cast_data_from_file(profile_file: str):
    """
    Read from the provided sound velocity file and return the data, method depends on file format

    Currently only svp file is supported

    Parameters
    ----------
    profile_file
        file containing the sound velocity data

    Returns
    -------
    list
        list of OrderedDict of (depth, soundvelocity) values for each cast
    list
        list of [Latitude in degrees of cast location, Longitude in degrees of cast location] for each cast
    list
        list of time of cast in UTC seconds for each cast
    list
        list of str names of each cast
    """

    if profile_file.endswith('.svp'):
        return _load_from_caris_svp(profile_file)


def _load_caris_svp_data(svpdata: list):
    """
    See load_from_caris_svp, takes in readlines output from svp file, returns dict

    Parameters
    ----------
    svpdata
        output from readlines call on svp file

    Returns
    -------
    dict
        keys are depth in meters and values are soundspeed in m/s
    """

    svpdat = [[float(dproc) for dproc in svp.rstrip().split()] for svp in svpdata]
    # insert zero point in profile with matching soundspeed as first actual layer, for when trans is above the
    #   shallowest point in the cast
    svpdat = [[0.0, svpdat[0][1]]] + svpdat

    profdata = {k: v for k, v in svpdat}
    return OrderedDict(sorted(profdata.items()))


def _parse_single_svp_cast(filname: str, svpdata: list):
    """
    Take in a list of lines corresponding to one svp cast in an svp file (which can have multiple casts) and return the
    processed data.

    Parameters
    ----------
    filname
        absolute file path to the svp file
    svpdata
        list of lines from reading the svp file that correspond to a single cast + header

    Returns
    -------
    OrderedDict
        dict of (depth, soundvelocity) values
    list
        [Latitude in degrees of cast location, Longitude in degrees of cast location]
    float
        time of cast in UTC seconds
    """

    header = svpdata[0]  # Section 2016-288 02:12 37:35:23 -076:06:35
    svpdata = svpdata[1:]
    try:
        jdate, tme, lat, lon = [header.split()[i] for i in [1, 2, 3, 4]]
    except IndexError:
        error = 'Error reading {}\n'.format(filname)
        error += 'Please verify that the svp file has the correct header'
        raise IOError(error)
    try:
        svpdata = _load_caris_svp_data(svpdata)
    except:
        error = 'Error reading {}\n'.format(filname)
        error += 'Unable to parse sound velocity profile data'
        raise IOError(error)

    jdate = jdate.split('-')
    tme = tme.split(':')
    prof_location = [parse_dms_to_dd(lat), parse_dms_to_dd(lon)]
    if len(tme) == 3:
        prof_time = julian_day_time_to_utctimestamp(int(jdate[0]), int(jdate[1]), int(tme[0]), int(tme[1]), int(tme[2]))
    elif len(tme) == 2:
        prof_time = julian_day_time_to_utctimestamp(int(jdate[0]), int(jdate[1]), int(tme[0]), int(tme[1]), 0)
    else:
        raise ValueError('Unrecognized timestamp: {}'.format(tme))

    return svpdata, prof_location, prof_time


def _load_from_caris_svp(profile_file: str):
    """
    Here we load from a caris svp file to generate the cast data.  We want a dictionary where we can look up the
    depth and get a soundspeed.

    Parameters
    ----------
    profile_file
        absolute file path to a caris svp file

    Returns
    -------
    list
        list of OrderedDict of (depth, soundvelocity) values for each cast
    list
        list of [Latitude in degrees of cast location, Longitude in degrees of cast location] for each cast
    list
        list of time of cast in UTC seconds for each cast
    list
        list of str names of each cast
    """

    if os.path.exists(profile_file) and profile_file.endswith('.svp'):
        with open(profile_file, 'r') as svp:
            cast_datasets = []
            end_cast_idx = 0
            all_lines = svp.readlines()
            version = all_lines[0]  # [SVP_VERSION_2]
            name = all_lines[1]  # 2016_288_021224.svp
            prof_name = os.path.split(name.rstrip())[1]
            all_lines = all_lines[2:]

            for cnt, lne in enumerate(all_lines):
                if lne[0:8].lstrip() == 'Section ' and cnt != 0:
                    cast_datasets.append(all_lines[end_cast_idx:cnt])
                    end_cast_idx = cnt
            cast_datasets.append(all_lines[end_cast_idx:])

            profiles = []
            locations = []
            times = []
            for cast in cast_datasets:
                svpdata, prof_location, prof_time = _parse_single_svp_cast(profile_file, cast)
                profiles.append(svpdata)
                locations.append(prof_location)
                times.append(prof_time)

            return profiles, locations, times, prof_name
    else:
        raise IOError('Not a valid caris svp file: {}'.format(profile_file))


def _interp(arr: np.ndarray, row_idx: np.array, col_idx: np.array, msk: np.array, interp_factor: np.ndarray):
    """
    See return_interp_beam_xy.  Takes in an array (arr) and some parameters that allow you to interp to the given
    interp factor.  Interp_factor represents location of actual values you are wanting, a ratio you can use to find
    those values.  Using this instead of numpy/scipy/xarray methods as I couldn't find anything that really handles
    this odd 2d indexing that I'm doing.  Probably because I don't know what I'm doing.

    EX: looking at first val in vectors

    In:  row_idx[0]
    Out: 0
    In:  col_idx[0]
    Out: 35
    In:  msk[0]
    Out: 0
    In:  arr[row_idx[0], col_idx[0]]
    Out: 74.2
    In:  arr[row_idx[0], col_idx[0] + 1]
    Out: 174.2
    ('0' is in msk, so we know to use the previous value instead, the desired value is before 74.2)
    In:  arr[row_idx[0], col_idx[0] - 1]
    Out: 60.67
    In:  interp_factor[0]
    Out: 0.28855453
    (actual value is this factor times diff away from start value)
    (74.2 - (0.28855453 * (74.2 - 60.67)))
    In:  np.round(start - (interp_factor * (start - end)), 2)
    Out: 70.30

    Parameters
    ----------
    arr
        numpy ndarray, 2d array that you are wanting an interpolated vector from
    row_idx
        numpy array, 1d array of index values for 1st dimension
    col_idx
        numpy array, 1d array of index values for 2nd dimension
    msk
        numpy array, 1d array of index values that mark when you want to interp using the value before the nearest value instead of ahead
    interp_factor
        numpy ndarray, 1d array of values that are the interp ratio to apply to achieve linear interpolation between the start/end values

    Returns
    -------
    np.ndarray

    """
    # use the mask and interpolation factor generated above to build across/alongtrack offsets
    start = arr[row_idx, col_idx]
    end = arr[row_idx, col_idx + 1]
    end[msk] = arr[row_idx, col_idx - 1][msk]
    interp_vals = np.round(start - (interp_factor * (start - end)), 3)  # round to mm
    return interp_vals


def _construct_across_down_vals(nearest_raytimes: np.ndarray, nearest_across: np.ndarray, nearest_down: np.ndarray,
                                one_way_travel_time: np.array):
    """
    Using SoundSpeedProfile as a lookup table, find the nearest raytime to the given one_way_travel_time, determine
    the proportional difference, and apply that same factor to across track/depth to get the interpolated xy at
    that time.

    Parameters
    ----------
    nearest_raytimes
        first dim is beams, second is cumulative ray time for water column
    nearest_across
        first dim is beams, second is cumulative across track distance for water column
    nearest_down
        first dim is beams, second is cumulative depth for water column
    one_way_travel_time
        one dimensional array of the one way travel time for each beam

    Returns
    -------
    np.array
        one dimensional array of across track distance for each beam
    np.array
        one dimensional array of depth for each beam
    """

    # get the index of the closest onewaytraveltime from nearest raytimes
    # self.dim_raytime will have nan for part of profile where beams reflect, angle>90, use nanargmin
    srch_vals = np.nanargmin(np.abs(nearest_raytimes - np.expand_dims(one_way_travel_time, axis=1)), axis=1)
    srch_rows = np.arange(len(nearest_raytimes))

    # get val before and after actual value nearest the index
    raytim1 = nearest_raytimes[srch_rows, srch_vals]
    raytim2 = nearest_raytimes[srch_rows, srch_vals + 1]
    # sometimes the bound is the other way, when the nearest is greater than the index
    msk = np.where(one_way_travel_time < raytim1)[0]
    raytim2[msk] = nearest_raytimes[srch_rows, srch_vals - 1][msk]

    # get the interp factor to apply to other arrays
    fac = (raytim1 - one_way_travel_time) / (raytim1 - raytim2)
    interp_acrossvals = _interp(nearest_across, srch_rows, srch_vals, msk, fac)
    interp_downvals = _interp(nearest_down, srch_rows, srch_vals, msk, fac)
    return interp_acrossvals, interp_downvals


def _convert_cast(cast: list, z_waterline_offset: float):
    """
    Take the original cast, a list of depth/sv lists and convert to numpy arrays.  Change the cast depth reference point
    from the waterline to the transducer, to match the beam angle/traveltime arrays.

    Parameters
    ----------
    cast
        list of [depth values, sv values] for this cast
    z_waterline_offset
        offset from transducer to waterline, positive down

    Returns
    -------
    np.ndarray
        cast depths rel transmitter
    np.ndarray
        cast sound velocity for each depth
    """

    cast_depth, cast_soundvelocity = np.array(cast[0]), np.array(cast[1])
    cast_depth_rel_tx = cast_depth + z_waterline_offset
    below_trans = cast_depth_rel_tx >= 0
    cast_depth_rel_tx = cast_depth_rel_tx[below_trans]
    cast_soundvelocity = cast_soundvelocity[below_trans]
    return cast_depth_rel_tx, cast_soundvelocity


def _process_cast_for_ssv(cast_depth: np.ndarray, cast_sv: np.ndarray, max_allowed_sv: float, ssv: float):
    """
    Process the cast to add the surface sound velocity layer as the initial layer.

    If the last layer(s) are greater than the max allowed sv value, we remove those layers and apply linear interpolation
    to determine the depth for the max allowed sv value and use that as the last layer.  Kongsberg extends casts down to
    12000 meters, so this happens quite often.

    Parameters
    ----------
    cast_depth
        cast depths rel transmitter
    cast_sv
        cast sound velocity for each depth
    max_allowed_sv
        maximum allowed sv value for the
    ssv
        surface sound velocity value for this cast

    Returns
    -------
    np.ndarray
        cast depths rel transmitter with ssv added at depth=0
    np.ndarray
        cast sound velocity for each depth with ssv added at depth=0
    """

    # later layers can't have sound velocity that exceeds the max allowed sv layer, breaks the gradient calculation
    if (cast_sv >= max_allowed_sv).any():
        first_invalid_layer = np.where(cast_sv >= max_allowed_sv)[0][0]
        layer_previous = first_invalid_layer - 1
        sv1, sv2 = cast_sv[layer_previous], cast_sv[first_invalid_layer]
        dpth1, dpth2 = cast_depth[layer_previous], cast_depth[first_invalid_layer]
        new_depth = dpth1 + (max_allowed_sv - sv1) * ((dpth2 - dpth1) / (sv2 - sv1))

        cast_sv = cast_sv[:int(first_invalid_layer) + 1]
        cast_depth = cast_depth[:int(first_invalid_layer) + 1]
        cast_sv[-1] = max_allowed_sv
        cast_depth[-1] = new_depth

    # insert surface sound speed layer
    if cast_depth[0] == 0:
        cast_sv[0] = ssv
    else:
        cast_depth = np.insert(cast_depth, 0, 0)
        cast_sv = np.insert(cast_sv, 0, ssv)

    cast_sv_diff = np.diff(cast_sv)

    # remove the duplicate sv values in the profile to maintain good gradient answers
    not_duplicate_sv_idx = cast_sv_diff != 0
    not_duplicate_sv_idx = np.insert(not_duplicate_sv_idx, 0, True)
    cast_depth = cast_depth[not_duplicate_sv_idx]
    cast_sv = cast_sv[not_duplicate_sv_idx]

    return cast_depth, cast_sv


def _build_beam_cumulative_tables(cast_depth: np.ndarray, cast_sv: np.ndarray, beam_angle: np.ndarray):
    """
    Take the initial angle (the provided beam launch angle) and calculate the ray distance and travel time for each
    layer.  Allows you to end up with an array of cumulative depth, horizontal distance and ray time for each layer.

    We use these cumulative tables to linearly interpolate the answer using our two way travel time

    Parameters
    ----------
    cast_depth
        cast depths rel transmitter
    cast_sv
        cast sound velocity for each depth
    beam_angle
        2dim (time, beam) values for beampointingangle at each beam, assume radians

    Returns
    -------
    np.ndarray
        1dim (cast_layer_number) cumulative depth for each cast layer
    np.ndarray
        3dim (cast_layer_number, time, beam) cumulative horizontal distance the beam traveled for each layer, time, beam
    np.ndarray
        3dim (cast_layer_number, time, beam) cumulative ray time for each layer, time, beam
    """

    cast_depth_diff = np.diff(cast_depth)
    layerangles = np.zeros((len(cast_sv), beam_angle.shape[0], beam_angle.shape[1]))
    layerangles[0, :, :] = beam_angle

    cumulative_depth = np.zeros_like(cast_sv)
    cumulative_h_dist = np.zeros_like(layerangles)
    cumulative_raytime = np.zeros_like(layerangles)
    for i in range(1, len(cast_sv)):
        across_dist = cast_depth_diff[i - 1] * np.tan(layerangles[i - 1])
        ray_dist = np.sqrt(cast_depth_diff[i - 1] ** 2 + across_dist ** 2)
        ray_time = ray_dist / cast_sv[i - 1]

        cumulative_depth[i] = cumulative_depth[i - 1] + cast_depth_diff[i - 1]
        cumulative_h_dist[i] = across_dist + cumulative_h_dist[i - 1]
        cumulative_raytime[i] = ray_time + cumulative_raytime[i - 1]

        # incidence angles for next layer
        # use clip to clamp values where beams are reflected, i.e. greater than 1 (90°)
        #  - this happens with a lot of the extended-to-1200m casts in kongsberg data, these angles should
        #    not be used, this is mostly to suppress the runtime warning
        _tmpdat = (cast_sv[i] / cast_sv[i - 1]) * np.sin(layerangles[i - 1])
        layerangles[i] = np.arcsin(np.clip(_tmpdat, -1, 1))
    return cumulative_depth, cumulative_h_dist, cumulative_raytime


def _interpolate_cumulative_table(cumulative_depth: np.ndarray, cumulative_h_dist: np.ndarray, cumulative_raytime: np.ndarray,
                                  cast_sv: np.ndarray, two_way_travel_time: np.ndarray):
    """
    Take the previously generated cumulative tables for each layer, time, beam and interpolate the answer for our two way
    travel time.

    Parameters
    ----------
    cumulative_depth
        1dim (cast_layer_number) cumulative depth for each cast layer
    cumulative_h_dist
        3dim (cast_layer_number, time, beam) cumulative horizontal distance the beam traveled for each layer, time, beam
    cumulative_raytime
        3dim (cast_layer_number, time, beam) cumulative ray time for each layer, time, beam
    cast_sv
        cast sound velocity for each depth
    two_way_travel_time
        2dim (time, beam) values for the beam two way travel time in seconds

    Returns
    -------
    np.ndarray
        2dim (time, beam) values for the actual horizontal distance that applies to each beam
    np.ndarray
        2dim (time, beam) values for the actual vertical distance that applies to each beam
    """

    oneway_traveltime = two_way_travel_time / 2
    nearest_next_layer_index = (cumulative_raytime - oneway_traveltime[None, :, :] < 0).argmin(axis=0)
    interp_across = np.zeros_like(oneway_traveltime)
    interp_down = np.zeros_like(oneway_traveltime)

    for layer_index in np.unique(nearest_next_layer_index):
        if layer_index == len(cast_sv):  # past the bounds of the cast
            print('Found beam traveltime outside the range of the provided cast')
            continue
        elif layer_index == 0:
            print('Found beam traveltime that places it above the transducer')
            continue
        layer_index_mask = nearest_next_layer_index == layer_index
        tt_one = cumulative_raytime[layer_index - 1][layer_index_mask]
        tt_two = cumulative_raytime[layer_index][layer_index_mask]
        oneway = oneway_traveltime[layer_index_mask]
        across_one = cumulative_h_dist[layer_index - 1][layer_index_mask]
        across_two = cumulative_h_dist[layer_index][layer_index_mask]
        down_one = cumulative_depth[layer_index - 1]
        down_two = cumulative_depth[layer_index]
        interp_across[layer_index_mask] = across_one + (oneway - tt_one) * (
                    (across_two - across_one) / (tt_two - tt_one))
        interp_down[layer_index_mask] = down_one + (oneway - tt_one) * ((down_two - down_one) / (tt_two - tt_one))
    return interp_across, interp_down


def run_ray_trace_v2(cast: list, beam_azimuth: xr.DataArray, beam_angle: xr.DataArray, two_way_travel_time: xr.DataArray,
                     surface_sound_speed: xr.DataArray, z_waterline_offset: float, additional_offsets: list):
    """
    Apply the provided sound velocity cast and surface sound speed value to ray trace the angles/traveltime through
    each layer.  We construct cumulative depth/distance/time for each layer and then apply linear interpolation using
    the provded twowaytraveltime to get the actual alongtrack/acrosstrack/depthoffset for each beam.

    Replaces the SoundSpeedProfile method

    Parameters
    ----------
    cast
        list of [depth values, sv values] for this cast
    beam_azimuth
        2dim (time, beam), beam-wise beam azimuth values relative to vessel heading at time of ping, assume radians
    beam_angle
        2dim (time, beam) values for beampointingangle at each beam, assume radians
    two_way_travel_time
        2dim (time, beam) values for the beam two way travel time in seconds
    surface_sound_speed
        1dim (time) values for surface sound speed in meters per second for each ping
    z_waterline_offset
        offset from transducer to waterline, positive down
    additional_offsets
        list of numpy arrays for [x (time, beam), y (time, beam), z (time, beam)] offsets

    Returns
    -------
    list
        [xarray DataArray (time, along track offset in meters), xarray DataArray (time, across track offset in meters),
         xarray DataArray (time, down distance in meters)]
    """

    # build the arrays to hold the result, retain the original xarray coordinates to reform the xarray at the end
    acrosstrack_answer = np.zeros_like(beam_azimuth)
    alongtrack_answer = np.zeros_like(beam_azimuth)
    depth_answer = np.zeros_like(beam_azimuth)
    orig_time_coord = beam_azimuth.time
    orig_beam_coord = beam_azimuth.beam

    # convert xarray to numpy
    beam_angle = beam_angle.values
    beam_azimuth = beam_azimuth.values
    two_way_travel_time = two_way_travel_time.values
    surface_sound_speed = surface_sound_speed.values

    # build the cast arrays, have them start at the transducer z depth
    cast_depth_rel_tx, cast_soundvelocity = _convert_cast(cast, z_waterline_offset)

    # each ssv value has it's own table of cumulative horiz dist/raytime
    unique_ssv_values = np.unique(surface_sound_speed)
    for ssv in unique_ssv_values:
        # use the index of where the ssv fits in time to populate the answer arrays
        idx = surface_sound_speed == ssv
        subset_beam_angle = beam_angle[idx]
        subset_beam_azimuth = beam_azimuth[idx]
        ray_parameter = np.sin(subset_beam_angle) / ssv
        max_allowed_sv_layer_value = float(1 / ray_parameter.max())

        # apply surface sv to the cast and clean it up
        cast_depth_rel_tx, cast_soundvelocity = _process_cast_for_ssv(cast_depth_rel_tx, cast_soundvelocity, max_allowed_sv_layer_value, ssv)

        # build out the cumulative depth, horizontal distance and raytime by iterating through the cast layers
        cumulative_depth, cumulative_h_dist, cumulative_raytime = _build_beam_cumulative_tables(cast_depth_rel_tx, cast_soundvelocity, subset_beam_angle)
        # determine the correct acrosstrack/depth for our two way travel time values
        interp_across, interp_down = _interpolate_cumulative_table(cumulative_depth, cumulative_h_dist, cumulative_raytime, cast_soundvelocity, two_way_travel_time[idx])

        # here we use the beam azimuth to go from xy sv corrected beams to xyz soundings
        # relying on the relative azimuth to determine direction/sign
        interp_across = np.abs(interp_across)
        newacross = interp_across * np.sin(subset_beam_azimuth) + additional_offsets[1][idx]
        newalong = interp_across * np.cos(subset_beam_azimuth) + additional_offsets[0][idx]
        newdown = interp_down + additional_offsets[2][idx]

        # populate the answer arrays with this value for this specific ssv value
        acrosstrack_answer[idx, :] = newacross
        alongtrack_answer[idx, :] = newalong
        depth_answer[idx, :] = newdown

    # reform the xarray dataarrays
    alongtrack_answer = xr.DataArray(np.round(alongtrack_answer, 3), coords=[orig_time_coord, orig_beam_coord], dims=['time', 'beam'])
    acrosstrack_answer = xr.DataArray(np.round(acrosstrack_answer, 3), coords=[orig_time_coord, orig_beam_coord], dims=['time', 'beam'])
    depth_answer = xr.DataArray(np.round(depth_answer, 3), coords=[orig_time_coord, orig_beam_coord], dims=['time', 'beam'])

    return [alongtrack_answer, acrosstrack_answer, depth_answer]


def distributed_run_sv_correct(worker_dat: list):
    """
    Convenience function for mapping run_ray_trace_v2 across cluster.  Assumes that you are mapping this function with a
    list of data.

    distrib functions also return a processing status array, here a beamwise array = 3, which states that all
    processed beams are at the 'soundvelocity' status level

    Parameters
    ----------
    worker_dat
        [cast, [beam_azimuth, beam_angle], two_way_travel_time, surface_sound_speed, z_waterline_offset, additional_offsets]

    Returns
    -------
    list
        [xr.DataArray (time, along track offset in meters), xr.DataArray (time, across track offset in meters),
         xr.DataArray (time, down distance in meters), processing_status]
    """

    ans = run_ray_trace_v2(worker_dat[0], worker_dat[1][0], worker_dat[1][1], worker_dat[2], worker_dat[3], worker_dat[4],
                           worker_dat[5])
    # return processing status = 3 for all affected soundings
    processing_status = xr.DataArray(np.full_like(worker_dat[2], 3, dtype=np.uint8),
                                     coords={'time': worker_dat[2].coords['time'],
                                             'beam': worker_dat[2].coords['beam']},
                                     dims=['time', 'beam'])
    ans.append(processing_status)
    return ans
