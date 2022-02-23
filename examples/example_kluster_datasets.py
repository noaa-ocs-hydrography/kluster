# Examples related to understanding kluster processed data, last updated 2/23/2022, Kluster 0.8.10
# uses the multibeam file hstb_kluster/test_data/0009_20170523_181119_FA2806.all
# Written by Eric Younkin

# See https://kluster.readthedocs.io/en/latest/indepth/datastructures.html to learn more

import numpy as np
import xarray as xr
from HSTB.kluster.fqpr_convenience import reload_data
from HSTB.kluster.fqpr_intelligence import intel_process

# we start with one of the preferred processing steps from the data_processing example
_, fq = intel_process(r"C:\data_dir\0009_20170523_181119_FA2806.all")
fq = fq[0]  # this should just be a list of one element if you have just one sonar/day of data, take the first one to get the data
# or we can just reload if you have data from before
fq = reload_data(r"C:\data_dir\em2040_40111_05_23_2017")

# here we are going to discuss exploring kluster processed data.  Kluster is essentially wrapped around xarray datasets,
#  so that users can easily interact with the data using familiar data structures

# in fact, you can even interact with Kluster processed data without using Kluster at all. Simply open the zarr data
#  using xarray and you have all of the data at your fingertips
ping_dset = xr.open_zarr(r"C:\data_dir\em2040_40111_05_23_2017\ping_40111.zarr",
                         consolidated=False, mask_and_scale=False, decode_coords=False, decode_times=False,
                         decode_cf=False, concat_characters=False)
ping_dset.z
# <xarray.DataArray 'z' (time: 216, beam: 400)>
# dask.array<open_dataset-9cfef8bc3a5bc673525761fb14b6ba56z, shape=(216, 400), dtype=float32, chunksize=(216, 400), chunktype=numpy.ndarray>
# Coordinates:
#   * beam     (beam) int32 0 1 2 3 4 5 6 7 8 ... 392 393 394 395 396 397 398 399
#   * time     (time) float64 1.496e+09 1.496e+09 ... 1.496e+09 1.496e+09
# Attributes:
#     _FillValue:  nan

fq.multibeam.raw_ping[0].z
# <xarray.DataArray 'z' (time: 216, beam: 400)>
# dask.array<open_dataset-ae6c0cf06b172dd0a6c7a3df37cafed0z, shape=(216, 400), dtype=float32, chunksize=(216, 400), chunktype=numpy.ndarray>
# Coordinates:
#   * beam     (beam) int32 0 1 2 3 4 5 6 7 8 ... 392 393 394 395 396 397 398 399
#   * time     (time) float64 1.496e+09 1.496e+09 ... 1.496e+09 1.496e+09
# Attributes:
#     _FillValue:  nan

# There are two main datasets in Kluster, attitude and ping.  You will have a ping dataset for each head of the sonar,
#   as the time index is different.  Attitude is separate because Kluster keeps the high freq attitude data whole, for
#   use in processing later.  Kluster used to have a separate navigation dataset, but that was later merged into ping.

fq.multibeam.raw_ping
# [<xarray.Dataset>
#  Dimensions:              (time: 216, beam: 400, xyz: 3)
#  Coordinates:
#    * beam                 (beam) int32 0 1 2 3 4 5 6 ... 394 395 396 397 398 399
#    * time                 (time) float64 1.496e+09 1.496e+09 ... 1.496e+09
#    * xyz                  (xyz) <U1 'x' 'y' 'z'
#  Data variables: (12/34)
#      acrosstrack          (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#      alongtrack           (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#      altitude             (time) float32 dask.array<chunksize=(216,), meta=np.ndarray>
#      beampointingangle    (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#      corr_altitude        (time) float32 dask.array<chunksize=(216,), meta=np.ndarray>
#      corr_heave           (time) float32 dask.array<chunksize=(216,), meta=np.ndarray>
#      ...                   ...
#      tx                   (time, beam, xyz) float32 dask.array<chunksize=(216, 400, 3), meta=np.ndarray>
#      txsector_beam        (time, beam) uint8 dask.array<chunksize=(216, 400), meta=np.ndarray>
#      x                    (time, beam) float64 dask.array<chunksize=(216, 400), meta=np.ndarray>
#      y                    (time, beam) float64 dask.array<chunksize=(216, 400), meta=np.ndarray>
#      yawpitchstab         (time) <U2 dask.array<chunksize=(216,), meta=np.ndarray>
#      z                    (time, beam) float32 dask.array<chunksize=(216, 400), meta=np.ndarray>
#  Attributes: (12/42)
#      _compute_beam_vectors_complete:    Tue Feb 22 18:29:15 2022
#      _compute_orientation_complete:     Tue Feb 22 18:29:14 2022
#      _conversion_complete:              Tue Feb 22 18:29:12 2022
#      _georeference_soundings_complete:  Tue Feb 22 18:29:17 2022
#      _sound_velocity_correct_complete:  Tue Feb 22 18:29:16 2022
#      _total_uncertainty_complete:       Tue Feb 22 18:29:19 2022
#      ...                                ...
#      system_identifier:                 40111
#      system_serial_number:              [40111]
#      units:                             {'acrosstrack': 'meters (+ starboard)'...
#      vertical_crs:                      Unknown
#      vertical_reference:                waterline
#      xyzrph:                            {'beam_opening_angle': {'1495563079': ...]

fq.multibeam.raw_att
# <xarray.Dataset>
# Dimensions:  (time: 5302)
# Coordinates:
#   * time     (time) float64 1.496e+09 1.496e+09 ... 1.496e+09 1.496e+09
# Data variables:
#     heading  (time) float32 dask.array<chunksize=(5302,), meta=np.ndarray>
#     heave    (time) float32 dask.array<chunksize=(5302,), meta=np.ndarray>
#     pitch    (time) float32 dask.array<chunksize=(5302,), meta=np.ndarray>
#     roll     (time) float32 dask.array<chunksize=(5302,), meta=np.ndarray>
# Attributes:
#     reference:  {'heading': 'reference point', 'heave': 'transmitter', 'pitch...
#     units:      {'heading': 'degrees (+ clockwise)', 'heave': 'meters (+ down...

# all of the metadata is stored as a xarray Dataset attribute
assert fq.multibeam.raw_ping[0].attrs['sonartype'] == 'em2040'
assert fq.multibeam.raw_ping[0].attrs['multibeam_files']['0009_20170523_181119_FA2806.all'] == [1495563079.364, 1495563133.171, 47.78890945494799, -122.47711319986821, 47.78942111430487, -122.47841440033638, 307.92]

# Let's say you want to get the xyz from your Kluster dataset
# first we can pull the arrays out of the dataset like so, and get either the xarray, numpy or dask version
x = fq.multibeam.raw_ping[0].x
dask_x = x.data
numpy_x = x.values
# which means building an xyz array simply involves flattening (from time,beam 2d array to 1d array) and concatenating
flatx = fq.multibeam.raw_ping[0].x.values.flatten()
flaty = fq.multibeam.raw_ping[0].y.values.flatten()
flatz = fq.multibeam.raw_ping[0].z.values.flatten()
xyz = np.column_stack([flatx, flaty, flatz])
assert flatx.shape == (86400,)
assert xyz.shape == (86400, 3)

# we could also use the kluster subset module to get the variables that we want intact in the dataset, if you want to keep the dataset around
dset_xyz = fq.subset_variables(['x', 'y', 'z'])
dset_xyz
# <xarray.Dataset>
# Dimensions:            (time: 216, beam: 400)
# Coordinates:
#   * time               (time) float64 1.496e+09 1.496e+09 ... 1.496e+09
#   * beam               (beam) int32 0 1 2 3 4 5 6 ... 394 395 396 397 398 399
# Data variables:
#     x                  (time, beam) float64 5.39e+05 5.39e+05 ... 5.392e+05
#     y                  (time, beam) float64 5.293e+06 5.293e+06 ... 5.293e+06
#     z                  (time, beam) float32 92.74 92.92 92.85 ... 91.94 92.02
#     system_identifier  (time) int32 40111 40111 40111 ... 40111 40111 40111
# Attributes: (12/42)
#     _compute_beam_vectors_complete:    Tue Feb 22 18:29:15 2022
#     _compute_orientation_complete:     Tue Feb 22 18:29:14 2022
#     _conversion_complete:              Tue Feb 22 18:29:12 2022
#     _georeference_soundings_complete:  Tue Feb 22 18:29:17 2022
#     _sound_velocity_correct_complete:  Tue Feb 22 18:29:16 2022
#     _total_uncertainty_complete:       Tue Feb 22 18:29:19 2022
#     ...                                ...
#     system_identifier:                 40111
#     system_serial_number:              [40111]
#     units:                             {'acrosstrack': 'meters (+ starboard)'...
#     vertical_crs:                      Unknown
#     vertical_reference:                waterline
#     xyzrph:                            {'beam_opening_angle': {'1495563079': ...
dset_xyz.to_dask_dataframe()
# Dask DataFrame Structure:
#                   time   beam        x        y        z system_identifier
# npartitions=1
# 0              float64  int32  float64  float64  float32             int32
# 86399              ...    ...      ...      ...      ...               ...
# Dask Name: concat-indexed, 31 tasks

# See https://kluster.readthedocs.io/en/latest/indepth/datastructures.html to learn more
