# Examples related to gridding, last updated 11/3/2021, Kluster 0.8.2
# uses the multibeam file hstb_kluster/test_data/0009_20170523_181119_FA2806.all
# Written by Eric Younkin

# gridding is the process of taking point data (x y and z) and grouping/binning the data within cells of a predetermined
# size (resolution).  Kluster uses the bathygrid module (https://github.com/noaa-ocs-hydrography/bathygrid) to do this
# gridding.  Points are split into tiles, and those tiles contain the points/grid data.  Grids can be single resolution,
# where all tiles contain grids of the same size, or a tile can be variable resolution, where each tile can be its own
# resolution.

from HSTB.kluster.fqpr_convenience import reload_data, generate_new_surface, update_surface, reload_surface
from HSTB.kluster.fqpr_intelligence import intel_process

# we start with one of the preferred processing steps from the data_processing example
_, fq = intel_process(r"C:\data_dir\0009_20170523_181119_FA2806.all")
fq = fq[0]  # this should just be a list of one element if you have just one sonar/day of data, take the first one to get the data
# or we can just reload if you have data from before
fq = reload_data(r"C:\data_dir\em2040_40111_05_23_2017")

# from here, we simply generate a new grid.  Let's start with a simple one, a single resolution grid (where the cells
#  are all the same size/resolution) and that resolution is automatically determined by the depth of the data.

# the points from the Fqpr Kluster object will be added to the grid object, and the grid object will automatically run
#  the gridding routine with the 'mean' algorithm

# the one setting we will use is the output_path, where we specify the output directory.  If we were not to do this,
#  the grid will exist entirely in memory, which might cause memory issues later
bs = generate_new_surface(fq, output_path=r"C:\data_dir\mygridpath")

# adding points and gridding will set some useful attributes in the class
bs.output_folder
# Out[8]: 'C:\\data_dir\\mygridpath'
bs.min_time  # this is based on the data time, not the time it was added
# Out[9]: '2017-05-23T18:11:19'
bs.max_time
# Out[10]: '2017-05-23T18:12:12'
bs.min_x
# Out[11]: 538624.0
bs.max_x
# Out[12]: 539648.0
bs.min_y
# Out[13]: 5292032.0
bs.max_y
# Out[14]: 5294080.0
bs.resolutions  # this grid is a single resolution 8 meter cell grid
# Out[16]: [8.0]

# as mentioned before, data is organized first in tiles.  These tiles correspond to the folders in the grid directory.
# here we have two total tiles that represent the grid.  Each is 1024 meters square.
bs.tile_size
# Out[19]: 1024.0
bs.tiles
# Out[17]:
# array([[<bathygrid.tile.SRTile object at 0x000002297B4FA430>],
#       [<bathygrid.tile.SRTile object at 0x000002297C655820>]], dtype=object)

# inside each of the tiles should be the grid and points that exist within the tile boundary
tile = bs.tiles[0][0]

# points
tile.data
# Out[23]: dask.array<from-npy-stack, shape=(48322,),
#          dtype=[('x', '<f8'), ('y', '<f8'), ('z', '<f4'), ('tvu', '<f4'), ('thu', '<f4')],
#          chunksize=(48322,), chunktype=numpy.ndarray>

# grid data
tile.cells
# Out[24]:
# {8.0: {'depth': dask.array<from-npy-stack, shape=(128, 128), dtype=float32, chunksize=(128, 128), chunktype=numpy.ndarray>,
#   'density': dask.array<from-npy-stack, shape=(128, 128), dtype=int32, chunksize=(128, 128), chunktype=numpy.ndarray>,
#   'vertical_uncertainty': dask.array<from-npy-stack, shape=(128, 128), dtype=float32, chunksize=(128, 128), chunktype=numpy.ndarray>,
#   'horizontal_uncertainty': dask.array<from-npy-stack, shape=(128, 128), dtype=float32, chunksize=(128, 128), chunktype=numpy.ndarray>}}

# depth gridded data for the 8.0 meter resolution grid, can see that it is mostly empty space in this case
tile.cells[8.0]['depth'].compute()
# Out[27]:
# array([[nan, nan, nan, ..., nan, nan, nan],
#        [nan, nan, nan, ..., nan, nan, nan],
#        [nan, nan, nan, ..., nan, nan, nan],
#        ...,
#        [nan, nan, nan, ..., nan, nan, nan],
#        [nan, nan, nan, ..., nan, nan, nan],
#        [nan, nan, nan, ..., nan, nan, nan]], dtype=float32)

# since we chose resolution=None, and auto_resolution_mode=depth (the default arguments in generate_new_surface), the resolution
#  for the tile cells will be created using the depth resolution lookup and the tile mean depth.  Next category after 90 meters is
#  the 160 meter depth range, which corresponds to 8 meter resolution.

from bathygrid.bgrid import depth_resolution_lookup

tile.mean_depth
# Out[28]: 90.84050750732422
depth_resolution_lookup[80]
# Out[31]: 4.0
depth_resolution_lookup[160]
# Out[32]: 8.0

# now we go back to the grid to look at data management.  When we added points to it from the fq object, they were registered
#  under a tag in the grid object container.  The grid retains the file that the points came from as well as the date they were
#  added.

# you'll see that the data key is 'em2040_40111_05_23_2017_0' with the _0 at the end.  Since there can be millions of points
#  in one Fqpr object, we sometimes have to add in chunks.  If that were the case, you would see 'em2040_40111_05_23_2017_1',
#  'em2040_40111_05_23_2017_2', etc.

bs.container
# Out[33]: {'em2040_40111_05_23_2017_0': ['0009_20170523_181119_FA2806.all']}
bs.container_timestamp
# Out[34]: {'em2040_40111_05_23_2017_0': '20211103_174307'}

# The tile is the object that actually holds the points, so we can look at the tile container to figure out which points are
#  from which container.  Here we can see that all the points from index 0 to index 48322 belong to this container.
tile.container
# Out[35]: {'em2040_40111_05_23_2017_0': [0, 48322]}

cont_data = tile.data[0:48322]

# this allows us to easily add/remove points and track where they come from.  If we update the points in Kluster by
#  re soundvelocity correcting or something and we need to then update the points in the grid, we can just remove all the points
#  associated with that Fqpr object and re-add them.  This is what update_surface does.

# lets show how to update the points by adding/removing this fqpr object
bs, oldrez, newrez = update_surface(bs, add_fqpr=fq, remove_fqpr=fq)
# Out[37]:
# Removing Points from em2040_40111_05_23_2017_0: |██████████████████████████████████████████████████████████████████████| 100.0% Complete
# Adding points from em2040_40111_05_23_2017 in 1 chunks...
# Adding Points from em2040_40111_05_23_2017_0: |██████████████████████████████████████████████████████████████████████| 100.0% Complete
# Gridding SRGrid_Root - mean: |██████████████████████████████████████████████████████████████████████| 100.0% Complete

# so we can see that the points were removed, added, and the tiles were then gridded.  It is kind of brute-force, non
#  elegant, but it is pretty reliable.

# there are other routines you can do with update_surface.  You can also do a full regrid of the data if you like, although
#  this really shouldn't ever be necessary.  Ideally you should be able to update the grid (regrid_option=update) while adding
#  removing points as we did earlier
bs, oldrez, newrez = update_surface(bs, regrid_option='full')
# Gridding SRGrid_Root - mean: |██████████████████████████████████████████████████████████████████████| 100.0% Complete

# so you can see that the grid was updated without adding or removing points.

# finally let's talk about exporting the grid.  bathygrid allows you to export to GDAL formats, although only GeoTIFF and
#  BAG are supported currently, as well as a more basic csv option.  GeoTIFF is the simplest, so let's start there.

bs.export(r'C:\data_dir\mygridpath\mytiff.tif', export_format='geotiff')

# you'll see a 'mytiff_8.0_1.tif' file in there, implying that it is a 8.0 meter tif and is the 1st of the series.  Bathygrid
#  will export in tiles, so you might get a 'mytiff_8.0_2.tif', etc. as well.  This prevents the exported grids from being
#  too large to interact with efficiently.

# if you were to generate a grid in variable resolution mode, you would get one tif per tile, per resolution.  Bathygrid
#  does not currently export using multipage tifs or variable resolution BAGs.

# you can do the same thing with BAG, although BAG is a little more metadata heavy.  You can just leave the defaults or you
# can populate some of the keyword arguments.  Check the docs to learn more about the possible options.
bs.export(r'C:\data_dir\mygridpath\mybag.bag', organizational_name='noaa', position_name='scientist?', export_format='bag')
