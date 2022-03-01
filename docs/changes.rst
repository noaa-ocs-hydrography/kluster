Changes List
============

Kluster v0.8.11 (3/1/2022)
---------------------------
 - sync with vyperdatum 0.1.9
    - refine vert datum wkt as seen in Kluster exported bags
    - previously we were setting all datums as heights (i.e. direction=up), ellipsoid vert ref will now be a height (+ up)
    - vert datum wkt axis is now correctly either depth/height with sign convention matching data
 - bug resolved, vessel offsets dialog no longer hides close dialog
 - resolve error messaging bugs
 - update requirements doc

Kluster v0.8.10 (2/24/2022)
----------------------------
 - sync with hstb.drivers 0.2.5
    - check for sonars supported across drivers/kluster
    - debug messaging in par module for failure to read required record in sequential_read_records
    - handle missing gg_data on sequential_read_records

 - bug fixes for different configurations of dual head sonar
 - update the sonar additional offsets for dual head sonar
 - add examples for data exploration and subsets
 - add exporting tracklines to geopackage
 - add routine to cleanly close dask client on closing main window to avoid dask worker cleanup warnings
 - fix the vertical_reference wkt strings, to reflect projected crs, and to include ellipse as a valid vert ref
 - default to sbet when exists for drawing/exporting tracklines
 - fixes for processing/plotting tools when sbets are included without error files
 - add articles to documentation for developers and requirements

Kluster v0.8.9 (2/18/2022)
----------------------------
 - sync with hstb.drivers 0.2.2
    - support for me70 bathymodule
    - convenience methods for file analyzer

 - fixes for running on Ubuntu, tested gui and it works
 - add Tools - File Analyzer for running driver utilities on raw files
 - fix path issues with export grid dialog
 - vdatum layer now masks invalid regions and the coverage bounding box
 - add guidance for running Kluster from source
 - add dockerfile and docker guidance

Kluster v0.8.8 (2/10/2022)
----------------------------
 - sync with Bathygrid 1.2.0
    - add compressed zarr backend as new default backend, roughly 50% space savings on new grids
    - add sounding density, layer plots

 - connect settings dialog and kluster_variables, can now customize default variables in Kluster
 - add plots and calculations to manage surface dialog (right click a surface in project tree)
 - reprocess dialog now allows customizable status setting in a new dialog
 - new right click show in explorer in project tree
 - project epsg setting is now validated for projected/units in meters
 - can now grid only selected lines
 - altered default horizontal/vertical positioning uncertainty to 1.0/0.5 meters respectively

 - reset layout no longer affects saved settings
 - bug resolved with export grid and loading selected data
 - bug resolved with qt/matplotlib backend resolved, reordered imports
 - removed old code that caused issues with graphics and smaller screens
 - resolved dask dashboard issue related to import typing

Kluster v0.8.7 (1/14/2022)
----------------------------
- Resolve patch test issue, will no correctly use the TX Pitch value during Patch Test, was previously incorrectly using RX Pitch
- update github/setup.py for new environment, resolve environment yml file issues
- resolve issues with converting and processing files without pings (that only have attitude/navigation) including drawing line navigation, plots, and geohash computation
- automatically exclude all 9999.all files

- bug fix skip attempting to reload data when path is not a directory
- bug fix skip color generation for soundings when Points View is empty
- bug fix resolve dask type checking issue that breaks sphinx
- bug fix exclude all 9999.all files from intelligence module
- bug fix skip color generation for soundings when Points View is empty
- bug fix skip reload converted data attempts for new path if it is not a directory

Kluster v0.8.6 (1/6/2022)
-------------------------
- move to new Python 3.8.12 environment, notable changes:

   - QGIS = 3.18.3
   - GDAL = 3.3.1
   - Dask = 2021.12.0
   - Xarray = 0.20.2
   - Zarr = 2.10.3

- ensure array passed as input to xarray interp
- bugfix for vyperdatum in georeferencing
- bugfix issue warning instead of exception on getting soundings without georeferencing
- bugfix correct the default arguments in fqpr_drivers.sequential_read_multibeam

Kluster v0.8.5 (1/4/2022)
---------------------------
- aligns with vyperdatum 0.1.8

  - allow including additional custom regions outside of the vdatum structure

- File - Open Converted to add existing converted data
- File - Open Surface to add existing surface
- add new Fqpr manage dialog (right click Converted data in project tree and select 'Manage')
- Fqpr manage dialog can remove sbets and sv profiles from Converted data
- add new Surface manage dialog (right click Surface data in project tree and select 'Manage')
- new Sonar Acceptance documentation
- refactor to move all driver related code to fqpr_drivers for upcoming s7k test
- add new sonar reference point attribute to track the sonar manufacturers chosen reference point
- lower the default vertical/horizontal uncertainty to 0.3/0.5 m 1sigma respectively
- Setting a new processing status will now issue a message

- bugfix with nan and geohash resolved
- bugfix with load sbet, set all sbet gaps to nan using max_gap_length
- bugfix with svcorrect, automatically extend profiles on sound velocity correction
- bugfix with monitor, check for file size change to ensure file has finished copying (should now work with kongsberg logging)
- bugfix accuracy test order 1 line to be grey
- bugfix accuracy test reverse angle x axis so that port side is on the left
- bugfix add accuracy test validation for horizontal and vertical crs between surface and data
- bugfix the Extinction test water depth line colors to work with dark mode

Kluster v0.8.4 (12/17/2021)
---------------------------
- aligns with drivers 0.1.19

  - bugfix support installation parameters in older .all files
  - bugfix catch .all EM124 start byte issue
  - bugfix KMALL move to MRZ for navigation, as I find some lines do not have navigation in the raw sample datagram
  - bugfix KMALL will correctly isolate last record for reading line end time

- add new manual patch test button to points view, will run on data currently displayed in points view
- add new show lines button to points view, will only show the lines you have checked in this dialog in points view
- add NAD83 ma11 pa11 as a new coordinate system option
- add more explicit roll/pitch/heading sign convention descriptions to GUI
- add ability to create subset of Fqpr by multiple time ranges
- added reprocessing by sonar head when desired
- added ability to get data by system/time from the project

- bugfix for building CRS from datum/projected
- bugfix solve reprocessing subset while keeping other data in Points View display
- bugfix with returning points in polygon for specific head
- bugfix turn off the points view title bar to prevent users from trying to undock (Vispy crashes when you try)
- bugfix will now not clean/select points in points view if they are not visible (see show rejected/hide lines)

Kluster v0.8.3
--------------
- aligns with vyperdatum 0.1.6

  - issue a warning when unable to read vdatum polygon files
  - select the correct geoid in the Puerto Rico region

- sync with bathygrid 1.1.6

  - bug fix for permissions error on rebuilding the grid and overwriting grid metadata
  - bug fix for searchsorted unsupported warning on bin indices with new numpy based tile edges
  - add ability to calculate slope in xy directions using least squares plane fit

- added new patch test module and dialog, currently not available in GUI as improvements are being made
- converting lines now logs start/end time, start/end position, and line azimuth as attributes to ping record
- selecting a line shows these attributes in the Explorer window
- added in depth data structures article
- installer now correctly copies over the bag_template file required for bag exports
- add ability to subset by line names
- disable adding additional converted data in plots, but allow loading from selected lines instead
- selecting lines in 2dview will now select line in the project tree correctly
- add new ability to copy a segregated copy of the Fqpr object
- test improvements by hannahmunoz with new setup/tear down capabilities
- new method for splitting xyzrph by timestamp

- if a sonar head is disabled (set to None) fqpr_generation will now correctly skip the disabled head
- fixed issues with in memory workflow used by reprocess_sounding_selection
- hide output messaging with in memory workflow
- do not update attributes on disk with in memory workflow
- reprocess_sounding_selection now optionally returns soundings for feeding patch test
- stop all drawing/redrawing events while surface is loading
- bug fix with using the basic/advanced plots and selecting the converted container
- add warning to wobbletest for when there isnt enough data
- fix bug with loading layers with 'depth' in name
- bug fix for sound velocity map, correctly computes navigation segments
- bug fix with updating surface, now correctly formats the resolution parameter

Kluster v0.8.2
--------------
- bathygrid tile outlines now change color in dark mode
- add string representation to kluster fqpr object
- add new examples folder with code examples for using Kluster in the console
- new jupyter notebooks matching examples additions
- bug fix for intel processing when input is directory

Kluster v0.8.1
--------------
- aligns with bathygrid 1.1.3

  - add density resolution estimation method
  - add density layer for display
  - add ability to set density color ranges
  - add hillshade layer for display
  - add tiles layer for display
  - allow loading pre 1.1.0 grids that do not have density
  - save lengthy metadata to array instead of json for bgrid metadata

- new dark mode view
- add smaller tile size options for variable resolution gridding
- better error message when unable to build epsg from user provided coordinate system, zone, hemisphere
- better messaging with force coordinate system
- warning message for when you are unable to load from surface/converted
- update tooltips, documentation

- bug fix for loading converted data after moving the folder
- bug - reset superselection on selecting new points to avoid index error
- correct status flag colorbar labels
- only query shown layers
- only color on select multibeam lines (not tiles)
- update surface correctly clears the loaded surface layers after reloading
- select skips tiles for loading line attribution

Kluster v0.8.0
--------------
- greatly improve performance with NOAA_MLLW NOAA_MHW datum selections by sampling points passed to vyperdatum
- 3d Points view now supports selection/cleaning mouse actions
- new re-accept detectioninfo flag for when the user chooses to accept points manually in Points View
- new clean/accept/undo events in Points view for manually cleaning data
- new show rejected flag to hide rejected soundings
- cleaning points updates a new _soundings_last_cleaned attribute for tracking date of last cleaning action, used to mark grids as out of date
- closing Kluster now saves the last used Points view settings to then reload on startup
- selection/cleaning mouse events now draw a rectangle to the screen to show the selection area

- add this changes list to track changes
- align with bathygrid 1.0.17 - resolve bugs on reloading after altering folder path or name
- align with vyperdatum 0.1.4 - support specific vdatum versions, include 3d transformation/geoid selection by region
- resolve bug with vyperdatum CRS, now correctly shows positive down
- resolve bug with loading bathygrid layers that have decimal point in name
- set new vdatum directory path will run the vyperdatum vdatum version check on setting global settings
- resolve bugs with matching sbet to converted multibeam data
- resolve bug with project not updating buffered attribution on attribution change
- resolve bug with rotation and cleaning actions in Points view 3d
- bug with filter_subset_by_polygon, intersect hashes cannot include inside hashes, was loading double the points in some instances
- bug with Points view - correctly maintain head index when adding points from dual head sonar

Kluster v0.7.11
---------------
- Remove duplicate installation parameters on conversion. Duplicates are determined only based on changes to important fields (i.e. offsets, angles, waterline)
- Simplify profiles when profile layers exceed maximum set by Kluster
- Correctly sort multibeam files by start time instead of file name when converting, eliminates need to reorder data on disk
- Correctly sort multibeam datasets post conversion when pings are found to be out of order.
- Bug fix - when reloading previous used string settings for a dialog, will now set value to '' instead of 'None' when value is not set

Kluster v0.7.10
---------------
- SBET import now imports to ping record instead of separate dataset
- Loading data for points view now occurs in its own thread
- Points view now has new toggleable 2d/3d view instead of separate tools
- Points tool now alters the color of the box to provide feedback on use
- improvements to returning variables by filter
- Show only offline docs in the built Kluster exe, online docs seem to be slow to change and should be used as a backup alternative
- Bug - kongsberg .all import would sometimes use the incorrect model number and or single/dual determination
- Bug - clear out worker data after running
- Bug - disable drag and drop in project tree
- Bug - with closing project using the right index in the project tree
- Bug - with loading force coordinate system setting on startup
- Bug - with stopping the progress bar on completing import sbet and overwrite raw nav
- Bug - with using the SBET datum instead of the default input datum on georeference
- Bug - sbet validation now works when no sbet has been imported yet

Kluster v0.7.9
--------------
- surfacing efficiency improvements during gridding
- new processing modes in settings - normal, convert only, concatenate
- updated CLI for intelligence changes
- updated docs, new docs for indepth info
- bugfix closing data keeps the log file open, this should not happen now
- bugfix using the subset time option in the processing convenience function now works correctly
- bugfix kmall driver and maintaining unique times across ping record
- bugfix resolve icon issue with pyinstaller

Kluster v0.7.8
--------------
- sync with bathygrid 1.0.14 - improvements to the gridding process to avoid looping in python
- new variable 'geohash' - tracks the geohash cell for each sounding, used as a spatial index when querying points for points view widget
- new attribute 'geohashes' - saves to the Dataset attrs the unique geohashes for each line
- geohash is an encoded bytestring, saves space (byte per char vs 4 bytes per char)
- return_soundings_in_polygon now uses the geohash to pre-filter the data before the brute force x y query
- allow for nadir_geohash during export of data
- pointsview - add head index to the system identifier, color by system shows head number
- bug fix - disable adding to project through dialogs
- bug fix - fix for loading project from kluster project json file

Kluster v0.7.7
--------------
- points view allows viewing points in the direction of the arrow displayed on the 2dview box
- change box display to be easier to see
- add property for finding the last data change date in an fqpr instance
- revamped the surface update dialog, allow for manual update of points, reads the last added date to determine which containers need to be updated
- all dialogs now retain settings correctly
- regridding correctly tracks existing resolutions where no updates to the grid are needed
- gridding will skip tiles if regrid option is update and points count hasnt changed
- add new ability to set sounding flag based on superselection in points view, not hooked up just yet
- add tvu/thu plots to basic plots - custom - uncertainty
- select tool now selects lines based on intersection using QGIS request, much faster and more powerful
- open project worker now only loads the fqpr/surface data
- worker results will add the newly loaded data to the project
- not passing the project to the worker seems to get past the intermittent hard crash seen on loading lots of data at once
- bug fix with surfaces, clear data will now correctly close all surfaces
- bug fix with savestatedialog, casts text values to string
- bug fix to ensure vessel setup only updates the selected fqpr container

Kluster v0.7.6
--------------
- allow drag and drop events on any part of the main window
- refine 3d models in Vessel Setup to have better default positions
- restructuring project, new 'subset' module to hold subset/sounding select code
- raise error on trying to reload data that has missing data
- new parameters for setting up Dask Client - LocalCluster mode
- docs and tests

Kluster v0.7.5
--------------
- Exporting LAS now includes the Kluster horizontal system in the header
- Exporting soundings now exports in chunks to resolve memory errors
- New - Export soundings for selected lines
- New - Export soundings for only those points in the Points View
- Selecting lines in Project Tree now shows data and highlights all lines selected
- Adding new instances to Project Tree now sorts alphabetically
- Update guidance for new 128 meter tile size benchmark
- Improve performance in gridding (approx 15% improvement) related to moving from flatiter to unravel_index
- Fix bug with QGIS not initializing properly on startup
- Fix bug with Shoalest gridding algorithm and grid initialization
- Fix bug with gridding not honoring rejected soundings
- Fix bug with progress bar halting while running multiple threads

Kluster v0.7.4
--------------
- New documentation system, help menu item for viewing online/offline
- changing latency in vessel setup generates full processing action on change, same as changing angle values
- vessel setup labels vessel files as 'Vessel File'
- saving changes to multibeam from vessel setup retains changes in vessel model setup (basic config)
- resolve bug with navigation in custom sound velocity map plot

Kluster v0.7.3
--------------
- Add the ability to handle two dataset instances in the plot data handlers
- Add ability to right click 'surfaces' category and set min max values
- Rebuild 2dview - constant scale, altering color/selecting points now does not force redraw, depth/x/y now track actual values,
- Clean up accuracy test, show full uncertainties, remove old percentage plots
- disable overscale layer in ENC
- show action tooltip on next action as well
- bug fixed with altering box after third click

Kluster v0.7.2
--------------
- Export variable/dataset now exports time as a float, add more precise rounding to the exported variables
- exports now support the reduction methods and zero_centered options that are in the plotting widget
- disable the export buttons for custom plots that have no export
- create unique filenames for the exported files
- add show youtube playlist to the help file menu bar
- Fix darkness in 3d plot based on camera direction
- Fix bug with selecting surface layer checkbox, now correctly hides/shows layers
- removing a surface now updates the global min_max band values for all surface layers
- Fix bug with accuracy test and soundings outside the surface extents
- Fix bug with VR Surfaces - will now load all resolutions on selecting layer checkbox
- Add message on drawing surface to indicate something is happening

Kluster v0.7.1
--------------
- added new advanced plot type 'Accuracy Test'
- added export variable option to basic plot
- added export dataset option to basic plot
- changed default coordinate system to WGS84 to handle out-of-bounds datasets without issue for new users
- force las exports to be z positive up
- sounding export files now have matching names with containing folder
- alter tvu/thu 2 sigma factor to 1.96 rather than 2
- update tests and docs
- update command line options

Kluster v0.7.0
--------------
- move navigation from external dataset to the ping record datasets. Should improve load times, decrease memory consumption and improve processing times.
- new setting under file - settings, "Force all days to have the same coordinate system", see tooltip
- Grids now load and export as tiles, will allow sparse grids over huge areas to load efficiently and export successfully
- Kluster will now skip successfully over multibeam files that are unable to be read
- Dask client will now automatically restart when memory leaks exceed memory capacity threshold
- Conversion now operates over chunks of files to handle memory errors seen when converting too many files at once
- You can now update offsets/angles/tpu values in the vessel setup window without having to use the vessel file
- add support for laspy >= 2.0 when exporting soundings
- improvements for writing to disk when datasets are very large, now correctly writes chunks of data, sorts, and resizes data on disk without loading the whole dataset to memory
- draw navigation, loading datasets, loading surface are all threaded now, will not lock up the screen
- progress moved to main window toolbar, progress bar will now stop running when action fails
- query tool now only displays layers that are under the cursor
- plots use the already loaded datasets instead of reloading
- import/overwrite navigation now handles dualhead times and returns safely if there is no time overlap between source and ping record dataset
- ping record retains min max georeferenced x and y as attribute
- improve performance in 2dview 3dview loading times
- bug - move h5py install recommendation to conda to avoid dll errors
- bug with show surface not correctly returning whether the surface was shown (forced rebuild where unnecessary)
- bug fixed where data chunk without attitude records will now be dropped
- bug fixed where georeference actions were generating based on the wrong CRS attribute

Kluster v0.6.6
--------------
- forgot the format string for surface generation
- hide gdal errors on checking if layers are loaded
- bug for zooming to surface extents

Kluster v0.6.5
--------------
- improvements to reload speed (thanks to work ensuring data is written in correct time order without duplicate times)
- improved reload speed by dropping unnecessary zone number calculation
- .all driver - sorts/drops unique times in attitude and navigation
- remove all NaN values before adding data to grid
- handle NaN values with georeference and MLLW/MHW selection
- Bug fix with clicking on surface name, no longer tries to load surface layer
- grids now contain minumum/maximum time from the data
- grids now contain the Vertical CRS WKT string if using MLLW/MHW
- gridding in parallel now dumps to disk between groups (no longer eats up huge memory)
- grids exported to BAG have correct band min/max values, handles the current bug in GDAL (resolved in GDAL 3.3.2)
- grids exported to BAG now have _rxl file that allows Caris to understand the coordinate system.
- export grid dialog is now populated with the vertical CRS WKT string
- clean up time elapsed strings so it isn't just 235980235 seconds elapsed.

Kluster v0.6.4
--------------
- kmall - fix for incorrectly translated detection info flag
- converting multibeam files now correctly drops empty files/chunks of data that have no pings
- add in .close() for the multibeam classes to clear file handler
- _zarr backend - now reorders data on disk to ensure data is in order of ascending times
- disable sorting/dropping duplicate times on reload to conserve memory, rely on data being in correct time order
- add Help - About screen with versions
- slice_xarray_by_dim no longer uses xarray sel, does it in numpy instead, this is much more memory efficient
- move to np.argmin instead of daskarray.argmin() to clear deprecation warnings
- fix for project return_project_folder incorrectly returning relative path
- fix for intel process using isdir on non string filname
- fix for intel process, will load an existing project now
- fix for pyinstaller routine - will carry over the correct qgis files for loading WMS layers

Kluster v0.6.3
--------------
- update setup to include later versions of modules
- removed old quadtree gridding
- include bathygrid for gridding routines
- bathygrid supports single and variable resolution surfaces
- bathygrid can export variable resolution with one file per resolution
- bathygrid supports gridding in parallel with Dask
- bathygrid supports updating surface (right click the surface) for new data
- bathygrid shows attribution in attribute window on left click

Kluster v0.6.2
--------------
- add ability to rotate 2d/3d point selection in map view
- clear old ping selection on selecting new 2d/3d point data
- force tooltips to show immediately
- added support for em304, em712 sonar
- fixes to KMALL driver to support new sonar data/formats
- bug resolved when duplicate times are found after converting multibeam data

Kluster v0.6.1
--------------
Skipped to v0.6.2 due to issues with the release

Kluster v0.6.0
--------------
- Move TPU parameters into the xyzrph record
- Add beamangle TPU calculation
- New class for managing vessel files and updates (fqpr_vessel)
- vessel file will update by carrying over the nearest tpu entries, and will only update when the entry is a new one (or on waterline change if option is checked)
- new actions generated when vessel_file presents new offsets or new tpu parameters
- new intelligence routine to build actions on comparing vessel file xyzrph and existing fqpr xyzrph
- new intelligence routine only triggers re-svc when new applicable casts are added
- kluster_main - Add new/open vessel file
- add entry in project tree for vessel file
- add right click - reprocess action in kluster main
- new procedure for point selection, add tooltips for map tools
- Vessel Setup now allows displaying offsets/angles from multiple serial numbers at a time
- Vessel Setup shows source and config file
- Vessel Setup allows for altering/adding timestamped entries
- add latency to vessel view
- add tpu to vessel view
- bug fixed where writing to zarr attributes now skips if doing the in memory workflow
- bug fixed with writing attributes, always generates deep copy first
- bug fixed with loading xyzrph entries that lacked imu/antenna entries in vesselview

Kluster v0.5.2
--------------
- added new backends for data storage, zarr backend the only one for now
- xarrayconversion/fqprgeneration now inherit from zarr backend
- fqpr_generation processes now load data - dump to disk all at once, eliminates memory issue with loading all the raw data and then chunking it off for processes.
- bug resolved with loading attribution in reload_data, now correctly combines attribution from all datasets
- stick with default two threads per worker, seen good results in tests
- set default number of chunks to a kluster_variable

Kluster v0.5.1
--------------
- added a 2d swath view
- querying in 2d and 3d
- queried points show attributes in explorer widget
- separate out commonly accessed variables to kluster_variables
- add axis to 2d and 3d
- add colorbar to 2d and 3d
- controls for showing/hiding colorbar/axis

Kluster v0.5.0
--------------
- new 3dview using vispy Scatter data
- added vdatum integration with vyperdatum, new NOAA MLLW and NOAA MHW options
- Pass vdatum path directly to georeferencing process
- Move all references to xyz_crs to horizontal_crs
- add new ability to return soundings in box, added to fqpr_gen and project
- add in intel convenience functions
- add command line functions for intel module
- update all vert_ref references to include the new noaa mllw/mhw
- dialogs now update the ini file settings
- project settings dialog issues warning regarding vdatum directory
- correctly update the vdatum_directory attribute for the 2dview
- scatter the times for xarray_helpers interp_across_chunks
- changing project settings no longer alters the current_processing_status attribute
- changing project settings generates processing actions based on new vert/coord system
- resolve bugs with settings dialogs not reading ini file properly, not writing new settings correctly
- correct docstrings

Kluster v0.4.10
---------------
- bug with _closest_key_value, need an abs there
- fqpr_generation.Fqpr should skip the logger build if not provided
- gdal.__version__ does work, skip the custom code ive written in gdal_helpers
- use update in VectorLayer when file does not exist as well, for write access
- VectorLayer must create layer with the same name as the file before I can create a layer with a custom name
- VectorLayer should set lyr = None to close and save the layer
- rename UTC to follow convention
- add svp, tif, shp test files
- add tests for most of the remaining modules

Kluster v0.4.9
--------------
- added import ppnav dialog back in to manually import sbet
- added new overwrite navigation dialog to import from posmv
- added overwrite navigation convenience/generation code
- trigger action progress bar on import/overwrite nav
- up the default number of retries on PermissionError