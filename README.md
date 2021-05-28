# kluster

[![Build Status](https://travis-ci.com/noaa-ocs-hydrography/kluster.svg?branch=master)](https://travis-ci.com/noaa-ocs-hydrography/kluster)
[![Documentation Status](https://readthedocs.org/projects/kluster/badge/?version=latest)](https://kluster.readthedocs.io/en/latest/?badge=latest)
[![Binder](https://binder.pangeo.io/badge_logo.svg)](https://binder.pangeo.io/v2/gh/noaa-ocs-hydrography/kluster/master)

Documentation: [readthedocs](https://kluster.readthedocs.io/en/latest/) 

Youtube Series: [Kluster Playlist](https://www.youtube.com/playlist?list=PLrjCvP_J9AA_memBs2ZyKXGHG1AMx0GWx)

A distributed multibeam processing system built on the [Pangeo ecosystem](https://pangeo.io/). Supports Kongsberg .all/.kmall multibeam formats, POS MV post-processed SBET/RMS navigation files and Caris svp sound velocity profile files.

Kluster provides a fully open source hydrographic processing package to produce accessible bathymetry products in support of ocean mapping.

Kluster is:

1. **Scalable** - uses [Dask](https://dask.org/) to provide distributed parallel processing on everything from a laptop to a cloud service (AWS Fargate for example)
2. **Cloud ready** - uses [Zarr](https://zarr.readthedocs.io/en/stable/) as a cloud ready storage format for converted multibeam records and processed soundings
3. **Open** - data are presented using [Xarray](http://xarray.pydata.org/en/stable/) objects for easy interactivity and stored with Zarr, all open formats
4. **Scriptable** - provides a GUI for visualization and processing, but can be run from the command line or scripted easily
5. **Extensible** - From data conversion to sound velocity correction, kluster is built using modules that can be replaced, enhanced or exchanged as needed.

Kluster has been tested on:

- EM2040/2040c/2040p
- EM2040 dual tx/dual rx
- EM710
- EM3002
- EM302
- EM122

Kluster is built from the ground up in Python, and was developed using Python 3.8.  Kluster includes modules developed by the hydrographic community such as (see [drivers](https://github.com/noaa-ocs-hydrography/drivers)):

- kmall - Kongsberg kmall file reader
- par3 - Kongsberg .all file reader
- sbet - POSPac sbet/rms file reader

Kluster is a work in progress that has been in development since November 2019 by a small 'team', and is by no means feature complete.  If you are interested in contributing or have questions, please contact Eric Younkin (eric.g.younkin@noaa.gov)

## Why Kluster?

There are three principle motivations behind kluster:

1. ##### Build a multibeam processing sandbox for scientists/engineers

The hydrographic community is continuously innovating.  Oftentimes, we want to experiment with an algorithm or technique, but the data is inaccessible, or relies on intermediate products that are locked within the software.  How do you get attitude corrected beam vectors into a numpy array?  How can I test a new gridding algorithm without exporting soundings to text first?

2. ##### Build a multibeam cloud processing system for field use/production

Cloud data storage and processing is quickly becoming a reality, as the advantages of not owning your own infrastructure become apparent.  Where does this leave processing software and our traditional workflow?  Kluster is designed from the ground up to address this issue, by providing processing that can be tailored and deployed in multiple different ways depending on the application.  In addition, using the multiprocessing capabilities of Dask, kluster provides a powerful tool that can compete with existing software packages in terms of performance.

3. ##### Evaluate the latest in open source scientific software

Much of the existing open source software related to multibeam processing has been in development for decades.  There has been an explosion in scientific libraries that can benefit the hydrographic community as a whole that have not been seriously evaluated.  Kluster relies on the state of the art in Python libraries to provide a sophisticated and modern software package.

## Installation

**We recommend that users try to run Kluster using the release attached to this GitHub repository, see [releases](https://github.com/noaa-ocs-hydrography/kluster/releases)**

Kluster is not on PyPi, but can be installed using pip alongside the HSTB-drivers and HSTB-shared modules that are required.

(For Windows Users) Download and install Visual Studio Build Tools 2019 (If you have not already): [MSVC Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

Download and install conda (If you have not already): [conda installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Download and install git (If you have not already): [git installation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

Some dependencies need to be installed from the conda-forge channel.  I have an example below of how to build this environment using conda.

Perform these in order:

`conda create -n kluster_test python=3.8.8 `

`conda activate kluster_test `

`conda install -c conda-forge qgis=3.18.0 vispy=0.6.6 pyside2=5.13.2 gdal=3.2.1`

`pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster `

## Quickstart

Here we will show how to process through a GUI or through a console.

1. ##### Kluster through the GUI

Start the main GUI by running the kluster gui module:

`C:\>python -m HSTB.kluster`

Once the Kluster window appears, simply:

- Go to 'Setup - Set Project Settings' and make sure the default settings look good for your system.
- Create a new project ('File' - 'New Project') and point to a new empty folder, if you want to put all your processed data somewhere. Otherwise, processed data will be created next to the raw multibeam files. 
- Drag a multibeam file (Kongsberg .all/.kmall) into the 'Project Tree' window. You can also drag in multiple files, but maybe stick with just one for this test.
- You will see a new action in the 'Actions' tab. Hit 'Start Process' to convert the multibeam data.
- Go to 'View - Layer Settings' to enable a background layer
- (Optional) Drag in SBET/SMRMSG/POSPac Export Log files or Caris SVP files, and note the new actions that pop up.
- Use the 'Actions' tab - 'Unmatched Files' to get information on why some files might not be matched with converted data (mouse over to view the ToolTip).
- Press 'Start Process' again to perform the multibeam processing.
- Select a Converted data instance in 'Project Tree' and look at the 'Attributes tab' to get all the processed data attribution. 
- Select the 'Points View' tab and use the Points Select or Swath Select tools in '2d View' to get a view of the soundings
- Select the 'Console' tab at the bottom and right click the converted data path under 'Converted' in the 'Project Tree' and click 'Load in console' to get access to the xarray Datasets in the console.  Try 'first_system.soundspeed.plot()' to plot the surface sound speed used for the sonar!
- Select a converted container in Project Tree and use 'File' - 'Export Soundings' to generate csv files for the processed soundings (x, y, z, uncertainty)
- Select a converted container in Project Tree and use 'Process' - 'New Surface' to generate a single resolution surface using the processed sounding set. Visualize the surface by checking one of the layers in the 'Project Tree' under 'Surfaces'. Use the magnifying glass in 2d view if you need to zoom in to see the surface. Surfaces are saved in the numpy compressed file format, and can be easily read using numpy.
- Select a converted container in Project Tree and use 'Visualize' - 'Basic Plots' to plot all the converted and Kluster made datasets.
- Select a converted container in Project Tree and use 'Visualize' - 'Advanced Plots' to see some of the more sophisticated tools available for data analysis

You can also reload the generated multibeam data and surface by:

- Going to 'File' - 'Open Project' and opening the kluster json file that is generated when you process data.
- dragging in the output directory (see the second bullet above) to the Project Tree to load the multibeam records
- dragging in the grid folder to the Project Tree to load the surface

2. ##### Kluster through the console

The quickest start would be to use the `perform_all_processing` function from the *fqpr_convenience* module.  This relies on you having a Kongsberg .all or .kmall file to experiment with.

`from HSTB.kluster.fqpr_convenience import perform_all_processing`

`data = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040_smallfil\0009_20170523_181119_FA2806.all")`

Leaving all the options as default for `perform_all_processing` generates georeferenced soundings at the waterline, using navigation and attitude from the .all file, and saves to a folder next to the .all file called 'converted'.  Check the API reference to see the available options, and the underlying modules used from *fqpr_generation*.

To export soundings to csv as we did in the GUI, simply use the following (default will assume you want the csv format):

`data.export_pings_to_file()`

To generate a surface, we'll need to import the surface generation function (default assumes 1 meter resolution):

`from fqpr_convenience import generate_new_surface`

`surf = generate_new_surface(data)`

And you will have the same products as the GUI workflow.  You can reload to examine later using:

`from fqpr_convenience import reload_data, reload_surface`

`data = reload_data(r"C:\data_dir\converted")`

`surf = reload_surface(r"C:\data_dir\surf.npz")`

