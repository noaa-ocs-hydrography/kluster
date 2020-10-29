.. kluster documentation master file, created by
   sphinx-quickstart on Tue Sep  8 08:59:20 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

kluster
=======

Documentation: `readthedocs`_

A distributed multibeam processing system built on the `Pangeo
ecosystem`_. Supports Kongsberg .all/.kmall multibeam formats, POS MV
post-processed SBET/RMS navigation files and Caris svp sound velocity
profile files.

Kluster is:

1. **Scalable** - uses `Dask`_ to provide distributed parallel
   processing on everything from a laptop to a cloud service (AWS
   Fargate for example)
2. **Cloud ready** - uses `Zarr`_ as a cloud ready storage format for
   converted multibeam records and processed soundings
3. **Open** - data are presented using `Xarray`_ objects for easy
   interactivity and stored with Zarr, all open formats
4. **Scriptable** - provides a GUI for visualization and processing, but
   can be run from the command line or scripted easily
5. **Extensible** - From data conversion to sound velocity correction,
   kluster is built using modules that can be replaced, enhanced or
   exchanged as needed.

Kluster **will not** read from multibeam sonars that use the Depth
datagram instead of the Range/Angle for .all conversion. Most modern
systems will work. Kluster has been tested on:

-  EM2040/2040c/2040p
-  EM2040 dual tx/dual rx
-  EM710
-  EM122

Kluster is built from the ground up in Python, and was developed using
Python 3.8. Kluster includes modules developed by the hydrographic
community such as (see `drivers`_):

-  kmall - Kongsberg kmall file reader
-  par3 - Kongsberg .all file reader
-  sbet - POSPac sbet/rms file reader

Kluster is a work in progress that has been in development since
November 2019 by a small team, and is by no means feature complete. If
you are interested in contributing or have questions, please contact
Eric Younkin (eric.g.younkin@noaa.gov)

Why Kluster?
------------

There are three principle motivations behind kluster:

1. .. rubric:: Build a multibeam processing sandbox for
      scientists/engineers
      :name: build-a-multibeam-processing-sandbox-for-scientistsengineers

The hydrographic community is continuously innovating. Oftentimes, we
want to experiment with an algorithm or technique, but the data is
inaccessible, or relies on intermediate products that are locked within
the software. How do you get attitude corrected beam vectors into a
numpy array? How can I test a new gridding algorithm without exporting
soundings to text first?

2. .. rubric:: Build a multibeam cloud processing system for field
      use/production
      :name: build-a-multibeam-cloud-processing-system-for-field-useproduction

Cloud data storage and processing is quickly becoming a reality, as the
advantages of not owning your own infrastructure become apparent. Where
does this leave processing software and our traditional workflow?
Kluster is designed from the ground up to address this issue, by
providing processing that can be tailored and deployed in multiple
different ways depending on the application. In addition, using the
multiprocessing capabilities of Dask, kluster provides a powerful tool
that can compete with existing software packages in terms of
performance.

3. .. rubric:: Evaluate the latest in open source scientific software
      :name: evaluate-the-latest-in-open-source-scientific-software

Much of the existing open source software related to multibeam
processing has been in development for decades. There has been an
explosion in scientific libraries that can benefit the hydrographic
community as a whole that have not been seriously evaluated. Kluster
relies on the state of the art in Python libraries to provide a
sophisticated and modern software package.

Installation
------------

Kluster is not on PyPi, but can be installed using pip alongside the
HSTB-drivers module that is required.

``pip install git+https://github.com/noaa-ocs-hydrography/drivers.git#egg=hstb.drivers``

``pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster``

Quickstart
----------

Here we will show how to process through a GUI or through a console.

1. .. rubric:: Kluster through the GUI
      :name: kluster-through-the-gui

Start the main GUI by running kluster_main, which will be in the gui
directory within the kluster site-package:

``C:\\>python ..\Lib\site-packages\HSTB\kluster\gui\kluster_main.py``

Once the Kluster window appears, simply:

-  Drag a multibeam file (Kongsberg .all/.kmall) into the Project Tree
   window. You can also drag in multiple files, but maybe stick with
   just one for this test.
-  Create a folder and point to it as the 'output directory for the
   converted data' and hit OK. Monitor the results in the Output tab at
   the bottom. View the data in the 2d view. Select a line in the
   Explorer tab at the bottom and view the Attribute tab for some
   details on the dataset.
-  Once conversion is complete, run 'Process' - 'All Processing' and
   leave the default options. This will run through all the steps and
   generate georeferenced soundings.
-  Use 'File' - 'Export Soundings' to generate csv files for the
   processed soundings (x, y, z, uncertainty)
-  Use 'Process' - 'New Surface' to generate a single resolution surface
   using the processed sounding set. Visualize the surface by checking
   one of the layers in the 'Project Tree' under 'Surfaces'. Use the
   magnifying glass in 2d view if you need to zoom in to see the
   surface. Surfaces are saved in the numpy compressed file format, and
   can be easily read using numpy.
   
You can also reload the generated multibeam data and surface by:

-  dragging in the output directory (see the second bullet above) to the
   Project Tree to load the multibeam records
-  dragging in the surface .npz file to the Project Tree to load the
   surface

2. .. rubric:: Kluster through the console
      :name: kluster-through-the-console

The quickest start would be to use the ``perform_all_processing``
function from the *fqpr_convenience* module. This relies on you having a
Kongsberg .all or .kmall file to experiment with.

``from HSTB.kluster.fqpr_convenience import perform_all_processing``

``data = perform_all_processing(r"C:\collab\dasktest\data_dir\EM2040_smallfil\0009_20170523_181119_FA2806.all")``

Leaving all the options as default for ``perform_all_processing``
generates georeferenced soundings at the waterline, using navigation and
attitude from the .all file, and saves to a folder next to the .all file
called 'converted'. Check the API reference to see the available
options, and the underlying modules used from *fqpr_generation*.

To export soundings to csv as we did in the GUI, simply use the
following (default will assume you want the csv format):

``data.export_pings_to_file()``

To generate a surface, we'll need to import the surface generation
function (default assumes 1 meter resolution):

``from fqpr_convenience import generate_new_surface``

``surf = generate_new_surface(data)``

And you will have the same products as the GUI workflow. You can reload
to examine later using:

``from fqpr_convenience import reload_data, reload_surface``

``data = reload_data(r"C:\data_dir\converted")``

``surf = reload_surface(r"C:\data_dir\surf.npz")``

.. _readthedocs: https://kluster.readthedocs.io/en/latest/
.. _Pangeo ecosystem: https://pangeo.io/
.. _drivers: https://github.com/noaa-ocs-hydrography/drivers
.. _Dask: https://dask.org/
.. _Zarr: https://zarr.readthedocs.io/en/stable/
.. _Xarray: http://xarray.pydata.org/en/stable/

.. toctree::
   :maxdepth: 4

   fqpr_convenience
   fqpr_generation   
   fqpr_sat
   fqpr_surface
   fqpr_project
   fqpr_visualizations
   xarray_conversion
   orientation
   beampointingvector
   svcorrect
   georeference
   tpu
   rotations
   pdal_entwine
   dask_helpers
   fqpr_helpers
   numba_helpers
   xarray_helpers
   
   

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
