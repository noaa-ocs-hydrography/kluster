.. kluster documentation master file, created by
   sphinx-quickstart on Tue Sep  8 08:59:20 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

kluster
=======

Known Issues
------------

Kluster does not support 'multifrequency' as seen in the most recent KMALL logged data.

Kluster .s7k is limited to certain records, see the 'Requirements' section.

Kluster .raw EK80 processing with Power workflow (and not ComplexSamples) has been seen to generate odd results.

Overview
--------

A distributed multibeam processing system built on the Pangeo ecosystem (https://pangeo.io/).

Kluster provides a fully open source hydrographic processing package to produce accessible bathymetry products in support of ocean mapping.

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

Kluster has been tested on:

- EK60, EK80 (Using Kluster amplitude detection, see Requirements in Documentation)
- Reson 7125, T20, T51
- EM2040/2040c/2040p
- EM2040 dual tx/dual rx
- EM710
- EM3002
- EM302
- EM122
- ME70 Bathy Module

Kluster is built from the ground up in Python, and was developed using
Python 3.8. Kluster includes modules developed by the hydrographic
community such as (see `drivers`):

-  kmall - Kongsberg kmall file reader
-  par3 - Kongsberg .all file reader
-  prr3 - Reson .s7k file reader
-  raw - Kongsberg .raw file reader
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

** We recommend that users try to run Kluster using the release attached to this GitHub repository, see `releases <https://kluster.readthedocs.io/en/latest/>`_ **

Kluster has been tested on Windows 10 and Ubuntu 20.04.

Kluster is not on PyPi, but can be installed using pip alongside the HSTB-drivers and HSTB-shared modules that are required.

(For Windows Users) Download and install Visual Studio Build Tools 2019 (If you have not already): MSVC Build Tools (https://visualstudio.microsoft.com/visual-cpp-build-tools/)

Download and install conda (If you have not already): conda installation (https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Download and install git (If you have not already): git installation (https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

Some dependencies need to be installed from the conda-forge channel.  I have an example below of how to build this environment using conda.

Perform these in order:

``conda create -n kluster_test python=3.8.8``

``conda activate kluster_test``

``conda install -c conda-forge qgis=3.18.0 vispy=0.6.6 pyside2=5.13.2 gdal=3.2.1 h5py``

``pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster``

Start the GUI by activating the new environment and run Kluster as a module

``(kluster_test) C:>python -m HSTB.kluster``

Docker
""""""

Build docker image using the provided dockerfile

``C:\Pydro21_Dev\NOAA\site-packages\Python38\git_repos\hstb_kluster>docker build -t kluster/ubuntu .``

``C:\Pydro21_Dev\NOAA\site-packages\Python38\git_repos\hstb_kluster>docker run -it kluster/ubuntu``

``(base) eyou102@faaec62a4c1c:~/kluster$ conda deactivate``

``eyou102@faaec62a4c1c:~/kluster$ conda activate kluster_test``

``(kluster_test) eyou102@faaec62a4c1c:~/kluster$ python``

.. _readthedocs: https://kluster.readthedocs.io/en/latest/
.. _Pangeo ecosystem: https://pangeo.io/
.. _drivers: https://github.com/noaa-ocs-hydrography/drivers
.. _Dask: https://dask.org/
.. _Zarr: https://zarr.readthedocs.io/en/stable/
.. _Xarray: http://xarray.pydata.org/en/stable/

.. toctree::
   :maxdepth: 4

   requirements/index
   quickstart/index
   quickstart_monitor/index
   faq/index
   vessel_setup/index
   processing/index
   surface/index
   mosaic/index
   sonar_acceptance/index
   cleaning/index
   products/index
   settings/index
   indepth/index
   for_developers/index
   kluster_api/index
   changes

Index
=====

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
