kluster
'''''''

A distributed multibeam processing system built on the `Pangeo
ecosystem`_. Supports Kongsberg .all/.kmall formats and POS MV
post-processed SBET/RMS navigation files. Kluster **will not** read from
multibeam sonars that use the Depth datagram instead of the Range/Angle
for .all conversion. Most modern systems will work. Kluster has been
tested on:

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

3. .. rubric:: Evaluate the latest in
      :name: evaluate-the-latest-in

.. _Pangeo ecosystem: https://pangeo.io/
.. _drivers: https://github.com/noaa-ocs-hydrography/drivers
.. _Dask: https://dask.org/
.. _Zarr: https://zarr.readthedocs.io/en/stable/
.. _Xarray: http://xarray.pydata.org/en/stable/