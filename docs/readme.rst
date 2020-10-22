kluster
=======

A distributed multibeam processing system built on the

. Supports Kongsberg .all/.kmall formats and POS MV post-processed
SBET/RMS navigation files.

Kluster is:

1. **Scalable** - uses Dask to provide distributed parallel processing
   on everything from a laptop to a cloud service (AWS Fargate for
   example)
2. **Cloud ready** - uses Zarr as a cloud ready storage format for
   converted multibeam records and processed soundings
3. **Open** - data are presented using Xarray objects for easy
   interactivity and stored with zarr, all open formats
4. **Scriptable** - provides a GUI for visualization and processing, but
   can be run from the command line or scripted easily
5. **Extensible** - From data conversion to sound velocity correction,
   kluster is built using modules that can be replaced, enhanced or
   exchanged as needed.

Why Kluster?
------------

There are two principle motivations behind kluster:

1. ##### Build a multibeam processing sandbox for scientists/engineers

The hydrographic community is continuously innovating. Oftentimes, we
want to experiment with an algorithm or technique, but the data is
inaccessible, or relies on intermediate products that are locked within
the software. How do you get attitude corrected beam vectors into a
numpy array? How can I test a new gridding algorithm without exporting
soundings to text first?

2. ##### Build a multibeam processing system for field use/production

