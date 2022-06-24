Adding a new Sonar
====================

Below are some tips on adding support for a new multibeam file format in Kluster.

First, you need a new driver.  All of our drivers are in the Drivers repo `here <https://github.com/noaa-ocs-hydrography/drivers/>`_.  You would need to add a driver ideally there for this new format.  The new driver needs to provide all the functionality of the existing drivers in fqpr_drivers.  fqpr_drivers is the module that acts as a layer between drivers and Kluster.

The main thing that we need is the sequential_read function.  Sequential_read will start at the given start pointer in the file, read the file up to the end pointer provided, and return a nested dictionary of records for all required records.  See the sequential_read validation routines in fqpr_drivers to learn more about the data and datatypes required.  Another good reference for this is the 'Requirements' page in the documentation, that lists the records we currently grab from multibeam formats.

You will also see the fast_read methods in fqpr_drivers.  These methods allow Kluster to quickly read the relevant metadata that it uses to categorize the file once it is dragged in to the application and added to the intelligence module.

You will also need to add the extension to the appropriate variables in kluster_variables.  Look at the existing sonar extensions to get a good idea as to how to do this.

The last thing that is needed is to add the sonar model number to the xarray_conversion.sonar_translator.  This lookup allows Kluster to know which offset/angle in the installation parameters corresponds to which transducer in the real world.
