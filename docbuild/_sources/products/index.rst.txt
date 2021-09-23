Products
========

Kluster provides a number of ways to build and export products that you can use in other software packages.  I'll go over those products here.

Export Variables
----------------

We've already looked at how you can use the Basic Plots tool (Visualize - Basic Plots) to analyze individual variables like beam angle or travel time and produce plots and other visualizations.  Another thing you can do is take the data used for those plots and export it to text.  This might be useful if you want to use this data in other software packages for further analysis.  It's fairly simple, just set up the Basic Plots tool for your plot and hit one of the export buttons.  

Here we have an example where we plot the TVU for a selection of this line.  See the blue slider bars we used to grab a subset of the dataset.  We plotted the TVU as a histogram, and then used both the Export Variable button and the Export Source button to get the data in CSV.  

.. image:: products_1.png
   :target: ../_images/products_1.png

export_multibeam_40111.csv is the result of the Export Source button.  This will export the dataset for whatever you have in the Source dropdown.  Since the multibeam is a 2 dimensional (ping, beam) dataset, csv exports are a bit tough.  What I do is give you the mean value for all beams for each ping.  So in this example, notice that you have time values in UTC seconds, mean_alongtrack values and altitude values, as well as others for all multibeam variables.  mean_alongtrack is the average alongtrack value for all beams for each ping.  altitude is a 1 dimensional variable (one value for each ping) so I give you the altitude value as it is.

export_multibeam_tvu_40111.csv is the result of the Export Variable button.  This will export just the variable (TVU for us here) in it's base form.  Since TVU is a 2 dimensional dataset (ping, beam), this export has time, beam and tvu columns to it.  You'll see that the time is the same as the beam changes, the time for each ping is the same across all beams.  If you were to want to export just the mean value for each ping, or the nadir value, use the different Plot Type options, such as 'Line - Mean' or 'Line - Nadir'.  This will simplify the export to a 1 dimensional result.

Export Soundings
----------------

Now let's discuss what is probably a more useful export, exporting the sounding position and depth as a point cloud that you can view in other software.  For this, we use the File - Export Soundings dialog, which looks like this.

.. image:: products_2.png
   :target: ../_images/products_2.png

Exporting to csv gives you a similar export as the Export Variables options above, but with just that data that allows you to visualize the soundings in 3d with uncertainty.  You can see in the image above that we get the eastings/northings, the depth value and the uncertainty value for each sounding, comma delimited.  Since we used the 'Make Z Positive Down', the depths are positive down.  Filter rejected only exports those soundings that do not have a rejected status for detectioninfo.  'Separate Files by Sector/Frequency' will export to separate files for each sector/frequency combination, which is a useful way to isolate just one sector/frequency at a time.  

Exporting to LAS provides soundings in a format that is usable by a wide variety of software geared towards processing LIDAR and other point cloud products.  Bathymetry doesn't exactly fit in the LAS format, as we can't carry over uncertainty, but it does allow us to store the full point cloud, which is the important thing.  There is no 'Make Z Positive Down', as LAS specifies Z as positive up.  Just like with csv, we can filter and separate files if we wish.

Export Surface
--------------

After creating a surface, Kluster allows you to export that surface to a GDAL supported format as well as csv.  Currently, Kluster supports GeoTIFF and BAG export options.

.. image:: products_8.png
   :target: ../_images/products_8.png

As always, we start with the text export just to illustrate what we can do when exporting.  Here we have done a csv export with Z positive up.  You can see the data in the image above.  We export eastings/northings and depth and uncertainty, just as we do with our soundings export.  The main difference is the gridded nature of the dataset.  You can see that there are many no data values (nan) where our square grid has no data.  Currently Kluster will export, keeping these no data areas.

The csv export doesn't do much for us, however.  Let's look at GeoTIFF, which is probably the most widely supported surface export across other software.  Here we export that surface to GeoTIFF and load it in QGIS.  You can see that we get the same data in both, with a slight difference in the display that I believe is related to QGIS viewing this in the projected UTM coordinate system that we exported it in and Kluster viewing it in WGS84 geographic.

.. image:: products_9.png
   :target: ../_images/products_9.png

In addition, Kluster supports BAG exports, which require more metadata to generate.  Kluster will autogenerate as much of the metadata as it can, and provide a dialog for you to enter the rest.

.. toctree:: 
