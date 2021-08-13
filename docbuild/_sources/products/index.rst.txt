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

Surface
-------

So far, we have discussed working with point clouds, or the soundings visualized in 3d space.  But what about viewing the depth and uncertainty in 2d?  As a picture?  This is where the gridding tools come into play.  Kluster uses the `Bathygrid <https://github.com/noaa-ocs-hydrography/bathygrid>`_ module which I designed specifically for gridding multibeam data in an efficient and flexible way.  Let's look at how to use this module in Kluster.  With a converted data instance selected, let's go to Process - New Surface.

.. image:: products_3.png
   :target: ../_images/products_3.png

This is going to create a surface with all the points from each converted instance we had highlighted here.  The default is to create a single resolution surface where each grid node has a value equal to the mean depth/uncertainty value of all the soundings in that grid cell.  The size of the grid cell (resolution) is set to AUTO, which will pick the resolution based on the depth of the soundings in that region (based on the NOAA Specifications and Deliverables):

- 0 to 20 meters = 0.5 meter resolution
- 20 to 40 meters = 1.0 meter resolution
- 40 to 80 meters = 4.0 meter resolution
- 80 to 160 meters = 8.0 meter resolution
- 160 to 320 meters = 16.0 meter resolution
- 320 to 640 meters = 32.0 meter resolution
- 640 to 1280 meters = 64.0 meter resolution
- 1280 to 2560 meters = 128.0 meter resolution
- 2560 to 5120 meters = 256.0 meter resolution
- 5120 to 10204 meters = 512.0 meter resolution
- greater than 10204 meters = 1024.0 meter resolution

You'll notice that our resolutions are all powers of two.  This is intentional, as it allows us to build nice square tiles of the same size (1024 meters for example) that are completely and cleanly filled by grids of these resolutions.  

The tile size is a parameter that you can change to affect the performance of the grid in both visualization and processing.  Bathygrid will tile the area into tiles and then run the gridding algorithm on those tiles one after the other.  If you Process in Parallel, it ill process those tiles in parallel, which will greatly increase the performance of the gridding process.

You want to adjust the tile size to have at least 100 tiles or so.  I've found this to be a rough guideline that gets me pretty good performance.  In this example here, using a Tile Sie of 1024 meters gives me 2 tiles (which I know because I just ran it to check).  So I probably want to drive the tile size down, maybe to 256 meters just to improve the performance of the parallel gridding process.  Let's look at the results of 1024 meter tile size versus 256 meter tile size.

.. image:: products_4.png
   :target: ../_images/products_4.png

What I have done here is grid with the two different tile sizes, went into the output grid directories (which are srgrid_mean_auto and srgrid_mean_auto_20210813_104634 and are right next to our processed multibeam data by default) and looked at the number of folders.  Each folder represents a tile (with the folder name being the origin point of the tile in eastings_northings).  So we can see that a 1024 meter tile size created 2 tiles and a 256 meter tile size created 5 tiles.  Better but not ideal.  Of course in this instance, our surface generation only took about 1 second, so it's not really worth trying to optimize.  But when you set the tile size, try to get more than just a few tiles for your survey area.  In the future, picking the tile size will be an automated process, so you won't have to worry too much about this part.

Let's try a variable resolution grid now.  Variable resolution is going to allow us to have tiles where each tile can have it's own resolution.  The only option currently is AUTO for resolution, and it uses the same lookup table as above to determine the resolution of the tile.  

.. image:: products_5.png
   :target: ../_images/products_5.png

Variable resolution is going to create tiles of "Tile Size" (1024 meters in this example) with sub tiles of "Subtile Size" (128 meters in this example).  The result we can see in the Explorer window shown above.  We have 7 subtiles (all 128 meters by 128 meters) in this 1024 meter by 1024 meter tile.  Each subtile has a mean depth, and we look up that depth in the depth to resolution table above to get the resolution of the subtile.  Again, each subtile has it's own resolution.  Since our depth in this example is basically flat across the line, we are probably going to see the same resolution in each of our Variable Resolution subtiles.  So the single resolution and variable resolution grids are probably basically the same.

OK, enough about folders and tiles.  Let's look at something already!  Let's expand the folders in the Surfaces dropdown in the Project Tree and try turning on one of the Depth layers.

.. image:: products_6.png
   :target: ../_images/products_6.png

Turning on a Depth layer (or any layer) should make the display zoom to the area and show you the grid.  I just have a little multibeam file here, so the grid is fairly small.  I have the Query tool turned on in 2dview which lets me view the Depth layer value where my cursor is.  You can also left click to print the value in the Output window.  We can also turn on multiple layers to get the values across all layers.  Let's do this now.

.. image:: products_7.png
   :target: ../_images/products_7.png

We can see that our single resolution grids with the different tile sizes and the variable resolution grid all produce the same answer for the same location.  That's a relief!  If they didn't, we'd have a real problem on our hands.  We can also see that the tile shown in all three is an 8 meter resolution tile.  Which makes sense since we used AUTO resolution and the depth is the same across all surfaces in this location.

Gridding is a powerful tool for visualizing bathymetry in 2d, but how do we get this grid into a file that we can use in other software?  How do we get from this weird Bathygrid folder structure into a common file format?

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
