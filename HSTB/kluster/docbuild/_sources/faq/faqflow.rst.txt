Can you describe the general data flow in Kluster?
***************************************************

When you drag data into the Kluster GUI, it adds those files to the Intelligence module.  See the 'Learn more about Kluster - Kluster Intelligence' article.  The intelligence module makes all the processing decisions for you, so that you don't have to know if sv correction comes before georeferencing or after.  If the intelligence module has an available action, it shows in the Actions window in Kluster and you either hit the start button to run or check 'Auto' to automatically process.

If you have a designated surface, actions will include updating that surface with any new data.

If you run the fqpr_intelligence.intel_process method directly (see Quickstart - Monitor), you get the same result as having 'Auto' checked in the GUI.  Actions are generated based on the state of the data and what source files exist.

As an example, let's say you drag in some multibeam files and an SVP file in Kluster and run in 'Auto', with a designated surface.  You will see the following actions:

 - A conversion action appears first
    - if this is the first process and a Dask cluster does not exist, a LocalCluster is automatically started (this takes a few seconds, the application locks up during this process)
    - conversion is the process that takes the raw multibeam data and converts it to the Kluster format.
    - conversion reads chunks of raw multibeam files, processes them in parallel, and writes those chunks to disk in the Kluster format.
    - the Kluster format is an Xarray Dataset saved to disk as Zarr, you will see folders for each variable and attribution in a JSON format.
 - An import action appears next
    - As there is now converted data available, the Intelligence module looks to see if the SVP file is in the converted data already, and if not, the new data is imported into the converted data.
    - This is how SBET imports work as well, it checks if the file exists, and if not, it is imported
    - Importing a new file starts processing over at the relevant step (i.e. if you import a new SVP file, it generates a new SV correct action, assuming you are at that stage in the state machine)
 - A processing action appears next
    - Processing encompasses orientation, beam correction, sound velocity correction, georeferencing, and TPU.
    - By reading the converted data processing status, we can determine which of these actions we need to perform.
    - Newly converted data will require the full stack of processing, you will see it listed as a new 'All Processing' action.
    - If you were to import sound velocity profiles after processing once, you might instead have a new action called 'Process Sound Velocity' which would include sound velocity correction, georeferencing, and TPU.
 - A gridding action appears last
    - The Intelligence module looks at the grid to see if the newly processed data exists within the grid.
    - If not, the data is automatically added to the grid, and all tiles in the grid that have new data are regridded.

In the end you will have two products, the processed data (which contains the raw multibeam data, the intermediate processed data, and the point cloud) and the grid.  If you choose, you can export the point cloud and/or the grid to a variety of formats.
