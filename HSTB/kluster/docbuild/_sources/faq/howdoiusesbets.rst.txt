How do I import POSPac SBETs in Kluster?
****************************************

POSPac SBET files contain the post processed navigation / ellipsoid height for the vessel.  If you are using one of the ellipsoidal vertical references (MLLW, MHW, ellipse), having a more accurate navigation data set (in particular ellipsoid height) is crucial.

Unfortunately, working with SBETs is a challenge.  The SBET itself has no metadata in the file, and only records the time in weekly seconds, which Kluster must convert to UTC seconds.  You can see the information Kluster needs to accomplish this in the Process - Import Post Processed Navigation dialog.

There are two paths (actually three paths, as you'll see later) for importing SBETs in Kluster.

1. You can provide the .sbet and .smrmsg files and explicitly give me the Date of the SBET and the Coordinate System (so I can correct the weekly seconds and the lack of coordinate system metadata in the SBET)
2. You can provide the .sbet and .smrmsg files AND a POSPac export log.  This log contains the date/coordinate system already, and is automatically generated when the SBET is exported.  Including this log allows you to get away with not telling me the date/coordinate system.

And, the third somewhat hidden way:

3. Drag the .sbet, .smrmsg, and export log straight into Kluster.  Kluster will then match the SBET with the correct converted data folder using the times of the SBET and the file name/file path of the sbet.

Once imported, you will see the following new attributes (click on the converted data folder in Project Tree and look at the Attribute window in the bottom right)

1. navigation_source = 'sbet'
2. nav_error_files = list of the .smrmsg files and the associated start, end times in weekly seconds
3. nav_files = list of the .sbet files and the associated start, end times in weekly seconds
4. sbet_datum = 'NAD83' or the datum of the sbet
5. sbet_logging_rate_hz = logging rate in hertz

You will also see new sbet_xxxxxx data variables that you can examine in Visualize - Basic Plots, Source=processed navigation