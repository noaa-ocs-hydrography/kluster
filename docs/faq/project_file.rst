How do I reload the processed data?
***********************************

If you drag in a file or group of files and convert (hit start process once), you'll see some new folders/files next to your raw data:

1. A folder for each sonar type / serial number / date combination in the raw data (ex: em2040_40111_05_23_2017).  This is the converted/processed data.
2. A file called kluster_project.json

The project file contains the locations of all the currently loaded data as well as the project settings set in Kluster.  You can ignore this if you like, when you drag in this file into
Kluster, it will set the project settings to whatever is in the project file, and also try and load all data listed in the file.  Open the kluster_project.json file in a text editor if you want to see what is in it.

The folders can be dragged into Kluster as you choose.  Each folder can be accessed and used independently from all others.