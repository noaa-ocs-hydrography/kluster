What is a project in Kluster?  How do I use it?
***********************************************

You'll see when you convert data that a kluster_project.json file gets automatically created next to the converted data folders.  This file contains the locations of all the currently loaded data as well as the project settings set in Kluster.  You can ignore this if you like, when you drag in this file into Kluster, it will set the project settings to whatever is in the project file, and also try and load all data listed in the file.

If you want the converted data to go somewhere other than next to the raw data, you should create a project manually.  This is a simple process where you go to File - New Project, and point to an empty folder that you want to use for the new project.  Kluster will then create a new kluster_project.json file in this directory, and that will be the new output location for all processed data.

Again, you don't have to think about the Kluster project file unless you want to put data somewhere specific or save project settings so that reloading data also reloads project settings.  Each converted folder can be accessed independently, so you can just drag folders into Kluster instead of using the project file.

The current project is always listed in the Project Tree in the top left of Kluster, under 'Project'.