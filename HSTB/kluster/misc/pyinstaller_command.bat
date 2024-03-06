REM setting env variable SETUPTOOLS_USE_DISTUTILS to resolve setuptools/xarray issue with setuptools/distutils conflict, see xarray pull request #6096 and setuptools issue #2353
set SETUPTOOLS_USE_DISTUTILS=stdlib
cd "%~dp0" && call "..\..\..\..\..\..\..\..\Scripts\activate" Pydro38 && pyinstaller ".\kluster_main.spec"
REM Gdal expects the bag_template.xml to be accessible, put it next to the executable
echo f | xcopy /f /y "C:\Pydro22_Dev\NOAA\site-packages\Python38\git_repos\kluster\HSTB\kluster\misc\dist\kluster_main\Library\share\gdal\bag_template.xml" "C:\Pydro22_Dev\NOAA\site-packages\Python38\git_repos\kluster\HSTB\kluster\misc\dist\kluster_main\bag_template.xml"
REM Removing the site-packages folder that has started showing up when OpenCV was added as a dependency
if exist "C:\Pydro22_Dev\NOAA\site-packages\Python38\git_repos\kluster\HSTB\kluster\misc\dist\kluster_main\site-packages" rmdir "C:\Pydro22_Dev\NOAA\site-packages\Python38\git_repos\kluster\HSTB\kluster\misc\dist\kluster_main\site-packages" /q /s