# setting env variable SETUPTOOLS_USE_DISTUTILS to resolve setuptools/xarray issue with setuptools/distutils conflict, see xarray pull request #6096 and setuptools issue #2353
export SETUPTOOLS_USE_DISTUTILS=stdlib
cd ~/Documents/GitHub/kluster/HSTB/kluster/misc
source /home/eyou102/miniconda3/bin/activate kluster_test
pyinstaller "/home/eyou102/Documents/GitHub/kluster/HSTB/kluster/misc/kluster_main.spec"
# Gdal expects the bag_template.xml to be accessible, put it next to the executable
cp "~/Documents/GitHub/kluster/HSTB/kluster/misc/dist/kluster_main/Library/share/gdal/bag_template.xml" "~/Documents/GitHub/kluster/HSTB/kluster/misc/dist/kluster_main/bag_template.xml"