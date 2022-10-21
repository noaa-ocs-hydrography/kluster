Building a new Release
==========================

Here are the steps I follow on generating a new release version of Kluster.  Note that this is all assuming Windows.  Linux can be used and run Kluster just fine, but the build scripts haven't been finished for Linux just yet.

First, I primarily work off of the dev branches of the Github repo(s).  Kluster has several other modules that are a part of the `Coast Survey organization`_ including:

- Bathygrid
- Bathycube
- Vyperdatum
- Drivers
- Shared

Kluster has instructions for pip installing from the git repository, the install will use the master branch of each of these repositories. So you first must ensure that any changes you make are committed to dev and merged into the master branch.  Kluster, Bathygrid, BathyCube and Vyperdatum each have their own automated test suite (see the tests folder) that must be run before merging to master.  The tests can be run with UnitTest/pytest depending on the implementation.

I generally also run my full test data suite through Kluster prior to merging dev to master.  I have this scripted `here (see Test Run Through)`_, so if you can get a general idea of how that process works.

You will also need to increment the version numbers in the __version__.py file for each module.

Ensure that changes made are documented in the `changes.rst`_ file with the new version information.

I also ensure that the built documentation is regenerated for this new version.  This is done by running the `Sphinx script`_ which creates a new docbuild folder that you replace the `release documentation`_ with.  We include built docs with the release, as the user often won't have the internet to get the online docs in the field.  You might have to adjust the script for your file locations.

After I am satisfied with the changes and that I haven't broken anything, I complete a pull request for dev to Master with the new build.  I ensure that:

- all __version__.py numbers have been incremented
- all dev branches have been merged with master.  If you modify the dev branch in another repo like Bathygrid, ensure that the branch is merged with master.
- new info is added to the changes.rst doc
- the docbuild folder is updated with the new documentation

I can now build the Windows release.  This is done using pyinstaller and the `build script here`_.  You'll note that there is also a bash script file for use on Linux, this has not been completed.  Running the build script will create a new Windows build in the 'dist' folder that can be uploaded to GitHub as a new release.  Note that you might have to update some paths in the script.

You now have updated the master branch on the Kluster github and issued a new release.  Users can now download the Windows build and/or install the python environment from the master branch.

Please generate a new GitHub issue ticket if you have any questions.

.. _Coast Survey organization: https://github.com/noaa-ocs-hydrography
.. _here (see Test Run Through): https://github.com/noaa-ocs-hydrography/kluster/blob/master/HSTB/kluster/script_tests.py#L38
.. _changes.rst: https://github.com/noaa-ocs-hydrography/kluster/blob/master/docs/changes.rst
.. _Sphinx script: https://github.com/noaa-ocs-hydrography/kluster/blob/master/docs/sphinx_command.bat
.. _release documentation: https://github.com/noaa-ocs-hydrography/kluster/tree/master/HSTB/kluster/docbuild
.. _build script here: https://github.com/noaa-ocs-hydrography/kluster/blob/master/HSTB/kluster/misc/pyinstaller_command.bat