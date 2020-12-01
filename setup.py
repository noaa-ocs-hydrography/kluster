import io
import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

# see https://github.com/navdeep-G/setup.py/blob/master/setup.py

# Package meta-data.
NAME = 'hstb.kluster'
DESCRIPTION = 'Distributed hydrographic multibeam processing system'
URL = 'https://github.com/noaa-ocs-hydrography/kluster'
EMAIL = 'eric.g.younkin@noaa.gov'
AUTHOR = 'Eric Younkin'
REQUIRES_PYTHON = '>=3.8.2'
VERSION = '0.1.1'

# What packages are required for this module to be executed?
REQUIRED = [
            'bokeh==2.2.3',
            'dask==2.17.2',
            'distributed==2.30.1',
            'fasteners==0.14.1',
            'laspy==1.7.0',
            'PySide2==5.13.2',
            'matplotlib==3.3.3',
            'numba==0.51.2',
            'openpyxl==3.0.3',
            'psutil==5.7.3',
            # 'numpy==1.19.4',
            # 'pandas==1.1.4',
            # 'pyshp==2.1.2',
            'vispy==0.6.4',
            'pyopengl==3.1.5',
            'pyproj==2.6.1.post1',
            'pyqtgraph==0.11.0',
            's3fs==0.5.1',
            # 'scipy==1.5.3',
            # 'shapely==1.7.1',
            'sortedcontainers==2.3.0',
            'watchdog==0.10.4',
            # 'xarray==0.16.2',
            'zarr==2.5.0'
            'cartopy==0.17.0'  # leave last to get dependencies
            ]

# What packages are optional?
EXTRAS = {
          'entwine export': ['entwine', 'nodejs'],
          }

# The rest you shouldn't have to touch too much :)
# ------------------------------------------------
# Except, perhaps the License and Trove Classifiers!
# If you do change the License, remember to change the Trove Classifier for that!

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    project_slug = NAME.lower().replace("-", "_").replace(" ", "_")
    with open(os.path.join(here, project_slug, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION


class UploadCommand(Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(here, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPI via Twine…')
        os.system('twine upload dist/*')

        self.status('Pushing git tags…')
        os.system('git tag v{0}'.format(about['__version__']))
        os.system('git push --tags')

        sys.exit()


# Where the magic happens:
setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*", "tests.*"]),
    # If your package is a single module, use this instead of 'packages':
    # py_modules=['mypackage'],

    # entry_points={
    #     'console_scripts': ['mycli=mymodule:cli'],
    # },
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    license='CC0-1.0',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ]
    # $ setup.py publish support.
    # cmdclass={
    #     'upload': UploadCommand,
    # },
)