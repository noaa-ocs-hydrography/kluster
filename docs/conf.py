import os
p, f = os.path.split(__file__)
root_p = os.path.normpath(p)

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'kluster'
copyright = '2020, Eric Younkin'
author = 'Eric Younkin'

# The full version, including alpha/beta/rc tags
release = '0.2.0'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

# m2r to translate the readme.md to rst
extensions = ['sphinx_automodapi.automodapi', 
              'sphinx.ext.napoleon', 
              'sphinx.ext.graphviz',
              'sphinx_autodoc_typehints'
              ]
numpydoc_show_class_members = False

# we disabled class inheritance diagrams, so this won't even be used
graphviz_dot = os.path.normpath(os.path.join(root_p, r"..\..\..\..\..\..\envs\Pydro38_Test\Library\bin\graphviz\dot.exe"))

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
