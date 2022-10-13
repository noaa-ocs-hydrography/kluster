For Developers
===============

This article is here to provide some guidance for potential developers in the Kluster environment.  As I get questions,
I'll be adding to this so that we have a repository of guidance.

For installation help, see the home page.

First, I recommend you get in touch with me through the GitHub Issues system, I am happy to discuss any features that you
would like to add or colaborate on.

Next, I recommend you look at the `code examples <https://github.com/noaa-ocs-hydrography/kluster/tree/master/examples>`_.
These code examples aren't comprehensive, but they should be a good starting point towards understanding the API.  I also
have some code blocks scattered throughout the documentation here, but most of my effort on developing examples will be
within that examples directory.

Finally, using Sphinx, I autogenerate API documentation and host all of that here in the
`API Page <https://kluster.readthedocs.io/en/latest/api/HSTB.kluster.fqpr_generation.Fqpr.html#HSTB.kluster.fqpr_generation.Fqpr>`_.
You probably want to start with the FQPR object, which is the object returned by
`intel_process <https://kluster.readthedocs.io/en/latest/api/HSTB.kluster.fqpr_intelligence.intel_process.html#HSTB.kluster.fqpr_intelligence.intel_process>`_,
`perform_all_processing <https://kluster.readthedocs.io/en/latest/api/HSTB.kluster.fqpr_convenience.perform_all_processing.html#HSTB.kluster.fqpr_convenience.perform_all_processing>`_,
and `reload_data <https://kluster.readthedocs.io/en/latest/api/HSTB.kluster.fqpr_convenience.reload_data.html#HSTB.kluster.fqpr_convenience.reload_data>`_.
These are all common processes to use when starting out.  The FQPR object (fully qualified ping record) is the parent object
for the kluster processed data.

.. toctree::

   fordev_sonar
   fordev_release
