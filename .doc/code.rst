API Reference
=============

Census Data Collector
---------------------

The Census Data Collector package allows users to provide geospatial boundaries to
query U.S. Census APIs. Below is a listing of the Census Data Collectors API reference.


datacollector
^^^^^^^^^^^^^

The datacollector subpackage contains objects for census data discovery and querying
census data APIs.


Contents:

.. toctree::
   :glob:
   :maxdepth: 4

   ./source/censusdc.datacollector.acs
   ./source/censusdc.datacollector.cbase
   ./source/censusdc.datacollector.data_discovery
   ./source/censusdc.datacollector.dec
   ./source/censusdc.datacollector.tigerweb

defaults
^^^^^^^^

The defaults subpackage contains a parent module for loading, writing, and creating
custom defaults for census data pulls

Contents:

.. toctree::
   :maxdepth: 4

   ./source/censusdc.datacollector.census_defaults


Utilities (utils)
^^^^^^^^^^^^^^^^^

The utilities subpackage contains internal and user facing utilities for querying
and resampling census data

Contents:

.. toctree::
   :glob:
   :maxdepth: 4

   ./source/censusdc.utils.*


