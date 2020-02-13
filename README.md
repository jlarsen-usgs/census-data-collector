# Census Data Collector  
The census data collector is a geographic based tool to query census data from
the TigerWeb REST services and the US Census API. Queryable census products
 include:  
* TigerWeb
* Decenial Census data
* ACS 1-Year
* ACS 5-Year

## Installation
_Method 1_: Download the census data collector from 
https://github.com/jlarsen-usgs/census-data-collector navigate to the package 
in either an anaconda prompt, terminal, or command prompt window and 
 run:
```shell script
pip install .
```

_Method 2_: Install directly from Github using"
```shell script
pip install https://github.com/jlarsen-usgs/census-data-collector/zipball/master
```

## Basic usage
The census data collector uses geographic information from shapefiles to query 
data.

__Importing census data collector__
```python
import censusdc
```

__Grabbing features from TigerWeb__  
TigerWeb hosts feature layers that are tagged with geographic id information
that can be used for creating census api data pulls. The `TigerWeb` class 
accepts polygon and *point*[future] shapefiles and can query a host of 
geographic information for the census.  

_Example_:  
Sacramento neighborhoods:  
Two polygons have been drawn; one for the Tahoe Park and one for the La Riviera
 neighborhood  
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Sacramento_neighborhoods.png" alt="Sacto"/>
</p>

We will use this shapefile and these polygon features to query block
information for the 2010 census
```python
from censusdc import TigerWeb
from censusdc import TigerWebVariables as TWV
import os

shp_file = os.path.join('data','Sacramento_neighborhoods.shp')

# if the shapefile has a label field for polygons we can tag data 
# using the field parameter
tigweb = TigerWeb(shp_file, field="name")
tigweb.get_data(2010, outfields=(TWV.geoid, TWV.state, TWV.county,
                                 TWV.tract, TWV.blkgrp, TWV.block))

features = tigweb.features
```
The features parameter returns a dictionary of geoJSON objects that can be
accessed and exported.  

To get the features from a single polygon we can use
```python
# get all polygon names
names = tigweb.feature_names

# get all GeoJSON features associated with a single polygon
feature = tigweb.get_feature("la_riviera")
```
 and we can visualize the geoJSON features using Descartes and matplotlib
 ```python
import matplotlib.pyplot as plt
from descartes import PolygonPatch
import numpy as np
import utm

fig = plt.figure()
ax = fig.gca()
for name in tigweb.feature_names:
    for feature in tigweb.get_feature(name):
        ax.add_patch(PolygonPatch(feature.geometry, alpha=0.5))
    
    # get the input polygon shapes and convert from UTM to WGS84
    y, x = np.array(tigweb.get_shape(name)).T
    y, x = utm.to_latlon(y, x, 11, zone_letter='N')
    ax.plot(x, y, 'r-')

ax.axis('scaled')
plt.show()
```
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Tigerweb_example.png" alt="TigerWeb"/>
</p>

## Development
This project is in active development and is in the pre-alpha stages. There 
will be many updates and changes to the source code in the near future.

## Authors
Joshua Larsen