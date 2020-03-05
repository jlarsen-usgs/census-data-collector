# Census Data Collector  
The census data collector is a geographic based tool to query census data from
the TigerWeb REST services and the US Census API. Queryable census products
 include:  
* TigerWeb
* Decenial Census data[*future*]
* ACS 1-Year[*future*]
* ACS 5-Year[*future*]

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
accepts polygon and point shapefiles and can query a host of 
geographic information for the census.  

*__Example 1__*:  
_Sacramento neighborhood polygons:_  
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
    x, y = np.array(tigweb.get_shape(name)).T
    y, x = utm.to_latlon(x, y, 11, zone_letter='N')
    ax.plot(x, y, 'r-')

ax.axis('scaled')
plt.show()
```
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Tigerweb_example.png" alt="TigerWeb"/>
</p>

*__Example 2__*:  
_Sacramento neighborhood points:_  
Point shapefiles can also be supplied to `TigerWeb`. Here we have on in the
Tahoe Park neighborhood and one in the La Riviera neighborhood
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Sacramento_points.png" alt="Sacto_pts"/>
</p>

We can either use the points by themselves or define a radius around the 
points to query data from. The `radius=` parameter accepts either a string
which references an attribute column in the point shapefile or a float that
applies a constant radius to all points.   
```python
from censusdc import TigerWeb
from censusdc import TigerWebVariables as TWV
import os

shp_file = os.path.join('data','Sacramento_points.shp')

# radius infromation must be in the same units as the shapefile projection!
tigweb = TigerWeb(shp_file, field="name", radius="radius")
tigweb.get_data(2013, outfields=(TWV.geoid, TWV.state, TWV.county,
                                 TWV.tract, TWV.blkgrp, TWV.block))
```
and here we can visualize the census block group features within the 
defined radius from our points
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

        x, y = np.array(tigweb.get_shape(name)).T
        y, x = utm.to_latlon(x, y, 11, zone_letter='N')
        px, py = np.array(tigweb.get_point(name))
        py, px = utm.to_latlon(px, py, 11, zone_letter='N')
        ax.plot(x, y, 'r-')
        ax.plot(px, py, 'ro')

    ax.axis('scaled')
    plt.show()
```
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Tigerweb_points_example.png" alt="TigerWeb_pts"/>
</p>

__*Using tigerweb features to grab census data*__

After we get a feature set from tigerweb, we can grab data. In this example we 
are getting data from the 2013 American Community Survey 5 year data set and 
plot population of census block groups. Data from the Census data pull is 
added to each GeoJSON feature to keep spatial correlation. Here is an example:

```python
from censusdc import TigerWeb, Acs5
from descartes import PolygonPatch
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
import numpy as np
import os
import utm

# the user must have a census api key to pull data from the Acs5
with open("api_key.dat") as api:
    apikey = api.readline().strip()

data = 'data'
point_name = "Sacramento_points.shp"

tigweb = TigerWeb(os.path.join(data, point_name), field='name',
                  radius='radius')
tigweb.get_data(2013)

# get ACS5 data
acs = Acs5(tigweb.features, 2013, apikey)
acs.get_data()

fig = plt.figure()
ax = fig.gca()
population = []
patches = []
for name in acs.feature_names:
    for feature in acs.get_feature(name):
        patches.append(PolygonPatch(feature.geometry))
        population.append(feature.properties["B01003_001E"])
    x, y = np.array(tigweb.get_shape(name)).T
    y, x = utm.to_latlon(x, y, 11, zone_letter='N')
    px, py = np.array(tigweb.get_point(name))
    py, px = utm.to_latlon(px, py, 11, zone_letter='N')
    ax.plot(x, y, 'r-')
    ax.plot(px, py, 'ro')

p = PatchCollection(patches, cmap="viridis", alpha=0.75)
p.set_array(np.array(population))
ax.add_collection(p)
ax.axis('scaled')
plt.colorbar(p, shrink=0.7)
plt.show()
```

<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Tigerweb_points_population.png" alt="Acs5_pop_2013"/>
</p>

## Development
This project is in active development and is in the pre-alpha stages. There 
will be many updates and changes to the source code in the near future.

## Authors
Joshua Larsen