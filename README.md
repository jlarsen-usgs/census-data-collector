# Census Data Collector  
The census data collector is a geographic based tool to query census data from
the TigerWeb REST services and the US Census API. Queryable census products
 include:  
* TigerWeb
* Decenial Census data from Sf3
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
from pathlib import Path
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# import censusdc functionality
from censusdc import TigerWeb, Acs1, Acs5, Sf3

shp_file = Path('data/Sacramento_neighborhoods.shp')
gdf = gpd.read_file(shp_file)

# if the geodataframe has a label field for polygons we can tag data 
# using the field parameter
tigweb = TigerWeb(gdf, field="name")

# default method gets all relevant tigerweb attributes
tigweb.get_data(2010)

# returns a geodataframe of census information
features = tigweb.features
```
we can visualize the features using geopandas built in plotting methods
 ```python
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
features.plot(ax=ax)
gdf.plot(ax=ax, facecolor="None", edgecolor="r")

ax.axis('scaled')
plt.show()
```
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Tigerweb_example.png" alt="TigerWeb"/>
</p>

__*Using tigerweb features to grab census data*__

After we get a feature set from tigerweb, we can grab data. In this example we 
are getting data from the 2013 American Community Survey 5 year data set and 
plot population of census block groups. Data from the Census data pull is 
added to each GeoJSON feature to keep spatial correlation. Here is an example:

```python
from censusdc import TigerWeb, Acs5
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np

# the user must have a census api key to pull data from the Acs5
with open("api_key.dat") as api:
    apikey = api.readline().strip()

shp_name = Path('data/Sacramento_neighborhoods.shp')
gdf = gpd.read_file(shp_name)

tigweb = TigerWeb(gdf, field='name')
tigweb.get_data(2013, level='tract')

# get ACS5 data
acs = Acs5(tigweb.features, 2013, apikey)
acs.get_data(retry=20)  # number of retries for connection issues
cen_feats = acs.features

fig, ax = plt.subplots()
# plot population
cen_feats.plot(column="B01003_001E", ax=ax)
# plot outlines
ax = gdf.plot(ax=ax, facecolor="None", edgecolor="k")
ax.axis('scaled')
plt.colorbar(ax.collections[0], shrink=0.4);
```
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Tigerweb_points_population.png" alt="Acs5_pop_2013"/>
</p>

__*Intersecting and area weight adjustment of geoJSON features*__

The `GeoFeature` class allows the user to intersect census designated place 
data with arbitrary polygons to create new area weighted features that 
represent the user's area of interest. Input polygons can be supplied to 
the `GeoFeature` class as a pyshp `shapefile.Reader` instance, a list
`shapefile.Shape` instances, a list of `shapely.geometry.Polygon` or
 `shapely.geometry.MultiPolygon` instances, or a list of x,y points that define
 a closed polygon.
 
 _Note: shapes must be supplied in the WGS84 projection to intersect with the 
 TigerLine data, as this is the projection returned from TigerWeb_ 
 
 Example:
 ```python
# building off of the previous example...

from censusdc.utils import GeoFeatures
import shapefile


ishp_name = "multipolygon_test.shp"
ishp = shapefile.Reader(os.path.join(data, ishp_name))
gf = GeoFeatures(acs.get_feature(acs.feature_names[0]))
gf.intersect(ishp)

    fig = plt.figure()
    ax = fig.gca()
    population = []
    patches = []
    census_patches = []
    for feature in acs.get_feature(acs.feature_names[0]):
        census_patches.append(PolygonPatch(feature.geometry))

    for feature in gf.intersected_features:
        patches.append(PolygonPatch(feature.geometry))
        population.append(feature.properties["B01003_001E"])

    p = PatchCollection(patches, cmap="viridis", alpha=0.75)
    cp = PatchCollection(census_patches, cmap='spring')
    p.set_array(np.array(population))
    ax.add_collection(cp)
    ax.add_collection(p)
    ax.axis('scaled')
    plt.colorbar(p, shrink=0.7)
    plt.show()
```
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census-data-collector/master/data/Tigerweb_points_intersection.png" alt="Acs5_intersect_2013"/>
</p>

__*Using the `CensusTimeSeries` class to get a pandas dataframe of timeseries data*__  

Instead of requesting data year by year, the `CensusTimeSeries` class allows 
the user to pull multiple timeseries of data from the US Census in a few simple
calls. The default parameters the `CensusTimeSeries` class pulls are defined
in the `AcsVariables`, `Sf3Variables`, and `Sf3Variables1990` classes. The 
`CensusTimeSeries` object uses data caching methods to pull all census data
within a shapefile defined region and allow the user to do multiple time
series with that data.

_Basic example_
```python
import os
import shapefile
from censusdc.utils import CensusTimeSeries

ws = os.path.abspath(os.path.dirname(__file__))
shp = os.path.join(ws, "data", "Sacramento_neighborhoods_WGS.shp")
apikey = os.path.join(ws, "api_key.dat")

with open(apikey) as api:
    apikey = api.readline().strip()

ts = CensusTimeSeries(shp, apikey, field="name")

shp = shapefile.Reader(shp)
polygon = shp.shape(0)
df = ts.get_timeseries("la_riviera", polygons=polygon)

polygon = shp.shape(1)
df1 = ts.get_timeseries("tahoe_park", polygons=polygon)
```
## Example Jupyter Notebooks
Fully functioning example notebooks can be found [here](https://github.com/jlarsen-usgs/census-data-collector/tree/master/examples)
## Development
This project is in active development and is in the pre-alpha stages. There 
will be many updates and changes to the source code in the near future.

## Authors
Joshua Larsen