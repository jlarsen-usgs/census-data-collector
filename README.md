# Census Data Collector  
The census data collector is a geographic based tool to query census data from
the TigerWeb REST services and the US Census API. Queryable census products
 include:  
* TigerWeb
* Decenial Census data
* ACS 1-Year
* ACS 5-Year

## Installation
_Method 1_: Download the census data collector from __*url*__ navigate to the
 package in either an anaconda prompt, terminal, or command prompt window and 
 run:
```shell script
pip install .
```

_Method 2_: Install directly from Github using"
```shell script
pip install url....
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
Sacramento neighborhoods  
<p align="center">
  <img src="https://raw.githubusercontent.com/jlarsen-usgs/census_data_collector/master/data/Sacramento_neighborhoods.png" alt="Sacto"/>
</p>

```python
from censusdc import TigerWeb, TigerWebVariables

shp_file = 'Sacramento_neighborhoods.shp'


```
