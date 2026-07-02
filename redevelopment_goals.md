Goals for redevelopment and refinement of CDC for publication
---------------------------------------------------------------

### Replace pyshp & pyproj dependencies (1st priority)
1) TigerLine data currently uses `pyshp`, `pyproj` deps. to handle shapefile data 
and stores feature information as geojson. This requires custom projection code
and a strict requirement of input data projections. Replace these data types with 
`geopandas` data. This will also change the internal storage mechanics of feature
data **Done**
2) Remove `TigerWebPoint` class and merge `TigerWebPolygon` and `TigerWeb` base class
into a singular object. **Done**
3) Once replacement is tested and dome, `utils.geometry` can be removed. **Testing in progress**


### Develop a data discovery tool for the CensusAPI. (Reach Goal)
Ideas below. Data storage type: pandas
* Start with a scraper to show certain data products. 
Start with ACS5, ACS5 profile, SF3.
* Create a method to go from initial discovery to census variable discovery for 
Census data products.
* Discovered variable information should be able to be parsed by user to get
census variable codes for individual census products. 
* Provide documentation and examples on how to work with the Data Discovery tool
* Idea: worked example of data discovery to new census defaults


### Develop new workflows for getting timeseries data
* remove existing timeseries class and re-imaging/re-develop user workflows for
creating timeseries information. Provide user templates for this process.


### Expose Acs, Sf3 defaults classes
* Expose classes and provide a template for users to set up their own defaults
for working in the datacollector


## Documentation updates
* Create API documentation via sphinx

* Remake Readme.md with new tested workflows

* Setup basic unit testing for TigerWeb, ACS, SF3, and resampling
