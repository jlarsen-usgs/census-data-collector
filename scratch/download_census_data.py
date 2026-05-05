import os
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
#matplotlib.use('Qt5Agg')
#matplotlib.use('qtagg')
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
import utm
import shapefile
from descartes import PolygonPatch
import censusdc
from censusdc import TigerWeb, Acs5
from censusdc import TigerWebVariables as TWV
from censusdc.utils import GeoFeatures
from censusdc.utils import CensusTimeSeries




#---- Example 1: Polygon shapefiles supplied to TigerWeb ---------------------####

# set filepath
shp_file = os.path.join('..', 'data','Sacramento_neighborhoods_WGS.shp')

# if the shapefile has a label field for polygons we can tag data
# using the field parameter
tigweb = TigerWeb(shp_file, field="name")

# # if you have only specific fields that you would like
# tigweb.get_data(2010, outfields=(TWV.geoid, TWV.state, TWV.county,
#                                  TWV.tract, TWV.blkgrp, TWV.block))

# default method gets all relevant tigerweb attributes
tigweb.get_data(2010)

# # get features
# features = tigweb.features
#
# # get all polygon names
# names = tigweb.feature_names
#
# # get all GeoJSON features associated with a single polygon
# feature = tigweb.get_feature("la_riviera")

# visualize
fig = plt.figure()
ax = fig.gca()
for name in tigweb.feature_names:
    for feature in tigweb.get_feature(name):
        ax.add_patch(PolygonPatch(feature.geometry, alpha=0.5))

    # get the input polygon shapes and convert from UTM to WGS84
    x, y = np.array(tigweb.get_shape(name)["geometry"]["coordinates"][0]).T
    #y, x = utm.to_latlon(x, y, 11, zone_letter='N')  # don't need to do this because already in lat/long
    ax.plot(x, y, 'r-')

ax.axis('scaled')
plt.show()
#plt.savefig('example_1.png')


# #---- Example 2: Point shapefiles supplied to TigerWeb ---------------------####
#
# # set file path
# shp_file = os.path.join('..', 'data','Sacramento_points_WGS.shp')
#
# # radius infromation must be in the same units as the shapefile projection!
# tigweb = TigerWeb(shp_file, field="name", radius="radius")
# #tigweb = TigerWeb(shp_file, field="name", radius=0.1)
# tigweb.get_data(2013, level='tract')
#
# # visualize
# fig = plt.figure()
# ax = fig.gca()
# for name in tigweb.feature_names:
#     for feature in tigweb.get_feature(name):
#         ax.add_patch(PolygonPatch(feature.geometry, alpha=0.5))
#
#     x, y = np.array(tigweb.get_shape(name)["geometry"]["coordinates"]).T
#     #y, x = utm.to_latlon(x, y, 11, zone_letter='N')  # don't need to do this because already in lat/long
#     px, py = np.array(tigweb.get_point(name))
#     #py, px = utm.to_latlon(px, py, 11, zone_letter='N') # don't need to do this because already in lat/long
#     ax.plot(x, y, 'r-')
#     ax.plot(px, py, 'ro')
#
# ax.axis('scaled')
# plt.show()   # TODO: why isn't this showing the map in the readme? Is the radius incorrectly converted into the shapefile units?
# #plt.savefig('example_2.png')
#
#
#
# #---- Using tigerweb features to grab census data -------------------------------------####
#
# # the user must have a census api key to pull data from the Acs5
# with open("api_key.txt") as api:
#     apikey = api.readline().strip()
#
# data = 'data'
# point_name = "Sacramento_points_WGS.shp"
#
# tigweb = TigerWeb(os.path.join('..', data, point_name), field='name', radius='radius')
# tigweb.get_data(2013, level='tract')
#
# # get ACS5 data
# acs = Acs5(tigweb.features, 2013, apikey)
# acs.get_data(retry=20)  # number of retries for connection issues
#
# fig = plt.figure()
# ax = fig.gca()
# population = []
# patches = []
# for name in acs.feature_names:
#     for feature in acs.get_feature(name):
#         patches.append(PolygonPatch(feature.geometry))
#         population.append(feature.properties["B01003_001E"])
#     x, y = np.array(tigweb.get_shape(name)["geometry"]["coordinates"]).T
#     #y, x = utm.to_latlon(x, y, 11, zone_letter='N')  # don't need to do this because already in lat/long
#     px, py = np.array(tigweb.get_point(name))
#     #py, px = utm.to_latlon(px, py, 11, zone_letter='N')   # don't need to do this because already in lat/long
#     ax.plot(x, y, 'r-')
#     ax.plot(px, py, 'ro')
#
# p = PatchCollection(patches, cmap="viridis", alpha=0.75)
# p.set_array(np.array(population))
# ax.add_collection(p)
# ax.axis('scaled')
# plt.colorbar(p, shrink=0.7)
# plt.show()  # TODO: why isn't this showing the map in the readme?
#
#
#
# #---- Intersecting and area weight adjustment of geoJSON features -------------------####
#
# # building off of the previous example...
# ishp_name = "multipolygon_test.shp"
# ishp = shapefile.Reader(os.path.join('..', data, ishp_name))
# gf = GeoFeatures(acs.get_feature(acs.feature_names[0]))
# gf.intersect(ishp)
#
# fig = plt.figure()
# ax = fig.gca()
# population = []
# patches = []
# census_patches = []
# for feature in acs.get_feature(acs.feature_names[0]):
#    census_patches.append(PolygonPatch(feature.geometry))
#
# for feature in gf.intersected_features:
#    patches.append(PolygonPatch(feature.geometry))
#    population.append(feature.properties["B01003_001E"])
#
# p = PatchCollection(patches, cmap="viridis", alpha=0.75)
# cp = PatchCollection(census_patches, cmap='spring')
# p.set_array(np.array(population))
# ax.add_collection(cp)
# ax.add_collection(p)
# ax.axis('scaled')
# plt.colorbar(p, shrink=0.7)
# plt.show()  # TODO: why isn't this showing the map in the readme?



#---- Using the CensusTimeSeries class to get a pandas dataframe of timeseries data -----------------------------####

ws = os.path.abspath(os.path.dirname(__file__))
shp = os.path.join(ws, '..', "data", "Sacramento_neighborhoods_WGS.shp")
apikey = os.path.join(ws, "api_key.txt")

with open(apikey) as api:
    apikey = api.readline().strip()

ts = CensusTimeSeries(shp, apikey, field="name")

shp = shapefile.Reader(shp)
polygon = shp.shape(0)
df = ts.get_timeseries("la_riviera", polygons=polygon)

polygon = shp.shape(1)
df1 = ts.get_timeseries("tahoe_park", polygons=polygon)

xx=1
