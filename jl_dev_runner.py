from censusdc import TigerWeb, Acs5
import geopandas as gpd
from pathlib import Path



with open("api_key.dat") as foo:
    api_key = foo.readline().strip()


shp_file = Path("data/Sacramento_neighborhoods.shp")
gdf = gpd.read_file(shp_file)
gdf["name"] = gdf["name"].str.lower()

tw = TigerWeb(gdf, "name")
x = tw.get_data(2018, "tract", )
feats = tw.features
lriv = tw.get_feature("la_riviera")


cen = Acs5(feats, 2018, api_key)
rsp = cen.get_data(multithread=True, thread_pool=4)
feats = cen.features

print('break')

