from ..utils import TigerWebMapServer, GeoFeatures
import copy
import shapefile
import pandas as pd


class CensusTimeSeries(object):
    """
    Wrapper object to get a census time series for selected
    variables.

    Parameters:
    ----------
        shp : str
            shapefile path
        apikey : str
            census api key for data pulls
        field : str
            shapefile field to id multiple polygons
        radius : float or str
            radius around points to build a query, or shapefile field with
            radius information

    """
    def __init__(self, shp, apikey, field=None, radius=0):
        self._shp = shp
        self.__apikey = apikey
        self._field = field
        self._radius = radius
        self._censusobj = None

    def get_timeseries(self, feature_name, sf3_variables=(), acs_variables=(),
                       polygons=None, hr_dict=None, retry=1000):
        """
        Method to get a time series from 1990 through 2018 of census
        data from available products

        Parameters
        ----------
        feature_name : str, int
            feature name to perform intersections on
        sf3_variables : tuple
            tuple of variables to grab from the sf3 census. Default is all
            variables defined in Sf3Variables class
        acs_variables : tuple
            tuple of variables to grab from the ACS1 and ACS5 census
        hr_dict : dict
            human readable label dict, assists in aligning data. If hr_dict
            is None, defaults are used that cover AcsVariables and Sf3_Variables
        polygons : shapefile path, shapefile.Reader, list(shapely Polygon,),
            list(shapefile.Shape,), or list([x,y],[x_n, y_n])
            shapes or shapefile to intersect the timeseries with.

        Returns
        -------

        """
        from .. import TigerWeb, Acs1, Acs5, Sf3
        from ..datacollector.dec import Sf3HR1990, Sf3HR
        from ..datacollector.acs import AcsHR
        refresh = False

        if self._censusobj is None:
            url0 = ""
            year0 = 0
            twobjs = {}
            for year, url in TigerWebMapServer.base.items():
                if url == url0:
                    twobjs[year] = copy.copy(twobjs[year0])
                else:
                    tw = TigerWeb(self._shp, self._field, self._radius)
                    if year in (2005, 2006, 2007, 2008, 2009):
                        tw.get_data(year, level="county")
                    else:
                        tw.get_data(year, level="tract")

                    twobjs[year] = tw
                url0 = url
                year0 = year

            censusobj = {}
            for year, tw in twobjs.items():
                print("Getting data for census year {}".format(year))
                if year in (1990, 2000):
                    cen = Sf3(tw.features, year, self.__apikey)
                    cen.get_data(level="tract", variables=sf3_variables,
                                 retry=retry)
                elif year in (2005, 2006, 2007, 2008, 2009):
                    cen = Acs1(tw.features, year, self.__apikey)
                    cen.get_data(level='county', variables=acs_variables,
                                 retry=retry)
                else:
                    cen = Acs5(tw.features, year, self.__apikey)
                    cen.get_data(level='tract', variables=acs_variables,
                                 retry=retry)

                censusobj[year] = cen

            self._censusobj = censusobj

        else:
            censusobj = self._censusobj

        if isinstance(polygons, str):
            polygons = shapefile.Reader(polygons)

        timeseries = {}
        for year, cen in censusobj.items():
            gf = GeoFeatures(cen.get_feature(feature_name), feature_name)
            if polygons is not None:
                gf.intersect(polygons)
                features = gf.intersected_features
            else:
                features = gf.features

            if features is None:
                raise AssertionError("Check that intersection polygons "
                                     "intersect the census AOI and that "
                                     "projection is WGS84")

            if hr_dict is None:
                refresh = True
                if year == 1990:
                    hr_dict = Sf3HR1990
                elif year == 2000:
                    hr_dict = Sf3HR
                else:
                    hr_dict = AcsHR

            df = GeoFeatures.features_to_dataframe(year, features, hr_dict)
            if feature_name not in timeseries:
                timeseries[feature_name] = df
            else:
                tsdf = timeseries[feature_name]
                tsdf = tsdf.append(df, ignore_index=True)
                timeseries[feature_name] = tsdf

            if refresh:
                hr_dict = None

        return timeseries[feature_name]
