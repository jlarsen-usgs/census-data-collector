from ..utils import TigerWebMapServer, GeoFeatures
import copy
import shapefile


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
        polygons : shapefile path, shapefile.Reader, list(shapely Polygon,),
            list(shapefile.Shape,), or list([x,y],[x_n, y_n])
            shapes or shapefile to intersect the timeseries with.
    """
    def __init__(self, shp, apikey, field=None, radius=0, polygons=None):
        self._shp = shp
        self.__apikey = apikey
        self._polygons = polygons
        self._field = field
        self._radius = radius
        self._timeseries = None

    @property
    def timeseries(self):
        if self._timeseries is None:
            print("Collecting census data")
            self.get_timeseries()
        return self._timeseries

    def get_timeseries(self, sf3_variables=(), acs_variables=(),
                       hr_dict=None, retry=1000):
        """
        Method to get a time series from 1990 through 2018 of census
        data from available products

        Parameters
        ----------
        sf3_variables : tuple
            tuple of variables to grab from the sf3 census. Default is all
            variables defined in Sf3Variables class
        acs_variables : tuple
            tuple of variables to grab from the ACS1 and ACS5 census
        hr_dict : dict
            human readable label dict, assists in aligning data. If hr_dict
            is None, defaults are used that cover AcsVariables and Sf3_Variables

        Returns
        -------

        """
        from .. import TigerWeb, Acs1, Acs5, Sf3

        url0 = ""
        year0 = 0
        twobjs = {}
        for year, url in TigerWebMapServer.base.items():
            if url == url0:
                twobjs[year] = copy.copy(twobjs[year0])
            else:
                tw = TigerWeb(self._shp, self._field, self._radius)
                if year in (2005, 2006, 2007, 2008, 2009):
                    tw.get_data(year, level="county_subdivision")
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

        if isinstance(self._polygons, str):
            self._polygons = shapefile.Reader(self._polygons)

        timeseries = {}
        for year, cen in censusobj.items():
            for name in cen.feature_names:
                gf = GeoFeatures(cen.get_feature(name), name)
                if self._polygons is not None:
                    gf.intersect(self._polygons)
                    features = gf.intersected_features
                else:
                    features = gf.features

                # print('break')
                # todo: craft a method to accumulate the data
                # todo: into a single pandas record....

