from ..utils import TigerWebMapServer, GeoFeatures
import copy
import shapefile
import calendar
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

    def get_timeseries(self, feature_name, sf3_variables=(),
                       sf3_variables_1990=(), acs_variables=(),
                       years=(), polygons=None, hr_dict=None, retry=1000,
                       verbose=1, multiproc=False, multithread=False,
                       thread_pool=4):
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
        sf3_variables_1990 : tuple
            tuple of variables to grab from the 1990 sf3 census. Default is all
            variables defined in Sf3Variables1990 class
        acs_variables : tuple
            tuple of variables to grab from the ACS1 and ACS5 census
        years : list, optional
            optional method to query only specific years from the Census API
        polygons : shapefile path, shapefile.Reader, list(shapely Polygon,),
            list(shapefile.Shape,), or list([x,y],[x_n, y_n])
            shapes or shapefile to intersect the timeseries with.
        hr_dict : dict
            human readable label dict, assists in aligning data. If hr_dict
            is None, defaults are used that cover AcsVariables and
            Sf3_Variables
        retry : int
            number of retries for connection issues
        verbose : int or bool
            verbosity flag. 0 is not verbose, 1 is minimum verbosity, > 1
            is maximum verbosity.
        multiproc : bool
            native multiprocessing support for linux only using ray.
        multithread : bool
            multithreaded operation flag
        thread_pool : int
            number of threads to run multithreading on

        Returns
        -------

        """
        from .. import TigerWeb, Acs1, Acs5, Sf3
        from ..datacollector.dec import Sf3HR1990, Sf3HR
        from ..datacollector.acs import AcsHR
        refresh = False

        verb = False
        if isinstance(verbose, int):
            if verbose > 1:
                verb = True

        if not years:
            years = [year for year in TigerWebMapServer.base.keys()]

        else:
            for year in years:
                if year not in TigerWebMapServer.base:
                    raise KeyError("No census API data available "
                                   "for {}".format(year))

        if self._censusobj is None:
            url0 = ""
            year0 = 0
            twobjs = {}
            for year, url in TigerWebMapServer.base.items():
                if year not in years:
                    continue

                if verbose:
                    print("Getting Tigerline data for census "
                          "year {}".format(year))
                if url == url0:
                    twobjs[year] = copy.copy(twobjs[year0])
                else:
                    tw = TigerWeb(self._shp, self._field, self._radius)
                    if year in (2005, 2006, 2007, 2008, 2009):
                        tw.get_data(year, level="county",
                                    verbose=verb,
                                    multithread=multithread,
                                    thread_pool=thread_pool)
                    else:
                        tw.get_data(year, level="tract",
                                    verbose=verb,
                                    multithread=multithread,
                                    thread_pool=thread_pool)

                    twobjs[year] = tw
                url0 = url
                year0 = year

            censusobj = {}
            for year, tw in twobjs.items():
                if verbose:
                    print("Getting data for census year {}".format(year))
                if year in (1990, 2000):
                    cen = Sf3(tw.features, year, self.__apikey)
                    if year == 1990:
                        cen.get_data(level='tract',
                                     variables=sf3_variables_1990,
                                     retry=retry, verbose=verb,
                                     multiproc=multiproc,
                                     multithread=multithread,
                                     thread_pool=thread_pool)
                    else:
                        cen.get_data(level="tract", variables=sf3_variables,
                                     retry=retry, verbose=verb,
                                     multiproc=multiproc,
                                     multithread=multithread,
                                     thread_pool=thread_pool)

                elif year in (2005, 2006, 2007, 2008, 2009):
                    cen = Acs1(tw.features, year, self.__apikey)
                    cen.get_data(level='county', variables=acs_variables,
                                 retry=retry, verbose=verb,
                                 multiproc=multiproc,
                                 multithread=multithread,
                                 thread_pool=thread_pool)
                else:
                    cen = Acs5(tw.features, year, self.__apikey)
                    cen.get_data(level='tract', variables=acs_variables,
                                 retry=retry, verbose=verb,
                                 multiproc=multiproc,
                                 multithread=multithread,
                                 thread_pool=thread_pool)

                censusobj[year] = cen

            self._censusobj = censusobj

        else:
            censusobj = self._censusobj

        if isinstance(polygons, str):
            polygons = shapefile.Reader(polygons)

        if verbose:
            print("Performing intersections and building DataFrame")

        timeseries = {}
        for year, cen in censusobj.items():
            if verbose > 1:
                print("performing intersections {}, {}".format(year,
                                                               feature_name))
            gf = GeoFeatures(cen.get_feature(feature_name), feature_name)
            if polygons is not None:
                gf.intersect(polygons, multithread=multithread,
                             thread_pool=thread_pool)
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
                tsdf = tsdf.append(df, ignore_index=True, sort=False)
                timeseries[feature_name] = tsdf

            if refresh:
                hr_dict = None

        return timeseries[feature_name]

    @staticmethod
    def interpolate(df, skip_years=(), drop=(), discretization='daily',
                    kind='linear', min_extrapolate=False,
                    max_extrapolate=False):
        """
        Interpolation method to get daily or monthly data from a census
        timeseries dataframe

        Parameters
        ----------
        df : pd.Dataframe object
        skip_years: tuple
            a tuple of years to exculude from interpolation
        drop : list
            columns to drop from the interpolation
        discretization : str
            string flag to indicate either monthly or daily interpolation
            results
        kind : str
            scipy.interpolate kind string. Specifies the kind of interpolation
             as a string (‘linear’, ‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’,
              ‘cubic’, ‘previous’, ‘next’, where ‘zero’, ‘slinear’, ‘quadratic’
               and ‘cubic’ refer to a spline interpolation of zeroth, first,
               second or third order; ‘previous’ and ‘next’ simply return the
               previous or next value of the point) or as an integer specifying
                the order of the spline interpolator to use. Default is
                ‘linear’.
        min_extrapolate: int
            minimum year to extrapolate data past bounds of census df data
        max_extrapolate: int
            maximum year to extrapolate data past bounds of census df data

        Returns
        -------
            pd.Dataframe
        """
        from scipy import interpolate

        if isinstance(drop, tuple):
            drop = list(drop)

        if skip_years:
            df = df[~df.year.isin(skip_years)]

        if drop:
            df = df.drop(columns=drop)

        years = df.year.values
        ymin = min(years)
        ymax = max(years)

        if max_extrapolate and max_extrapolate > ymax:
            ymax = max_extrapolate
        elif min_extrapolate and min_extrapolate < ymin:
            ymin = min_extrapolate
        else:
            pass

        if discretization == "daily":
            x = []
            for year in range(ymin, ymax + 1):
                ndays = 365
                dec = 0
                if calendar.isleap(year):
                    ndays = 366

                if year == ymin:
                    if calendar.isleap(year):
                        dec = 183 / ndays
                    else:
                        dec = 182 / ndays

                while dec < 0.999:
                    x.append(year + dec)
                    step = 1 / ndays
                    dec += step
                    if year == ymax:
                        if calendar.isleap(year):
                            stop = 183 / ndays
                        else:
                            stop = 182 / ndays

                        if dec > stop:
                            break
        else:
            x = []
            for year in range(ymin, ymax + 1):
                dec = 0
                stop = 0.5
                if year == ymin:
                    if calendar.isleap(year):
                        dec = 6 / 12
                    else:
                        dec = 6 / 12

                while dec < 0.94:
                    x.append(year + dec)
                    step = 1 / 12
                    dec += step
                    if year == ymax:

                        if dec > stop:
                            break

        dyear = []
        for year in df.year.values:
            if calendar.isleap(year):
                dy = year + (183 / 366)
            else:
                dy = year + (182 / 366)
            dyear.append(dy)

        d = {'dyear': x}
        for column in list(df.columns):
            if column in ("year", "dyear"):
                continue

            f = interpolate.interp1d(dyear, df[column].values,
                                     kind=kind, fill_value='extrapolate')

            cnew = f(x)

            d[column] = cnew

        return pd.DataFrame.from_dict(d)
