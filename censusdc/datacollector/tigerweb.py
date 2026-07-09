"""
Module for TigerWeb REST data collection.
"""
import geojson
import requests
import pandas as pd
from ..utils import TigerWebMapServer, thread_count
import threading
import geopandas as gpd
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json import JSONDecodeError


try:
    import ray
    ENABLE_MULTIPROC = True
except ImportError:
    # fake ray wrapper function for windows
    from ..utils import ray
    ENABLE_MULTIPROC = False


class TigerWebVariables(object):
    """
    Tigerweb variable names for querying data
    """
    mtfcc = 'MTFCC'
    oid = 'OID'
    geoid = 'GEOID'
    state = 'STATE'
    place = 'PLACE'
    county = 'COUNTY'
    cousub = 'COUSUB'
    tract = 'TRACT'
    blkgrp = 'BLKGRP'
    block = 'BLOCK'
    basename = 'BASENAME'
    name = 'NAME'
    lsadc = 'LSADC'
    funcstat = 'FUNCSTAT'
    arealand = 'AREALAND'
    areawater = 'AREAWATER'
    stgeometry = 'STGEOMETRY'
    centlat = 'CENTLAT'
    centlon = 'CENTLON'
    intptlat = 'INTPTLAT'
    intptlon = 'INTPTLON'
    objectid = 'OBJECTID'
    population = 'POP100'


class TigerWeb(object):
    """
    Class object for TigerWeb geospatial data collection. This class takes user
    provided geospatial information and sends it to the TigerWeb rest service to
    get census geometries and US census geographic ID information.

    Parameters
    ----------
    gdf : geopandas GeoDataFrame
        geo dataframe
    field : str
        geo dataframe attribute column to join census data

    """

    _ESRI_CODE = 4326

    def __init__(self, gdf, field):

        self._field = field

        original_crs = gdf.crs
        gdf = gdf.to_crs(epsg=TigerWeb._ESRI_CODE)
        gdf = gdf.explode()
        gdf["geometry"] = gdf["geometry"].buffer(0)
        self._gdf = gdf
        self._ocrs = original_crs
        self.esri_wkid = TigerWeb._ESRI_CODE

        self._esri_json = []
        self._features = {}
        self._get_polygons()

    def _get_polygons(self):
        """
        GeoPandas-based method to read and store polygons from self._gdf for
        later TigerWeb processing. Populates:
          - self._esri_json[name] : ESRI polygon geometry (single ring) JSON string
        """

        gdf = self._gdf
        if gdf is None or gdf.empty:
            return  # nothing to do

        # Resolve the name column (case-insensitive) if user provided `field`
        field_col = None
        if self._field is not None:
            for col in gdf.columns:
                if col.lower() == self._field:
                    field_col = col
                    break

        if field_col is None:
            field_col = "uid"
            self._field_col = field_col
            gdf[field_col] = range(len(gdf))

        def exterior_vertex_count(geom):
            """Count exterior vertices across Polygon/MultiPolygon."""

            if geom.geom_type == 'Polygon':
                return len(list(geom.exterior.coords))
            return 0

        for geom, name in zip(gdf["geometry"], gdf[field_col]):

            # Skip invalid/empty geometry
            if geom is None or geom.is_empty:
                continue
            if geom.geom_type not in ("Polygon", "MultiPolygon"):
                # Ignore non-polygon types in a polygon workflow
                continue

            if geom.geom_type == "MultiPolygon":
                geoms = list(geom.geoms)
            else:
                geoms = [geom,]

            for geom in geoms:
                # Decide whether to use bbox or the actual ring
                vcount = exterior_vertex_count(geom)
                minx, miny, maxx, maxy = geom.bounds

                if vcount > 20:
                    # Use bounding box to reduce request size / server complexity
                    ring = [(minx, miny), (maxx, miny), (maxx, maxy),
                            (minx, maxy), (minx, miny)]
                else:
                    # Use the exterior of the geometry (one ring, no holes)
                    if geom.geom_type == 'Polygon':
                        ring = list(map(tuple, geom.exterior.coords))
                    else:  # MultiPolygon, Point, etc...
                        raise AssertionError(
                            f"{geom.geom_type} is unsupported for feature {name}"
                        )

                # Build ESRI JSON (single ring)
                esri_json = self.polygon_to_esri_json(ring)
                self._esri_json.append((name, esri_json))

    @property
    def features(self):
        """
        Method to get all features in the feature dictionary

        Returns
        -------
            dict : {name: geoJSON object}
        """
        if not self._features:
            raise ValueError("No features have been collected yet, please run get_data()")

        features = pd.concat(self._features.values(), ignore_index=True)
        features = features.drop_duplicates(subset=["geometry", self._field])
        features = features.reset_index(drop=True)
        features = features.to_crs(self._ocrs)
        return features

    @property
    def feature_names(self):
        """
        Gets uniquer identifier names from the field parameter

        Returns
        -------
        list
        """
        return list(self._gdf[self._field].unique())


    def get_feature(self, name):
        """
        Method to get a single GeoJSON feature from the feature dict

        Parameters
        ----------
        name : str or int
            feature name

        Returns
        -------
            geodataframe
        """
        if name not in self._features:
            name = str(name)

        if name not in self._features:
            raise KeyError("Name: {} not present in feature dict".format(name))
        else:
            return self._features[name].to_crs(self._ocrs)

    def clear_features(self):
        """
        Method to clear features before running a new data pull on TigerWeb.
        This is useful for changing census discretization without remaking
        esri json strings for large data pulls

        """
        self._features = {}

    def polygon_to_esri_json(self, polygon):
        """
        Method to create an esri json string for defining geometry to
        query

        Parameters
        ----------
        polygon : list
            list of x,y coordinates

        Returns
        -------
        str : json geometry string
        """
        d = {"geometryType": "esriGeometryPolygon",
             "rings": [],
             "spatialReference": {}}
        ring = []
        for pts in polygon:
            if isinstance(pts, tuple):
                pts = list(pts)
            elif isinstance(pts, list):
                pass
            else:
                try:
                    pts = list(pts)
                except ValueError:
                    raise Exception("Points must be supplied as a list")

            if len(pts) == 2:
                ring.append(pts)

            elif len(pts) == 3:
                # polygonZ
                ring.append(pts[:-1])

            else:
                raise AssertionError("Each point must consist of only "
                                     "two coordinates")

        d["rings"].append(ring)

        if isinstance(self.esri_wkid, int):
            d['spatialReference'] = {"wkid": self.esri_wkid}
        else:
            raise TypeError("wkid must be a well known id number string")

        s = d.__str__()
        s = s.replace("'", '"')
        s = s.replace(" ", "")
        return s


    def get_data(self, year, level='finest', outfields=(), verbose=True,
                 multiproc=False, multithread=False, thread_pool=4, retry=100):
        """
        Method to pull data feature data from tigerweb

        Parameters
        ----------
        year : int
            data year to grab features from
        level : str
            block, block group, tract, or "finest"...
        outfields : tuple
            tuple of output variables to grab from tigerweb
            default is () which grabs GEOID,BLKGRP,STATE,COUNTY,TRACT
            from TigerWeb
        verbose : bool
            verbose operation mode
        multiproc : bool
            flag to enable multiprocessing on linux using ray
        multithread : bool
            flag to enable/disable multithreaded data collection
        thread_pool : int
            number of threads requested for multithreaded operations
        retry : int
            number of retries if connection is interupted

        Returns
        -------

        """
        level = level.lower()

        lut = None
        if level == 'finest':
            for level in TigerWebMapServer.levels:
                lut = TigerWebMapServer.__dict__[level]
                if year in lut:
                    break
                else:
                    lut = None

        else:
            if level in TigerWebMapServer.__dict__:
                lut = TigerWebMapServer.__dict__[level]
                if year in lut:
                    pass
                else:
                    lut = None

        if lut is None:
            raise KeyError("No TigerWeb server could be found for {} and {}"
                           .format(year, level))

        if level == "county_subdivision":
            base = TigerWebMapServer.cousub_base[year]
        elif level == "county":
            base = TigerWebMapServer.cobase[year]
        elif level == "place":
            base = TigerWebMapServer.place_base[year]
        else:
            base = TigerWebMapServer.base[year]

        mapserver = lut[year]['mapserver']


        geotype = 'esriGeometryPolygon'

        if outfields:
            if isinstance(outfields, str):
                outfields = (outfields,)
            outfields = ','.join(outfields).upper()
        else:
            outfields = lut[year]['outFields']

        if "GEOID" not in outfields:
            outfields += ",GEOID"

        if multiproc and not ENABLE_MULTIPROC:
            multiproc = False
            multithread = True
            thread_pool = thread_count() - 1

        if multiproc:
            actors = []
            for key, esri_json in self._esri_json:

                actor = multiproc_request_data.remote(key, base, mapserver,
                                                      esri_json, geotype,
                                                      outfields, verbose,
                                                      retry)
                actors.append(actor)

            output = ray.get(actors)

            for out in output:
                if out is None:
                    continue
                else:
                    key, features = out
                    features[self._field] = key
                    if features.crs is None:
                        features = features.set_crs(epsg=self._ESRI_CODE)
                    self._features[key] = features

        elif multithread:
            thread_list = []
            container = threading.BoundedSemaphore(thread_pool)
            for key, esri_json in self._esri_json:

                x = threading.Thread(target=self.threaded_request_data,
                                     args=(key, base, mapserver, esri_json,
                                           geotype, outfields, verbose,
                                           retry, container))
                thread_list.append(x)

            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()

        else:
            for key, esri_json in self._esri_json:

                self.__request_data(key, base, mapserver, esri_json, geotype,
                                    outfields, verbose, retry)\

        return self.features

    def __request_data(self, key, base, mapserver, esri_json, geotype,
                       outfields, verbose, retry):
        """
        Request data method for serial and multithread applications

        Parameters
        ----------
        key : str or int
            feature identifier
        base : str
            base url
        mapserver : int, list
            map server number, will be a list in the case of place
        esri_json : str
            json geometry string
        geotype : str
            geometery type
        outfields : str
            string of requested variables
        verbose : bool
            verbose operation mode
        retry : int
            number of retries

        Returns
        -------

        """
        features = []
        s = requests.session()
        if not isinstance(mapserver, (list, tuple)):
            mapserver = [mapserver]

        for mps in mapserver:
            url = '/'.join([base, str(mps), "query?"])

            s.params = {'where': '',
                        'text': '',
                        'objectIds': '',
                        'geometry': esri_json,
                        'geometryType': geotype,
                        'inSR': '',
                        'spatialRel': 'esriSpatialRelIntersects',
                        'relationParam': '',
                        'outFields': outfields,
                        'returnGeometry': True,
                        'returnTrueCurves': False,
                        'maxAllowableOffset': '',
                        'geometryPrecision': '',
                        'outSR': '',
                        'returnIdsOnly': False,
                        'returnCountOnly': False,
                        'orderByFields': '',
                        'groupByFieldsForStatistics': '',
                        'outStatistics': '',
                        'returnZ': False,
                        'returnM': False,
                        'gdbVersion': '',
                        'returnDistinctValues': False,
                        'f': 'geojson',
                        }
            start = 0
            done = False
            n = 0

            while not done:
                try:
                    r = s.get(url, params={'resultOffset': start,
                                           'resultRecordCount': 32})
                    r.raise_for_status()
                except (requests.exceptions.HTTPError,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ReadTimeout) as e:
                    n += 1
                    if verbose:
                        print("ConnectionError: retry number {}".format(n))

                    if n == retry:
                        raise Exception(e)
                    else:
                        continue

                collection = geojson.loads(r.text)

                try:
                    newdf =  gpd.GeoDataFrame.from_features(collection["features"])
                    newdf[self._field] = key
                except (KeyError, AttributeError):
                    newdf = []

                if len(newdf) > 0:
                    if newdf.crs is None:
                        newdf = newdf.set_crs(epsg=self._ESRI_CODE)
                    features.append(newdf)

                    start += len(newdf)
                    if verbose:
                        print("Received", len(newdf), "entries,",
                              start, "total,", "from server", str(mps))
                else:
                    done = True

        if len(features) > 1:
            features = pd.concat(features, ignore_index=True)
        else:
            if not features:
                return
            else:
                features = features[0]

        if key in self._features:
            features = pd.concat((self._features[key], features), ignore_index=True)
            features = features.drop_duplicates(subset=["GEOID"])

        self._features[key] = features

    def threaded_request_data(self, key, base, mapserver, esri_json, geotype,
                              outfields, verbose, retry, container):
        """
        Multithread handler method to request data from the TigerWeb server

        Parameters
        ----------
        key : str or int
            feature identifier
        base : str
            base url
        mapserver : int, list
            map server number, in case of place this will be a list
        esri_json : str
            json geography string
        geotype : str
            geometery type
        outfields : str
            string of requested variables
        verbose : bool
            verbose operation mode
        retry : int
            number of retries
        container : threading.BoundSemaphore

        """
        container.acquire()
        self.__request_data(key, base, mapserver, esri_json, geotype,
                            outfields, verbose, retry)
        container.release()


@ray.remote
def multiproc_request_data(key, base, mapserver, esri_json, geotype,
                           outfields, verbose, retry):
    """
    Ray multiprocessing handler method to request data from the TigerWeb server

    Parameters
    ----------
    key : str or int
        feature identifier
    base : str
        base url
    mapserver : int, list
        map server number, in case of place we use a list
    esri_json : str
        json geography string
    geotype : str
        geometery type
    outfields : str
        string of requested variables
    verbose : bool
        verbose operation mode
    retry : int
        number of retries

    """
    features = []
    s = requests.session()
    if not isinstance(mapserver, (list, tuple)):
        mapserver = [mapserver]

    for mps in mapserver:
        url = '/'.join([base, str(mps), "query?"])

        s.params = {'where': '',
                    'text': '',
                    'objectIds': '',
                    'geometry': esri_json,
                    'geometryType': geotype,
                    'inSR': '',
                    'spatialRel': 'esriSpatialRelIntersects',
                    'relationParam': '',
                    'outFields': outfields,
                    'returnGeometry': True,
                    'returnTrueCurves': False,
                    'maxAllowableOffset': '',
                    'geometryPrecision': '',
                    'outSR': '',
                    'returnIdsOnly': False,
                    'returnCountOnly': False,
                    'orderByFields': '',
                    'groupByFieldsForStatistics': '',
                    'outStatistics': '',
                    'returnZ': False,
                    'returnM': False,
                    'gdbVersion': '',
                    'returnDistinctValues': False,
                    'f': 'geojson',
                    }
        start = 0
        done = False

        n = 0
        while not done:
            try:
                r = s.get(url, params={'resultOffset': start,
                                       'resultRecordCount': 32})
                r.raise_for_status()
            except (requests.exceptions.HTTPError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ReadTimeout) as e:
                n += 1
                if verbose:
                    print("ConnectionError: retry number {}".format(n))

                if n == retry:
                    raise Exception(e)
                else:
                    continue

            collection = geojson.loads(r.text)
            newdf = gpd.GeoDataFrame.from_features(collection["features"])

            if len(newdf) > 0:
                features.append(newdf)

                start += len(newdf)
                if verbose:
                    print("Received", len(newdf), "entries,",
                          start, "total,", "from server", str(mps))
            else:
                done = True

    if len(features) > 1:
        features = pd.concat(features, ignore_index=True)
    else:
        features = features[0]

    return key, features
