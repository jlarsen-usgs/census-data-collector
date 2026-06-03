"""
Development code for TigerWeb REST data collection.
"""
import geojson
import requests
import pandas as pd
import copy
from ..utils import TigerWebMapServer, thread_count
from ..utils.geometry import lat_lon_geojson_to_albers_geojson  # TODO: delete after making sure it doesn't break anything
import threading
import geopandas as gpd
from shapely.geometry import shape as shapely_shape
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
    Base class object for all Tigerweb spatial queries

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
        self.gdf = gdf
        self._ocrs = original_crs
        self.crs = gdf.crs.name
        self.esri_wkid = TigerWeb._ESRI_CODE

        self._wkid = None  # TODO: delete?
        self._shapes = {}
        self._albers_shapes = {}  # TODO: delete after making sure it doesn't break anything
        self._points = {}  # TODO: delete after making sure it doesn't break anything
        self._esri_json = []
        self._features = {}
        self._albers_features = {}  # TODO: may not need this - check - don't remove yet
        self._get_polygons()

    def _get_polygons(self):
        """
        GeoPandas-based method to read and store polygons from self.gdf for
        later TigerWeb processing. Populates:
          - self._esri_json[name] : ESRI polygon geometry (single ring) JSON string
          - self._shapes[name]    : GeoJSON Feature of the full input geometry
        """

        gdf = self.gdf
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

        name_counter = -1

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
                else:  # MultiPolygon
                   raise AssertionError(f"{geom.geom_type} is unsupported")

            # Build ESRI JSON (single ring)
            esri_json = self.polygon_to_esri_json(ring)
            self._esri_json.append((name, esri_json))

            # todo: remove this at some point. Do not need to maintain self._shapes
            # Store the input geometry as a GeoJSON Feature (full geometry)
            gi = geom.__geo_interface__
            if gi['type'].lower() == 'polygon':
                gj_geom = geojson.Polygon(gi['coordinates'])
            else:  # multipolygon
                gj_geom = geojson.MultiPolygon(gi['coordinates'])
            self._shapes[name] = geojson.Feature(geometry=gj_geom)

    @property
    def shapes(self):
        """
        Method to get the shape vertices for each shape

        Returns
        -------
            dict : {name: [vertices]}
        """
        return copy.deepcopy(self._shapes)

    #TODO: delete after also deleting albers_shapes from timeseries.py and/or cbase.py
    @property
    def albers_shapes(self):
        """
        Method to get the geoJSON representation of albers projection
        for each input shape

        Returns
        -------
            dict : {name: [vertices]}
        """
        if not self._albers_shapes:
            self.__geojson_to_albers_geojson('shapes')

        return copy.deepcopy(self._albers_shapes)

    # TODO: delete?
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
    def features_gdf(self):
        """
        Returns a copy of the features as a GeoDataFrame.
        If not computed yet, builds it from current self._features.
        """
        if not hasattr(self, '_features_gdf') or self._features_gdf is None:
            self._features_gdf = self._features_to_geodataframe(self._features, crs_epsg=4326, dedupe=True)
        return self._features_gdf.copy()

    # TODO: delete after deleting from timeseries.py and/or cbase.py
    @property
    def albers_features(self):
        """
        Method to get all features in the feature dictionary in an
        albers projection

        Returns
        -------
            dict : {name: geoJSON objects}
        """
        if not self._albers_features:
            self.__geojson_to_albers_geojson('features')

        return copy.deepcopy(self._albers_features)

    # TODO: delete?
    @property
    def feature_names(self):
        """
        Gets the feature names from the feature dict

        Returns
        -------
            list: feature names
        """
        return list(self._features.keys())

    @property
    def feature_names_gdf(self):
        """
        Gets the unique feature names from the geodataframe

        Returns
        -------
            list: unique feature names
        """
        return list(self._features_gdf['source_key'].unique())

    # TODO: delete?
    def get_feature(self, name):
        """
        Method to get a single GeoJSON feature from the feature dict

        Parameters
        ----------
        name : str or int
            feature dictionary key

        Returns
        -------
            geoJSON object
        """
        if name not in self._features:
            name = str(name)

        if name not in self._features:
            raise KeyError("Name: {} not present in feature dict".format(name))
        else:
            return self._features[name].to_crs(self._ocrs)

    def get_feature_gdf(self, name):
        """
        Method to get all features associated with a single polygon from the geodataframe

        Parameters
        ----------
        name : str or int
            feature name

        Returns
        -------
            geodataframe
        """
        if name not in list(self._features_gdf['source_key']):
            name = str(name)

        if name not in list(self._features_gdf['source_key']):
            raise KeyError("Name: {} not present in geodataframe".format(name))
        else:
            return copy.deepcopy(self._features_gdf[self._features_gdf['source_key'] == name])  # TODO: why does this need to return a copy?

    # TODO: delete?
    def get_shape(self, name):
        """
        Method to get the shapefile shapes from the shapes dict

        Parameters
        ----------
        name : str or int
            feature dictionary key

        Returns
        -------
            geoJSON feature
        """
        if name not in self._shapes:
            raise KeyError("Name: {} not present in shapes dict".format(name))
        else:
            return self._shapes[name]


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

    # TODO: delete after deleting in timeseries.py and/or cbase.py
    def __geojson_to_albers_geojson(self, which='features'):
        """
        Method to convert data in the features or shapes dict to
        albers projection and set the albers_features or albers_shapes
        dictionary

        Parameters
        ----------
        which : str
            <'features' or 'shapes'>

        Returns
        -------
            None
        """
        if which == 'features':
            data = self.features
        elif which == "shapes":
            data = self.shapes
        else:
            raise Exception("Not a valid selection")

        alb_data = {}
        for name, features in data.items():
            if isinstance(features, list):
                alb_features = []
                for feature in features:
                    geofeat = lat_lon_geojson_to_albers_geojson(feature,
                                                                precision=100.)
                    alb_features.append(geofeat)
                alb_data[name] = alb_features

            else:
                geofeat = lat_lon_geojson_to_albers_geojson(features,
                                                            precision=100.)
                alb_data[name] = geofeat

        if which == "features":
            self._albers_features = alb_data
        elif which == "shapes":
            self._albers_shapes = alb_data
        else:
            raise Exception("Code shoudn't have made it here")


    def _features_to_geodataframe(self, features_dict=None, dedupe=True):
        """
        Convert the internal features dict to a GeoDataFrame.

        Parameters
        ----------
        features_dict : dict or None
            Dictionary {source_key: [features]} as stored in self._features.
            If None, uses self._features.
        dedupe : bool
            If True, drop duplicate rows per (source_key, GEOID), keeping first.

        Returns
        -------
        gpd.GeoDataFrame
            One row per feature, Shapely geometry, CRS = EPSG:crs_epsg.
        """
        if features_dict is None:
            features_dict = self._features

        rows = []
        for key, feats in (features_dict or {}).items():
            if len(feats) == 0:
                continue

            for feat in feats:
                # Normalize access for dict vs geojson.Feature
                if isinstance(feat, dict):
                    props = feat.get('properties', {}) or {}
                    geom_dict = feat.get('geometry', None)
                else:
                    # geojson.Feature: properties & geometry attributes
                    props = getattr(feat, 'properties', {}) or {}
                    geom_dict = getattr(feat, 'geometry', None)

                if not geom_dict:
                    # Skip features with missing geometry
                    continue

                try:
                    geom = shapely_shape(geom_dict)
                except Exception:
                    # Skip malformed geometries
                    continue

                # Assemble a flat row
                row = {**props}
                row['source_key'] = key
                row['geometry'] = geom
                rows.append(row)

        gdf = gpd.GeoDataFrame(rows, geometry='geometry', crs=f'EPSG:{self._ESRI_CODE}')
        gdf = gdf.to_crs(self._ocrs)

        # Optional: ensure string columns are consistently cased
        # gdf.columns = [c.upper() for c in gdf.columns]  # uncomment if desired

        # Deduplicate per (source_key, GEOID), mirroring original behavior
        if dedupe and 'GEOID' in gdf.columns:
            gdf = (
                gdf.sort_values(['source_key', 'GEOID'])
                .drop_duplicates(subset=['source_key', 'GEOID'], keep='first')
            )

        return gdf


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
            base = TigerWebMapServer.lcdbase[year]
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
                                    outfields, verbose, retry)

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
            json geography string
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
            features = features[0]

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
