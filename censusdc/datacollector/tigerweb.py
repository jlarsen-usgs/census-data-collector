"""
Development code for TigerWeb REST data collection.
"""
import geojson
import requests
import os
import shapefile
import pycrs
from ..utils import get_wkt_wkid_table


class TigerWebVariables(object):
    """
    Tigerweb variable names for querying data
    """
    mtfcc = 'MTFCC'
    oid = 'OID'
    geoid = 'GEOID'
    state = 'STATE'
    county = 'COUNTY'
    tract = 'TRACT'
    blkgrp = 'BLKGRP'
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
    Class to query data from TigerWeb by using shapefile polygons

    Parameters
    ----------
    shp : str
        shapefile path
    field : str
        shapefile field to id multiple polygons

    """
    def __init__(self, shp, field=None):

        if not os.path.isfile(shp):
            raise FileNotFoundError("{} not a valid file path".format(shp))
        prj = shp[:-4] + ".prj"
        if not os.path.isfile(prj):
            raise FileNotFoundError("{}: projection file not found"
                                    .format(prj))

        self._shpname = shp
        self._prjname = prj
        self._field = field.lower()

        self.sf = shapefile.Reader(self._shpname)
        self.prj = pycrs.load.from_file(self._prjname)

        self._wkid = None
        self._shapes = {}
        self._polygons = {}
        self._features = {}

        self._get_polygons()

    @property
    def esri_wkid(self):
        """
        Method to grab the esri WKID from projection information

        Returns
        -------
            wkid : int
        """
        if self._wkid is None:
            df = get_wkt_wkid_table()
            iloc = df.index[df['name'].str.lower() == self.prj.name.lower()].values
            if len(iloc) == 1:
                wkid = df.loc[iloc, 'wkid'].values[0]
                self._wkid = int(wkid)

            else:
                raise Exception("Can't find a matching esri wkid "
                                "for current projection")

        return self._wkid

    @property
    def shapes(self):
        """
        Method to get the shape vertices for each shape

        Returns
        -------
            dict : {name: [vertices]}
        """
        return self._shapes

    @property
    def features(self):
        """
        Method to get all features in the feature dictionary

        Returns
        -------
            dict : {name: geoJSON object}
        """
        if not self._features:
            print("Warning, no features in feature dict,")
            print('Please run get_data() to get TigerWeb features')

        return self._features

    @property
    def feature_names(self):
        """
        Gets the feature names from the feature dict

        Returns
        -------
            list: feature names
        """
        return list(self._features.keys())

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
            raise KeyError("Name: {} not present in feature dict".format(name))
        else:
            return self._features[name]

    def get_shape(self, name):
        """
        Method to get the shapefile shapes from the shapes dict

        Parameters
        ----------
        name : str or int
            feature dictionary key

        Returns
        -------
            list
        """
        if name not in self._shapes:
            raise KeyError("Name: {} not present in shapes dict".format(name))
        else:
            return self._shapes[name]

    def _get_polygons(self):
        """
        Method to read and store polygons from a shapefile for later
        processing.

        Returns
        -------
        """
        if self.sf.shapeType not in (5, 15, 25):
            raise TypeError('Shapetype: {}, is not a valid polygon'
                            .format(self.sf.shapeTypeName))

        named = False
        fidx = 0
        if self._field is None:
            pass
        else:
            for ix, field in enumerate(self.sf.fields):
                if field[0].lower() == self._field:
                    named = True
                    fidx = ix - 1
                    break

        name = -1
        for ix, shape in enumerate(self.sf.shapes()):
            esri_json = self.polygon_to_esri_json(shape.points)
            if named:
                rec = self.sf.record(ix)
                name = rec[fidx]
                if isinstance(name, str):
                    name = name.lower()
            else:
                name += 1

            self._polygons[name] = esri_json
            self._shapes[name] = shape.points

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

    def get_data(self, year, outfields=(), filter=()):
        """
        Method to pull data feature data from tigerweb

        Parameters
        ----------
        year : int
            data year to grab features from

        outFields : tuple
            tuple of output variables to grab from tigerweb
            default is () which grabs GEOID,BLKGRP,STATE,COUNTY,TRACT
            from TigerWeb

        filter : tuple
            tuple of names or polygon numbers to pull from
            default is () which grabs all polygons


        Returns
        -------

        """
        base = "https://tigerweb.geo.census.gov/arcgis/rest/services/" \
               "TIGERweb/Tracts_Blocks/MapServer"

        lut = {2010: {'mapserver': 12,
                      'outFields': 'GEOID,BLOCK,BLKGRP,STATE,'
                                   'COUNTY,TRACT,POP100'},
               -2010: {'mapserver': 11,
                       'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT,POP100'},
               2015: {'mapserver': 4,
                      'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
               2016: {'mapserver': 4,
                      'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
               2017: {'mapserver': 4,
                      'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
               2018: {'mapserver': 8,
                      'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'},
               2019: {'mapserver': 5,
                      'outFields': 'GEOID,BLKGRP,STATE,COUNTY,TRACT'}}

        if year not in lut:
            raise ValueError("year must be either, 2010, 2018, or 2019")

        mapserver = lut[year]['mapserver']

        if outfields:
            if isinstance(outfields, str):
                outfields = (outfields,)
            outfields = ','.join(outfields).upper()
        else:
            outfields = lut[year]['outFields']


        if filter:
            if isinstance(filter, (int, str, float)):
                filter = (filter,)
            filter = tuple([i.lower() if isinstance(i, str) else
                            i for i in filter])

        for key, polygon in self._polygons.items():
            if filter:
                if key not in filter:
                    continue

            s = requests.session()
            url = '/'.join([base, str(mapserver), "query?"])

            s.params = {'where': '',
                        'text': '',
                        'objectIds': '',
                        'geometry': polygon,
                        'geometryType': 'esriGeometryPolygon',
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
            features = []

            while not done:
                r = s.get(url, params={'resultOffset': start,
                                       'resultRecordCount': 32})
                r.raise_for_status()
                # print(r.text)
                counties = geojson.loads(r.text)
                newfeats = counties.__geo_interface__['features']
                if newfeats:
                    features.extend(newfeats)
                    # crs = counties.__geo_interface__['crs']
                    start += len(newfeats)
                    print("Received", len(newfeats), "entries,",
                          start, "total")
                else:
                    done = True

            self._features[key] = features
