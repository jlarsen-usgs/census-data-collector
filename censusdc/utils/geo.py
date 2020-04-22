import numpy as np
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import cascaded_union
import geojson
import shapefile


def _IGNORE():
    from ..datacollector.tigerweb import TigerWebVariables
    from ..datacollector.acs import AcsVariables
    from ..datacollector.dec import Sf3Variables, Sf3Variables1990

    return (TigerWebVariables.mtfcc,
            TigerWebVariables.oid,
            TigerWebVariables.geoid,
            TigerWebVariables.state,
            TigerWebVariables.county,
            TigerWebVariables.cousub,
            TigerWebVariables.tract,
            TigerWebVariables.blkgrp,
            TigerWebVariables.basename,
            TigerWebVariables.name,
            TigerWebVariables.lsadc,
            TigerWebVariables.funcstat,
            TigerWebVariables.arealand,
            TigerWebVariables.areawater,
            TigerWebVariables.stgeometry,
            TigerWebVariables.centlat,
            TigerWebVariables.centlon,
            TigerWebVariables.intptlat,
            TigerWebVariables.intptlon,
            TigerWebVariables.objectid,
            AcsVariables.median_income,
            Sf3Variables1990.median_income,
            Sf3Variables.median_income)


def _AVERAGE():
    from ..datacollector.acs import AcsVariables
    from ..datacollector.dec import Sf3Variables, Sf3Variables1990
    return (AcsVariables.median_income,
            Sf3Variables1990.median_income,
            Sf3Variables.median_income)


class GeoFeatures(object):
    """
    Class that facilitates the intersection and adjustment
    of geoJSON geometries with arbitrary polygons

    Parameters
    ----------
    features : list
        list of geoJSON features

    """
    def __init__(self, features, name=None):

        self.__name = name
        self._features = features
        self._shapely_features = []
        self._ifeatures = None

        self._create_shapely_geoms()
        self.IGNORE = _IGNORE()

    def _create_shapely_geoms(self):
        """
        Method to set geoJSON features to shapely geometry objects
        """
        for feature in self._features:
            # coords, utm_zone = geoJSON_lat_lon_to_utm(feature)
            coords = feature.geometry.coordinates[0]
            poly = Polygon(coords)
            self._shapely_features.append(poly)

    @property
    def name(self):
        return self.__name

    @property
    def features(self):
        return self._features

    @property
    def intersected_features(self):
        return self._ifeatures

    def intersect(self, polygons):
        """
        Intersection method that creates a new dictionary of geoJSON
        features. input polygons must be provided in WGS84 (decimal lat lon.)

        Parameters
        ----------
        polygons: list
            list of shapely polygons, list of shapefile.Shape objects,
            shapefile.Reader object, shapefile.Shape object, or
            list of xy points [[(lon, lat)...(lon_n, lat_n)],[...]]

        """
        flag = ""

        if isinstance(polygons, shapefile.Reader):
            polygons = [shape for shape in polygons.shapes()]
            flag = "shapefile"

        elif isinstance(polygons, list):
            if isinstance(polygons[0], shapefile.Shape):
                flag = 'shapefile'
            elif isinstance(polygons[0], Polygon):
                flag = "shapely"
            elif isinstance(polygons[0], list):
                polygons = np.array(polygons)
                if len(polygons.shape) == 2:
                    polygons = np.array([polygons])
                elif len(polygons.shape) == 3:
                    pass
                else:
                    raise IndexError(
                        'Polygons must be in a 2 or 3 dimensional list')

                flag = "list"

        elif isinstance(polygons, shapefile.Shape):
            polygons = [polygons, ]
            flag = "shapefile"

        elif isinstance(polygons, (Polygon, MultiPolygon)):
            polygons = [polygons, ]
            flag = "shapely"

        else:
            raise TypeError("{}: not yet supported".format(type(polygons)))

        if flag == "shapefile":
            t = []
            for shape in polygons:

                shape_type = shape.__geo_interface__['type']
                coords = shape.points
                if shape_type.lower() == "polygon":
                    t.append(Polygon(coords),)

                elif shape_type.lower() == "multipolygon":
                    parts = list(shape.parts)
                    for ix in range(1, len(parts)):
                        i0 = parts[ix - 1]
                        i1 = parts[ix]
                        t.append(Polygon(coords[i0:i1]))

                        if len(parts) == ix + 1:
                            t.append(Polygon(coords[i1:]))

                        else:
                            pass

                else:
                    raise NotImplementedError(
                        "{} intersection is not supported".format(shape_type))

            polygons = t

        elif flag == "shapely":
            t = []
            for shape in polygons:
                if isinstance(shape, Polygon):
                    t.append(Polygon)

                elif isinstance(shape, MultiPolygon):
                    for geom in shape.geoms:
                        if isinstance(geom, Polygon):
                            t.append(geom)
                        else:
                            continue

                else:
                    raise NotImplementedError("{}".format(shape.geom_type))

            polygons = t

        elif flag == "list":
            t = []
            for shape in polygons:
                t.append(Polygon(shape))

            polygons = t

        else:
            raise Exception("Code shouldn't have made it here!")

        for polygon in polygons:
            for ix, feature in enumerate(self._shapely_features):
                properties = self._features[ix].properties

                a = polygon.intersection(feature)
                area = a.area
                if area == 0:
                    continue
                geoarea = feature.area
                ratio = area / geoarea

                adj_properties = {}
                for k, v in properties.items():
                    if k in self.IGNORE:
                        adj_properties[k] = v
                    else:
                        try:
                            adj_properties[k] = v * ratio
                        except TypeError:
                            adj_properties[k] = v
                            print("DEBUG NOTE: ", k, v)

                xy = np.array(a.exterior.xy, dtype=float).T
                xy = [(i[0], i[1]) for i in xy]

                geopolygon = geojson.Polygon([xy])
                geofeature = geojson.Feature(geometry=geopolygon,
                                             properties=adj_properties)

                if self._ifeatures is None:
                    self._ifeatures = [geofeature, ]
                else:
                    self._ifeatures.append(geofeature)

    @staticmethod
    def features_to_dataframe(year, features, hr_dict=None):
        """
        Method to take a group of features and accumulate values from
        that dataframe into a single record.

        Parameters
        ----------
        year : int
        features : dict
            geoJSON features
        hr_dict : dict
            human readable column labels for census fields

        Returns
        -------

        """
        IGNORE = _IGNORE()
        AVERAGE = _AVERAGE()
        d = {}
        for feature in features:
            props = feature.properties
            for prop, value in props.items():
                if prop in IGNORE:
                    if prop in AVERAGE:
                        if prop in d:
                            d[prop].append(value)
                        else:
                            d[prop] = [value, ]
                else:
                    if prop in d:
                        d[prop] += value
                    else:
                        d[prop] = value

        for prop in AVERAGE:
            if prop in d:
                d[prop] = np.nanmean(d[prop])

        if hr_dict is not None:
            keys = list(d.keys())
            for key in keys:
                try:
                    d[hr_dict[key]] = [d.pop(key), ]
                except KeyError:
                    d[key] = [d.pop(key), ]
        else:
            for key, value in d.items():
                d[key] = [value, ]

        d["year"] = [year, ]

        df = pd.DataFrame.from_dict(d)
        return df
