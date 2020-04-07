import numpy as np
import pandas as pd
from .geometry import geoJSON_lat_lon_to_utm, shapefile_lat_lon_to_utm
from shapely.geometry import Polygon, MultiPolygon
import shapefile


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
        self.__name = None
        self._features = features
        self._shapely_features = []
        self._ifeatures = None

        self._create_shapely_geoms()

    def _create_shapely_geoms(self):
        """
        Method to set geoJSON features to shapely geometry objects
        """
        for feature in self._features:
            coords = geoJSON_lat_lon_to_utm(feature)
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
            list of shapely polygons

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
            polygons = [polygons,]
            flag = "shapefile"

        elif isinstance(polygons, (Polygon, MultiPolygon)):
            polygons = [polygons,]
            flag = "shapely"

        else:
            raise TypeError("{}: not yet supported".format(type(polygons)))

        if flag == "shapefile":
            # assume there is only one shape, let user do preprocessing
            # if there is more than one
            t = []
            for shape in polygons:

                shape_type = shape.__geo_interface__['type']
                coords = shapefile_lat_lon_to_utm(shape)

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
            pass

        else:
            raise Exception("Code shouldn't have made it here!")

        for polygon in polygons:
            for feature in self._shapely_features:
                a = polygon.intersection(feature)
                import matplotlib.pyplot as plt

                plt.plot(*feature.exterior.xy, label="feature")
                plt.plot(*polygon.exterior.xy, label="polygon")
                plt.plot(*a.exterior.xy, label='A')
                plt.legend()
                plt.show()
                print('break')

    @staticmethod
    def feature_to_dataframe(year, features):
        """

        Parameters
        ----------
        year : int
        features : dict
            geoJSON features

        Returns
        -------

        """
