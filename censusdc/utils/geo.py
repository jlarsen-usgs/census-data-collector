import numpy
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
        if isinstance(polygons, shapefile.Reader) or \
                isinstance(polygons, shapefile.Shape):
            # assume there is only one shape, let user do preprocessing
            # if there is more than one
            if isinstance(polygons, shapefile.Reader):
                shape = polygons.shape(0)
            else:
                shape = polygons

            shape_type = shape.__geo_interface__['type']
            coords = shapefile_lat_lon_to_utm(shape)

            if shape_type.lower() == "polygon":
                polygons = [Polygon(coords),]

            elif shape_type.lower() == "multipolygon":
                parts = list(shape.parts)
                polygons = []
                for ix in range(1, len(parts)):
                    i0 = parts[ix - 1]
                    i1 = parts[ix]
                    polygons.append(Polygon(coords[i0:i1]))

                    if len(parts) == ix + 1:
                        polygons.append(Polygon(coords[i1:]))
                    else:
                        pass

            else:
                raise NotImplementedError("{} intersection is "
                                          "not supported".format(shape_type))

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
