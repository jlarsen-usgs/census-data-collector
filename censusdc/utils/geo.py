import numpy
import pandas as pd
from .geometry import geoJSON_lat_lon_to_utm
from shapely.geometry import Polygon
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
        pass

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
