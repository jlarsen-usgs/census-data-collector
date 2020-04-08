import numpy as np
import utm


def calculate_circle(x, y, radius):
    """
    Method to calculate a polygon of radius r around a point

    Parameters
    ----------
    x : float
        x-coordinate (ie. longitude or easting)
    y : float
        y-coordinate (ie. latitude or Northing)
    radius : float
        radius of circle

    Returns
    -------
        np.array([x,], [y,])
    """
    radians = np.radians(np.arange(0, 361))

    x = x + (np.cos(radians) * radius)
    y = y + (np.sin(radians) * radius)
    return np.array([x, y])


def geoJSON_lat_lon_to_utm(feature):
    """
    Method to convert geoJSON lat_lon to UTM for
    intersection and scaling operations

    Parameters
    ----------
    feature: geoJSON feature

    Returns
    -------
        list
    """
    coords = feature.geometry.coordinates[0]
    utm_coords = []
    utm_zones = []
    for t in coords:
        temp = utm.from_latlon(t[1], t[0])
        utm_coords.append(temp[0:2])
        utm_zones.append(temp[2:])
    return utm_coords, utm_zones


def shapefile_lat_lon_to_utm(shape):
    """
    Method to convert shapefile lat_lon to UTM for
    intersection and scaling operations

    Parameters
    ----------
    shape : shapefile.Shape object

    Returns
    -------
        list
    """
    coords = shape.points
    coords = [utm.from_latlon(t[1], t[0])[0:2] for t in coords]
    return coords