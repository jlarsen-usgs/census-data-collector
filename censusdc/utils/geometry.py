import numpy as np
import geojson


# Constants for lon_lat_to_albers and albers_to_lon_lat
# Developed from Snyder, 1987. USGS Professional paper 1395
TO_RAD = np.pi / 180.
SP1 = 0 * TO_RAD
SP2 = 60 * TO_RAD
LON_ORIG = 0 * TO_RAD
LAT_ORIG = 0 * TO_RAD
N = 0.5 * (np.sin(SP1) + np.sin(SP2))
C = np.cos(SP1) ** 2 + (2 * N * np.sin(SP1))
RHO0 = np.sqrt((C - (2 * N * np.sin(LAT_ORIG)))) / N


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


def lon_lat_to_albers(lon, lat):
    """
    Method to convert decimal longitute and latitude to albers equal
    area projection

    Developed from Snyder, 1987. USGS Professional paper 1395

    Parameters
    ----------
    lon : list or numpy array of longitude
    lat : list of numpy array of latitude

    Returns
    -------
    x, y: tuple
        tuple of projected coordinates

    """
    if not isinstance(lon, np.ndarray):
        lon = np.array(lon, dtype=float)

    if not isinstance(lat, np.ndarray):
        lat = np.array(lat, dtype=float)

    if lat.shape != lon.shape:
        raise AssertionError("The shape of the lat and lon array must be "
                             "exactly the same")

    lat *= TO_RAD
    lon *= TO_RAD

    RHO = np.sqrt((C - (2 * N * np.sin(lat)))) / N
    THETA = N * (lon - LON_ORIG)

    x = RHO * np.sin(THETA)
    y = RHO0 - (RHO * np.cos(THETA))

    # h, scale factor along meridians
    # k, scale factor along parallels... 1/h

    return x, y


def albers_to_lon_lat(x, y):
    """
    Method to convert from albers equal area projection back to WGS84
    lat. lon.

    Developed from Snyder, 1987. USGS Professional paper 1395

    Parameters
    ----------
    x : list or numpy array of x points
    y : list or numpy array of y points

    Returns
    -------
        lon, lat: tuple
    """
    if not isinstance(x, np.ndarray):
        lon = np.array(x)

    if not isinstance(y, np.ndarray):
        lat = np.array(y)

    if x.shape != y.shape:
        raise AssertionError("The shape of the lat and lon array must be "
                             "exactly the same")

    # do not covert x, y to radians, because they are already in radial
    # coordinates

    THETA = np.arctan(x / (RHO0 - y))
    t = THETA / TO_RAD
    RHO = np.sqrt(np.square(x) + np.square(RHO0 - y))

    lon = (LON_ORIG + (THETA / N)) / TO_RAD
    lat = np.arcsin((C - (RHO**2 * N**2)) / (2 * N)) / TO_RAD

    return lon, lat


def lat_lon_geojson_to_albers_geojson(feature, invert=False):
    """
    Method to convert geojson polygons and multipolygon features
    from lat lon to albers or inverse

    Parameters
    ----------
    feature : geoJSON feature
        geoJSON feature class that contains a geoJSON polygon

    invert : bool
        when true method converts from albers to lat lon

    Returns
    -------
        geoJSON feature
    """
    if feature.geometry.type == "Polygon":
        coords = feature.geometry.coordinates
        conv_coords = []
        for coord in coords:
            if invert:
                conv = albers_to_lon_lat(*np.array(coord).T)
            else:
                conv = lon_lat_to_albers(*np.array(coord).T)
            conv_coords.append(list(zip(*conv)))
        geopoly = geojson.Polygon(conv_coords)

    elif feature.geometry.type == "MultiPolygon":
        polys = feature.geometry.coordinates
        conv_polys = []
        for poly in polys:
            conv_coords = []
            for coord in poly:
                if invert:
                    conv = albers_to_lon_lat(*np.array(coord.T))
                else:
                    conv = lon_lat_to_albers(*np.array(coord).T)
                conv_coords.append(list(zip(*conv)))
            conv_polys.append(conv_coords)
        geopoly = geojson.MultiPolygon(conv_polys)

    else:
        msg = "Geometry type {}, not yet " \
              "supported".format(feature.geometry.type)
        raise Exception(msg)

    geofeat = geojson.Feature(geometry=geopoly, properties=feature.properties)
    return geofeat
