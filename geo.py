import pyproj
from geoalchemy2.shape import to_shape

_lambert = pyproj.Proj("+proj=lcc +lat_1=33 +lat_2=45 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +ellps=GRS80 "
                       "+datum=NAD83 +units=m +no_defs")


def mercator_to_lcc(longitude, latitude):
    return _lambert(longitude, latitude)


def lcc_to_mercator(x, y):
    return _lambert(x, y, inverse=True)


def point_list_to_multilinestring(point_list):
    return "MULTILINESTRING((" + ",".join("%s %s" % (lon, lat) for (lon, lat) in point_list) + "))"


def point_list_to_multipolygon(point_list):
    return "MULTIPOLYGON(((" + ",".join("%s %s" % (lon, lat) for (lon, lat) in point_list) + ")))"


def point_to_point(point):
    (lng, lat) = point
    return "POINT(%s %s)" % (lng, lat)


def multipolygon_to_point_list(geometry):
    return list((c[0], c[1]) for c in to_shape(geometry).geoms[0].boundary.coords)


def multilinestring_to_point_list(geometry):
    return list(to_shape(geometry).geoms[0].coords)


def bounds_to_polygon(lat_min, lon_min, lat_max, lon_max):
    return 'POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))' % (
        lon_min, lat_min,
        lon_min, lat_max,
        lon_max, lat_max,
        lon_max, lat_min,
        lon_min, lat_min
    )