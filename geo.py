import pyproj

_lambert = pyproj.Proj("+proj=lcc +lat_1=33 +lat_2=45 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +ellps=GRS80 "
                       "+datum=NAD83 +units=m +no_defs")


def mercator_to_lcc(longitude, latitude):
    return _lambert(longitude, latitude)


def lcc_to_mercator(x, y):
    return _lambert(x, y, inverse=True)