__author__ = 'nathan'

import uuid
import os
import tempfile
import shutil
import subprocess
from collections import namedtuple
import glob

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSON
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape

from ctools_backend import geo
import settings

engine = sa.create_engine(settings.connection_string)
Base = declarative_base(metadata=sa.MetaData(bind=engine))
Session = orm.scoped_session(orm.sessionmaker(bind=engine))

def point_wkt_to_array(point):
    return list(v[0] for v in to_shape(point).xy)

def null_data(d):
    if d is None:
        return -999
    else:
        return d

class Receptor(object):
    fields = ["id", "x", "y", "lat", "lng"]
    namedtuple_class = namedtuple("Receptor", fields)

    def __init__(self, id_=None, lat=None, lng=None, x=None, y=None):
        if lat and lng:
            self.lat = lat
            self.lng = lng
            (self.x, self.y) = geo.mercator_to_lcc(lng, lat)
        elif x and y:
            self.x = x
            self.y = y
            (self.lng, self.lat) = geo.lcc_to_mercator(x, y)
        else:
            raise ValueError("Must specify either x and y or lat and lng")
        self.id = id_

    def as_namedtuple(self):
        return self.namedtuple_class(self.id, self.x, self.y, self.lat, self.lng)

    @staticmethod
    def instance_factory(fields, instance_tuples):
        return [Receptor(**{k: v for (k, v) in zip(fields, instance)}) for
                instance in instance_tuples]


class Road(Base):
    __tablename__ = "roads"
    fields = ["gid", "id", "sign1", "from_x", "from_y", "to_x", "to_y", "sf_id",
              "stfips", "ctfips", "fclass_rev", "aadt", "mph", "geom", "gas_car_multiplier",
              "gas_truck_multiplier", "diesel_car_multiplier",
              "diesel_truck_multiplier"]
    namedtuple_class = namedtuple("Road", fields)
    gid = sa.Column(sa.Integer, primary_key=True)
    id = sa.Column(sa.Numeric(asdecimal=False))
    sign1 = sa.Column(sa.String(100))
    from_x = sa.Column(sa.Numeric(asdecimal=False))
    from_y = sa.Column(sa.Numeric(asdecimal=False))
    to_x = sa.Column(sa.Numeric(asdecimal=False))
    to_y = sa.Column(sa.Numeric(asdecimal=False))
    sf_id = sa.Column(sa.Numeric(asdecimal=False))
    stfips = sa.Column(sa.Numeric(asdecimal=False))
    ctfips = sa.Column(sa.Numeric(asdecimal=False))
    fclass_rev = sa.Column(sa.Numeric(asdecimal=False))
    aadt = sa.Column("aadt07", sa.Numeric(asdecimal=False))
    mph = sa.Column("am_spd", sa.Integer)
    geom = sa.Column(Geometry('MULTILINESTRING'))

    def __init__(self, **kwargs):
        self.gas_car_multiplier = kwargs.pop("gas_car_multiplier", 1)
        self.gas_truck_multiplier = kwargs.pop("gas_truck_multiplier", 1)
        self.diesel_car_multiplier = kwargs.pop("diesel_car_multiplier", 1)
        self.diesel_truck_multiplier = kwargs.pop("diesel_truck_multiplier", 1)
        super(Road, self).__init__(**kwargs)

    @classmethod
    def construct_namedtuple(cls, *args):
        return cls.namedtuple_class(*[null_data(a) for a in args])

    def as_namedtuple(self):
        return self.namedtuple_class(
            self.gid, self.id, self.sign1, self.from_x, self.from_y, self.to_x, self.to_y,
            self.sf_id, self.stfips, self.ctfips, self.fclass_rev, self.aadt,
            self.mph, geo.multilinestring_to_point_list(self.geom), 1, 1, 1, 1)

    @staticmethod
    def instance_factory(fields, instance_tuples):
        return [Road(**{k: v for (k, v) in zip(fields, instance)}) for
                instance in instance_tuples]


class Railway(Base):
    __tablename__ = "railways"
    fields = ["gid", "rrowner1", "fromx", "fromy", "tox", "toy", "sf_id", "nox", "benz", "pm25",
              "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3", "toluene", "so2", "geom"]
    namedtuple_class = namedtuple("Railway", fields)
    gid = sa.Column("objectid", sa.Integer, primary_key=True)
    rrowner1 = sa.Column(sa.String)
    fromx = sa.Column(sa.Numeric(asdecimal=False))
    fromy = sa.Column(sa.Numeric(asdecimal=False))
    tox = sa.Column(sa.Numeric(asdecimal=False))
    toy = sa.Column(sa.Numeric(asdecimal=False))
    sf_id = sa.Column(sa.Integer)
    nox = sa.Column(sa.Numeric(asdecimal=False))
    pm25 = sa.Column(sa.Numeric(asdecimal=False))
    co = sa.Column(sa.Numeric(asdecimal=False))
    benz = sa.Column(sa.Numeric(asdecimal=False))
    dies_pm25 = sa.Column(sa.Numeric(asdecimal=False))
    ec = sa.Column(sa.Numeric(asdecimal=False))
    oc = sa.Column(sa.Numeric(asdecimal=False))
    form = sa.Column(sa.Numeric(asdecimal=False))
    ald2 = sa.Column(sa.Numeric(asdecimal=False))
    acro = sa.Column(sa.Numeric(asdecimal=False))
    butal_3 = sa.Column(sa.Numeric(asdecimal=False))
    toluene = sa.Column(sa.Numeric(asdecimal=False))
    so2 = sa.Column(sa.Numeric(asdecimal=False))
    geom = sa.Column(Geometry('MULTILINESTRING'))

    @classmethod
    def construct_namedtuple(cls, *args):
        return cls.namedtuple_class(*[null_data(a) for a in args])

    def as_namedtuple(self):
        return self.namedtuple_class(self.gid, self.rrowner1, self.fromx, self.fromy, self.tox, self.toy,
                                     self.sf_id, null_data(self.nox), null_data(self.benz), null_data(self.pm25),
                                     null_data(self.dies_pm25), null_data(self.ec),
                                     null_data(self.oc), null_data(self.co), null_data(self.form),
                                     null_data(self.ald2), null_data(self.acro), null_data(self.butal_3),
                                     null_data(self.toluene), null_data(self.so2),
                                     geo.multilinestring_to_point_list(self.geom))

    @staticmethod
    def instance_factory(fields, instance_tuples):
        return [Railway(**{k: v for (k, v) in zip(fields, instance)}) for
                instance in instance_tuples]


class AreaSource(Base):
    __tablename__ = "area_sources"
    fields = ["facility", "gid", "sf_id", "nox", "benz", "pm2_5", "dies_pm25", "ec", "oc",
              "co", "form", "ald2", "acro", "butal_3", "toluene", "so2", "geom"]
    namedtuple_class = namedtuple("AreaSource", fields)
    facility = sa.Column(sa.Text)
    gid = sa.Column("object_id", sa.Integer, primary_key=True)
    sf_id = sa.Column(sa.Integer)
    nox = sa.Column(sa.Numeric(asdecimal=False))
    pm2_5 = sa.Column(sa.Numeric(asdecimal=False))
    co = sa.Column(sa.Numeric(asdecimal=False))
    benz = sa.Column(sa.Numeric(asdecimal=False))
    dies_pm25 = sa.Column(sa.Numeric(asdecimal=False))
    ec = sa.Column(sa.Numeric(asdecimal=False))
    oc = sa.Column(sa.Numeric(asdecimal=False))
    form = sa.Column(sa.Numeric(asdecimal=False))
    ald2 = sa.Column(sa.Numeric(asdecimal=False))
    acro = sa.Column(sa.Numeric(asdecimal=False))
    butal_3 = sa.Column(sa.Numeric(asdecimal=False))
    toluene = sa.Column(sa.Numeric(asdecimal=False))
    so2 = sa.Column(sa.Numeric(asdecimal=False))
    geom = sa.Column(Geometry('MULTIPOLYGON'))

    @classmethod
    def construct_namedtuple(cls, *args):
        return cls.namedtuple_class(*[null_data(a) for a in args])

    def as_namedtuple(self):
        geom = geo.multipolygon_to_point_list(self.geom)
        return self.namedtuple_class(self.facility, self.gid, self.sf_id, null_data(self.nox), null_data(self.benz),
                                     null_data(self.pm2_5), null_data(self.dies_pm25), null_data(self.ec),
                                     null_data(self.oc), null_data(self.co), null_data(self.form),
                                     null_data(self.ald2), null_data(self.acro), null_data(self.butal_3),
                                     null_data(self.toluene), null_data(self.so2), geom)

    @staticmethod
    def to_vertices(source):
        results = []
        for (lng, lat) in source.geom:
            (x, y) = geo.mercator_to_lcc(lng, lat)
            results.append(
                [source.gid, source.sf_id, x, y, null_data(source.nox), null_data(source.benz),
                 null_data(source.pm2_5),
                 null_data(source.dies_pm25), null_data(source.ec),
                 null_data(source.oc), null_data(source.co), null_data(source.form),
                 null_data(source.ald2), null_data(source.acro), null_data(source.butal_3),
                 null_data(source.toluene), null_data(source.so2)])
        return results


class ShipInTransit(Base):
    __tablename__ = "ships_in_transit"
    fields = ["facility", "gid", "startx", "starty", "endx", "endy", "sf_id", "nox", "benz", "pm2_5",
              "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3", "toluene", "so2", "stack_height",
              "stack_diameter", "stack_velocity", "stack_temperature", "geom"]
    namedtuple_class = namedtuple("ShipInTransit", fields)
    gid = sa.Column("object_id", sa.Integer, primary_key=True)
    facility = sa.Column(sa.Text)
    startx = sa.Column(sa.Numeric(asdecimal=False))
    starty = sa.Column(sa.Numeric(asdecimal=False))
    endx = sa.Column(sa.Numeric(asdecimal=False))
    endy = sa.Column(sa.Numeric(asdecimal=False))
    sf_id = sa.Column(sa.Integer)
    nox = sa.Column(sa.Numeric(asdecimal=False))
    pm2_5 = sa.Column(sa.Numeric(asdecimal=False))
    co = sa.Column(sa.Numeric(asdecimal=False))
    benz = sa.Column(sa.Numeric(asdecimal=False))
    dies_pm25 = sa.Column(sa.Numeric(asdecimal=False))
    ec = sa.Column(sa.Numeric(asdecimal=False))
    oc = sa.Column(sa.Numeric(asdecimal=False))
    form = sa.Column(sa.Numeric(asdecimal=False))
    ald2 = sa.Column(sa.Numeric(asdecimal=False))
    acro = sa.Column(sa.Numeric(asdecimal=False))
    butal_3 = sa.Column(sa.Numeric(asdecimal=False))
    toluene = sa.Column(sa.Numeric(asdecimal=False))
    so2 = sa.Column(sa.Numeric(asdecimal=False))
    stack_height = sa.Column(sa.Numeric(asdecimal=False))
    stack_diameter = sa.Column(sa.Numeric(asdecimal=False))
    stack_velocity = sa.Column(sa.Numeric(asdecimal=False))
    stack_temperature = sa.Column(sa.Numeric(asdecimal=False))
    geom = sa.Column(Geometry('MULTILINESTRING'))

    @classmethod
    def construct_namedtuple(cls, *args):
        return cls.namedtuple_class(*[null_data(a) for a in args])

    def as_namedtuple(self):
        return self.namedtuple_class(self.facility, self.gid, self.startx, self.starty, self.endx,
                                     self.endy, self.sf_id, null_data(self.nox), null_data(self.benz),
                                     null_data(self.pm2_5), null_data(self.dies_pm25), null_data(self.ec),
                                     null_data(self.oc), null_data(self.co), null_data(self.form),
                                     null_data(self.ald2), null_data(self.acro), null_data(self.butal_3),
                                     null_data(self.toluene), null_data(self.so2), self.stack_height, self.stack_diameter,
                                     self.stack_velocity, self.stack_temperature,
                                     geo.multilinestring_to_point_list(self.geom))


class PointSource(Base):
    __tablename__ = "point_sources"
    fields = ["pltname", "gid", "x", "y", "sf_id", "stkht", "stkdm", "stktmp", "stkvel", "nox", "benz",
              "pm25", "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3", "toluene", "so2", "geom",
              "in_port"]
    namedtuple_class = namedtuple("PointSource", fields)
    pltname = sa.Column(sa.Text)
    gid = sa.Column(sa.Integer, primary_key=True)
    y = sa.Column(sa.Numeric(asdecimal=False))
    x = sa.Column(sa.Numeric(asdecimal=False))
    sf_id = sa.Column(sa.Integer)
    stkht = sa.Column(sa.Numeric(asdecimal=False))
    stkdm = sa.Column(sa.Numeric(asdecimal=False))
    stktmp = sa.Column(sa.Numeric(asdecimal=False))
    stkvel = sa.Column(sa.Numeric(asdecimal=False))
    nox = sa.Column(sa.Numeric(asdecimal=False))
    pm25 = sa.Column(sa.Numeric(asdecimal=False))
    co = sa.Column(sa.Numeric(asdecimal=False))
    benz = sa.Column(sa.Numeric(asdecimal=False))
    dies_pm25 = sa.Column(sa.Numeric(asdecimal=False))
    ec = sa.Column(sa.Numeric(asdecimal=False))
    oc = sa.Column(sa.Numeric(asdecimal=False))
    form = sa.Column(sa.Numeric(asdecimal=False))
    ald2 = sa.Column(sa.Numeric(asdecimal=False))
    acro = sa.Column(sa.Numeric(asdecimal=False))
    butal_3 = sa.Column(sa.Numeric(asdecimal=False))
    toluene = sa.Column(sa.Numeric(asdecimal=False))
    so2 = sa.Column(sa.Numeric(asdecimal=False))
    geom = sa.Column(Geometry('POINT'))
    in_port = sa.Column(sa.Boolean)

    @classmethod
    def construct_namedtuple(cls, *args):
        return cls.namedtuple_class(*[null_data(a) for a in args])

    def as_namedtuple(self):
        shape = geo.to_shape(self.geom)
        return self.namedtuple_class(self.pltname, self.gid, self.x, self.y, self.sf_id, self.stkht,
                                     self.stkdm, self.stktmp, self.stkvel, null_data(self.nox),
                                     null_data(self.benz), null_data(self.pm25),
                                     null_data(self.dies_pm25), null_data(self.ec),
                                     null_data(self.oc), null_data(self.co), null_data(self.form),
                                     null_data(self.ald2), null_data(self.acro), null_data(self.butal_3),
                                     null_data(self.toluene), null_data(self.so2), [shape.x, shape.y], self.in_port)


class Scenario(Base):
    __tablename__ = "scenario"
    scenario_id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.Text)
    tool = sa.Column(sa.Text)
    name = sa.Column(sa.Text)
    hour = sa.Column(sa.Integer)
    season = sa.Column(sa.Integer)
    day = sa.Column(sa.Integer)
    met_conditions = sa.Column(sa.Integer)
    area_sources = sa.Column(JSON)
    point_sources = sa.Column(JSON)
    railways = sa.Column(JSON)
    roads = sa.Column(JSON)
    ships_in_transit = sa.Column(JSON)
    center = sa.Column(Geometry("POINT"))
    zoom = sa.Column(sa.Integer)
    area_source_fields = sa.Column(JSON)
    point_source_fields = sa.Column(JSON)
    railway_fields = sa.Column(JSON)
    road_fields = sa.Column(JSON)
    ship_in_transit_fields = sa.Column(JSON)
    include_area_sources = sa.Column(sa.Boolean)
    include_point_sources = sa.Column(sa.Boolean)
    include_railways = sa.Column(sa.Boolean)
    include_roads = sa.Column(sa.Boolean)
    include_ships_in_transit = sa.Column(sa.Boolean)

    @property
    def safe_name(self):
        return "".join([x if x.isalnum() else "_" for x in self.name])

    @property
    def to_dict(self):
        return {
            "scenario_id": self.scenario_id,
            "tool": self.tool,
            "name": self.name,
            "hour": self.hour,
            "season": self.season,
            "day": self.day,
            "met_conditions": self.met_conditions,
            "area_sources": self.area_sources,
            "point_sources": self.point_sources,
            "railways": self.railways,
            "roads": self.roads,
            "ships_in_transit": self.ships_in_transit,
            "center": point_wkt_to_array(self.center),
            "zoom": self.zoom,
            "area_source_fields": self.area_source_fields,
            "point_source_fields": self.point_source_fields,
            "railway_fields": self.railway_fields,
            "road_fields": self.road_fields,
            "ship_in_transit_fields": self.ship_in_transit_fields,
            "include_area_sources": self.include_area_sources,
            "include_point_sources": self.include_area_sources,
            "include_railways": self.include_railways,
            "include_roads": self.include_roads,
            "include_ships_in_transit": self.include_ships_in_transit
        }


class AbstractScenarioRun(object):

    @property
    def current_status(self):
        select = sa.select([self.__table__.c.status]).where(type(self).scenario_run_id == self.scenario_run_id)
        return select.execute().fetchone()[0]

    def _get_bounds_helper(self, scenario):
        if scenario.include_area_sources:
            for source in scenario.area_sources:
                for (lng, lat) in source[16]:
                    if lat < self.min_lat or self.min_lat is None:
                        self.min_lat = lat
                    if lat > self.max_lat or self.max_lat is None:
                        self.max_lat = lat
                    if lng < self.min_lng or self.min_lng is None:
                        self.min_lng = lng
                    if lng > self.max_lng or self.max_lng is None:
                        self.max_lng = lng
        if scenario.include_point_sources:
            for source in scenario.point_sources:
                (lng, lat) = source[22]
                if lat < self.min_lat or self.min_lat is None:
                    self.min_lat = lat
                if lat > self.max_lat or self.max_lat is None:
                    self.max_lat = lat
                if lng < self.min_lng or self.min_lng is None:
                    self.min_lng = lng
                if lng > self.max_lng or self.max_lng is None:
                    self.max_lng = lng
        if scenario.include_roads:
            for source in scenario.roads:
                for (lng, lat) in source[13]:
                    if lat < self.min_lat or self.min_lat is None:
                        self.min_lat = lat
                    if lat > self.max_lat or self.max_lat is None:
                        self.max_lat = lat
                    if lng < self.min_lng or self.min_lng is None:
                        self.min_lng = lng
                    if lng > self.max_lng or self.max_lng is None:
                        self.max_lng = lng
        if scenario.include_railways:
            for source in scenario.railways:
                for (lng, lat) in source[20]:
                    if lat < self.min_lat or self.min_lat is None:
                        self.min_lat = lat
                    if lat > self.max_lat or self.max_lat is None:
                        self.max_lat = lat
                    if lng < self.min_lng or self.min_lng is None:
                        self.min_lng = lng
                    if lng > self.max_lng or self.max_lng is None:
                        self.max_lng = lng
        if scenario.include_ships_in_transit:
            for source in scenario.ships_in_transit:
                for (lng, lat) in source[24]:
                    if lat < self.min_lat or self.min_lat is None:
                        self.min_lat = lat
                    if lat > self.max_lat or self.max_lat is None:
                        self.max_lat = lat
                    if lng < self.min_lng or self.min_lng is None:
                        self.min_lng = lng
                    if lng > self.max_lng or self.max_lng is None:
                        self.max_lng = lng

    def _get_bounds(self):
        self.min_lat = None
        self.min_lng = None
        self.max_lat = None
        self.max_lng = None
        if isinstance(self, ScenarioRun):
            self._get_bounds_helper(self.scenario)
        else:
            self._get_bounds_helper(self.scenario_1)
            self._get_bounds_helper(self.scenario_2)


class ScenarioRun(Base, AbstractScenarioRun):
    __tablename__ = "scenario_run"
    scenario_run_id = sa.Column(sa.Integer, primary_key=True)
    scenario_id = sa.Column(sa.Integer, sa.ForeignKey("scenario.scenario_id"))
    user_id = sa.Column(sa.Text)
    tool = sa.Column(sa.Text)
    status = sa.Column(sa.Text)
    output_directory = sa.Column(sa.Text)
    results_file_name = sa.Column(sa.Text)
    model_type = sa.Column(sa.Integer)
    pollutant = sa.Column(sa.Text)
    model_min_value = sa.Column(sa.Float)
    model_max_value = sa.Column(sa.Float)
    scenario = orm.relationship(Scenario)
    min_lat = sa.Column(sa.Numeric(asdecimal=False))
    max_lat = sa.Column(sa.Numeric(asdecimal=False))
    min_lng = sa.Column(sa.Numeric(asdecimal=False))
    max_lng = sa.Column(sa.Numeric(asdecimal=False))

    def __init__(self, *args, **kwargs):
        super(ScenarioRun, self).__init__(*args, **kwargs)
        dir_name = str(uuid.uuid4())
        self.output_directory = os.path.join(settings.scenario_run_directory, dir_name)
        self.results_file_name = "%s.tar.gz" % self.scenario.safe_name
        try:
            os.mkdir(self.output_directory)
        except OSError:
            pass
        self._get_bounds()

    @property
    def to_dict(self):
        return {
            "scenario_run_id": self.scenario_run_id,
            "tool": self.tool,
            "status": self.status,
            "model_type": self.model_type,
            "min_value": self.model_min_value,
            "max_value": self.model_max_value,
            "min_lat": self.min_lat,
            "max_lat": self.max_lat,
            "min_lng": self.min_lng,
            "max_lng": self.max_lng,
            "pollutant": self.pollutant,
            "scenario_name": self.scenario.name
        }

    @property
    def image_file(self):
        return os.path.join(self.output_directory, "concentrations.png")

    @property
    def legend_file(self):
        return os.path.join(self.output_directory, "concentrations_legend.png")

    def finalize_run(self):
        self.status = "completed"
        starting_dir = os.getcwd()
        os.chdir(settings.output_tar_directory)
        subprocess.call(["tar", "-C", self.output_directory, "-czf", self.results_file_name, "."])
        os.chdir(starting_dir)


class ComparisonScenarioRun(Base, AbstractScenarioRun):
    __tablename__ = "comparison_scenario_run"
    scenario_run_id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.Text)
    status = sa.Column(sa.Text)
    tool = sa.Column(sa.Text)
    output_directory_1 = sa.Column(sa.Text)
    output_directory_2 = sa.Column(sa.Text)
    results_file_name = sa.Column(sa.Text)
    model_type = sa.Column(sa.Integer)
    pollutant = sa.Column(sa.Text)
    model_min_value = sa.Column(sa.Float)
    model_max_value = sa.Column(sa.Float)
    comparison_mode = sa.Column(sa.Integer)
    min_lat = sa.Column(sa.Numeric(asdecimal=False))
    max_lat = sa.Column(sa.Numeric(asdecimal=False))
    min_lng = sa.Column(sa.Numeric(asdecimal=False))
    max_lng = sa.Column(sa.Numeric(asdecimal=False))
    scenario_1_id = sa.Column(sa.Integer, sa.ForeignKey("scenario.scenario_id"))
    scenario_2_id = sa.Column(sa.Integer, sa.ForeignKey("scenario.scenario_id"))
    scenario_1 = orm.relationship(Scenario, primaryjoin=scenario_1_id == Scenario.scenario_id)
    scenario_2 = orm.relationship(Scenario, primaryjoin=scenario_2_id == Scenario.scenario_id)

    def __init__(self, *args, **kwargs):
        super(ComparisonScenarioRun, self).__init__(*args, **kwargs)
        self.results_file_name = "%s_vs_%s.tar.gz" % (self.scenario_1.safe_name, self.scenario_2.safe_name)
        dir_name_1 = str(uuid.uuid4())
        dir_name_2 = str(uuid.uuid4())
        self.output_directory_1 = os.path.join(settings.scenario_run_directory, dir_name_1)
        self.output_directory_2 = os.path.join(settings.scenario_run_directory, dir_name_2)
        self.temp_dir = tempfile.mkdtemp()
        try:
            os.mkdir(self.output_directory_1)
            os.mkdir(self.output_directory_2)
        except OSError:
            pass
        self._get_bounds()

    @property
    def to_dict(self):
        return {
            "scenario_run_id": self.scenario_run_id,
            "status": self.status,
            "tool": self.tool,
            "model_type": self.model_type,
            "min_value": self.model_min_value,
            "max_value": self.model_max_value,
            "min_lat": self.min_lat,
            "max_lat": self.max_lat,
            "min_lng": self.min_lng,
            "max_lng": self.max_lng,
            "pollutant": self.pollutant,
            "comparison_mode": self.comparison_mode,
            "scenario_1_name": self.scenario_1.name,
            "scenario_2_name": self.scenario_2.name
        }

    @property
    def image_file(self):
        return os.path.join(self.output_directory_1, "concentrations.png")

    @property
    def legend_file(self):
        return os.path.join(self.output_directory_1, "concentrations_legend.png")

    def finalize_run(self):
        self.status = "completed"
        output_directory_1 = os.path.join(self.temp_dir, self.scenario_1.safe_name)
        output_directory_2 = os.path.join(self.temp_dir, self.scenario_2.safe_name)
        os.mkdir(output_directory_1)
        os.mkdir(output_directory_2)
        shutil.copy(os.path.join(self.output_directory_1, "concentrations.png"), self.temp_dir)
        shutil.copy(os.path.join(self.output_directory_1, "concentrations_legend.png"), self.temp_dir)
        shutil.copy(os.path.join(self.output_directory_1, "CTOOLS_Inputs.txt"), output_directory_1)
        for filename in glob.glob(os.path.join(self.output_directory_1, "*.csv")):
            shutil.copy(filename, output_directory_1)
        shutil.copy(os.path.join(self.output_directory_2, "CTOOLS_Inputs.txt"), output_directory_2)
        for filename in glob.glob(os.path.join(self.output_directory_2, "*.csv")):
            shutil.copy(filename, output_directory_2)
        starting_dir = os.getcwd()
        os.chdir(settings.output_tar_directory)
        subprocess.call(["tar", "-C", self.temp_dir, "-czf", self.results_file_name, "."])
        os.chdir(starting_dir)