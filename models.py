__author__ = 'nathan'

import uuid
import os
import tempfile
import shutil
import subprocess
from collections import namedtuple

import sqlalchemy as sa
import pika
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSON
from geoalchemy2 import Geometry

import geo

import settings


engine = sa.create_engine(settings.connection_string)
Base = declarative_base(metadata=sa.MetaData(bind=engine))
Session = orm.scoped_session(orm.sessionmaker(bind=engine))

Receptor = namedtuple("Receptor", ["id", "x", "y", "lat", "lng"])

AreaSource = namedtuple("AreaSource",
                        ["facility", "gid", "sf_id", "nox", "benz", "pm2_5", "dies_pm25", "ec", "oc",
                         "co", "form", "ald2", "acro", "butal_3", "geom"])
PointSource = namedtuple("PointSource",
                         ["pltname", "gid", "x", "y", "sf_id", "stkht", "stkdm", "stktmp", "stkvel", "nox",
                          "benz", "pm25", "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3",
                          "geom"])
Railway = namedtuple("Railway",
                     ["gid", "rrowner1", "fromx", "fromy", "tox", "toy", "sf_id", "nox", "benz", "pm25",
                      "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3", "geom"])
Road = namedtuple("Road", ["gid", "id", "sign1", "from_x", "from_y", "to_x", "to_y", "sf_id",
                           "stfips", "ctfips", "fclass_rev", "aadt", "mph_am", "mph_md",
                           "mph_pm", "mph_op", "geom", "gas_car_multiplier",
                           "gas_truck_multiplier", "diesel_car_multiplier",
                           "diesel_truck_multiplier"])
ShipInTransit = namedtuple("ShipInTransit",
                           ["facility", "gid", "startx", "starty", "endx", "endy", "sf_id", "nox", "benz",
                            "pm2_5", "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3",
                            "stack_height", "stack_diameter", "stack_velocity", "stack_temperature", "geom"])


def area_source_to_vertices(source):
    results = []
    for (lng, lat) in source.geom:
        (x, y) = geo.mercator_to_lcc(lng, lat)
        results.append(
            [source.gid, source.sf_id, x, y, source.nox, source.benz, source.pm2_5,
             source.dies_pm25, source.ec, source.oc, source.co, source.form, source.ald2, source.acro,
             source.butal_3])
    return results


class Scenario(Base):
    __tablename__ = "scenario"
    scenario_id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.Integer)
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
    location = sa.Column(Geometry("POINT"))
    zoom = sa.Column(sa.Integer)
    include_area_sources = sa.Column(sa.Boolean)
    include_point_sources = sa.Column(sa.Boolean)
    include_rail_sources = sa.Column(sa.Boolean)
    include_road_sources = sa.Column(sa.Boolean)
    include_ship_sources = sa.Column(sa.Boolean)

    @property
    def safe_name(self):
        return "".join([x if x.isalnum() else "_" for x in self.name])


class ScenarioRun(Base):
    __tablename__ = "scenario_run"
    scenario_run_id = sa.Column(sa.Integer, primary_key=True)
    scenario_id = sa.Column(sa.Integer, sa.ForeignKey("scenario.scenario_id"))
    status = sa.Column(sa.Text)
    read_queue = sa.Column(sa.Text)
    write_queue = sa.Column(sa.Text)
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
        super(Base, self).__init__(*args, **kwargs)
        dir_name = uuid.uuid4()
        self.output_directory = os.path.join(settings.scenario_run_directory, dir_name)
        self.results_file_name = "%s.tar.gz" % self.scenario.safe_name
        try:
            os.mkdir(self.output_directory)
        except OSError:
            pass

    def finalize_run(self):
        self.status = "completed"
        starting_dir = os.getcwd()
        os.chdir(settings.output_tar_directory)
        subprocess.call(["tar", "-C", self.output_directory, "-czf", self.results_file_name, "."])
        os.chdir(starting_dir)
        connection = pika.SelectConnection(pika.ConnectionParameters('treehug.its.unc.edu'))
        channel = connection.channel()
        channel.basic_publish(exchange='', routing_key=self.write_queue, body='completed')
        connection.close()


class ComparisonScenarioRun(Base):
    __tablename__ = "comparison_scenario_run"
    comparison_scenario_run_id = sa.Column(sa.Integer, primary_key=True)
    status = sa.Column(sa.Text)
    read_queue = sa.Column(sa.Text)
    write_queue = sa.Column(sa.Text)
    output_directory_1 = sa.Column(sa.Text)
    output_directory_2 = sa.Column(sa.Text)
    results_file_name = sa.Column(sa.Text)
    model_type = sa.Column(sa.Integer)
    pollutant = sa.Column(sa.Text)
    model_min_value = sa.Column(sa.Float)
    model_max_value = sa.Column(sa.Float)
    comparison_mode = sa.Column(sa.Text)
    min_lat = sa.Column(sa.Numeric(asdecimal=False))
    max_lat = sa.Column(sa.Numeric(asdecimal=False))
    min_lng = sa.Column(sa.Numeric(asdecimal=False))
    max_lng = sa.Column(sa.Numeric(asdecimal=False))
    scenario_1_id = sa.Column(sa.Integer, sa.ForeignKey("scenario.scenario_id"))
    scenario_2_id = sa.Column(sa.Integer, sa.ForeignKey("scenario.scenario_id"))
    scenario_1 = orm.relationship(Scenario, primaryjoin=scenario_1_id == Scenario.scenario_id)
    scenario_2 = orm.relationship(Scenario, primaryjoin=scenario_2_id == Scenario.scenario_id)

    def __init__(self, *args, **kwargs):
        super(Base, self).__init__(*args, **kwargs)
        self.results_file_name = "%s_vs_%s.tar.gz" % (self.scenario_1.safe_name, self.scenario_2.safe_name)
        dir_name_1 = uuid.uuid4()
        dir_name_2 = uuid.uuid4()
        self.output_directory_1 = os.path.join(settings.scenario_run_directory, dir_name_1)
        self.output_directory_2 = os.path.join(settings.scenario_run_directory, dir_name_2)
        try:
            os.mkdir(self.output_directory_1)
            os.mkdir(self.output_directory_2)
        except OSError:
            pass

    def finalize_run(self):
        self.status = "completed"
        temp_directory = tempfile.mkdtemp()
        output_directory_1 = os.path.join(temp_directory, self.scenario_1.safe_name)
        output_directory_2 = os.path.join(temp_directory, self.scenario_2.safe_name)
        shutil.copy(os.path.join(self.output_directory_1, "receptors.csv"), temp_directory)
        shutil.copy(os.path.join(self.output_directory_1, "concentrations.png"), temp_directory)
        shutil.copy(os.path.join(self.output_directory_1, "concentration_legend.png"), temp_directory)
        shutil.copy(os.path.join(self.output_directory_1, "C-LINE_Inputs.txt"), output_directory_1)
        shutil.copy(os.path.join(self.output_directory_1, "concentrations.csv"), output_directory_1)
        shutil.copy(os.path.join(self.output_directory_1, "sources.csv"), output_directory_1)
        shutil.copy(os.path.join(self.output_directory_2, "C-LINE_Inputs.txt"), output_directory_2)
        shutil.copy(os.path.join(self.output_directory_2, "concentrations.csv"), output_directory_2)
        shutil.copy(os.path.join(self.output_directory_2, "sources.csv"), output_directory_2)
        starting_dir = os.getcwd()
        os.chdir(settings.output_tar_directory)
        subprocess.call(["tar", "-C", self.output_directory, "-czf", self.results_file_name, "."])
        os.chdir(starting_dir)
        connection = pika.SelectConnection(pika.ConnectionParameters('treehug.its.unc.edu'))
        channel = connection.channel()
        channel.basic_publish(exchange='', routing_key=self.write_queue, body='completed')
        connection.close()
