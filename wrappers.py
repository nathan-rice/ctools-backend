import math
import os
import subprocess
import itertools
import time
import numpy

import pika
from mako.lookup import TemplateLookup
import tablib

import geo
import settings
import models


_lookup = TemplateLookup([settings.template_directory], strict_undefined=True)


def create_source_csv(type_, instances, ignored_fields, file_name):
    try:
        instances_namedtuple = [i.as_namedtuple() for i in instances]
    except AttributeError:
        instances_namedtuple = instances
    dataset = tablib.Dataset(headers=type_.fields, *instances_namedtuple)
    for field in ignored_fields:
        del dataset[field]
    with open(file_name, 'w') as f:
        f.write(dataset.csv)


class CLine(object):
    _program = os.path.join(settings.cline_dir, "CLINE_D+E.ifort.x")
    _annual_program = os.path.join(settings.cline_dir, "AA_CLINE_D+E.ifort.x")

    def __init__(self, scenario, scenario_run):
        self.scenario = scenario
        self.scenario_run = scenario_run
        self.roads = self._split_roads(models.Road(*r) for r in scenario.roads)
        # self.roads = [models.Road.namedtuple_class(*r) for r in roads]
        # Created roads aren't going to have a from x
        self.receptor_file = os.path.join(scenario_run.output_directory, "receptors.csv")
        self.road_source_file = os.path.join(scenario_run.output_directory, "sources.csv")
        self.concentrations_file = os.path.join(scenario_run.output_directory, "concentrations.csv")

    @staticmethod
    def _split_roads(roads):
        split_roads = []
        for road in roads:
            road_dict = road._asdict()
            if not len(road.geom) > 2:
                (from_lat, from_lng) = road.geom[0]
                (to_lat, to_lng) = road.geom[1]
                (from_x, from_y) = geo.mercator_to_lcc(from_lng, from_lat)
                (to_x, to_y) = geo.mercator_to_lcc(to_lng, to_lat)
                road_dict["from_x"] = from_x
                road_dict["from_y"] = from_y
                road_dict["to_x"] = to_x
                road_dict["to_y"] = to_y
                split_roads.append(models.Road(**road_dict))
                continue
            # The road is curved and we need to reduce it to straight line segments
            for segment in zip(road.geom[:-1], road.geom[1:]):
                (from_lat, from_lng) = segment[0]
                (to_lat, to_lng) = segment[1]
                (from_x, from_y) = geo.mercator_to_lcc(from_lng, from_lat)
                (to_x, to_y) = geo.mercator_to_lcc(to_lng, to_lat)
                road_dict["from_x"] = from_x
                road_dict["from_y"] = from_y
                road_dict["to_x"] = to_x
                road_dict["to_y"] = to_y
                road_dict["geom"] = segment
                split_roads.append(models.Road(**road_dict))
        return split_roads

    def _run(self):
        starting_dir = os.getcwd()
        os.chdir(settings.cline_dir)
        self._generate_receptor_file()
        self._generate_road_file()
        if self.scenario_run.model_type > 1:
            program = self._annual_program
        else:
            program = self._program
        connection = pika.SelectConnection(pika.ConnectionParameters('treehug.its.unc.edu'))
        channel = connection.channel()
        # The run dir must end in a slash or the program blows up...
        process = subprocess.Popen([program, self.scenario_run.output_directory + "/"])

        def _callback(ch, method, properties, body):
            if body == "terminate":
                process.terminate()

        channel.basic_consume(_callback, queue=self.scenario_run.read_queue, no_ack=True)
        channel.start_consuming()
        while True:
            time.sleep(1)
            process.poll()
            if process.returncode:
                break
        os.chdir(starting_dir)
        channel.close()
        return process.returncode

    def _generate_receptor_file(self):
        create_source_csv(models.Receptor, self.receptors, ["lat", "lng"], self.receptor_file)

    def _generate_road_file(self):
        create_source_csv(models.Road, self.roads, ["gid", "sign1", "geom"], self.road_source_file)

    def _generate_input_file(self):
        template = _lookup.get_template("C-LINE_Inputs.txt")
        inputs = template.render(scenario=self.scenario, scenario_run=self.scenario_run)
        with open(os.path.join(self.run_dir, "C-LINE_Inputs.txt"), 'w') as f:
            f.write(inputs)

    def _generate_concentration_array_meters(self):
        ds = tablib.Dataset(headers=["id", "x", "y", "pollutant"])
        with open(self.concentrations_file) as f:
            lines = f.readlines()[1:]
            ds.csv = "".join(lines)
        min_non_zero = 10 ** -6
        if self.scenario_run.model_type > 1:
            model_field = self.scenario_run.model_type + 1
        else:
            model_field = 3
        receptor_dict = {}
        for receptor in self.receptors:
            receptor_dict[receptor.id] = (receptor.x, receptor.y, min_non_zero)
        for receptor in ds:
            try:
                concentration = max(float(receptor[model_field]), min_non_zero)
                receptor_dict[int(receptor[0])] = (float(receptor[1]), float(receptor[2]), concentration)
            except KeyError:
                pass
        return numpy.array([receptor_dict[receptor.id] for receptor in self.receptors])

    def _load_concentrations_file(self):
        ds = tablib.Dataset(headers=["id", "x", "y", "pollutant"])
        with open(self._concentrations_file) as f:
            lines = f.readlines()[1:]
            ds.csv = "".join(lines)
        concentration_dict = {}
        for e in ds:
            concentration_dict[int(e[0])] = float(e[self.model_type + 2])
        return concentration_dict

    def _generate_concentration_array(self, receptors, concentrations):
        min_non_zero = 10 ** -6
        receptor_dict = {}
        for receptor in self.receptors:
            receptor_dict[receptor.id] = (receptor.x, receptor.y, min_non_zero)
        for receptor in receptors:
            try:
                concentration = concentrations[receptor.id]
                concentration = max(min_non_zero, concentration)
                receptor_dict[receptor.id] = (receptor.x, receptor.y, concentration)
            except KeyError:
                pass
        return numpy.array([receptor_dict[receptor.id] for receptor in self.receptors])

    @staticmethod
    def _unique_path(path):
        if os.path.exists(path):
            for n in itertools.count(1):
                new_path = path + "." + str(n)
                if not os.path.exists(new_path):
                    return new_path
        else:
            return path

    def calculate_concentrations(self):
        self._generate_input_file()
        self._run()
        return self._generate_concentration_array_meters()

    @property
    def receptors(self):
        count = itertools.count(1)
        grid_size = 50
        lat_spread = self.scenario.data["lat_max"] - self.scenario.data["lat_min"]
        lon_spread = self.self.scenario.data["lon_max"] - self.scenario.data["lon_min"]
        lat_delta = lat_spread / grid_size
        lon_delta = lon_spread / grid_size
        lat_start = self.scenario.data["lat_min"] + 0.5 * lat_delta
        lon_start = self.scenario.data["lon_min"] + 0.5 * lon_delta
        receptors = []
        for i in range(grid_size):
            for j in range(grid_size):
                receptor = models.Receptor(
                    count.next(),
                    lat=(lat_start + i * lat_delta),
                    lng=(lon_start + j * lon_delta)
                )
                receptors.append(receptor)
        for road in self.roads:
            for receptor in self._receptors_for_road(road, count):
                in_lat_range = self.lat_min < receptor.lat < self.lat_max
                in_lon_range = self.lon_min < receptor.lng < self.lon_max
                if in_lat_range and in_lon_range:
                    receptors.append(receptor)
        return receptors

    def _receptors_for_road(self, road, count):
        x_delta = road.to_x - road.from_x
        y_delta = road.to_y - road.from_y
        squared_dist = x_delta ** 2 + y_delta ** 2
        road_distance = math.sqrt(squared_dist)
        # if self.zoom <= 12:
        # receptor_spacing = 500
        # elif self.zoom <= 15:
        #     receptor_spacing = 300
        # else:
        #     receptor_spacing = 100
        receptor_spacing = 200
        receptor_count = max(int(math.floor(road_distance / receptor_spacing)), 1)
        receptor_x_delta = x_delta / receptor_count
        receptor_y_delta = y_delta / receptor_count
        # x and y offsets are switched here to get the slope of the perpendicular line
        x_offset = y_delta * abs(y_delta) / squared_dist
        y_offset = x_delta * abs(x_delta) / squared_dist
        receptors = [
            models.Receptor(count.next(),
                            y=(road.from_y + i * receptor_y_delta - 0.5 * receptor_y_delta),
                            x=(road.from_x + i * receptor_x_delta - 0.5 * receptor_x_delta))
            for i in range(receptor_count)
        ]
        receptors += [
            models.Receptor(count.next(),
                            y=(road.from_y + i * receptor_y_delta - 0.5 * receptor_y_delta + 5 * y_offset),
                            x=(road.from_x + i * receptor_x_delta - 0.5 * receptor_x_delta - 5 * x_offset))
            for i in range(receptor_count)
        ]
        receptors += [
            models.Receptor(count.next(),
                            y=(road.from_y + i * receptor_y_delta - 0.5 * receptor_y_delta - 5 * y_offset),
                            x=(road.from_x + i * receptor_x_delta - 0.5 * receptor_x_delta + 5 * x_offset))
            for i in range(receptor_count)
        ]
        receptors += [
            models.Receptor(count.next(),
                            y=(road.from_y + i * receptor_y_delta - 0.5 * receptor_y_delta + 25 * y_offset),
                            x=(road.from_x + i * receptor_x_delta - 0.5 * receptor_x_delta - 25 * x_offset))
            for i in range(receptor_count)
        ]
        receptors += [
            models.Receptor(count.next(),
                            y=(road.from_y + i * receptor_y_delta - 0.5 * receptor_y_delta - 25 * y_offset),
                            x=(road.from_x + i * receptor_x_delta - 0.5 * receptor_x_delta + 25 * x_offset))
            for i in range(receptor_count)
        ]
        return receptors


class CPort(CLine):
    _area_program = os.path.join(settings.cport_dir, "CPORT_Area.ifort.x")
    _point_program = os.path.join(settings.cport_dir, "CPORT_Point.ifort.x")
    _rail_program = os.path.join(settings.cport_dir, "CPORT_Rail.ifort.x")
    _road_program = os.path.join(settings.cline_dir, "CLINE_D+E.ifort.x")
    _sit_program = os.path.join(settings.cport_dir, "CPORT_SIT.ifort.x")

    def __init__(self, scenario, scenario_run):
        for a in scenario.data["area_sources"]:
            a[0] = a[0].replace(" ", "_").replace(",", "")
        self.area_sources = [models.AreaSource(*a) for a in scenario.area_sources]
        for p in scenario.data["point_sources"]:
            p[0] = p[0].replace(" ", "_").replace(",", "")
        self.point_sources = [models.PointSource(*p) for p in scenario.point_sources]
        self.railways = [models.Railway(*r) for r in scenario.railways]
        self.roads = [models.Road(*r) for r in scenario.roads]
        for sit in scenario.data["ships_in_transit"]:
            sit[0] = sit[0].replace(" ", "_").replace(",", "")
        self.ships_in_transit = [models.ShipInTransit(*r) for r in scenario.ships_in_transit]
        self.inputs_file = os.path.join(self.scenario_run.output_directory, "CPORT_Inputs.txt")
        self.receptor_file = os.path.join(self.scenario_run.output_directory, "receptors.csv")
        self.area_source_file = os.path.join(self.scenario_run.output_directory, "area.csv")
        self.point_source_file = os.path.join(self.scenario_run.output_directory, "points.csv")
        self.railway_source_file = os.path.join(self.scenario_run.output_directory, "railways.csv")
        self.road_source_file = os.path.join(self.scenario_run.output_directory, "roads.csv")
        self.sit_source_file = os.path.join(self.scenario_run.output_directory, "sit.csv")
        self.area_file = os.path.join(self.scenario_run.output_directory, "results_CPORT_AREA_Output.csv")
        self.point_file = os.path.join(self.scenario_run.output_directory, "results_CPORT_POINT_Output.csv")
        self.rail_file = os.path.join(self.scenario_run.output_directory, "results_CPORT_RAIL_Output.csv")
        self.road_file = os.path.join(self.scenario_run.output_directory, "results_CPORT_ROAD_Output.csv")
        self.sit_file = os.path.join(self.scenario_run.output_directory,
                                     "results_CPORT_SHIPINTRANSIT_Output.csv")

    def _generate_input_file(self):
        template = _lookup.get_template("CPORT_Inputs.txt")
        inputs = template.render(scenario=self.scenario, scenario_run=self.scenario_run)
        with open(self.inputs_file, 'w') as f:
            f.write(inputs)
        # generate C-LINE input file as well
        template = _lookup.get_template("CLINE_for_CPORT.txt")
        inputs = template.render(scenario=self.scenario, scenario_run=self.scenario_run)
        with open(os.path.join(self.scenario_run.output_directory, "C-LINE_Inputs.txt"), 'w') as f:
            f.write(inputs)

    def _run(self):
        starting_dir = os.getcwd()
        os.chdir(settings.cport_dir)
        self._generate_receptor_file()
        self._generate_area_file()
        self._generate_point_file()
        self._generate_rail_file()
        self._generate_road_file()
        self._generate_sit_file()
        connection = pika.SelectConnection(pika.ConnectionParameters('treehug.its.unc.edu'))
        channel = connection.channel()
        # The run dir must end in a slash or the program blows up...
        processes = []
        if self.area_sources:
            processes.append(subprocess.Popen([self._area_program, self.scenario_run.output_directory + "/"]))
        if self.point_sources:
            processes.append(
                subprocess.Popen([self._point_program, self.scenario_run.output_directory + "/"]))
        if self.railways:
            processes.append(subprocess.Popen([self._rail_program, self.scenario_run.output_directory + "/"]))
        if self.roads:
            processes.append(subprocess.Popen([self._road_program, self.scenario_run.output_directory + "/"]))
        if self.ships_in_transit:
            processes.append(subprocess.Popen([self._sit_program, self.scenario_run.output_directory + "/"]))

        def _callback(ch, method, properties, body):
            if body == "terminate":
                for p in processes:
                    p.terminate()

        channel.basic_consume(_callback, queue=self.scenario_run.read_queue, no_ack=True)
        channel.start_consuming()
        while True:
            time.sleep(1)
            for p in processes:
                p.poll()
            if all(p.returncode for p in processes):
                break
        os.chdir(starting_dir)

    def _generate_area_file(self):
        results = []
        for source in self.area_sources:
            results.extend(models.area_source_to_vertices(source))
        dataset = tablib.Dataset(headers=["object_id", "sf_id", "x", "y", "nox", "benz", "pm2_5",
                                          "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3"],
                                 *results)
        with open(self.area_source_file, 'w') as f:
            f.write(dataset.csv)

    def _generate_point_file(self):
        create_source_csv(models.PointSource, self.point_sources, ["pltname", "geom"], self.point_source_file)

    def _generate_rail_file(self):
        create_source_csv(models.Railway, self.railways, ["rrowner1", "geom"],
                          self.railway_source_file)

    def _generate_sit_file(self):
        create_source_csv(models.ShipInTransit, self.ships_in_transit, ["facility", "geom"],
                          self.sit_source_file)

    @staticmethod
    def _merge_concentration_dicts(*dicts):
        result_dict = dict()
        all_keys = set()
        for d in dicts:
            for k in d.keys():
                all_keys.add(k)
        for key in all_keys:
            result_dict[key] = sum(d.get(key, 0) for d in dicts)
        return result_dict

    def _load_concentrations_file(self):
        ds = tablib.Dataset()
        result_dicts = []
        if self.scenario_run.model_type > 1:
            model_field = self.scenario_run.model_type + 1
        else:
            model_field = 3
        if self.area_sources:
            with open(self.area_file) as area_output:
                ds.csv = area_output.read()
                area_concentrations = {int(r[0]): float(r[model_field]) for
                                       r in ds}
                result_dicts.append(area_concentrations)
        if self.point_sources:
            with open(self.point_file) as point_output:
                ds.csv = point_output.read()
                point_concentrations = {int(r[0]): float(r[model_field]) for
                                        r in ds}
                result_dicts.append(point_concentrations)
        if self.railways:
            with open(self.rail_file) as rail_output:
                ds.csv = rail_output.read()
                rail_concentrations = {int(r[0]): float(r[model_field]) for
                                       r in ds}
                result_dicts.append(rail_concentrations)
        if self.roads:
            with open(self.road_file) as road_output:
                ds.csv = road_output.read()
                road_concentrations = {int(r[0]): float(r[model_field]) for
                                       r in ds}
                result_dicts.append(road_concentrations)
        if self.ships_in_transit:
            with open(self.sit_file) as sit_output:
                ds.csv = sit_output.read()
                sit_concentrations = {int(r[0]): float(r[model_field]) for
                                      r in ds}
                result_dicts.append(sit_concentrations)
        concentrations = self._merge_concentration_dicts(*result_dicts)
        return concentrations

    def calculate_concentrations(self):
        self._generate_input_file()
        self._run()
        concentrations = self._load_concentrations_file()
        return self._generate_concentration_array(self.receptors, concentrations)
