import math
import os
import subprocess
import itertools
import time
import numpy

from mako.lookup import TemplateLookup
import tablib

from ctools_backend import geo
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


class CTools(object):

    _program = os.path.join(settings.ctools_dir, "CTOOLS_HOURLY.ifort.x")
    _annual_program = os.path.join(settings.ctools_dir, "CTOOLS_ANNUAL.ifort.x")

    def __init__(self, scenario, scenario_run, output_directory=None):
        self.scenario = scenario
        self.scenario_run = scenario_run
        if scenario.include_area_sources:
            for a in scenario.area_sources:
                a[0] = a[0].replace(" ", "_").replace(",", "")
            self.area_sources = [models.AreaSource.construct_namedtuple(*a) for a in scenario.area_sources]
        if scenario.include_point_sources:
            for p in scenario.point_sources:
                p[0] = p[0].replace(" ", "_").replace(",", "")
            self.point_sources = [models.PointSource.construct_namedtuple(*p) for p in scenario.point_sources]
        if scenario.include_railways:
            self.railways = [models.Railway.construct_namedtuple(*r) for r in scenario.railways]
        if scenario.include_roads:
            self.roads = self._split_roads(models.Road.construct_namedtuple(*r) for r in scenario.roads)
        if scenario.include_ships_in_transit:
            for sit in scenario.ships_in_transit:
                sit[0] = sit[0].replace(" ", "_").replace(",", "")
            self.ships_in_transit = [models.ShipInTransit.construct_namedtuple(*r) for r in scenario.ships_in_transit]
        if output_directory:
            self.output_directory = output_directory
        else:
            self.output_directory = scenario_run.output_directory
        self.inputs_file = os.path.join(self.output_directory, "CTOOLS_Inputs.txt")
        self.receptor_file = os.path.join(self.output_directory, "receptors.csv")
        self.area_source_file = os.path.join(self.output_directory, "area.csv")
        self.point_source_file = os.path.join(self.output_directory, "points.csv")
        self.railway_source_file = os.path.join(self.output_directory, "railways.csv")
        self.road_source_file = os.path.join(self.output_directory, "roads.csv")
        self.sit_source_file = os.path.join(self.output_directory, "sit.csv")
        if self.scenario_run.model_type == 1:
            mode = "HOURLY"
        else:
            mode = "ANNUAL"
        self.area_file = os.path.join(self.output_directory, "results_CTOOLS_%s_AREA_Output.csv" % mode)
        self.point_file = os.path.join(self.output_directory, "results_CTOOLS_%s_POINT_Output.csv" % mode)
        self.rail_file = os.path.join(self.output_directory, "results_CTOOLS_%s_RAIL_Output.csv" % mode)
        self.road_file = os.path.join(self.output_directory, "results_CTOOLS_%s_ROAD_Output.csv" % mode)
        self.sit_file = os.path.join(self.output_directory, "results_CTOOLS_%s_SIT_Output.csv" % mode)

    @staticmethod
    def _split_roads(roads):
        split_roads = []
        for road in roads:
            road_dict = road._asdict()
            if not len(road.geom) > 2:
                (from_lng, from_lat) = road.geom[0]
                (to_lng, to_lat) = road.geom[1]
                (from_x, from_y) = geo.mercator_to_lcc(from_lng, from_lat)
                (to_x, to_y) = geo.mercator_to_lcc(to_lng, to_lat)
                road_dict["from_x"] = from_x
                road_dict["from_y"] = from_y
                road_dict["to_x"] = to_x
                road_dict["to_y"] = to_y
                split_roads.append(models.Road.namedtuple_class(**road_dict))
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
                split_roads.append(models.Road.namedtuple_class(**road_dict))
        return split_roads

    def _generate_input_file(self):
        template = _lookup.get_template("CTOOLS_Inputs.txt")
        inputs = template.render(scenario=self.scenario, scenario_run=self.scenario_run)
        with open(self.inputs_file, 'w') as f:
            f.write(inputs)

    def _run(self):
        starting_dir = os.getcwd()
        os.chdir(settings.ctools_dir)
        if self.scenario_run.model_type > 1:
            program = self._annual_program
        else:
            program = self._program
        processes = []
        self._generate_receptor_file()
        if self.scenario.include_area_sources:
            self._generate_area_file()
            processes.append(subprocess.Popen([program, "AREA", self.output_directory + "/"]))
        if self.scenario.include_point_sources:
            self._generate_point_file()
            processes.append(subprocess.Popen([program, "POINT", self.output_directory + "/"]))
        if self.scenario.include_railways:
            self._generate_rail_file()
            processes.append(subprocess.Popen([program, "RAIL", self.output_directory + "/"]))
        if self.scenario.include_roads:
            self._generate_road_file()
            processes.append(subprocess.Popen([program, "ROAD", self.output_directory + "/"]))
        if self.scenario.include_ships_in_transit:
            self._generate_sit_file()
            processes.append(subprocess.Popen([program, "SIT", self.output_directory + "/"]))
        while True:
            time.sleep(1)
            for p in processes:
                p.poll()
            if all(p.returncode is not None for p in processes):
                break
            status = self.scenario_run.current_status
            if status == "terminated":
                for p in processes:
                    p.terminate()
        os.chdir(starting_dir)
        return [p.returncode for p in processes]

    def _generate_receptor_file(self):
        create_source_csv(models.Receptor, self.receptors, ["lat", "lng"], self.receptor_file)

    def _generate_road_file(self):
        create_source_csv(models.Road, self.roads, ["gid", "sign1", "geom"], self.road_source_file)

    def _generate_area_file(self):
        results = []
        for source in self.area_sources:
            results.extend(models.AreaSource.to_vertices(source))
        dataset = tablib.Dataset(headers=["object_id", "sf_id", "x", "y", "nox", "benz", "pm2_5",
                                          "dies_pm25", "ec", "oc", "co", "form", "ald2", "acro", "butal_3", "toluene",
                                          "so2"],
                                 *results)
        with open(self.area_source_file, 'w') as f:
            f.write(dataset.csv)

    def _generate_point_file(self):
        create_source_csv(models.PointSource, self.point_sources, ["pltname", "geom"], self.point_source_file)

    def _generate_rail_file(self):
        create_source_csv(models.Railway, self.railways, ["rrowner1", "geom"], self.railway_source_file)

    def _generate_sit_file(self):
        create_source_csv(models.ShipInTransit, self.ships_in_transit, ["facility", "geom"], self.sit_source_file)

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
        if self.scenario.include_area_sources:
            with open(self.area_file) as area_output:
                ds.csv = area_output.read()
                area_concentrations = {int(r[0]): float(r[model_field]) for
                                       r in ds}
                result_dicts.append(area_concentrations)
        if self.scenario.include_point_sources:
            with open(self.point_file) as point_output:
                ds.csv = point_output.read()
                point_concentrations = {int(r[0]): float(r[model_field]) for
                                        r in ds}
                result_dicts.append(point_concentrations)
        if self.scenario.include_railways:
            with open(self.rail_file) as rail_output:
                ds.csv = rail_output.read()
                rail_concentrations = {int(r[0]): float(r[model_field]) for
                                       r in ds}
                result_dicts.append(rail_concentrations)
        if self.scenario.include_roads:
            with open(self.road_file) as road_output:
                ds.csv = road_output.read()
                road_concentrations = {int(r[0]): float(r[model_field]) for
                                       r in ds}
                result_dicts.append(road_concentrations)
        if self.scenario.include_ships_in_transit:
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
        return self._generate_concentration_array(concentrations)

    @property
    def receptors(self):
        count = itertools.count(1)
        grid_size = 50
        min_lat = self.scenario_run.min_lat
        max_lat = self.scenario_run.max_lat
        min_lng = self.scenario_run.min_lng
        max_lng = self.scenario_run.max_lng
        lat_spread = self.scenario_run.max_lat - self.scenario_run.min_lat
        lon_spread = self.scenario_run.max_lng - self.scenario_run.min_lng
        lat_delta = lat_spread / grid_size
        lon_delta = lon_spread / grid_size
        lat_start = self.scenario_run.min_lat + 0.5 * lat_delta
        lon_start = self.scenario_run.min_lng + 0.5 * lon_delta
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
                in_lat_range = min_lat < receptor.lat < max_lat
                in_lon_range = min_lng < receptor.lng < max_lng
                if in_lat_range and in_lon_range:
                    receptors.append(receptor)
        return receptors

    @staticmethod
    def _receptors_for_road(road, count):
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

    def _generate_concentration_array(self, concentrations):
        min_non_zero = 10 ** -6
        receptor_dict = {}
        for receptor in self.receptors:
            receptor_dict[receptor.id] = [receptor.x, receptor.y, min_non_zero]
        for receptor in concentrations:
            try:
                concentration = max(float(concentrations[receptor]), min_non_zero)
                receptor_dict[int(receptor)][2] = concentration
            except KeyError:
                pass
        return numpy.array(receptor_dict.values())
