import argparse
import os
import time
import numpy as np

import tablib

import raster
import models
from wrappers import CLine, CPort


def create_results_file(concentrations, log_dir, header="concentration"):
    ds = tablib.Dataset(*concentrations, headers=["x", "y", header])
    target_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), log_dir, "results.csv")
    with open(target_dir, 'w') as f:
        f.write(ds.csv)


def transform_comparison_data(datum):
    abs_val = np.abs(datum)
    if -1 <= abs_val <= 1:
        return 0
    return np.sign(datum) * np.log10(abs_val)


def cline(pollutant, model_type, scenario_id, read_queue, write_queue):
    session = models.Session()
    scenario = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id).first()
    scenario_run = models.ScenarioRun(status="pending", pollutant=pollutant, model_type=model_type,
                                      scenario=scenario, read_queue=read_queue, write_queue=write_queue)
    session.add(scenario_run)
    session.commit()
    cline_ = CLine(scenario=scenario, scenario_run=scenario_run)
    concentrations = cline_.calculate_concentrations()
    concentrations[:, 2] = np.log10(concentrations[:, 2])
    raster_generator = raster.RasterGenerator(scenario_run=scenario_run)
    raster_generator.create_pollution_raster(concentrations)
    scenario_run.finalize_run()
    session.commit()


def cport(pollutant, model_type, scenario_id, read_queue, write_queue):
    session = models.Session()
    scenario = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id).first()
    scenario_run = models.ScenarioRun(status="pending", pollutant=pollutant, model_type=model_type,
                                      scenario=scenario, read_queue=read_queue, write_queue=write_queue)
    session.add(scenario_run)
    session.commit()
    cport_ = CPort(scenario=scenario, scenario_run=scenario_run)
    concentrations = cport_.calculate_concentrations()
    concentrations[:, 2] = np.log10(concentrations[:, 2])
    raster_generator = raster.RasterGenerator(scenario_run=scenario_run)
    raster_generator.create_pollution_raster(concentrations)
    scenario_run.finalize_run()
    session.commit()


def cline_comparison(pollutant, model_type, comparison_type, scenario_id_1, scenario_id_2, read_queue,
                     write_queue):
    session = models.Session()
    scenario_1 = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id_1).first()
    scenario_2 = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id_2).first()
    scenario_run = models.ComparisonScenarioRun(status="pending", pollutant=pollutant, model_type=model_type,
                                                scenario_1=scenario_1, scenario_2=scenario_2,
                                                read_queue=read_queue, write_queue=write_queue)
    cline_1 = CLine(scenario=scenario_1, scenario_run=scenario_run)
    cline_2 = CLine(scenario=scenario_2, scenario_run=scenario_run)
    concentrations = cline_1.calculate_concentrations(scenario_1, pollutant)
    concentrations_2 = cline_2.calculate_concentrations(scenario_2, pollutant)
    raster_generator = raster.RasterGenerator(scenario_run=scenario_run)
    if comparison_type == "Relative":
        title = "concentration difference"
        concentrations[:, 2] = [transform_comparison_data(d) for d in
                                (concentrations[:, 2] - concentrations_2[:, 2])]
    else:
        title = "concentration difference (%)"
        concentrations[:, 2] = (concentrations[:, 2] - concentrations_2[:, 2]) / concentrations_2[:, 2] * 100
    raster_generator.create_pollution_raster(concentrations)
    create_results_file(concentrations, scenario_run.temp_dir, title)


def cport_comparison(pollutant, model_type, comparison_type, scenario_id_1, scenario_id_2, read_queue,
                     write_queue):
    session = models.Session()
    scenario_1 = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id_1).first()
    scenario_2 = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id_2).first()
    scenario_run = models.ComparisonScenarioRun(status="pending", pollutant=pollutant, model_type=model_type,
                                                scenario_1=scenario_1, scenario_2=scenario_2,
                                                read_queue=read_queue, write_queue=write_queue)
    cline_1 = CLine(scenario=scenario_1, scenario_run=scenario_run)
    cline_2 = CLine(scenario=scenario_2, scenario_run=scenario_run)
    concentrations = cline_1.calculate_concentrations(scenario_1, pollutant)
    concentrations_2 = cline_2.calculate_concentrations(scenario_2, pollutant)
    raster_generator = raster.RasterGenerator(scenario_run=scenario_run)
    if comparison_type == "Relative":
        title = "concentration difference"
        concentrations[:, 2] = [transform_comparison_data(d) for d in
                                (concentrations[:, 2] - concentrations_2[:, 2])]
    else:
        title = "concentration difference (%)"
        concentrations[:, 2] = (concentrations[:, 2] - concentrations_2[:, 2]) / concentrations_2[:, 2] * 100
    raster_generator.create_pollution_raster(concentrations)
    create_results_file(concentrations, scenario_run.temp_dir, title)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_type", help="the type of run to perform")
    parser.add_argument("-s", "--scenario", help="the scenario ID to use for run parameters")
    parser.add_argument("-r", "--read", help="the read queue to use for job management")
    parser.add_argument("-w", "--write", help="the write queue to use for job completion notification")
    parser.add_argument("-t", "--type", help="""the model type
    1: hourly concentrations
    2: annual average concentrations
    3: cancer risk from annual averages
    4: non-cancer risk from annual averages""")


if __name__ == "__main__":
    main()
