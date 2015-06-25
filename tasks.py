import argparse
import os
import numpy as np

import tablib

import raster
import models
from wrappers import CTools


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


def ctools(pollutant, model_type, scenario_id):
    session = models.Session()
    scenario = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id).first()
    scenario_run = models.ScenarioRun(status="pending", pollutant=pollutant, model_type=model_type,
                                      scenario=scenario)
    session.add(scenario_run)
    session.commit()
    cline_ = CTools(scenario=scenario, scenario_run=scenario_run)
    concentrations = cline_.calculate_concentrations()
    concentrations[:, 2] = np.log10(concentrations[:, 2])
    raster_generator = raster.RasterGenerator(scenario_run=scenario_run)
    raster_generator.create_pollution_raster(concentrations)
    scenario_run.finalize_run()
    session.commit()


def ctools_comparison(pollutant, model_type, comparison_type, scenario_id_1, scenario_id_2):
    session = models.Session()
    scenario_1 = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id_1).first()
    scenario_2 = session.query(models.Scenario).filter(models.Scenario.scenario_id == scenario_id_2).first()
    scenario_run = models.ComparisonScenarioRun(status="pending", pollutant=pollutant, model_type=model_type,
                                                scenario_1=scenario_1, scenario_2=scenario_2)
    session.add(scenario_run)
    session.commit()
    cline_1 = CTools(scenario=scenario_1, scenario_run=scenario_run, output_directory=scenario_run.output_directory_1)
    cline_2 = CTools(scenario=scenario_2, scenario_run=scenario_run, output_directory=scenario_run.output_directory_2)
    concentrations = cline_1.calculate_concentrations()
    concentrations_2 = cline_2.calculate_concentrations()
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
    scenario_run.finalize_run()
    session.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_type", help="the type of run to perform")
    parser.add_argument("-s", "--scenario", help="the scenario ID to use for run parameters")
    parser.add_argument("-p", "--pollutant", help="""The pollutant to model
    1: NOx
    2: Benz
    3: pm2
    4: D_pm25
    5: EC25
    6: OC25
    7: CO
    8: FORM
    9: ALD2
    10: ACRO
    11: 1,3-BUTA
    """)
    parser.add_argument("-t", "--type", help="""the model type
    1: hourly concentrations
    2: annual average concentrations
    3: cancer risk from annual averages
    4: non-cancer risk from annual averages""")


if __name__ == "__main__":
    main()
