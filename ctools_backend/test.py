import json

from ctools_backend import models
import tasks

lat_min = 32.6191597574
lat_max = 32.9335124214
lon_min = -80.2494830542
lon_max = -79.6126193458

def add_scenarios_from_json(scenario_file):
    with open(scenario_file) as f:
        two_scenario_data = json.loads(f.read())
    s1 = two_scenario_data.get("scenario_1")
    s2 = two_scenario_data.get("scenario_2")
    scenario_1 = models.Scenario(name=s1["name"], hour=s1["hour"], season=s1["season"], day=s1["day"],
                                 zoom=s1["zoom"], met_conditions=s1["met_conditions"], roads=s1["roads"],
                                 railways=s1["railways"], area_sources=s1["area_sources"],
                                 point_sources=s1["point_sources"], ships_in_transit=s1["ships_in_transit"],
                                 include_area_sources=s1["include_area_sources"],
                                 include_point_sources=s1["include_point_sources"],
                                 include_railways=s2["include_railways"],
                                 include_roads=s1["include_roads"],
                                 include_ships_in_transit=s1["include_ships_in_transit"])
    scenario_2 = models.Scenario(name=s2["name"], hour=s2["hour"], season=s2["season"], day=s2["day"],
                                 zoom=s2["zoom"], met_conditions=s2["met_conditions"], roads=s2["roads"],
                                 railways=s2["railways"], area_sources=s2["area_sources"],
                                 point_sources=s2["point_sources"], ships_in_transit=s2["ships_in_transit"],
                                 include_area_sources=s2["include_area_sources"],
                                 include_point_sources=s2["include_point_sources"],
                                 include_railways=s2["include_railways"],
                                 include_roads=s2["include_roads"],
                                 include_ships_in_transit=s2["include_ships_in_transit"])
    session = models.Session()
    session.add(scenario_1)
    session.add(scenario_2)
    session.commit()

def run_ctools_single_scenario():
    session = models.Session()
    scenario = session.query(models.Scenario).first()
    tasks.ctools("1", "1", scenario.scenario_id)

def run_ctools_comparison_scenario():
    session = models.Session()
    s = session.query(models.Scenario).all()
    tasks.ctools_comparison("1", "1", "Relative", s[0].scenario_id, s[1].scenario_id)

if __name__ == "__main__":
    # add_scenarios_from_json("two_scenario_data.json")
    add_scenarios_from_json("two_scenario_cport_data.json")
    # run_cline_single_scenario()
    run_ctools_comparison_scenario()