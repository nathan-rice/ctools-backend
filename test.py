import json
import models

if __name__ == "__main__":
    with open("one_scenario.json") as f:
        one_scenario_data = json.loads(f.read())
    with open("two_scenario.json") as f:
        two_scenario_data = json.loads(f.read())
    s1 = two_scenario_data.get("scenario_1")
    scenario_2_data = two_scenario_data.get("scenario_2")
    scenario_1 = models.Scenario(name=s1["name"], hour=s1["hour"], season=s1["season"], day=s1["day"],
                                 zoom=s1["zoom"], met_conditions=s1["met_conditions"], roads=s1["roads"],
                                 railways=s1["railways"], area_sources=s1["area_sources"],
                                 point_sources=s1["point_sources"], ships_in_transit=s1["ships_in_transit"],
                                 include_area_sources=s1["include_area_sources"],
                                 include_point_sources=s1["include_point_sources"],
                                 include_railways=["include_railways"],
                                 include_roads=s1["include_roads"],
                                 include_ships_in_transit=s1["include_ships_in_transit"])
