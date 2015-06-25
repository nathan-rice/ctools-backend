import os

connection_string = ""
scenario_run_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "scenario_runs")
output_tar_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "output")
template_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "templates/")
ctools_dir = os.path.join("/home/nathan", "CTOOLS")