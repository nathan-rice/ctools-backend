import os

connection_string = "postgresql://ctools:hackmeplz@treehug.its.unc.edu/ctools"
rabbitmq_server = "localhost"
message_exchange = ""
scenario_run_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "scenario_runs")
output_tar_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "output")
template_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "templates/")
cline_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "CLINE")
cport_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "CPORT")