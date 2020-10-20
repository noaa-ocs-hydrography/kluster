import subprocess
import os
import webbrowser
from time import sleep

from HSTB.kluster.pydro_helpers import retrieve_activate_batch


def build_entwine_points(input_las_directory: str, output_folder: str):
    """
    Take in exported LAS files and build entwine point tiles in the given output folder

    Parameters
    ----------
    input_las_directory
        folder path to the directory of las files
    output_folder
        folder path to where you want to build the entwine point tiles

    """
    activate_file = retrieve_activate_batch()
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    args = ["cmd.exe", "/C", "set pythonpath=", "&&", activate_file, "Pydro38_test", "&&",
            'entwine', 'build', '-i', input_las_directory, '-o', output_folder]

    subprocess.Popen(' '.join(args), creationflags=subprocess.CREATE_NEW_CONSOLE)


def visualize_entwine(entwine_dir: str):
    """
    Start an HTTP server and use potree to visualize the entwine point tiles in the given entwine_dir

    Requires nodejs/http-server
    # conda install nodejs -y
    # npm install http-server -g

    Parameters
    ----------
    entwine_dir
        folder path to the entwine point tile directory

    """
    # start the nodejs http server
    base_path, ent_dir = os.path.split(entwine_dir)

    args = ["cmd.exe", "/K", 'http-server', base_path, '-p', '8080', '--cors']
    subprocess.Popen(' '.join(args), creationflags=subprocess.CREATE_NEW_CONSOLE)

    # wait a sec
    sleep(2)
    webbrowser.open_new('https://potree.entwine.io/data/view.html?r=%22http://localhost:8080/{}%22'.format(ent_dir))