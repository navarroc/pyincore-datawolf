#!/usr/bin/env python3

""" DataWolf wrapper for pyincore analyses
"""
import importlib
import os
from os import path
# from pathlib import Path
# import json
from pyincore import IncoreClient, InsecureIncoreClient, FragilityService, DataService, MappingSet
# from pyincore.dataset import Dataset
from pyincore.analyses.buildingdamage import BuildingDamage
# from pyincore.utils.dataprocessutil import DataProcessUtil
# import pyincore.analyses.buildingdamage.buildingdamage
import argparse
import datetime


def main(cl_args):
    # TODO - get this from the cl_args
    client = IncoreClient("https://incore-dev.ncsa.illinois.edu")
    print("start time:-", datetime.datetime.now())
    full_analysis = args.analysis


    analysis_parts = full_analysis.split(":")
    analysis_path = analysis_parts[0]
    analysis_name = analysis_parts[1]

    module = importlib.import_module(analysis_path)
    print("Loaded " + analysis_path + " successfully")
    analysis_class = getattr(module, analysis_name)

    # Class to dynamically create
    MyClazz = type(str(analysis_class), (analysis_class,), {})

    # Create pyincore class
    py_analysis = MyClazz(client)

    print("This class was dynamically created")
    print(type(py_analysis))

    # Create pyincore analysis explicitly
    bldg_dmg = BuildingDamage(client)
    print("This class was explicitly created")
    print(type(bldg_dmg))

    # Create connection to Fragility service
    fragility_service = FragilityService(client)

    # TODO - go through the spec and auto-load all dataset/mappings
    set_analysis_inputs(py_analysis, fragility_service, cl_args)

    py_analysis.run_analysis()

    print(analysis_class + " completed", datetime.datetime.now())


def set_analysis_inputs(analysis, fragility_service, args):
    spec = analysis.get_spec()

    # Set the analyis parameters
    input_parameters = spec["input_parameters"]
    for input_param in input_parameters:
        # print(input_param)
        param_id = input_param["id"]
        if args.__contains__(param_id):
            param_arg = args[param_id]
            param_type = input_param["type"]
            if param_type == int:
                param_arg = int(param_arg)
            elif param_type == float:
                param_arg = float(param_arg)
            elif param_type == bool:
                param_arg = bool(param_arg)
            if param_arg is not None and param_arg != "":
                analysis.set_parameter(param_id, param_arg)

    # Load the input datasets and any fragility mapping
    input_datasets = spec["input_datasets"]
    for input_dataset in input_datasets:
        input_id = input_dataset["id"]
        input_types = input_dataset["type"]
        is_dfr3 = False

        for input_type in input_types:
            if input_type == "incore:dfr3MappingSet":
                is_dfr3 = True
                print("found dfr3 mapping, load differently")
                print(input_id)

        if args.__contains__(input_id):
            dataset_id = args[input_id]
            if not is_dfr3:
                analysis.load_remote_input_dataset(input_id, dataset_id)
                print("loaded input dataset")
            else:
                mapping_set = MappingSet(fragility_service.get_mapping(dataset_id))
                analysis.set_input_dataset(input_id, mapping_set)
                print("loaded fragility mapping")


if __name__ == '__main__':

    # Name of the pyincore analysis to create and run
    parser = argparse.ArgumentParser(description='Run pyincore building damage analysis.')
    parser.add_argument('--analysis', dest='analysis', help='Analysis name')

    args, unknown = parser.parse_known_args()

    # The unknown arguments are arguments that were not defined in ArgumentParser since the wrapper doesn't know which
    # will be ran until runtime. The expectation is each argument in unknown will match the pyincore analysis
    # parameters or dataset input ids so we can automatically map them later

    # Parse all parameters into a key/value dict to map into the analysis
    args_dict = {}
    key = None
    values = ""

    for val in unknown:
        if val.startswith("--"):
            if key is not None:
                if len(values.split()) > 1:
                    values = values.split()
                else:
                    values = values.strip()
                args_dict[key] = values
            key = val[2:]
            values = ""
        else:
            values += val + " "

    if key not in args_dict:
        if len(values.split()) > 1:
            values = values.split()
        args_dict[key] = values.strip()

    # Pass the command line args to main to setup and run the pyincore analysis
    main(args_dict)
