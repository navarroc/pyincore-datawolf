#!/usr/bin/env python3

""" DataWolf wrapper for pyincore analyses
"""
import importlib
import os
from pyincore import IncoreClient, FragilityService, MappingSet, Dataset
import argparse
import datetime


def main(cl_args):
    print("start time:-", datetime.datetime.now())

    # For local testing, add path to the token
    # token = /path/to/token

    # IN-CORE user
    incore_user = os.environ['DATAWOLF_USER']
    print("user in environment")
    print(incore_user)

    # IN-CORE Service URL
    service_url = args.service_url

    user = {
        "service_url": service_url,
        "internal": True,
        "username": incore_user,
        "usergroups": ["incore_user"],
    }
    # Create IN-CORE client, use the first client if testing locally
    # client = IncoreClient(service_url, token)
    client = IncoreClient(**user)

    # pyIncore analysis in the form of module path and analysis class to load
    # e.g. pyincore.analyses.buildingdamage:BuildingDamage
    full_analysis = args.analysis

    analysis_parts = full_analysis.split(":")
    analysis_path = analysis_parts[0]
    analysis_name = analysis_parts[1]

    module = importlib.import_module(analysis_path)
    print("Loaded " + analysis_path + " successfully")
    analysis_class = getattr(module, analysis_name)

    # Create pyincore analysis specified
    py_analysis = analysis_class(client)

    # Create connection to Fragility service
    fragility_service = FragilityService(client)

    # Read input definition - determines which input datasets should come from chained analyses
    # input_def_file = open("input_definition.json")
    # analysis_input = json.load(input_def_file)
    analysis_input_definitions = []
    # if analysis_parts[1] in analysis_input:
    #     analysis_input_definitions = analysis_input[analysis_parts[1]]

    # Set the analysis inputs
    print("Set input datasets and parameters for " + analysis_name)
    set_analysis_inputs(py_analysis, fragility_service, cl_args)

    # Execute the analysis
    print("Execute " + analysis_name)
    py_analysis.run_analysis()

    print(analysis_name + " completed", datetime.datetime.now())

    # Handles the case where combined dmg analysis doesn't give a name for datawolf to find the dataset
    # TODO remove after pyincore is fixed
    if analysis_name == "CombinedWindWaveSurgeBuildingDamage":
        result_name = args_dict["result_name"]
        old_filename = result_name + ".csv"
        new_filename = result_name + "_max_state.csv"
        os.rename(old_filename, new_filename)


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
            if param_type == int and len(param_arg) > 0:
                param_arg = int(param_arg)
            elif param_type == float and len(param_arg) > 0:
                param_arg = float(param_arg)
            elif param_type == bool and len(param_arg) > 0:
                param_arg = bool(param_arg)
            if param_arg is not None and len(str(param_arg)) > 0:
                analysis.set_parameter(param_id, param_arg)

    # Load the input datasets and any fragility mapping
    input_datasets = spec["input_datasets"]
    for input_dataset in input_datasets:
        input_id = input_dataset["id"]
        input_types = input_dataset["type"]
        input_required = input_dataset["required"]
        is_dfr3 = False

        # TODO - if input type is not required AND the file size is zero, ignore it
        # This is a workaround because datawolf assumes all datasets are required
        for input_type in input_types:
            if input_type == "incore:dfr3MappingSet":
                is_dfr3 = True

        if args.__contains__(input_id):
            dataset_id = args[input_id]
            if len(dataset_id) > 0:
                if not input_required and os.path.getsize(dataset_id) == 0:
                    print("no input set")
                elif not is_dfr3:
                    print("found an chained analysis output or local input file")
                    print("input id is: "+input_id)
                    print("data type is "+input_types[0])
                    analysis.set_input_dataset(input_id, Dataset.from_file(dataset_id, input_types[0]))
                else:
                    mapping_set = MappingSet(fragility_service.get_mapping(dataset_id))
                    analysis.set_input_dataset(input_id, mapping_set)

            # if input_id in analysis_input_definitions:
                #     print("found an chained anlaysis output")
                #     print("input id is: "+input_id)
                #     print("data type is "+input_types[0])
                #     analysis.set_input_dataset(input_id, Dataset.from_file(dataset_id, input_types[0]))
                # else:
                #     if not is_dfr3:
                #         analysis.load_remote_input_dataset(input_id, dataset_id)
                #     else:
            #         mapping_set = MappingSet(fragility_service.get_mapping(dataset_id))
            #         analysis.set_input_dataset(input_id, mapping_set)


if __name__ == '__main__':

    # Name of the pyincore analysis to create and run
    parser = argparse.ArgumentParser(description='Run pyincore analysis.')
    parser.add_argument('--analysis', dest='analysis', help='Analysis name')
    parser.add_argument('--service_url', dest='service_url', help='Service endpoint')

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
                # Hack for fragility keys, which can contain spaces, in which case we don't want to split the string
                if len(values.split()) > 1 and key != "fragility_key":
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
