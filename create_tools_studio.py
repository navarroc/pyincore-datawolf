import importlib
import pkgutil
import pyincore.analyses
from pyincore import BaseAnalysis, IncoreClient
import pyincore.globals as pyglobals
from pyincore.analyses.core_cge_ml import CoreCGEML
import json
import requests
import os
import zipfile
import uuid
import hashlib
from datetime import datetime

datawolf_host = os.getenv('DATAWOLF_HOST', "http://localhost:8888")
my_person_id = os.getenv("DATAWOLF_USER")

# For now, use this to specify whether a tool should be a command line or kubernetes tool
kube_tool = bool(os.getenv("KUBE_TOOLS", False) == 'True')
use_auth = bool(os.getenv("USE_AUTH", False) == 'True')

def get_token():
    token_file_name = "." + hashlib.sha256(str.encode(datawolf_host)).hexdigest() + "_token"
    token_file = os.path.join(pyglobals.PYINCORE_USER_CACHE, token_file_name)
    headers = {'Authorization': 'Bearer some-token'}

    if (os.path.exists(token_file)):
        with open(token_file, "r") as f:
            auth = f.read().splitlines()

        headers = {'Authorization' : auth[0]}

    return headers


def import_submodules(package, recursive=True):
    """ Import all submodules of a module, recursively, including subpackages

    :param recursive:
    :param package: package (name or actual module)
    :type package: str | module
    :rtype: dict[str, types.ModuleType]
    """
    if isinstance(package, str):
        package = importlib.import_module(package)
    results = {}
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = package.__name__ + '.' + name
        results[full_name] = importlib.import_module(full_name)
        if recursive and is_pkg:
            results.update(import_submodules(full_name))
    return results


def create_tool(analysis, input_definitions, output_definitions, headers):
    creator = get_creator(my_person_id, headers)

    incore_analysis = analysis(IncoreClient())
    # print("find module path")
    analysis_info = get_analysis_info(incore_analysis)
    # print(analysis_info)

    analysis_output_definitions = output_definitions[analysis_info[1]]
    # print(analysis_output_definitions)

    # Check if any input definitions should be chained from other analyses
    analysis_input_definitions = []
    if analysis_info[1] in input_definitions:
        analysis_input_definitions = input_definitions[analysis_info[1]]

    spec = incore_analysis.get_spec()
    tool = {}
    if not kube_tool:
        tool = create_workflow_tool(analysis_info[1], spec["description"], "1.0", "commandline", creator)
    else:
        tool = create_workflow_tool(analysis_info[1], spec["description"], "1.0", "kubernetes", creator)
    tool_inputs = []
    tool_outputs = []
    tool_blobs = []
    tool_parameters = []
    # This is all of the tool_parameters
    command_line_options = []

    # Each tool will need 2 parameters - the analysis name, the service URL and 1 input dataset - an IN-CORE token
    analysis_value = analysis_info[0] + ":" + analysis_info[1]
    analysis_param = create_workflow_tool_parameter("Analysis", "pyIncore Analysis", False, "STRING", True,
                                                    analysis_value)
    tool_parameters.append(analysis_param)
    cl_option = create_command_line_option("PARAMETER", "", "--analysis", analysis_param["parameterId"],
                                           "", "", True)
    command_line_options.append(cl_option)

    #analysis_param = create_workflow_tool_parameter("IN-CORE Service", "IN-CORE Service endpoint", False, "STRING",
    #                                                True, "")
    #tool_parameters.append(analysis_param)
    # cl_option = create_command_line_option("PARAMETER", "", "--service_url", analysis_param["parameterId"],
    #                                        "", "", True)
    # command_line_options.append(cl_option)

    analysis_parameters = spec["input_parameters"]
    for input_param in analysis_parameters:
        # print(input_param)
        param_id = input_param["id"]
        param_type = input_param["type"]
        param_arg = "STRING"
        if param_type == int or param_type == float:
            param_arg = "NUMBER"
        elif param_type == bool:
            param_arg = "BOOLEAN"

        analysis_param = create_workflow_tool_parameter(param_id, input_param["description"], not input_param[
            'required'], param_arg, False, "")
        tool_parameters.append(analysis_param)
        flag = "--" + param_id
        cl_option = create_command_line_option("PARAMETER", "", flag, analysis_param["parameterId"], "", "", True)
        command_line_options.append(cl_option)

    analysis_inputs = spec["input_datasets"]
    for analysis_input in analysis_inputs:
        param_id = analysis_input["id"]

        # Any inputs not listed in the analysis input definition will be loaded from the service by ID
        # 9-23-2024 - changing this to the opposite now that we have incore as a service
        if param_id in analysis_input_definitions:
            analysis_param = create_workflow_tool_parameter(param_id, analysis_input["description"],
                                                            not analysis_input['required'], "STRING", False, "")
            tool_parameters.append(analysis_param)
            flag = "--" + param_id
            cl_option = create_command_line_option("PARAMETER", "", flag, analysis_param["parameterId"], "", "", True)
            command_line_options.append(cl_option)
        else:
            # Inputs listed in the analysis input definition should be chained and will be passed in as files
            # TODO make this more generalized
            analysis_input_param = create_workflow_tool_data(param_id, analysis_input["description"],
                                                             not analysis_input["required"], "text/csv")
            tool_inputs.append(analysis_input_param)
            flag = "--" + param_id
            cl_option = create_command_line_option("DATA", "", flag, analysis_input_param["dataId"], "INPUT",
                                                   "", True)
            command_line_options.append(cl_option)

    stdout = create_workflow_tool_data("stdout", "stdout of external tool", False,"text/plain")
    stdout["dataId"] = "stdout"
    tool_outputs.append(stdout)

    # TODO define analysis outputs, here we will rely on parsing the file that gives information about files to look for
    analysis_outputs = spec["output_datasets"]
    for analysis_output in analysis_outputs:
        output_id = analysis_output['id']
        analysis_output_info = analysis_output_definitions[output_id]
        mime_type = analysis_output_info["mimeType"]
        filename = analysis_output_info["filename"]

        description = ""
        if type(analysis_output["type"]) == list:
            description = analysis_output['type'][0] + "," + analysis_output_info['format']
        else:
            description = analysis_output['type'] + "," + analysis_output_info['format']

        #if "description" in analysis_output:
        #    description = analysis_output["description"]

        output = create_workflow_tool_data(output_id, description, False, mime_type)
        tool_outputs.append(output)

        cl_option = create_command_line_option("DATA", "", "", output["dataId"], "OUTPUT", filename, True)
        command_line_options.append(cl_option)

    if not kube_tool:
        command_line_impl = create_command_line_tool()
    else:
        command_line_impl = create_kubernetes_tool()

    command_line_impl["captureStdOut"] = stdout["dataId"]
    command_line_impl["commandLineOptions"] = command_line_options

    zip_file = zipfile.ZipFile("new-tool.zip", "w")

    tool["implementation"] = json.dumps(command_line_impl).replace('"', '\"')
    tool["inputs"] = tool_inputs
    tool["outputs"] = tool_outputs
    tool["parameters"] = tool_parameters
    tool["blobs"] = tool_blobs

    with open("tool.json", "w") as json_file:
        json.dump(tool, json_file)

    add_blobs(zip_file, "tool.json", "", False)
    
    zip_file.close()
    upload_tool(zip_file.filename, headers)


def upload_tool(zip_tool, headers):
    print("upload a tool")
    with open(zip_tool, "rb") as file:
        files = {"tool": file}
        response = requests.post(datawolf_host + "/datawolf/workflowtools", files=files, headers=headers)
        if response.ok:
            print("uploaded tool")
        else:
            print("error creating tool")


def add_blobs(zip_file, filename, mime_type, isBlob):
    file_descriptor = {}
    if isBlob:
        file_descriptor["id"] = str(uuid.uuid4())
        file_descriptor["filename"] = filename
        file_descriptor["mimeType"] = mime_type
        file_descriptor["size"] = os.path.getsize(filename)
        zip_file.write(filename, os.path.join("blobs", file_descriptor["id"], filename), zipfile.ZIP_DEFLATED)
    else:
        zip_file.write(filename)

    return file_descriptor


def create_workflow_tool(title, description, version, executor, creator):
    tool = {}

    tool_id = str(uuid.uuid4())
    current_date = datetime.utcnow().isoformat()[:-3]+'Z'

    tool["id"] = tool_id
    tool["title"] = "Studio-Kube-" + title
    tool["date"] = str(current_date)
    tool["description"] = description
    tool["version"] = version
    tool["executor"] = executor
    tool["creator"] = creator

    return tool


def create_workflow_tool_data(title, description, allowNull, mimetype):
    wf_tool_data = {}
    wf_tool_data["id"] = str(uuid.uuid4())
    wf_tool_data["dataId"] = str(uuid.uuid4())
    wf_tool_data["title"] = title
    wf_tool_data["description"] = description
    wf_tool_data["mimeType"] = mimetype
    wf_tool_data["allowNull"] = allowNull

    return wf_tool_data


def create_command_line_tool():
    command_line_impl = {}
    command_line_impl["commandLineOptions"] = []
    command_line_impl["env"] = {}
    command_line_impl["captureStdOut"] = None
    command_line_impl["captureStdErr"] = None
    command_line_impl["joinStdOutStdErr"] = False
    #command_line_impl["executable"] = "./run.sh"
    # testing for next gen app
    command_line_impl["executable"] = "/home/datawolf/tool/run_pyincore/run_studio.sh"

    return command_line_impl

def create_kubernetes_tool():
    command_line_impl = {}
    command_line_impl["commandLineOptions"] = []
    command_line_impl["env"] = {}
    command_line_impl["resources"] = {"cpu": 16, "memory": 12}
    command_line_impl["pullSecretName"] = "regcred"
    command_line_impl["captureStdOut"] = None
    command_line_impl["captureStdErr"] = None
    command_line_impl["joinStdOutStdErr"] = False
    command_line_impl["image"] = "hub.ncsa.illinois.edu/incore/dw-pyincore"

    return command_line_impl

def create_command_line_option(type, value, flag, option_id, input_output, filename, commandline):
    cl_option = {}
    cl_option["type"] = type
    cl_option["commandline"] = commandline
    cl_option["optionId"] = option_id

    # TODO test removing the len test, I think it's unnecessary
    if value is not None and len(value) > 0:
        # print("adding value")
        cl_option["value"] = value
    if flag is not None and len(flag) > 0:
        cl_option["flag"] = flag
    if input_output is not None and len(input_output) > 0:
        cl_option["inputOutput"] = input_output
    if filename is not None and len(filename) > 0:
        cl_option["filename"] = filename

    return cl_option


def create_workflow_tool_parameter(title, description, allowNull, type, hidden, value):
    # print("description length: ")
    if len(description) > 255:
        print("Warning - description is too long for "+title)
        print(len(description))
    wf_param = {}
    wf_param["id"] = str(uuid.uuid4())
    wf_param["parameterId"] = str(uuid.uuid4())
    wf_param["title"] = title
    wf_param["description"] = description
    wf_param["type"] = type
    wf_param["hidden"] = hidden
    wf_param["allowNull"] = allowNull
    wf_param["value"] = value

    return wf_param


def get_creator(person_id, headers):
    response = requests.get(datawolf_host + "/datawolf/persons/"+person_id, headers=headers)
    return response.json()


def get_analysis_info(obj):
    cls = type(obj)
    module = cls.__module__
    name = cls.__qualname__
    return [module, name]


if __name__ == '__main__':
    import_submodules(pyincore.analyses)
    print("these are the subclasses")
    print([cls.__name__ for cls in BaseAnalysis.__subclasses__()])
    print([cls.__name__ for cls in CoreCGEML.__subclasses__()])

    # Gets the IN-CORE token for the IN-CORE host specified
    if use_auth:
        auth_headers = get_token()
    else:
        auth_headers = {}

    # Hack to inform datawolf of how to collect the analysis outputs, this should come from somewhere else
    output_def_file = open("output_definition.json")
    analysis_output = json.load(output_def_file)

    # Read list of analysis inputs that are special and need to be handled (e.g. dfr3 mappings)
    input_def_file = open("input_definition_studio.json")
    analysis_input = json.load(input_def_file)

    total = 0
    # Create all analyses
    for cls in BaseAnalysis.__subclasses__():
        # CoreCGE should be ignored
        if (cls.__name__ == "CoreCGEML"):
            print("cge, skip")
        elif cls.__name__ == "ExampleAnalysis":
            print("skipping "+cls.__name__)
        else:
            print("create the tool")
            print(cls)
            total = total + 1
            create_tool(cls, analysis_input, analysis_output, auth_headers)

    print("ML enabled cges to create")
    for cls in CoreCGEML.__subclasses__():
        print("create")
        print(cls)
        total = total + 1
        create_tool(cls, analysis_input, analysis_output, auth_headers)

    print("how many analyses = "+str(total))
    # Next steps are to:
    # 1. Finish auto creation of the tool through the datawolf endpoint, currently only tested by manually posting
    # the zip generated
    # 2. Generalize so you can configure which datawolf service to talk to and what user to create the tools under
