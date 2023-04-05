#!/usr/bin/env python3
import os
import argparse
from pyincore import IncoreClient, DataService, SpaceService
import zipfile
import signal


def handler(signum, frame):
    print("Forever is over!")
    raise Exception("end of time")


def main():
    # IN-CORE token (optional)
    token = args.token

    # IN-CORE Service URL
    service_url = args.service_url

    # Create IN-CORE client
    client = IncoreClient(service_url, token)

    # Create connection to Data Service
    dataservice = DataService(client)
    spaceservice = SpaceService(client)

    # verify whether we need to set source_id to None if empty string
    source_id = args.source_id
    title = args.result_name
    local_file = args.output_dataset
    data_type = args.output_type
    output_format = args.output_format
    spaces = []
    if args.spaces is not None and len(args.spaces) > 0:
        spaces_list = args.spaces
        spaces = parse_list(spaces_list)

    # TODO perhaps from semantic service we can get the actual file type, for now assume CSV
    # Another alternative will be to store file types is a JSON file that says the type
    if output_format == "table":
        base_file = os.path.splitext(local_file)[0]
        os.rename(local_file, base_file + ".csv")
        store_results(dataservice, spaceservice, source_id, title, base_file + ".csv", data_type, output_format, spaces)
    elif output_format == "shapefile":
        base_file = os.path.splitext(local_file)[0]
        os.rename(local_file, base_file + ".zip")
        with zipfile.ZipFile(base_file + ".zip", "r") as zip_file_ref:
            zip_file_ref.extractall(".")

        shp_file = None
        # Find shapefile in the zip
        for file in os.listdir("."):
            if file.endswith(".shp"):
                shp_file = file

        store_results(dataservice, spaceservice, source_id, title, shp_file, data_type, output_format, spaces)


def parse_list(space_list):
    # Convert spaces to a list
    if "," in space_list:
        print("comma separate list")
        categories = space_list.strip().split(",")
    else:
        print("space separated list")
        categories = space_list.split(" ")
    return categories


def store_results(dataservice, spaceservice, source_id, title, local_file, data_type, output_format, spaces):
    output_properties = {"dataType": data_type, "title": title + "- test", "format": output_format,
                         "sourceDataset": source_id}

    # register the handler
    signal.signal(signal.SIGALRM, handler)

    if output_format == "shapefile":
        files = []
        file_name = os.path.splitext(local_file)[0]
        print("Looking for shapefiles matching on: "+file_name)
        for shp_file in os.listdir("."):
            if shp_file.startswith(file_name):
                files.append(os.path.join(".", shp_file))
    else:
        files = [str(os.path.join(".", local_file))]

    # print("does file exist?")
    # print(path.exists(os.path.join(".", local_file)))

    dataset_id = "temp-id"
    # Use this for testing locally
    save_to_service = True
    if save_to_service:
        response = dataservice.create_dataset(output_properties)
        dataset_id = response['id']
        print("created dataset" + dataset_id)
        # Add file to spaces
        for space in spaces:
            spaceservice.add_to_space_by_name(space, dataset_id)

    print("saving output dataset id")
    output_dataset_id = open(title + "-output_id.txt", "w")
    output_dataset_id.write(dataset_id)
    output_dataset_id.close()

    if save_to_service:
        try:
            # time out after 1 min so tool doesn't get stuck waiting for big files that return responses slowly
            signal.alarm(60)

            print("adding files to dataset")
            dataservice.add_files_to_dataset(dataset_id, files)
        except Exception as exc:
            print(exc)


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Store pyincore results`.')
    parser.add_argument('--result_name', dest='result_name', help='Result name')
    parser.add_argument('--source_id', dest='source_id', help='Analysis name')
    parser.add_argument('--token', dest='token', help='Service token')
    parser.add_argument('--service_url', dest='service_url', help='Service endpoint')
    parser.add_argument('--output_type', dest='output_type', help='Dataset type')
    parser.add_argument('--output_format', dest='output_format', help='Dataset format')
    parser.add_argument('--output_dataset', dest='output_dataset', help='Output dataset')
    parser.add_argument('--spaces', dest='spaces', help='Spaces to add the result to')

    args = parser.parse_args()

    main()
