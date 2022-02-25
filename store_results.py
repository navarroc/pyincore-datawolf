#!/usr/bin/env python3
import os
from os import path
import argparse
from pyincore import IncoreClient, DataService


def main():
    # IN-CORE token (optional)
    token = args.token

    # IN-CORE Service URL
    service_url = args.service_url

    # Create IN-CORE client
    client = IncoreClient(service_url, token)

    # Create connection to Data Service
    dataservice = DataService(client)

    # verify whether we need to set source_id to None if empty string
    source_id = args.source_id
    title = args.result_name
    local_file = args.output_dataset
    data_type = args.output_type
    format = args.output_format

    # TODO perhaps from semantic service we can get the actual file type, for now assume CSV
    # Another alternative will be to store file types is a JSON file that says the type
    base_file = os.path.splitext(local_file)[0]
    os.rename(local_file, base_file + ".csv")

    store_results(dataservice, source_id, title, base_file + ".csv", data_type, format)


def store_results(dataservice, source_id, title, local_file, data_type, format):
    output_properties = {"dataType": data_type, "title": title + "- test", "format": format, "sourceDataset": source_id}

    if format == "shapefile":
        files = []
        file_name = os.path.splitext(local_file)[0]
        for shp_file in os.listdir("."):
            if shp_file.startswith(file_name):
                files.append(os.path.join(".", shp_file))
    else:
        files = [str(os.path.join(".", local_file))]

    print("does file exist?")
    print(path.exists(os.path.join(".", local_file)))

    dataset_id = "temp-id"

    save_to_service = True
    # Use this for testing locally
    if save_to_service:
        response = dataservice.create_dataset(output_properties)
        dataset_id = response['id']
        dataservice.add_files_to_dataset(dataset_id, files)

    output_dataset_id = open(title + "-output_id.txt", "w")
    output_dataset_id.write(dataset_id)
    output_dataset_id.close()


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Store pyincore results`.')
    parser.add_argument('--result_name', dest='result_name', help='Result name')
    parser.add_argument('--source_id', dest='source_id', help='Analysis name')
    parser.add_argument('--token', dest='token', help='Service token')
    parser.add_argument('--service_url', dest='service_url', help='Service endpoint')
    parser.add_argument('--output_type', dest='output_type', help='Dataset type')
    parser.add_argument('--output_format', dest='output_format', help='Dataset format')
    parser.add_argument('--output_dataset', dest='output_dataset', help='Output dataset')

    args = parser.parse_args()

    main()
