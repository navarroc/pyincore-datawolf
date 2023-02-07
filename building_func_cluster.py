#!/usr/bin/env python3
import argparse
from pyincore import Dataset, IncoreClient, DataService
from pyincore.utils.dataprocessutil import DataProcessUtil
import json
import pandas as pd


def main():
    # IN-CORE token (optional)
    token = args.token

    # IN-CORE Service URL
    service_url = args.service_url

    # Create IN-CORE client
    client = IncoreClient(service_url, token)

    # Data Service
    dataservice = DataService(client)

    # Archetype mapping file
    archetype_mapping = args.archetype_mapping
    archetype_mapping_dataset = Dataset.from_data_service(archetype_mapping, dataservice)
    archetype_mapping_path = archetype_mapping_dataset.get_file_path()
    arch_mapping = pd.read_csv(archetype_mapping_path)

    # Building functionality
    building_func = args.functionality_probability

    # Building Dataset
    building_dataset_id = args.buildings

    bldg_dataset = Dataset.from_data_service(building_dataset_id, dataservice)
    buildings = bldg_dataset.get_dataframe_from_shapefile()

    bldg_func_df = pd.read_csv(building_func)
    bldg_func_df.rename(columns={'building_guid': 'guid'}, inplace=True)
    
    arch_column = "archetype"
    if args.arch_col is not None and len(args.arch_col) > 0:
        arch_column = args.arch_col

    ret_json = DataProcessUtil.create_mapped_func_result(buildings, bldg_func_df, arch_mapping, arch_column)
    with open(args.result_name + "_building_functionality_cluster.json", "w") as f:
        json.dump(ret_json, f, indent=2)


if __name__ == '__main__':
    # result file
    parser = argparse.ArgumentParser(description='Cluster building functionality results.')
    parser.add_argument('--result_name', dest='result_name', help='Result Name')
    parser.add_argument('--token', dest='token', help='Service token')
    parser.add_argument('--service_url', dest='service_url', help='Service endpoint')
    parser.add_argument('--buildings', dest='buildings', help='Building Inventory')
    parser.add_argument('--functionality_probability', dest='functionality_probability',
                        help='Building Functionality Probability')
    parser.add_argument('--archetype_mapping', dest='archetype_mapping', help='Archetype Mapping')
    parser.add_argument('--arch_col', dest='arch_col', help='Column to match on')

    args = parser.parse_args()
    main()
