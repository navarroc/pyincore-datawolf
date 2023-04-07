#!/usr/bin/env python3
import argparse
from pyincore import Dataset, IncoreClient, DataService
from pyincore.utils.dataprocessutil import DataProcessUtil
import json
import pandas as pd
from statistics import mean


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

    # Building failure probability
    failure_state = args.failure_probability

    # Building dataset id
    building_dataset_id = args.buildings

    bldg_dataset = Dataset.from_data_service(building_dataset_id, dataservice)
    buildings = bldg_dataset.get_dataframe_from_shapefile()

    # Cluster the mcs building failure probability - essentially building functionality without electric power being
    # considered
    fail_state_df = pd.read_csv(failure_state, usecols=['guid', 'failure'])

    # I think you need this for the map layer? work around would be calculate this on the fly
    fail_state_df['probability'] = \
        fail_state_df["failure"].apply(lambda x: [int(x) for x in x.split(",")]).apply(lambda x: 1 - mean(x))
    fail_state_df.to_csv(args.result_name + "_mcs_building_failure_probability_cluster.csv",
                       columns=['guid', 'probability'], index=False)

    arch_column = "archetype"
    if args.arch_col is not None and len(args.arch_col) > 0:
        arch_column = args.arch_col

    ret_json = DataProcessUtil.create_mapped_func_result(buildings, fail_state_df, arch_mapping, arch_column)
    
    with open(args.result_name + "_mcs_building_failure_probability_cluster.json", "w") as f:
        json.dump(ret_json, f, indent=2)


if __name__ == '__main__':
    # result file
    parser = argparse.ArgumentParser(description='Cluster MCS building failure probability.')
    parser.add_argument('--result_name', dest='result_name', help='Result Name')
    parser.add_argument('--token', dest='token', help='Service token')
    parser.add_argument('--service_url', dest='service_url', help='Service endpoint')
    parser.add_argument('--buildings', dest='buildings', help='Building Inventory')
    parser.add_argument('--failure_probability', dest='failure_probability',
                        help='MCS Building Failure Probability')
    parser.add_argument('--archetype_mapping', dest='archetype_mapping', help='Archetype Mapping')
    parser.add_argument('--arch_col', dest='arch_col', help='Column to match on')

    args = parser.parse_args()
    main()
