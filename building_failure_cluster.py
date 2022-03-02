#!/usr/bin/env python3
import os
from pathlib import Path
import argparse
from pyincore import Dataset, IncoreClient, DataService
from pyincore.utils.dataprocessutil import DataProcessUtil
import json
import pandas as pd
import geopandas as gpd


def main():
    # IN-CORE token (optional)
    token = args.token

    # IN-CORE Service URL
    service_url = args.service_url

    # Create IN-CORE client
    client = IncoreClient(service_url, token)

    # Data Service
    dataservice = DataService(client)

    archetype_mapping = args.archetype_mapping
    archetype_mapping_dataset = Dataset.from_data_service(archetype_mapping, dataservice)
    archetype_mapping_path = archetype_mapping_dataset.get_file_path()
    arch_mapping = pd.read_csv(archetype_mapping_path)

    failure_probability = args.failure_probability
    building_dataset_id = args.buildings

    inventory_path = str(Path.home()) + "/.incore/cache_data/" + building_dataset_id
    for invfile in os.listdir(inventory_path):
        if invfile.endswith(".zip"):
            inventory_path = os.path.join(inventory_path, invfile)

    buildings = pd.DataFrame(gpd.read_file("zip://" + inventory_path))

    # Cluster the mcs building failure probability
    bldg_dmg_df = pd.read_csv(failure_probability, usecols=['guid', 'failure_probability'])
    for index, row in bldg_dmg_df.iterrows():
        bldg_dmg_df['probability'] = 1.0 - bldg_dmg_df.failure_probability

    bldg_dmg_df.to_csv(args.result_name + "_mcs_building_failure_probability_cluster.csv",
                       columns=['guid', 'probability'], index=False)

    ret_json = DataProcessUtil.create_mapped_func_result(buildings, bldg_dmg_df, arch_mapping)
    
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

    args = parser.parse_args()
    main()
