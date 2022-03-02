#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
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

    building_dataset_id = args.buildings

    inventory_path = str(Path.home()) + "/.incore/cache_data/" + building_dataset_id
    for invfile in os.listdir(inventory_path):
        if invfile.endswith(".zip"):
            inventory_path = os.path.join(inventory_path, invfile)
    buildings = pd.DataFrame(gpd.read_file("zip://" + inventory_path))

    archetype_mapping = args.archetype_mapping

    archetype_mapping_dataset = Dataset.from_data_service(archetype_mapping, dataservice)
    archetype_mapping_path = archetype_mapping_dataset.get_file_path()
    arch_mapping = pd.read_csv(archetype_mapping_path)

    max_dmg_state_df = pd.read_csv(args.max_dmg_state)

    ret_json = DataProcessUtil.create_mapped_dmg_result(buildings, max_dmg_state_df, arch_mapping)

    with open(args.result_name + "-bldg-dmg-summary.json", 'w') as csv_file:
        json.dump(ret_json, csv_file)


if __name__ == '__main__':
    # result file
    parser = argparse.ArgumentParser(description='Summarize building damage from max damage state.')
    parser.add_argument('--result_name', dest='result_name', help='Result Name')
    parser.add_argument('--token', dest='token', help='Service token')
    parser.add_argument('--service_url', dest='service_url', help='Service endpoint')
    parser.add_argument('--buildings', dest='buildings', help='Building Inventory')
    parser.add_argument('--max_dmg_state', dest='max_dmg_state', help='Building Max Damage State')
    parser.add_argument('--archetype_mapping', dest='archetype_mapping', help='Archetype Mapping')

    args = parser.parse_args()
    main()
