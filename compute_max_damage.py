#!/usr/bin/env python3
import pandas as pd
import argparse
from pyincore.utils.dataprocessutil import DataProcessUtil


def main():
    # Result name
    result_name = args.result_name

    # Damage output from analysis
    output_dataset = args.output_dataset

    # Read damage result
    dmg_result = pd.read_csv(output_dataset)

    # Compute max damage state
    max_dmg_state_df = DataProcessUtil.get_max_damage_state(dmg_result)
    max_dmg_state_df.to_csv(result_name + "maxDamageState.csv", columns=['guid', 'max_state'], index=False)


if __name__ == '__main__':
    # result file
    parser = argparse.ArgumentParser(description='Store pyincore results`.')
    parser.add_argument('--result_name', dest='result_name', help='Result Name')
    parser.add_argument('--output_dataset', dest='output_dataset', help='Output dataset')

    args = parser.parse_args()
    main()
