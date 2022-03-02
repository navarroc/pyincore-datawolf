#!/usr/bin/env python3
import pandas as pd
import argparse
from pyincore.utils.cgeoutputprocess import CGEOutputProcess


def main():
    # CGE output
    domestic_supply = args.domestic_supply
    gross_income = args.gross_income
    pre_disaster_factor_demand = args.pre_disaster_factor_demand
    post_disaster_factor_demand = args.post_disaster_factor_demand

    cge_json = CGEOutputProcess()

    # CGE output to JSON
    cge_json.get_cge_domestic_supply(pd.read_csv(domestic_supply), None, "cge_domestic_supply.json")
    cge_json.get_cge_gross_income(pd.read_csv(gross_income), None, "cge_total_household_income.json")
    cge_json.get_cge_employment(pd.read_csv(pre_disaster_factor_demand), pd.read_csv(post_disaster_factor_demand),
                                None, None, "cge_employment.json")


if __name__ == '__main__':
    # result file
    parser = argparse.ArgumentParser(description='Post Process CGE output`.')
    parser.add_argument('--domestic-supply', dest='domestic_supply', help='CGE domestic supply')
    parser.add_argument('--gross-income', dest='gross_income', help='CGE gross income')
    parser.add_argument('--pre-disaster-factor-demand', dest='pre_disaster_factor_demand',
                        help='CGE pre disaster factor demand')
    parser.add_argument('--post-disaster-factor-demand', dest='post_disaster_factor_demand',
                        help='CGE post disaster factor demand')

    args = parser.parse_args()
    main()
