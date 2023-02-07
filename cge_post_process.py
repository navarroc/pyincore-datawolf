#!/usr/bin/env python3
import argparse
from pyincore.utils.cgeoutputprocess import CGEOutputProcess


def main():
    # CGE output
    household_count = args.household_count
    domestic_supply = args.domestic_supply
    gross_income = args.gross_income
    pre_disaster_factor_demand = args.pre_disaster_factor_demand
    post_disaster_factor_demand = args.post_disaster_factor_demand

    # Categories
    hh_cat = ["HH1", "HH2", "HH3", "HH4", "HH5"]
    if args.hh_cat is not None and len(args.hh_cat) > 0:
        hh_cat_list = args.hh_cat
        hh_cat = parse_list(hh_cat_list)

    demand_cat = ["GOODS", "TRADE", "OTHER"]
    if args.demand_cat is not None and len(args.demand_cat) > 0:
        demand_cat_list = args.demand_cat
        demand_cat = parse_list(demand_cat_list)

    supply_cat = ["Goods", "Trade", "Other", "HS1", "HS2", "HS3"]
    if args.supply_cat is not None and len(args.supply_cat) > 0:
        supply_cat_list = args.supply_cat
        supply_cat = parse_list(supply_cat_list)
        
    regions = []
    if args.regions is not None and len(args.regions) > 0:
        region_list = args.regions
        regions = parse_list(region_list)

    print(hh_cat)
    print(demand_cat)
    print(supply_cat)

    cge_json = CGEOutputProcess()

    # CGE output to JSON
    categories = []
    if len(regions) > 0:
        for h in hh_cat :
            for r in regions:
                categories.append(h + "_" + r)
    else:
        categories = hh_cat

    print("Household categories")
    print(categories)
    cge_json.get_cge_household_count(None, household_count, args.result_name + "_cge_total_household_count.json", 
                                     income_categories=categories)
    cge_json.get_cge_gross_income(None, gross_income, args.result_name + "_cge_total_household_income.json",
                                  income_categories=categories)
    
    categories = []
    if len(regions) > 0:
        for d in demand_cat:
            for r in regions:
                categories.append(d + "_" + r)
    else:
        categories = demand_cat
    print("Demand categories")
    print(categories)
    cge_json.get_cge_employment(None, None, pre_disaster_factor_demand, post_disaster_factor_demand,
                                args.result_name + "_cge_employment.json", demand_categories=categories)

    categories = []
    if len(regions) > 0:
        for d in supply_cat:
            for r in regions:
                categories.append(d + "_" + r)
    else:
        categories = supply_cat
    print("Supply categories")
    print(categories)
    cge_json.get_cge_domestic_supply(None, domestic_supply, args.result_name + "_cge_domestic_supply.json",
                                     supply_categories=categories)


def parse_list(categories):
    # Convert timesteps to a list
    if "," in categories:
        print("comma separate list")
        categories = categories.strip().split(",")
    else:
        print("space separated list")
        categories = categories.split(" ")
    return categories


if __name__ == '__main__':
    # result file
    parser = argparse.ArgumentParser(description='Post Process CGE output`.')
    parser.add_argument('--result_name', dest='result_name', help='Result Name')
    parser.add_argument('--household-count', dest='household_count', help='CGE household count')
    parser.add_argument('--domestic-supply', dest='domestic_supply', help='CGE domestic supply')
    parser.add_argument('--gross-income', dest='gross_income', help='CGE gross income')
    parser.add_argument('--pre-disaster-factor-demand', dest='pre_disaster_factor_demand',
                        help='CGE pre disaster factor demand')
    parser.add_argument('--post-disaster-factor-demand', dest='post_disaster_factor_demand',
                        help='CGE post disaster factor demand')
    parser.add_argument('--regions', dest='regions', help='Regions')
    parser.add_argument('--hh_cat', dest='hh_cat', help='Household Categories')
    parser.add_argument('--demand_cat', dest='demand_cat', help='Household Categories')
    parser.add_argument('--supply_cat', dest='supply_cat', help='Supply Categories')

    args = parser.parse_args()
    main()
