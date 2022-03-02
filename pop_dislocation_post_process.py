#!/usr/bin/env python3
import argparse
import os
import zipfile
from pyincore import Dataset
from pyincore.utils.popdisloutputprocess import PopDislOutputProcess


def main():
    # Population Dislocation output
    pop_dislocation_result = args.pop_dislocation_result

    # Population dislocation output to JSON
    pd_process = PopDislOutputProcess(Dataset.from_file(pop_dislocation_result, "incore:popDislocation"))
    pd_process.pd_by_race("HUA_dislocation_by_race.json")
    pd_process.pd_by_income("HUA_dislocation_by_income.json")
    pd_process.pd_by_tenure("HUA_dislocation_by_tenure.json")
    pd_process.pd_by_housing("HUA_dislocation_by_housing_type.json")
    pd_process.pd_total("HUA_total_dislocation.json")

    # Temporary - remove once pd_process can compute this
    tmp_output_dataset = open("HUA_dislocation_by_disability.json", "w")
    tmp_output_dataset.close()

    # Heatmap
    pd_process.get_heatmap_shp("pop-dislocation-numprec.shp")

    # TODO need to put the dislocation shapefile in a zip
    zip_file = zipfile.ZipFile("pop-disl-numprec.zip", "w")
    for shp_file in os.listdir("."):
        if shp_file.startswith("pop-dislocation-numprec") and not shp_file.endswith(".zip"):
            print("should write file "+shp_file)
            zip_file.write(os.path.join(".", shp_file))
    zip_file.close()


if __name__ == '__main__':
    # result file
    parser = argparse.ArgumentParser(description='Post Process population dislocation results`.')
    parser.add_argument('--result', dest='pop_dislocation_result', help='Population Dislocation result')

    args = parser.parse_args()
    main()
