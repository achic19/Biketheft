import geopandas as gpd
from theft_recovery import *

if __name__ == '__main__':
    SIGN = '_'
    parameters = {'create_gis_file_of_pnts_theft': False, 'create_flow_line_theft_rec': False,
                  'classify_distance': True, 'same_city': False, 'california_counties': True}
    if parameters['create_gis_file_of_pnts_theft']:
        print('create_gis_file_of_pnts_theft')
        create_gis_file_of_pnts_theft(gpd.read_file('data/res_with_geo_loc.csv')).to_file("Biketheft_qgis/data.gpkg",
                                                                                          layer='res', driver="GPKG")
    if parameters['create_flow_line_theft_rec']:
        print('create_flow_line_theft_rec')
        df1, df2, df3 = create_flow_line_theft_rec(gpd.read_file('data/res_with_geo_loc_rec.csv'))
        df1.to_file("Biketheft_qgis/data.gpkg", layer='flow_map', driver="GPKG")
        df2.to_file("Biketheft_qgis/data.gpkg", layer='theft_rec_same', driver="GPKG")
        df3.to_file("Biketheft_qgis/data.gpkg", layer='theft_rec_diff', driver="GPKG")

    if parameters['classify_distance']:
        print('classify_distance')
        classify_distance(gpd.read_file("Biketheft_qgis/data.gpkg", layer='flow_map', driver="GPKG"))

    if parameters['same_city']:
        print('same_city')
        same_city(gpd.read_file("Biketheft_qgis/data.gpkg", layer='theft_rec_same', driver="GPKG")).to_file(
            "Biketheft_qgis/data.gpkg", layer='city_classification', driver="GPKG")

    if parameters['california_counties']:
        print('california_counties')
        my_counties = gpd.read_file("Biketheft_qgis/data.gpkg", layer='california_county', driver="GPKG")
        my_thefts = gpd.read_file("Biketheft_qgis/data.gpkg", layer='res', driver="GPKG")
        my_recoveries = gpd.read_file("Biketheft_qgis/data.gpkg", layer='theft_rec_diff', driver="GPKG")
        california_counties(counties=my_counties, thefts=my_thefts, recoveries=my_recoveries).to_file(
            "Biketheft_qgis/data.gpkg", layer='count_by_county', driver="GPKG")
