import geopandas as gpd
import pandas as pd
from python.theft_recovery import *

if __name__ == '__main__':
    parameters = {'create_gis_file_of_pnts_theft': False, 'create_flow_line_theft_rec': False,
                  'classify_distance': False, 'same_city': False, 'california_counties': False,
                  'merge_two_dataset': False, 'grouping_by': True}
    bike_index = 'data/stolen_bikes_dec_09.csv'
    location_col = 'bikes.stolen_coordinates'
    index_col = 'field_1'
    name_new_source = 'bike_index'
    data_folder = "general/Biketheft_qgis/data.gpkg"
    scale_units = {'countries': ['world_countries_after_projection', 'cntry_name'], 'counties': ['california_county', 'NAME']}

    if parameters['create_gis_file_of_pnts_theft']:
        print('create_gis_file_of_pnts_theft')
        create_gis_file_of_pnts_theft(gpd.read_file('data/res_with_geo_loc.csv')).to_file(data_folder,
                                                                                          layer='res', driver="GPKG")
    if parameters['create_flow_line_theft_rec']:
        print('create_flow_line_theft_rec')
        df1, df2, df3 = create_flow_line_theft_rec(gpd.read_file('data/res_with_geo_loc_rec.csv'))
        df1.to_file(data_folder, layer='flow_map', driver="GPKG")
        df2.to_file(data_folder, layer='theft_rec_same', driver="GPKG")
        df3.to_file(data_folder, layer='theft_rec_diff', driver="GPKG")

    if parameters['classify_distance']:
        print('classify_distance')
        classify_distance(gpd.read_file(data_folder, layer='flow_map', driver="GPKG"))

    if parameters['same_city']:
        print('same_city')
        same_city(gpd.read_file(data_folder, layer='theft_rec_same', driver="GPKG")).to_file(
            data_folder, layer='city_classification', driver="GPKG")

    if parameters['california_counties']:
        print('california_counties')
        my_counties = gpd.read_file(data_folder, layer='california_county', driver="GPKG")
        my_thefts = gpd.read_file(data_folder, layer='res', driver="GPKG")
        my_recoveries = gpd.read_file(data_folder, layer='theft_rec_diff', driver="GPKG")
        california_counties(counties=my_counties, thefts=my_thefts, recoveries=my_recoveries).to_file(
            data_folder, layer='count_by_county', driver="GPKG")

    if parameters['merge_two_dataset']:
        print('Merging data from BikeIndex and the survey')
        merge_two_dataset(new_source=gpd.read_file(bike_index),
                          df_bike_survey=gpd.read_file('data/res_with_geo_loc.csv'),
                          location_col_new_source=location_col, index_col_new_source=index_col,
                          name_new_source=name_new_source).to_file(data_folder, layer='merge_data', driver="GPKG")

    if parameters['grouping_by']:
        print('grouping_by')
        res = grouping_by(gdf=gpd.read_file(data_folder, layer='merge_data', driver="GPKG"), scales_data=scale_units,
                          data_folder=data_folder)
        [x.value[-1].to_file(data_folder, layer='records_by_' + x.key, driver="GPKG") for x in res]
