import geopandas as gpd
import pandas as pd
from pandas import DataFrame
from geopandas import GeoDataFrame
import mapclassify as mc
import numpy as np
from shapely.geometry import Point, LineString

SIGN = '_'
CRS = 'epsg:4326'
PRO_CRS = 'epsg:3857'


def create_gis_file_of_pnts_theft(df, mode='theft', more_cols=None):
    """
    Convert the csv file (@df) to a gis file containing pertinent information and geometry
    :return:
    """

    if mode == 'theft':
        df.geometry = df.apply(lambda x: Point(float(x['lat']), float(x['lon'])), axis=1)
    else:
        print('number of places before removing are {}'.format(len(df)))
        df = df[df['stolen_bikes_place'] != df['recover_bikes_place']]
        print('number of places after removing are {}'.format(len(df)))
        df.geometry = df.apply(
            lambda x: LineString([(float(x['lat']), float(x['lon'])), (float(x['lat_rec']), float(x['lon_rec']))]),
            axis=1)
    df.crs = CRS
    df = df.to_crs(PRO_CRS)
    if mode != 'theft':
        df['length'] = df.length
    # df = df[
    #     ['LocationLatitude', 'LocationLongitude', 'Q2', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 'Q11', 'Q12', 'Q13',
    #      'Q14', 'Q15',
    #      'Q16_1_TEXT', 'Q17', 'Q18', 'Q19', 'Q21', 'Q22_1_TEXT', 'Q23', 'Q23_15_TEXT', 'Q24', 'Q25', 'Q26', 'Q27', 'Q28_1',
    #      'Q28_2', 'Q28_3', 'Q28_4', 'Q29', 'Q30', 'Q31', 'Q34', 'Q35', 'Q36', 'Q37', 'Q38', 'Q39', 'Q39_10_TEXT',
    #      'score',
    #      'geometry']]
    # df.rename(columns={'LocationLatitude': 'response_loc_lat',
    #                    'LocationLongitude': 'response_loc_lon', 'Q2': 'stolen_part', 'Q4': "stolen_bikes_year",
    #                    'Q5': 'stolen_bikes_month',
    #                    'Q6': 'stolen_bikes_day_time', 'Q7': 'bicycle locked_status', 'Q8': 'locked_type',
    #                    'Q9': 'bike_parking', 'Q10': 'is_registered_recovered_sys', 'Q11': 'is_reported',
    #                    'Q12': 'is_insured', 'Q13': 'bike_price', 'Q14': 'is_electronic', 'Q15': 'bike_type',
    #                    "Q16_1_TEXT": "stolen_bikes_place", 'Q17': 'is_near_college', 'Q18': 'is_recovered',
    #                    'Q19': 'is_sold_online', 'Q21': 'is_police_assist', 'Q22_1_TEXT': "recovered_bikes_place",
    #                    'Q23': 'recovered_bikes_loc', 'Q23_15_TEXT': 'recovered_bikes_loc_more_data',
    #                    'Q24': 'bike_condition', 'Q25': 'is_bike_replaced', 'Q26': 'time_to_replace',
    #                    'Q27': 'days_recover_use', 'Q28_1': 'cycle_frequent_fall',
    #                    'Q28_2': 'cycle_frequent_winter', 'Q28_3': 'cycle_frequent_spring',
    #                    'Q28_4': 'cycle_frequent_summer', 'Q29': 'trip_type', 'Q30': 'substitute_mode_trns',
    #                    'Q31': 'behaviour', 'Q34': 'age', 'Q35': 'gender', 'Q36': 'income', 'Q37': 'num_of_bikes',
    #                    'Q38': 'education', 'Q39': 'ethnic_origin', 'Q39_10_TEXT': 'ethnic_origin_other'},
    #           inplace=True)

    cols_to_work_with = ['index', 'stolen_bikes_place', 'score', 'geometry']
    if more_cols is not None:
        cols_to_work_with.extend(more_cols)
    return df[cols_to_work_with]


def create_flow_line_theft_rec(df):
    """
    Create two GIS files of theft and recovery location. One when these the same place and second when different place
    :param df:
    :return:
    """
    print(SIGN + 'rec different place')
    df_1 = create_gis_file_of_pnts_theft(df[df['stolen_bikes_place'] != df['recover_bikes_place']], 'rec',
                                         ['recover_bikes_place', 'score_rec', 'length'])
    print(SIGN + 'rec same place')
    df_2 = create_gis_file_of_pnts_theft(df[df['stolen_bikes_place'] == df['recover_bikes_place']])

    print(SIGN + 'rec different place points')
    df_3 = create_gis_file_of_pnts_theft(df[df['stolen_bikes_place'] != df['recover_bikes_place']])
    return df_1, df_2, df_3


def classify_distance(df: GeoDataFrame):
    """
    1-d classification based on Natural Breaks
    :param df:
    :return:
    """
    cal = (df['length'] * 10 ** -3).to_numpy()
    return mc.NaturalBreaks(cal, k=5)


def same_city(df: GeoDataFrame):
    """
    This method finds the number of bike that stolen and recovered in the same city
    :param df:
    :return:
    """
    new_df = df.drop_duplicates('stolen_bikes_place').set_index('stolen_bikes_place')
    new_df['count'] = df.groupby('stolen_bikes_place').count()['geometry']
    new_df.reset_index(inplace=True)
    return new_df[~new_df['stolen_bikes_place'].apply(lambda x: x.startswith(','))]


def spatial_join_dissolve(left_file: GeoDataFrame, to_join: GeoDataFrame, by: str):
    join_left_df = left_file.sjoin(to_join, how='left').reset_index()
    join_left_agg = join_left_df.dissolve(by=by, aggfunc='count')
    join_left_df.groupby('cntry_name').count()
    return join_left_agg[join_left_agg.columns[0]]


def california_counties(counties: GeoDataFrame, thefts: GeoDataFrame, recoveries: GeoDataFrame):
    counties_clean = counties[['NAME', 'geometry']].set_index('NAME')
    counties_clean['count_thefts'] = spatial_join_dissolve(left_file=counties_clean, to_join=thefts, by="NAME")
    counties_clean['count_recovery'] = spatial_join_dissolve(left_file=counties_clean, to_join=recoveries, by="NAME")
    return counties_clean


def merge_two_dataset(new_source: DataFrame, df_bike_survey: DataFrame, location_col_new_source: str,
                      index_col_new_source: str,
                      name_new_source: str) -> GeoDataFrame:
    """
    It merges two different datasource about bike theft and return GeoDataFrame
    :param new_source:
    :param df_bike_survey:
    :param location_col_new_source: the column name of the coordinates
    :param index_col_new_source: the column name of the index
     (help to restore the locations of records in the original dataset
    :param name_new_source: the name of the new source dataset
    :return:
    """
    index_survey = 'index'
    df_bike_survey = df_bike_survey[['lat', 'lon', index_survey]]
    df_bike_survey['source'] = 'survey'

    print(SIGN + '{} len is: {} '.format(name_new_source, len(new_source)))
    only_with_loc = new_source[new_source[location_col_new_source] != '']
    print(SIGN + '{} len after removing records without location is: {} '.format(name_new_source, len(only_with_loc)))
    with_rel_cols = only_with_loc[[location_col_new_source, index_col_new_source]]
    with_rel_cols['source'] = name_new_source
    with_rel_cols.rename(columns={index_col_new_source: index_survey}, inplace=True)

    with_rel_cols[['lon', 'lat']] = with_rel_cols[location_col_new_source].str.split(',', expand=True)
    after_merge = pd.concat([df_bike_survey, with_rel_cols]).drop(columns=[location_col_new_source])
    print(SIGN + 'the number of records after merging is:{}'.format(len(after_merge)))
    return GeoDataFrame(data=after_merge, crs=CRS,
                        geometry=after_merge.apply(lambda x: Point(float(x['lat']), float(x['lon'])), axis=1)).to_crs(
        PRO_CRS)


def grouping_by(gdf: GeoDataFrame, scales_data: dict, data_folder: str) -> dict:
    for spatial_scale in scales_data.values():
        name_col = spatial_scale[1]
        df_left = gpd.read_file(data_folder, layer=spatial_scale[0], driver="GPKG").set_index(name_col)
        df_left = df_left[[df_left.columns[0], 'geometry']]
        records_data = gdf[['index', 'geometry']]
        df_left['count'] = spatial_join_dissolve(left_file=df_left, to_join=records_data, by=name_col)
        spatial_scale.append(df_left)
    return scales_data
