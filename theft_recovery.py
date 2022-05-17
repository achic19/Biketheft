import geopandas as gpd
from geopandas import GeoDataFrame
import mapclassify as mc
import numpy as np

SIGN = '_'


def create_gis_file_of_pnts_theft(df, mode='theft', more_cols=None):
    """
    Convert the csv file (@df) to a gis file containing pertinent information and geometry
    :return:
    """
    from shapely.geometry import Point, LineString
    if mode == 'theft':
        df.geometry = df.apply(lambda x: Point(float(x['lat']), float(x['lon'])), axis=1)
    else:
        print('number of places before removing are {}'.format(len(df)))
        df = df[df['stolen_bikes_place'] != df['recover_bikes_place']]
        print('number of places after removing are {}'.format(len(df)))
        df.geometry = df.apply(
            lambda x: LineString([(float(x['lat']), float(x['lon'])), (float(x['lat_rec']), float(x['lon_rec']))]),
            axis=1)
    df.crs = 'epsg:4326'
    df = df.to_crs('epsg:3857')
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


def classify_distance(df):
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


def california_counties(counties: GeoDataFrame, thefts: GeoDataFrame, recoveries: GeoDataFrame):
    def spatial_join_dissolve(to_join: GeoDataFrame):
        join_left_df = counties_clean.sjoin(to_join, how='left')
        join_left_agg = join_left_df.dissolve(by="NAME", aggfunc='count')
        return join_left_agg[['stolen_bikes_place']]

    counties_clean = counties[['NAME', 'geometry']].set_index('NAME')
    counties_clean['count_thefts'] = spatial_join_dissolve(to_join=thefts)
    counties_clean['count_recovery'] = spatial_join_dissolve(to_join=recoveries)
    return counties_clean
