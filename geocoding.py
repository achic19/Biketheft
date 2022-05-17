from arcgis.gis import GIS
from arcgis.geometry import Point
from arcgis.geocoding import geocode
import pandas as pd
from pandas import DataFrame, Series
import zipfile
import glob
import os
import re

# Should be used with arcgis interpreter #
SIGN = '_'


def unzip_file(file_to_zip: str, new_location: str):
    """
    Unzip @file_to_zip into @new_location
    :param file_to_zip:
    :param new_location:
    :return:
    """
    with zipfile.ZipFile(file_to_zip, 'r') as zip_ref:
        zip_ref.extractall(new_location)


def geo_coding(df: DataFrame, col_to_geo_code: str, new_cols: list, is_exist: str = False):
    """
    Geocoding the place appear in &col_to_geo_code
    :param is_exist:
    :param new_cols: The name of new columns to store the results
    :param col_to_geo_code:
    :param df:
    :return:
    """

    def make_geocoding(row, index: int):
        """
        Each row is converted to coordinate with the geocoding score using arcgis geocoding api
        :param index:
        :param row:
        :return:
        """
        if len(row.split(' ')) > 1 or len(row.split(',')) > 1 or len(row.split('/')) > 1:
            # that means there is enough detail to make geocoding
            esrihq = geocode(row)[0]
        else:
            try:
                # in case it include only postal zip code
                row_int = int(row)
                row_dic = {"Postal": row_int}
                esrihq = geocode(row_dic)[0]
            except ValueError:
                # try first to geocode by region
                row_dic = {"Region": row}
                esrihq = geocode(row_dic)
                if len(esrihq) == 0 or (not esrihq[0]['attributes']['Country'] == 'USA'
                                        and not esrihq[0]['attributes']['Country'] == 'CAN'):
                    row_dic = {"City": row}
                    esrihq = geocode(row_dic)[0]
                else:
                    esrihq = esrihq[0]
        esrihq_att = esrihq['attributes']
        esrihq_country = esrihq_att['Country']

        if not esrihq_country == 'USA' and not esrihq_country == 'CAN':
            print('the row {} with a place {}  is not in US or Canada {}'.format(index, row, esrihq))
            # input("Continue?")
        score = esrihq['score']
        if score < 100:
            print('the score of the row {} with place {} is {} ,{}'.format(index, row, score, esrihq))
            # input("Continue?")
        loc = esrihq['location']
        new_location = ','.join([esrihq_att['City'], esrihq_att['Region'], esrihq_country, str(esrihq_att['Postal'])])
        return loc['x'], loc['y'], score, new_location

    print(SIGN + 'number of rows without any anlysis = {}'.format(len(df)))
    if is_exist:
        df_to_combine = pd.read_csv(is_exist)
        max_date = df_to_combine['EndDate'].max()
        df = df[df['EndDate']] > max_date
    # Rows with no place recorded should be removed
    df.dropna(subset=[col_to_geo_code], inplace=True)
    df.reset_index(inplace=True, drop=True)
    print(SIGN + 'number of rows after removing responses without locations = {}'.format(len(df)))

    df[new_cols] = df.apply(lambda x: make_geocoding(x[col_to_geo_code], x['index']), axis=1, result_type='expand')
    if is_exist:
        df = pd.read_csv(is_exist).append(df)

    return df


if __name__ == '__main__':
    gis = GIS()
    parameters = {'unzip_file': True, 'geo_coding_theft': False, 'geo_coding_recovery': False}
    folder_to_un_zip = 'zip/Bicycle+Theft_May+16,+2022_11.29.zip'
    my_new_location = 'data/process'
    is_the_first_time = False

    if parameters['unzip_file']:
        print('unzip_file')
        unzip_file(folder_to_un_zip, my_new_location)

    if parameters['geo_coding_theft']:
        print('geo_coding_theft')

        my_df = pd.read_csv(glob.glob(os.path.join(my_new_location, 'process/*'))[0], skiprows=[1, 2]).reset_index()
        res_file_name = my_new_location + '/res_with_geo_loc.csv'
        if is_the_first_time:
            res = geo_coding(my_df, "Q16_1_TEXT", ['lat', 'lon', 'score', 'stolen_bikes_place'])
        else:
            res = geo_coding(my_df, "Q16_1_TEXT", ['lat', 'lon', 'score', 'stolen_bikes_place'], res_file_name)
        res.to_csv(res_file_name)

    if parameters['geo_coding_recovery']:
        print('geo_coding_recovery')
        my_df = pd.read_csv(my_new_location + '/res_with_geo_loc.csv')
        res = geo_coding(my_df, 'Q22_1_TEXT', ['lat_rec', 'lon_rec', 'score_rec', "recover_bikes_place"])
        res.to_csv(my_new_location + '/res_with_geo_loc_rec.csv')
