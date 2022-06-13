from arcgis.gis import GIS
from arcgis.geometry import Point
from arcgis.geocoding import geocode, reverse_geocode
import pandas as pd
from pandas import DataFrame, Series
import zipfile
import glob
import os
import re
from os.path import join

# The code is based on this website - https://developers.arcgis.com/python/guide/using-the-geocode-function/
# Should be used with arcgis interpreter #
SIGN = '_'


def unzip_file(file_to_zip: str, new_location: str):
    """
    Unzip @file_to_zip into @new_location with a new name
    :param file_to_zip:
    :param new_location:
    :return:
    """
    zipdata = zipfile.ZipFile(file_to_zip)
    zipinfos = zipdata.infolist()
    zipinfos[0].filename = 'new_data.csv'
    zipdata.extract(zipinfos[0], path=new_location)


def geo_coding(df: DataFrame, col_to_geo_code: list, new_cols: list, what_data_file: str = 'theft'):
    """
    Geocoding the place appear in &col_to_geo_code
    :param what_data_file:
    :param is_exist:
    :param new_cols: The name of new columns to store the results
    :param col_to_geo_code:
    :param df:
    :return:
    """

    def make_geocoding(row):
        """
        Each row is converted to coordinate with the geocoding score using arcgis geocoding api
        :param index:
        :param row:
        :return:
        """

        def advanced():
            temp_row = row[col_to_geo_code[-1]]
            if len(temp_row.split(' ')) > 1 or len(temp_row.split(',')) > 1 or len(temp_row.split('/')) > 1:
                # that means there is enough detail to make geocoding
                return geocode(temp_row)
            else:
                try:
                    # in case it include only postal zip code
                    row_int = int(temp_row)
                    row_dic = {"Postal": row_int}
                    return geocode(row_dic)
                except ValueError:
                    # try first to geocode by region
                    row_dic = {"Region": temp_row}
                    esrihq = geocode(row_dic)
                    if len(esrihq) == 0 or (not esrihq[0]['attributes']['Country'] == 'USA'
                                            and not esrihq[0]['attributes']['Country'] == 'CAN'):
                        row_dic = {"City": temp_row}
                        return geocode(row_dic)
                    else:
                        return esrihq

        def test_geo_coding():
            """
            Using the participant's response location, this method ensures that the correct option
            is selected out of all the options
            :return:
            """

            def compare_to_response_location():
                """
                From country level to state/province level, the correct geocoding can be identified by comparing
                 participant location to theft location. For CA, subregional data is also considered.
                :return:
                """
                # The loc_scale provides information about the current scale
                loc_scale = locations_list[mygenerator.__next__()]
                new_esrihq = [locy for locy in esrihq if
                              locy['attributes'][loc_scale] == temp_reverse['address'][loc_scale]]
                if len(new_esrihq) == 0:
                    return esrihq[0]
                elif len(new_esrihq) == 1 or (
                        loc_scale == 'Region' and temp_reverse_region != 'California') or loc_scale == 'Subregion':
                    return new_esrihq[0]
                else:
                    return compare_to_response_location()

            if len(esrihq) < 2:
                return esrihq[0]
            else:
                if pd.isna(row['LocationLongitude']) or (esrihq[0]['score'] == 100 and esrihq[1]['score'] < 100):
                    # The following are situations when no further check is applied: when the participant location
                    # is unknown, the first option is graded 100 and the rest are not
                    return esrihq[0]
                else:
                    temp_reverse = reverse_geocode(row[:-1].to_list())
                    temp_reverse_region = temp_reverse['address']['Region']
                    locations_list = ['CntryName', 'Region', 'Subregion']
                    mygenerator = (x for x in range(3))
                    return compare_to_response_location()

        index = row.name
        print("Progress {:2.1%}".format(generator_2.__next__() / df_len))
        if what_data_file == 'theft':
            esrihq = advanced()
        else:
            # that way of geocoding is based on given data
            if pd.isna(row['City']):
                multi_field_address = ','.join([row['Country'], str(row['PostalCode'])])
            else:
                multi_field_address = ','.join([row['City'], row['Country'], str(row['PostalCode'])])
            try:
                esrihq = geocode(multi_field_address)
            except:
                print('no geocoding to  row {} with {} '.format(index, row))
                return

        if what_data_file == 'theft':
            esrihq = test_geo_coding()
        esrihq_att = esrihq['attributes']
        esrihq_country = esrihq_att['Country']
        score = esrihq['score']
        if what_data_file == 'theft':
            if not esrihq_country == 'USA' and not esrihq_country == 'CAN':
                print('the row {} with a place {}  is not in US or Canada {}'.format(index, row[-1], esrihq))
            if score < 100:
                print('the score of the row {} with place {} is {} ,{}'.format(index, row[-1], score, esrihq))
            # input("Continue?")
        else:
            if score < 70:
                print('the score of the row {}  is {} '.format(index, score))

        loc = esrihq['location']
        new_location = ','.join([esrihq_att['City'], esrihq_att['Region'], esrihq_country])
        return loc['x'], loc['y'], score, new_location

    print(SIGN + 'number of rows without any anlysis = {}'.format(len(df)))

    if what_data_file == 'theft':
        # Rows with no place recorded should be removed
        df.dropna(subset=[col_to_geo_code[-1]], inplace=True)
        print(SIGN + 'number of rows after removing responses without locations = {}'.format(len(df)))
    else:
        # working only on US and Canada
        print(SIGN + 'number of all rows are:{}'.format(len(df)))
        df = df[(df['Country'] == 'Canada') | (df['Country'] == 'United States')]
        print(SIGN + 'number of America and Canada rows are:{}'.format(len(df)))
    df_len = len(df)
    # Using this generator, we can track the progress of the process
    generator_2 = (x for x in range(df_len))
    df[new_cols] = df.apply(lambda x: make_geocoding(x[col_to_geo_code]), axis=1, result_type='expand')
    return df[df['score'].notna()]


if __name__ == '__main__':
    gis = GIS()
    parameters = {'unzip_file': False, 'geo_coding_theft': False, 'geo_coding_recovery': True,
                  'geo_coding_registration': False}
    folder_to_un_zip = 'data/zip/Bicycle+Theft_June+13,+2022_09.41.zip'
    my_new_location = 'data/process'
    is_the_first_time = False

    if parameters['unzip_file']:
        print('unzip_file')
        unzip_file(folder_to_un_zip, my_new_location)

    if parameters['geo_coding_theft']:
        print('geo_coding_theft')

        my_df = pd.read_csv(join(my_new_location, 'new_data.csv'), skiprows=[1, 2]).reset_index()
        res_file_name = my_new_location + '/res_with_geo_loc.csv'
        res = geo_coding(my_df, ['LocationLongitude', 'LocationLatitude', "Q16_1_TEXT"],
                         ['lat', 'lon', 'score', 'stolen_bikes_place'])
        res.to_csv(res_file_name)

    if parameters['geo_coding_recovery']:
        print('geo_coding_recovery')
        my_df = pd.read_csv(my_new_location + '/res_with_geo_loc.csv')
        res = geo_coding(my_df, ['LocationLongitude', 'LocationLatitude', 'Q22_1_TEXT'],
                         ['lat_rec', 'lon_rec', 'score_rec', "recover_bikes_place"])
        res.to_csv(my_new_location + '/res_with_geo_loc_rec.csv')
    if parameters['geo_coding_registration']:
        my_df = pd.read_csv('data/Location_Counts.csv')
        res = geo_coding(df=my_df, col_to_geo_code=['Country', 'City', 'PostalCode'],
                         new_cols=['lat', 'lon', 'score', 'location '], what_data_file='registration')
        res.to_csv(my_new_location + '/registration_geocode.csv')
