import geopandas as gpd

from python.theft_recovery import *

if __name__ == '__main__':
    parameters = {'BikeTheft': True, 'BikeIndex': False, 'FindPatterns': False}
    if parameters['BikeTheft']:
        locals_parameters = {'create_gis_file_of_pnts_theft': False, 'create_flow_line_theft_rec': True,
                             'classify_distance': False, 'same_city': True,
                             'california_counties': False}
        print('BikeTheft')
        theft_recovery = BikeTheft(data_survey=r'data/process/res_with_geo_loc.csv',
                                   data_folder=r"general\Biketheft_esri\data.gpkg",
                                   scale_units={
                                       'states': ['us_canada_provinces', 'NAME'],
                                       'counties': ['california_county.shp', 'NAME']},
                                   input_folder=r'general\Biketheft_esri\input')

        if locals_parameters['create_gis_file_of_pnts_theft']:
            print('create_gis_file_of_pnts_theft')
            theft_recovery.create_gis_file_of_pnts_theft(df=gpd.read_file(theft_recovery.data_survey),
                                                         layer=theft_recovery.res_path,
                                                         more_cols=[theft_recovery.location_name])

        if locals_parameters['create_flow_line_theft_rec']:
            print('create_flow_line_theft_rec')
            theft_recovery.create_flow_line_theft_rec(gpd.read_file('data/process/res_with_geo_loc_rec.csv'))

        if locals_parameters['classify_distance']:
            print('classify_distance')
            print(theft_recovery.classify_distance())

        if locals_parameters['same_city']:
            print('same_city')
            theft_recovery.recovery_city().to_file(theft_recovery.data_folder, layer='city_classification')

        if locals_parameters['california_counties']:
            print('california_counties')
            theft_recovery.california_counties().to_file(theft_recovery.data_folder, layer='count_by_county')

    if parameters['BikeIndex']:
        locals_parameters = {'merge_two_dataset': False, 'registration_file_to_gis': False, 'combine_canada_us': False,
                             'grouping_by': False, 'combine_census_data': True}

        print('BikeIndex')
        register = BikeIndex(data_survey='data/res_with_geo_loc.csv', location_col='bikes.stolen_coordinates',
                             index_col='field_1', name_new_source='__bike_index',
                             data_folder=r"general\Biketheft_esri\data.gpkg",
                             scale_units={
                                 'states': ['us_canada_provinces', 'NAME'],
                                 'counties': ['california_county.shp', 'NAME']},
                             user_bike_index='data/stolen_bikes_dec_09.csv'
                             , input_folder=r'general\Biketheft_esri\input', register_name='Total')
        if locals_parameters['merge_two_dataset']:
            print('Merging data from BikeIndex and the survey')
            register.merge_two_dataset()
        if locals_parameters['registration_file_to_gis']:
            print('registration_file_to_gis')
            register.create_gis_file_of_pnts_theft(
                df=GeoDataFrame(pd.read_csv(r'data/process/registration_geocode.csv')),
                layer='registration',
                more_cols=[register.register, 'location'])
        if locals_parameters['combine_canada_us']:
            print('combine_canada_us')
            df_1 = gpd.read_file(join(register.inputs, 'us_states.shp'))
            df_2 = gpd.read_file(join(register.inputs, 'canada.shp'))
            df_2.rename(columns={'PRENAME': register.scale_units['states'][1]}, inplace=True)
            register.combine_data(df_1=df_1, df_2=df_2)
        if locals_parameters['grouping_by']:
            print('grouping_by')
            register.grouping_by()
        if locals_parameters['combine_census_data']:
            print('combine_census_data')
    if parameters['FindPatterns']:
        locals_parameters = {'prepare_training_set': False, 'unsupervised_model': False,
                             'classification_analysis': True}
        dic_ver = {'bike_attributes': [{'Q2': 'stolen_part', 'Q4': 'stolen_bikes_year', 'Q5': 'stolen_bikes_month',
                                        'Q6': 'stolen_bikes_day_time', 'Q7': 'bicycle locked_status',
                                        'Q8': 'locked_type',
                                        'Q9': 'bike_parking', 'Q13': 'bike_price', 'Q14': 'is_electronic',
                                        'Q15': 'bike_type'}, True],
                   'cycling_behaviour': [{'Q28_1': 'cycle_frequent_fall',
                                          'Q28_2': 'cycle_frequent_winter', 'Q28_3': 'cycle_frequent_spring',
                                          'Q28_4': 'cycle_frequent_summer', 'Q29': 'trip_type'}, False],
                   'person_attributes': [{'Q34': 'age', 'Q35': 'gender', 'Q36': 'income', 'Q37': 'num_of_bikes',
                                          'Q38': 'education', 'Q39': 'ethnic_origin'}, False]}
        for item in dic_ver.items():
            print(item[0])
            if item[1][1]:
                find_patterns = FindPatterns(output_folder=r'data\process\find_patterns', rel_data=item[1][0],
                                             variable_type=item[0])
                if locals_parameters['prepare_training_set']:
                    print('_prepare_training_set')
                    find_patterns.prepare_training_set(raw_data_path='data/res_with_geo_loc.csv')
                if locals_parameters['unsupervised_model']:
                    print('_unsupervised_model')
                    find_patterns.unsupervised_model(n_clusters =4)
                if locals_parameters['classification_analysis']:
                    print('classification_analysis')
                    find_patterns.classification_analysis()
