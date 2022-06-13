import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame

import mapclassify as mc

from shapely.geometry import Point, LineString
from os.path import join

SIGN = '_'
CRS = 'epsg:4326'
PRO_CRS = 'epsg:3857'


class MyGeoDataBase:
    def __init__(self, data_folder: str, scale_units: dict, data_survey: str, input_folder: str,
                 register_name: str = None):
        # Name for files in the geodata base
        self.data_survey = data_survey
        self.inputs = input_folder
        self.data_folder = data_folder
        self.register = register_name
        # Public Fields
        self.scale_units = scale_units

    def __spatial_join_dissolve(self, left_file: GeoDataFrame, to_join: GeoDataFrame, by: str, register: bool = False):
        """
        spatial join between the two geo dataframe and then aggregate teh results
        :param left_file:
        :param to_join:
        :param by:
        :return:
        """
        join_left_df = left_file.sjoin(to_join, how='inner')
        if register:
            # sum the total amount of registered bikes
            return join_left_df.groupby(by).sum()[self.register]
        else:
            # count the number of bike theft per area
            join_left_agg = join_left_df.groupby(by).count()
            return join_left_agg[join_left_agg.columns[0]]

    def create_gis_file_of_pnts_theft(self, df: GeoDataFrame, layer: str, is_line: bool = False,
                                      more_cols: list = None):
        """
        Convert the csv file (@df) to a gis file containing pertinent information and geometry
        :return:
        """
        if is_line:
            # to create gis file of stolen and recovery places
            print('number of places before removing are {}'.format(len(df)))
            df = df[df['stolen_bikes_place'] != df['recover_bikes_place']]
            print('number of places after removing are {}'.format(len(df)))
            df.geometry = df.apply(
                lambda x: LineString([(float(x['lat']), float(x['lon'])), (float(x['lat_rec']), float(x['lon_rec']))]),
                axis=1)
        else:
            df.geometry = df.apply(lambda x: Point(float(x['lat']), float(x['lon'])), axis=1)

        df.crs = CRS
        df = df.to_crs(PRO_CRS)
        if is_line:
            df['length'] = df.length
        cols_to_work_with = ['index', 'score', 'geometry']
        if more_cols is not None:
            cols_to_work_with.extend(more_cols)
        df[cols_to_work_with].to_file(self.data_folder, layer=layer, driver="GPKG")


class BikeTheft(MyGeoDataBase):
    def __init__(self, data_survey: str, data_folder: str, scale_units: dict, input_folder: str):
        super().__init__(data_folder, scale_units, data_survey=data_survey, input_folder=input_folder)

        # # Name for files in the geodata base
        # Parameters
        self.res_path = 'res'

        # # public members
        self.location_name = 'stolen_bikes_place'  # name of the column with the location of the stolen bikes

        # # private members
        self.__flow_map_path = 'flow_map'
        self.__theft_rec_same_path = 'theft_rec_same'
        self.__theft_rec_diff_path = 'theft_rec_diff'

    def create_flow_line_theft_rec(self, df: GeoDataFrame):
        """
        Create two GIS files of theft and recovery location. One when these the same place and second when different place
        :param df:
        :return:
        """
        print(SIGN + 'rec different place')
        self.create_gis_file_of_pnts_theft(df[df['stolen_bikes_place'] != df['recover_bikes_place']],
                                           self.__flow_map_path, is_line=True,
                                           more_cols=[self.location_name, 'recover_bikes_place', 'score_rec', 'length'])
        print(SIGN + 'rec same place')
        self.create_gis_file_of_pnts_theft(df[df['stolen_bikes_place'] == df['recover_bikes_place']],
                                           self.__theft_rec_same_path, more_cols=[self.location_name])

        print(SIGN + 'rec different place points')
        self.create_gis_file_of_pnts_theft(df[df['stolen_bikes_place'] != df['recover_bikes_place']],
                                           self.__theft_rec_diff_path, more_cols=[self.location_name])

    def classify_distance(self):
        """
        1-d classification based on Natural Breaks
        """
        df = gpd.read_file(self.data_folder, layer=self.__flow_map_path)
        cal = (df['length'] * 10 ** -3).to_numpy()
        return mc.NaturalBreaks(cal, k=5)

    def same_city(self):
        """
        This method finds the number of bike that stolen and recovered in the same city
        """
        df = gpd.read_file(self.data_folder, layer=self.__theft_rec_same_path)
        new_df = df.drop_duplicates('stolen_bikes_place').set_index('stolen_bikes_place')
        new_df['count'] = df.groupby('stolen_bikes_place').count()['geometry']
        new_df.reset_index(inplace=True)
        return new_df[~new_df['stolen_bikes_place'].apply(lambda x: x.startswith(','))]

    def california_counties(self):
        """
        Count for each county the number of theft and the number of recovery
        :return:
        """
        # files for the process
        name = 'NAME'
        counties = gpd.read_file(join(self.inputs, self.scale_units['counties'][0]))
        thefts = gpd.read_file(self.data_folder, layer=self.res_path)
        recoveries = gpd.read_file(self.data_folder, layer=self.__theft_rec_diff_path)
        counties_clean = counties[['NAME', 'geometry']].set_index(name)
        counties_clean['count_thefts'] = self.__spatial_join_dissolve(left_file=counties_clean, to_join=thefts,
                                                                      by=name)
        counties_clean['count_recovery'] = self.__spatial_join_dissolve(left_file=counties_clean,
                                                                        to_join=recoveries,
                                                                        by=name)
        return counties_clean


class BikeIndex(MyGeoDataBase):
    def __init__(self, data_survey: str, location_col: str, index_col: str, name_new_source: str, data_folder: str,
                 scale_units: dict, user_bike_index: str, input_folder: str, register_name: str):

        super().__init__(data_folder, scale_units, data_survey=data_survey, input_folder=input_folder,
                         register_name=register_name)
        # # private members
        self.__registration_file = 'registration'  # gis file for registration bike points
        self.__location_col = location_col  # the name of the column where the date about the location is store
        self.__index_col = index_col
        self.__name_new_source = name_new_source
        self.__bike_index = user_bike_index
        self.__merge_data_path = 'merge_data'

    def merge_two_dataset(self):
        """
        It merges two different datasource about bike theft and return GeoDataFrame
        : param df_bike_survey:
        :return:
        """
        df_bike_survey = pd.read_csv(self.data_survey)
        print(SIGN + 'the number of rows in the data surveys is:{}'.format(len(df_bike_survey)))
        # don't combine registered bikes obtained from the survey
        df_bike_survey = df_bike_survey[df_bike_survey['Q10'].str.contains('Yes') is False]
        print(
            SIGN + 'the number of rows without registered bikes  in the data surveys is:{}'.format(len(df_bike_survey)))
        new_source = gpd.read_file(self.__bike_index)
        location_col_new_source = self.__location_col
        index_col_new_source = self.__index_col
        name_new_source = self.__name_new_source
        index_survey = 'index'
        df_bike_survey = df_bike_survey[['lat', 'lon', index_survey]]
        df_bike_survey['source'] = 'survey'

        print(SIGN + '{} len is: {} '.format(name_new_source, len(new_source)))
        only_with_loc = new_source[new_source[location_col_new_source] != '']
        print(
            SIGN + '{} len after removing records without location is: {} '.format(name_new_source, len(only_with_loc)))
        with_rel_cols = only_with_loc[[location_col_new_source, index_col_new_source]]
        with_rel_cols['source'] = name_new_source
        with_rel_cols.rename(columns={index_col_new_source: index_survey}, inplace=True)

        with_rel_cols[['lon', 'lat']] = with_rel_cols[location_col_new_source].str.split(',', expand=True)
        after_merge = pd.concat([df_bike_survey, with_rel_cols]).drop(columns=[location_col_new_source])
        print(SIGN + 'the number of records after merging is:{}'.format(len(after_merge)))
        GeoDataFrame(data=after_merge, crs=CRS, geometry=after_merge.apply(lambda x: Point(float(x['lat']),
                                                                                           float(x['lon'])),
                                                                           axis=1)).to_crs(PRO_CRS).to_file(
            self.data_folder, layer=self.__merge_data_path, driver="GPKG")

    def combine_data(self, df_1: GeoDataFrame, df_2: GeoDataFrame):
        """
        This method combine two dataframe into one  (include CRS projection to the same)
        :param df_1:
        :param df_2:
        :return:
        """
        fields_to_use = [self.scale_units['states'][1], 'geometry']
        final_df = pd.concat(
            [df_1.to_crs(PRO_CRS)[fields_to_use], df_2.to_crs(PRO_CRS)[fields_to_use]], ignore_index=True)

        final_df.to_file(self.data_folder, layer=self.scale_units['states'][0], driver="GPKG")

    def grouping_by(self, registration: bool = True, is_no_survey=True):
        """
        group (count the numbers of records) based on the selected spatial unit
        :return:
        """

        def iterate_over():
            """
            calculate the number of thefts per state/county, the number of registrations, and their rates
            :return:
            """
            count = 'count'
            name_scale = item[0]
            print(SIGN + '{}'.format(name_scale))
            spatial_scale = item[1]
            name_col = spatial_scale[1]
            # state file is saved in our database whereas counties file saved in  input folder
            if name_scale == 'states':
                df_left = gpd.read_file(self.data_folder, layer=spatial_scale[0])
            else:
                df_left = gpd.read_file(join(self.inputs, spatial_scale[0]))
            df_left = df_left[['geometry', name_col]]
            res = self.__spatial_join_dissolve(left_file=df_left, to_join=records_data, by=name_col)
            df_left.set_index(name_col, inplace=True)
            df_left[count] = res
            if registration:
                regi = 'regi'
                count_regi = 'count_regi'
                df_left.reset_index(inplace=True)
                res = self.__spatial_join_dissolve(left_file=df_left, to_join=records_data_reg, by=name_col,
                                                   register=True)
                df_left.set_index(name_col, inplace=True)
                df_left[regi] = res
                df_left[count_regi] = df_left[count] / df_left[regi] * 100
                df_left = df_left[['geometry', count, regi, count_regi]]
            else:
                df_left = df_left[['geometry', count]]
            df_left.fillna(0, inplace=True)
            df_left = df_left[df_left[count] > 0]
            df_left.reset_index(inplace=True)
            spatial_scale.append(df_left)

        # data with location of thefts
        gdf = gpd.read_file(self.data_folder, layer=self.__merge_data_path)
        # if is_no_survey delete the survey points
        if is_no_survey:
            gdf = gdf[gdf['source'] != 'survey']
        records_data = gdf[['index', 'geometry']]
        # if registration upload points with registration
        if registration:
            gdf_reg = gpd.read_file(self.data_folder, layer=self.__registration_file)
            records_data_reg = gdf_reg[['index', self.register, 'geometry']]
        scales_data = self.scale_units
        # iterate over each unit scale (different shp file)
        for item in scales_data.items():
            iterate_over()
        [x[1][-1].to_file(self.data_folder, layer='records_by_' + x[0], driver="GPKG") for x in
         scales_data.items()]


class FindPatterns:
    def __init__(self, output_folder: str, rel_data: dict, variable_type: str):
        """

        : param output_folder: all the new files will save into this folder
        """
        self._output_folder = output_folder
        # This dictionary includes the relevant columns and their new name (with more meaning name)
        self._rel_data = rel_data
        self._new_col = list(self._rel_data.values())
        self._new_col = list(self._rel_data.values())
        self._dataframe = join(self._output_folder, 'training_dataset_{0}.csv'.format(variable_type))
        self._clustering = join(self._output_folder, 'clustering_{0}.csv'.format(variable_type))

    def prepare_training_set(self, raw_data_path: str):
        """
        1. read from the original dataframe only the columns in @self._rel_data.keys
        2. encoding the columns values
        3. save the new dataframe into csv file
        :param raw_data_path:
        :return:
        """
        raw_data_file = pd.read_csv(raw_data_path)
        rel_cols = raw_data_file[list(self._rel_data.keys())]
        rel_cols[self._new_col] = rel_cols.apply(lambda x: x.astype('category').cat.codes)
        rel_cols.to_csv(self._dataframe)

    def unsupervised_model(self, n_clusters):
        """
        Deploy classification
        :return:
        """
        from sklearn.cluster import AgglomerativeClustering
        import numpy as np

        df = pd.read_csv(self._dataframe)
        X = df[self._new_col].to_numpy()
        clustering = AgglomerativeClustering(n_clusters=n_clusters).fit(X)
        df['cluster'] = clustering.labels_
        df.to_csv(self._clustering)

    def classification_analysis(self):
        import matplotlib.pyplot as plt
        df = pd.read_csv(self._clustering)

        x = df[df['cluster'] == 4][['Q4', 'stolen_bikes_year']]
        x = x.fillna('-1')

        # fig, ((ax0, ax1), (ax2, ax3)) = plt.subplots(nrows=2, ncols=2)
        # table = x.value_counts().reset_index()[['Q4', 'stolen_bikes_year']]
        # print(table)
        x['stolen_bikes_year'].value_counts().plot(kind='pie')

        # ax0.set_title('bars with legend')

        plt.show()
