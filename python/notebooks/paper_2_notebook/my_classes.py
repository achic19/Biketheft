import pandas as pd
import scipy.stats as stats
import numpy as np


# region
class PreTest:
    """
    This class contains variables that are utilized by the MyTest class and typically remain constant across different
    tests.
    """

    def __init__(self, rows_to_delete: list, data_to_explore: pd.DataFrame, cols_to_explore: list, more_files_path: list):
        """
        :param cols_to_delete:
        :param data_to_explore:
        :param cols_to_explore:
        :param more_files_path:
        :return:
        """
        self.rows_to_delete = rows_to_delete
        self.pre_test_data = data_to_explore.fillna(-1)[cols_to_explore].applymap(
            lambda x: -1 if x in rows_to_delete + [-1] else x)
        self.data_path = more_files_path
        self.res = {}
        self.contingency_table = {}


class MyTests:
    def __init__(self, col_1: str, col_2: str, rel_data: tuple, pre_data: PreTest):
        print(f'\n{col_1}::{col_2}')
        self.col_1 = col_1
        self.col_2 = col_2
        self.fields_to_test = rel_data[1]
        self.rel_data = rel_data
        self.constant_data = pre_data

        if 'apply_chi' in rel_data[0]:
            print('apply_chi')
            self.__apply_chi()
        else:
            print('apply_spearmanr')
            self.__apply_spearmanr(rel_data[2])

    def __apply_chi(self):
        """
        apply chi_square test based on the cols and data
        :param new_names: more relevant data to employ when run the test
        :param col_1:
        :param col_2:
        :return:
        """
        # b.	Clean the data
        test_data = self.constant_data.pre_test_data[[self.col_1, self.col_2]]
        # In case more data should be remove prior to the analysis
        if isinstance(self.rel_data[-1], list):
            test_data = test_data[~test_data.isin([-1] + self.rel_data[-1]).any(axis=1)]
        else:
            test_data = test_data[~test_data.isin([-1]).any(axis=1)]

        # c.	Create group
        for item in self.fields_to_test.items():
            internal_dict = item[1]
            var_temp = item[0]
            test_data[var_temp] = test_data[var_temp].apply(lambda x: internal_dict[x] if x in internal_dict else x)

        # d.	Cross tub
        contingency_table = pd.crosstab(test_data[self.col_1], test_data[self.col_2], normalize='columns') * 100

        # e.	test
        chi2, p, _, _ = stats.chi2_contingency(contingency_table)

        # Print the chi-square test statistic and p-value
        print(contingency_table)
        print('Chi-square test statistic:', chi2)
        print('p-value:', p)
        self.constant_data.res[self.col_1] = p
        self.constant_data.contingency_table[self.col_1] = contingency_table

    def __apply_spearmanr(self, more_cols_to_delete):

        # The data to read (the Excel name- @col_1 and sheet name  - @col_2)
        df = pd.read_excel(f'{self.constant_data.data_path}/{self.col_1}.xlsx', sheet_name=self.col_2)
        # Obtain only the contingency_table from the file, update the cols names and remove irrelevant data
        new_df = df.iloc[df.loc[df[df.columns[0]].isnull()].index.item() + 1:]
        new_df.columns = new_df.iloc[0]
        cols_to_delete_temp = [col for col in self.constant_data.rows_to_delete if col in new_df.columns] + \
                              more_cols_to_delete
        new_df = new_df.reset_index(drop=True).drop(0).drop(columns=cols_to_delete_temp).set_index(self.col_1)
        # extract the required data for test
        data_1 = np.array(range(len(new_df.columns)))
        # the data to test which can be based on one or more fields (if it is list is more than one)
        if isinstance(self.fields_to_test, list):
            data_2 = np.array(new_df.loc[self.fields_to_test].sum())
        else:
            data_2 = np.array(new_df.loc[self.fields_to_test])
        # Perform the Cochran-Armitage test of trend
        result = stats.spearmanr(data_1, data_2)

        # Extract the test statistic and p-value
        test_statistic = result.correlation
        p_value = result.pvalue

        # Print the test statistic and p-value
        print("Test Statistic:", test_statistic)
        print("p-value:", p_value)
# endregion
