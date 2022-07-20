import pandas as pd
import os

from pandas import DataFrame

os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


def count_per_city_question_var(my_data: DataFrame, old_name: str, new_name: str, is_state: str):
    # For total work with all data anf for state work only with the state in @is_state
    # Select only the data of question name (@old_name)
    if is_state == 'total':
        rel_demographic = DataFrame(my_data[old_name])
    else:
        rel_demographic = DataFrame(my_data[my_data['state'] == is_state][old_name])
    # Rearrange data
    rel_demographic = rel_demographic.rename(columns={old_name: new_name})
    rel_demographic = rel_demographic[rel_demographic[new_name].notna()].reset_index()

    # calculate absolute and relative count for each veritable
    sum_temp = rel_demographic.groupby(by=[new_name]).count().rename(columns={'index': is_state})
    sum_temp['per_' + is_state] = sum_temp[is_state] / sum_temp[is_state].sum() * 100
    return sum_temp


if __name__ == '__main__':
    data_states = pd.read_csv(r'data/process/res_stolen_loc_splitted.csv', skiprows=[1, 2]).reset_index()
    data_general = pd.read_csv(r'data/process/new_data.csv', skiprows=[1, 2]).reset_index()
    cols = ['total', 'California', 'Alberta', 'Washington']
    colors = ['red', 'blue', 'green', 'orange']
    data_store = []
    # The coe run over each tuple
    for pair in {'Q34': 'year', 'Q35': 'gender', 'Q36': 'income', 'Q38': 'education', 'Q39': 'ethnic_origin'}.items():
        print(pair)
        # Get the question number and its new name
        question_number = pair[0]
        per_name = pair[1]
        # count stat per state/total and question_number
        sumy = count_per_city_question_var(data_general, old_name=question_number, new_name=per_name, is_state=cols[0])
        for name in cols[1:]:
            sumy[[name, 'per_' + name]] = count_per_city_question_var(data_states, old_name=question_number,
                                                                      new_name=per_name,
                                                                      is_state=name)
        if per_name == 'year':
            sumy.index = sumy.index.astype('int32')
        # add the new data to datastore which will present latter
        data_store.append(sumy.fillna(0).astype('int32'))
# stolen_bikes_place to City/State/Country
# df = pd.read_csv(r'data/process/res_with_geo_loc.csv')
# df[['city', 'state', 'country']] = df['stolen_bikes_place'].str.split(',', expand=True)
# df.to_csv(r'data/process/res_stolen_loc_splitted.csv')
