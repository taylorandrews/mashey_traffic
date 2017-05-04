import numpy as np
import pandas as pd
import glob

def concat_years(col_lists):
    '''
    INPUT
        - dictionary with key = category: value = list of column headers to keep

    OUTPUT
        - dictionary with key = category: value = dataframe of all combined data from the different year files.

    Creates full dataframe for each category of data
    '''

    dfs = {}
    for direc in glob.iglob('../data/*'):
        loc = direc[8:]
        new_df = pd.DataFrame(columns=col_lists[loc])
        for f in glob.iglob(direc + '/*.dta'):
            df = pd.read_stata(f)
            df = df[col_lists[loc]]
            df['year'] = int(f[len(f)-8:][:4])
            df['primary_key'] = (df['year'].astype(str) + df['st_case'].astype(str)).astype(int)
            # if loc == 'accident':
            #     df.set_index
            new_df = pd.concat([new_df, df])
        dfs[loc] = new_df
    dfs['accident'].set_index('primary_key', inplace=True)
    return dfs

def explore_cols():
    '''
    INPUT
        none

    OUTPUT
        - dictionary with key = category of data (accident, preson or vehicle) as a string and value = list of column headers that appear across all year files in that category

    Creates lists of column names that appear in every different years file for each type of file (accident, person and vehicle) and puts them into a dictionary
    '''

    acc_cols, per_cols, veh_cols = {}, {}, {}
    set_up = {'accident': acc_cols, 'person': per_cols, 'vehicle': veh_cols}
    col_lists = {}
    for category in set_up:
        for f in glob.iglob('../data/' + category + '/*.dta'):
            df = pd.read_stata(f)
            for col in df:
                if col in set_up[category]:
                    set_up[category][col] += 1
                else:
                    set_up[category][col] = 1
            # attempted dict comp - come back if time
            # {set_up[category][col] : (set_up[category][col] += 1 if col in set_up[category] else set_up[category][col] = 1) for col in df}
        cols_keep = [k for k in set_up[category] if set_up[category][k] == max(set_up[category].values())]
        col_lists[category] = cols_keep
    return col_lists

if __name__ == '__main__':
    col_lists = explore_cols()
    dfs = concat_years(col_lists)
    for item in dfs:
        dfs[item].to_csv('../data/' + item + '.csv')
