from operator import index
import pandas as pd
from pathlib2 import Path

def split_thesaurus(data):
    '''
    按照原始名与标准名是否一一对应将原叙词表拆分成两部分
    '''
    dup_data = pd.DataFrame()
    fine_data = pd.DataFrame()
    for name, df in data.groupby('origin_name'):
        df = df.loc[pd.isna(df['standard_name']) == False,:]
        if len(df) >= 2:
            dup_data = dup_data.append(df)
        else:
            fine_data = fine_data.append(df)
    if len(dup_data) == 0:
        return fine_data.loc[:, ['origin_name', 'standard_name']], dup_data
    else:
        return fine_data.loc[:, ['origin_name', 'standard_name']], dup_data.loc[:,['origin_name', 'standard_name']]

def refine_thesaurus_data(data_loc):
    '''
    将叙词表拆分成无冲突的和有冲突的两部分，保存在原文件夹下
    '''
    data_loc = Path(data_loc)
    folder = data_loc.parent
    name = str(data_loc.name).split('.')[0]
    refine_loc = folder.joinpath(name + '_refine.xlsx')
    dup_loc = folder.joinpath(name + 'duplicate.xlsx')
    fine_data, dup_data = split_thesaurus(pd.read_excel(data_loc))
    if len(dup_data) != 0:
        dup_data.to_excel(dup_loc, index=False)
    fine_data.to_excel(refine_loc, index=False)

if __name__ == '__main__':
    refine_thesaurus_data('/Users/biomap/Documents/school wroks/situation_analysis/utils/中科院各所叙词表-论文.xlsx')
