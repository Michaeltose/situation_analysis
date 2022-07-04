from operator import index
import pandas as pd
from pathlib2 import Path

def to_lower(data):
    '''
    transform the thesaurus to lower type
    '''
    lower_table = {
        'origin_name':[],
        'standard_name':[]
    }
    key = set()
    for i,r in data.iterrows():
        on = r['origin_name'].lower()
        sn = r['standard_name']
        if on not in key:
            key.add(on)
            lower_table['origin_name'].append(on)
            lower_table['standard_name'].append(sn)
    return pd.DataFrame(lower_table)

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

def flit_old_words(new_words, fine_words):
    '''
    remove recorded words in new_words
    :params new_words: new thesaurus table
    '''
    new_words['recorded'] = [0] * len(new_words)
    recorded = set([x.lower() for x in fine_words['origin_name']])
    for i,r in new_words.iterrows():
        if r['origin_name'].lower() in recorded:
            new_words.loc[i, 'recorded'] = 1
    return new_words

def merge_thesaurus(new_words, fine_words):
    '''
    merge two thesaurus, add new words to fine_words
    :params new_data:   包含新叙词的数据
    :params fine_data:  正在使用的叙词表
    :return new_data:   合并后的叙词表
    '''
    new_words = new_words.loc[pd.isna(new_words['standard_name']) == False, ['origin_name', 'standard_name']]
    old_words = set(fine_words['origin_name'])
    add = {'origin_name':[], 'standard_name':[]}
    for i,r in new_words.iterrows():
        if r['origin_name'] not in old_words:
            add['origin_name'].append(r['origin_name'])
            add['standard_name'].append(r['standard_name'])
            old_words.add(r['origin_name'])
    add = pd.DataFrame(add)
    return fine_words.append(add)


    

if __name__ == '__main__':
    data = pd.read_excel('/Users/biomap/Documents/school wroks/situation_analysis/data/0616/农经2_机构_过滤.xlsx')
    fine = pd.read_excel('/Users/biomap/Documents/school wroks/situation_analysis/utils/机构叙词表-论文.xlsx')

    new = merge_thesaurus(data, fine)
    new.to_excel('/Users/biomap/Documents/school wroks/situation_analysis/data/0616/增加农经机构.xlsx')
    