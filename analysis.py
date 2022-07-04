from itertools import groupby

import pandas as pd
from tqdm import tqdm

from preprocess import (BREADTH, CITED_NUM, HIGH_CITED, IMPORTANT,
                        PATENT_TYPE, INTER_COOP, KEY, Q1, YEAR, YEAR_PAPER, get_processed_data_patent)


def annual_trends(data:pd.DataFrame, by_column:str, year = YEAR_PAPER):
    '''
    按照特定字段分析年度趋势
    '''
    annual_trends = {
        'by':[],
        '总量':[]
    }
    years = []
    for y in set(data[year]):
        try:
            y = int(y)
            years.append(y)
        except Exception:
            continue
    years = sorted(years)
    annual_trends['by'].append('全部')
    annual_trends['总量'].append(len(set(data[KEY])))
    for y in years:
        annual_trends[str(y)] = []
        annual_trends[str(y)].append(len(set(data.loc[data[year] == y, :][KEY])))

    for by, df_i in tqdm(data.groupby(by_column)):
        year_counts = {}
        for y in years:
            df_iy = df_i.loc[df_i[year] == y, :]
            if len(df_iy) != 0:
                year_counts[str(y)] = len(set(df_iy[KEY]))
            else:
                year_counts[str(y)] = 0
        annual_trends['by'].append(by)
        for y,c in year_counts.items():
            annual_trends[y].append(c)
        total = len(set(df_i[KEY]))
        annual_trends['总量'].append(total)
    annual_trends = pd.DataFrame(annual_trends)
    annual_trends.fillna(0, inplace=True)
    return annual_trends

def annual_trends_patent(data:pd.DataFrame, by_column:str, year = '公开（公告）日'):
    '''
    按照特定字段分析年度趋势
    '''
    annual_trends = {
        'by':[],
        '总量':[]
    }
    times = []
    data[YEAR] = 0
    for i,r in data.iterrows():
        try:
            y = r[year].year
        except Exception:
            y = 0
        times.append(y)
        data.loc[i, YEAR] = y
    times = list(set(times))
    years = sorted(times)
    annual_trends['by'].append('全部')
    annual_trends['总量'].append(len(set(data[KEY])))
    for y in years:
        annual_trends[str(y)] = []
        annual_trends[str(y)].append(len(set(data.loc[data[YEAR] == y, :][KEY])))

    for by, df_i in tqdm(data.groupby(by_column)):
        year_counts = {}
        for y in years:
            df_iy = df_i.loc[df_i[YEAR] == y, :]
            if len(df_iy) != 0:
                year_counts[str(y)] = len(set(df_iy[KEY]))
            else:
                year_counts[str(y)] = 0
        annual_trends['by'].append(by)
        for y,c in year_counts.items():
            annual_trends[y].append(c)
        total = len(set(df_i[KEY]))
        annual_trends['总量'].append(total)
    annual_trends = pd.DataFrame(annual_trends)
    annual_trends.fillna(0, inplace=True)
    return annual_trends

def importance_analysis(data:pd.DataFrame, by_column:str):
    data = get_processed_data_patent(data)
    importance_result = {
        'by':[],
        '专利总量':[],
        '发明授权专利量':[],
        '专利布局广度':[],
        '合享值10的专利数量':[]}
    for by, df_i in tqdm(data.groupby(by_column)):
        importance_result['by'].append(by)
        importance_result['专利总量'].append(len(set(df_i[KEY])))
        importance_result['专利布局广度'].append(sum(df_i[BREADTH]) / importance_result['专利总量'][-1])
        importance_result['合享值10的专利数量'].append(len(set(df_i.loc[df_i[IMPORTANT] == 1, :][KEY])))
        importance_result['发明授权专利量'].append(len(set(df_i.loc[df_i[PATENT_TYPE] == '发明授权', :][KEY])))
    
    importance_result['by'].append('全部')
    patent_all = len(set(data[KEY]))
    importance_result['专利总量'].append(patent_all)
    importance_result['专利布局广度'].append(sum(data[BREADTH]) / importance_result['专利总量'][-1])
    importance_result['合享值10的专利数量'].append(len(data.loc[data[IMPORTANT] == 1, :][KEY]))
    invent_num = len(set(data.loc[data[PATENT_TYPE] == '发明授权', :][KEY]))
    importance_result['发明授权专利量'].append(invent_num)

    importance_result = pd.DataFrame(importance_result)
    importance_result['专利总量占全球比例'] = importance_result['专利总量'] / int(importance_result.loc[importance_result['by'] == '全部', '专利总量'])
    importance_result['合享值10的专利数量占全球发明授权专利比例'] = importance_result['合享值10的专利数量'] / invent_num
    importance_result['合享值10的专利数量占国家发明授权专利比例'] = importance_result['合享值10的专利数量'] / importance_result['发明授权专利量']
    importance_result['发明授权专利占全球比例'] = importance_result['发明授权专利量'] / patent_all
    importance_result['合享值10的专利数量占全球发明专利比例'] = importance_result['合享值10的专利数量'] / invent_num

    return importance_result

def wos_paper_analysis(data:pd.DataFrame, by_column:str):
    high_cited_result = {
        'by':[],
        '发文数量':[],
        '高被引论文数量':[],
        '总被引频次':[],
        '国际合作论文数量':[], # preprocess中生成国际合作标签
        'Q1期刊论文数量':[]}
    
    for by, df_i in data.groupby(by_column):
        # df_i为属于同一国家的所有论文
        high_cited_result['by'].append(by)
        high_cited_result['发文数量'].append(len(set(df_i[KEY])))
        high_cited_num = len(set(df_i.loc[df_i[HIGH_CITED] == 1, :][KEY]))   # 获取当前国家下标签为高被引的论文key数量
        high_cited_result['高被引论文数量'].append(high_cited_num)
        inter_coop_num = len(set(df_i.loc[df_i[INTER_COOP] == 1, :][KEY]))
        high_cited_result['国际合作论文数量'].append(inter_coop_num)
        q1_num = len(set(df_i.loc[df_i[Q1] == 1, :][KEY]))
        high_cited_result['Q1期刊论文数量'].append(q1_num)
        keys = set()
        cited_num_i = 0 # 当前国家的被引总数
        for i,r in df_i.iterrows():
            if r[KEY] not in keys:
                cited_num_i += r[CITED_NUM]
                keys.add(r[KEY])
        high_cited_result['总被引频次'].append(cited_num_i)
    keys = set()
    cited_num_all = 0   # 所有论文被引总数
    for i,r in data.iterrows():
        if r[KEY] not in keys:
            cited_num_all += r[CITED_NUM]
            keys.add(r[KEY])
    
    high_cited_result['by'].append('全部')
    global_paper_num = len(set(data[KEY]))  # 计算不同key的数量
    high_cited_num_global = len(set(data.loc[data[HIGH_CITED] == 1, :][KEY]))   # 高被引论文总数
    inter_coop_num_all = len(set(data.loc[data[INTER_COOP] == 1, :][KEY]))  # 国际合作论文总数
    q1_all = len(set(data.loc[data[Q1] == 1, :][KEY]))  # Q1期刊论文数

    high_cited_result['总被引频次'].append(cited_num_all)
    high_cited_result['发文数量'].append(global_paper_num)  # 论文总数
    high_cited_result['高被引论文数量'].append(high_cited_num_global)
    high_cited_result['国际合作论文数量'].append(inter_coop_num_all)
    high_cited_result['Q1期刊论文数量'].append(q1_all)

    high_cited_result = pd.DataFrame(high_cited_result)
    high_cited_result['高被引占本国比例'] = high_cited_result['高被引论文数量'] / high_cited_result['发文数量']
    high_cited_result['发文数量占全球总量比例'] = high_cited_result['发文数量'] / global_paper_num
    high_cited_result['高被引占全球高被引总量比例'] = high_cited_result['高被引论文数量'] / high_cited_num_global
    high_cited_result['篇均被引'] = high_cited_result['总被引频次']/ high_cited_result['发文数量']
    high_cited_result['国际合作论文占本国论文总量比例'] = high_cited_result['国际合作论文数量'] / high_cited_result['发文数量']
    high_cited_result['国际合作论文数量占国际合作论文总量比例'] = high_cited_result['国际合作论文数量'] / inter_coop_num_all
    return high_cited_result
