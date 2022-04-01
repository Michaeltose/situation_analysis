from itertools import groupby
from numpy import append
import pandas as pd
from preprocess import KEY, COUNTRY, BREADTH, IMPORTANT, APPLICANT, YEAR, HIGH_CITED
from tqdm import tqdm

def annual_trends(data:pd.DataFrame, by_column:str, year = YEAR):
    '''
    按照特定字段分析年度趋势
    '''
    annual_trends = {
        'by':[],
        '总量':[]
    }
    years = sorted([x for x in set(data[year])])
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
    times = list(set([x.year for x in data[year]]))
    years = sorted(times)
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

def importance_analysis(data:pd.DataFrame, by_column:str):
    importance_result = {
        'by':[],
        '专利总量':[],
        '专利布局广度':[],
        '重要专利数量':[]}
    for by, df_i in data.groupby(by_column):
        importance_result['by'].append(by)
        importance_result['专利总量'].append(len(set(df_i[KEY])))
        importance_result['专利布局广度'].append(sum(df_i[BREADTH]) / importance_result['专利总量'][-1])
        importance_result['重要专利数量'].append(len(set(df_i.loc[df_i[IMPORTANT] == 1, :][KEY])))
    
    importance_result['by'].append('全部')
    importance_result['专利总量'].append(len(set(data[KEY])))
    importance_result['专利布局广度'].append(sum(data[BREADTH]) / importance_result['专利总量'][-1])
    importance_result['重要专利数量'].append(len(data.loc[data[IMPORTANT] == 1, :][KEY]))

    importance_result = pd.DataFrame(importance_result)
    importance_result['专利总量占全球比例'] = importance_result['专利总量'] / int(importance_result.loc[importance_result['by'] == '全部', '专利总量'])
    importance_result['重要专利数量全球占比'] = importance_result['重要专利数量'] / int(importance_result.loc[importance_result['by'] == '全部', '重要专利数量'])
    importance_result['重要专利占自身比例'] = importance_result['重要专利数量'] / importance_result['专利总量']

    return importance_result

def high_cited_analysis(data:pd.DataFrame, by_column:str):
    high_cited_result = {
        'by':[],
        '发文数量':[],
        '高被引论文数量':[]}
    
    for by, df_i in data.groupby(by_column):
        high_cited_result['by'].append(by)
        high_cited_result['发文数量'].append(len(set(df_i[KEY])))
        high_cited_num = len(set(df_i.loc[df_i[HIGH_CITED] == 1, :][KEY]))
        high_cited_result['高被引论文数量'].append(high_cited_num)
    
    high_cited_result['by'].append('全部')
    global_paper_num = len(set(data[KEY]))
    high_cited_result['发文数量'].append(global_paper_num)
    high_cited_num_global = len(set(data.loc[data[HIGH_CITED] == 1, :][KEY]))
    high_cited_result['高被引论文数量'].append(high_cited_num_global)

    high_cited_result = pd.DataFrame(high_cited_result)
    high_cited_result['高被引占自身总量比例'] = high_cited_result['高被引论文数量'] / high_cited_result['发文数量']
    high_cited_result['发文数量占全球总量比例'] = high_cited_result['发文数量'] / global_paper_num
    high_cited_result['高被引占全球高被引总量比例'] = high_cited_result['高被引论文数量'] / high_cited_num_global
    return high_cited_result
