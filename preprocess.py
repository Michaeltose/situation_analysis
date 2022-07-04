from typing import List
import pandas as pd
from pathlib2 import Path
from tqdm import tqdm
import re


'''
年份：公开日
国别：申请人国别代码
广度：同族国家
重要专利：合享价值度=10
机构：申请人
'''
YEAR_SOURCE = '公开（公告）日'
YEAR = 'year'
PATENT_TYPE = '专利类型'
COUNTRY = '申请人国别代码'
KEY = 'key'
KEY_PATENT = '公开（公告）号'
IMPORTANT = '合享价值度10'
BREADTH = '广度'
TITLE = '标题'
ABS = '摘要'
APPLICANT = '申请人'
IMPORTANCE_VALUE = '合享价值度'
GROUP_NATION= '同族国家/地区'
COUNTRY_PAPER = '国家'
INSTI_PAPER = '机构'
INSTI_PAPER_II = '二级机构'
YEAR_PAPER = 'PY'
CITED_NUM = 'Z9'
HIGH_CITED = '高被引'
INTER_COOP = '国际合作'
Q1 = 'Q1期刊论文'
SOURCE = 'SOURCE'
TYPE = 'TYPE'
NEW_COLUMN_NAME = {
    'article.title':'TI',
    'source.title':'SOURCE',
    'article.type':'TYPE',
    'article.pub-year':'PY',
    'article.abstract':'AB',
    'corresponding.inst.address':'RP',
    'institution.inst.address':'C1',
    'reference.title count':'Z9',
    'article.uid':'UT'
}

UK = {'england', 'scotland', 'north ireland', 'wales'}
COLUMNS = ['TI', 'AB', 'Z9', YEAR_PAPER, TYPE, SOURCE,'C1', 'RP', KEY]
NEW_ADD = [COUNTRY_PAPER, INSTI_PAPER, INSTI_PAPER_II]

def load_data_wos(loc, key, sep = '\t'):
    '''
    读取WOS导出数据集
    从loc中读取文件
    loc有三种情况：路径、csv、xlsx
    '''
    loc = Path(loc)
    if loc.is_dir():
        data = pd.DataFrame()
        for f in tqdm(loc.iterdir()):
            print(f)
            suffix = f.suffix
            if suffix == '.xls' or suffix == '.xlsx':
                data = data.append(pd.read_excel(f))
            elif suffix == '.csv':
                data = data.append(pd.read_csv(str(f), sep = sep, index_col= False, encoding='utf-8'))
            else:
                continue
    else:
        suffix = loc.suffix
        if suffix == '.xls' or suffix == '.xlsx':
            data = pd.read_excel(loc)
        else:
            data = pd.read_csv(loc, sep = sep)
    data.rename(columns = NEW_COLUMN_NAME, inplace=True)
    data[KEY] = data[key]
    # 处理数字类型的空值
    data[CITED_NUM] = data[CITED_NUM].fillna(0)
    data[YEAR_PAPER] = data[YEAR_PAPER].fillna(0)
    data.fillna('None', inplace=True)
    return data

def load_data_patent(loc, key, sep = ','):
    loc= Path(loc)
    if loc.is_dir():
        data = pd.DataFrame()
        for f in tqdm(loc.iterdir()):
            suffix = f.suffix
            if suffix == '.xls' or suffix == '.xlsx':
                data = data.append(pd.read_excel(f))
            elif suffix == '.csv':
                data = data.append(pd.read_csv(str(f), sep = sep, index_col= False, encoding='utf-8'))
            else:
                continue
    else:
        suffix = loc.suffix
        if suffix == '.xls' or suffix == '.xlsx':
            data = pd.read_excel(loc)
        else:
            data = pd.read_csv(loc, sep = sep)
    data.fillna(0, inplace=True)
    data[KEY] = data[key]
    return data

def clean_field(data: pd.DataFrame, column_name: str, mapping_data: str):
    '''
    根据给定的名称映射表清洗任意字段
    :param data:    要清洗的数据，类型为dataframe
    :column_name:   要清洗的列名，str
    :mapping_data:  名称映射表地址
    '''
    print('清洗%s字段'%column_name)
    mapping_data = pd.read_excel(mapping_data)
    mapping_dict = {}
    if ('origin_name' not in mapping_data.columns) or ('standard_name' not in mapping_data.columns):
        print('WARNING！！！名称映射表格式不正确，名称映射表必须包含origin_name、standard_name两列')
        return
    # 根据mapping_data创建mapping_dict
    # key为小写的origin_name
    for i,r in mapping_data.iterrows():
        if pd.isna(r['standard_name']):
            continue
        origin = r['origin_name']
        origin_lower = origin.lower()
        if origin_lower not in mapping_dict:
            # origin_lower不在mapping_dict内
            mapping_dict[origin_lower] = r['standard_name']
        else:
            # origin_lower已经存在
            if mapping_dict[origin_lower] == r['standard_name']:
                # 判断standard_name是否一致
                continue
            else:
                print('origin_name: “%s”存在不同standard_name: "%s"和"%s"，后续请修改名称映射表'%(origin, mapping_dict[origin_lower], r['standard_name']))
    data[column_name + '_Cleaned'] = [0] * len(data)
    for i,r in tqdm(data.iterrows()):
        name = r[column_name].lower()
        if name in mapping_dict:
            data.loc[i, column_name] = mapping_dict[name]
            data.loc[i, column_name + '_Cleaned'] = 1
    print('清洗成功')
    return data

def split_cooperate(data, column_name, sep = ', '):
    '''
    处理两国、两机构合作数据
    拆分后增加新行
    '''
    mask = data[column_name].str.contains(sep)  # 获取有合作行为的行
    individuals = {}
    no_coop = data.loc[mask == False, :]
    coop = data.loc[mask == True, :]
    print('不需分割', len(no_coop), '需要分割', len(coop))
    if '\\' in sep:
        sep = sep.replace('\\', '')
        print(sep)
    for names in set(coop[column_name]):
        individuals[names] = []
        for name in names.split(sep):
            name = name.strip() # 删除前后空白字符
            if name.lower() in individuals[names]:
                continue
            else:
                individuals[names].append(name)
    
    new_coop = pd.DataFrame()
    for names, name_list in tqdm(individuals.items()):
        df = coop.loc[coop[column_name] == names, :]
        df = df.copy(deep = True)
        for name in name_list:
            df.loc[:, column_name] = name
            new_coop = new_coop.append(df)
    # 合并有合作关系和无合作关系的数据
    data = no_coop.append(new_coop)
    data.reset_index(drop = True, inplace = True)
    return data

def extract_country_institution_wos_paper(data:pd.DataFrame, column = 'RP'):
    '''
    从通讯作者字段获取wos论文数据的国家和机构，新增“国家”列、“机构”列、“二级机构”列
    column只能为C1或RP
    '''
    print('提取机构国家，依据列：',column)
    data.reset_index(drop = True, inplace = True)
    data[COUNTRY_PAPER] = 'None'
    data[INSTI_PAPER] = 'None'
    data[INSTI_PAPER_II] = 'None'
    for i,r in tqdm(data.iterrows()):
        country = 'None'
        institution = 'None'
        institution2 = 'None'
        info = r[column]
        if info != 'None':
            if column == 'RP':
                try:
                    if '，' in info:
                        info = info.split('，')[1].split(',')
                    else:
                        info = info.split('), ')[1].split(',')
                    if len(info) >= 3:
                        country = info[-1].strip().lower()
                        institution = info[0].strip()
                        institution2 = info[1].strip()
                    elif len(info) == 2:
                        country = info[-1].strip().lower()
                        institution = info[0].strip()
                except Exception:
                    pass
            else:
                try:
                    if ']' in info:
                        info = info.split(']')[1].split(',')
                        country = info[-1].strip().lower()
                        institution = info[0].strip()
                        institution2 = info[1].strip()
                    else:
                        info = info.split(',')
                        country = info[-1].strip().lower()
                        institution = info[0].strip()
                        institution2 = info[1].strip()
                except Exception:
                    pass
            if country == 'ussr':
                country = 'russia'
            if 'usa' in country:
                country = 'usa'
            if country in UK:
                country = 'uk'
        data.loc[i, COUNTRY_PAPER] = country
        data.loc[i, INSTI_PAPER] = institution
        data.loc[i, INSTI_PAPER_II] = institution2
    return data

def get_processed_data_patent(data:pd.DataFrame):
    '''
    获取年份、是否重要专利、专利广度
    '''
    data[IMPORTANT] = ''
    data[BREADTH] = ''
    for i,r in tqdm(data.iterrows()):
        if_important = 0
        if r[IMPORTANCE_VALUE] == 10:
            if_important = 1
        breadth_value = len(r[GROUP_NATION].split(','))
        data.loc[i, BREADTH] = breadth_value
        data.loc[i, IMPORTANT] = if_important
    
    return data

def get_high_cited_paper(data:pd.DataFrame, column_name = CITED_NUM, rate = 0.1):
    '''
    按照设定比例筛选高被引论文
    '''
    print('查找高被引论文')
    cited_nums = sorted(data[CITED_NUM], reverse= True)
    index = int(len(cited_nums) * rate)
    threshold = cited_nums[index]
    data[HIGH_CITED] = 0
    data.loc[data[CITED_NUM] >= threshold, HIGH_CITED] = 1
    return data

def get_international_cooperate(data:pd.DataFrame):
    '''
    查看同一个key下的论文是否有多个国籍
    '''
    print('查找国际合作论文')
    data[INTER_COOP] = 0
    # keys = set(data[KEY])
    # for k in keys:
    #     idx = data[KEY] == k
    #     country_num = len(set(data.loc[idx, COUNTRY_PAPER]))
    #     if country_num > 1:
    #         # 多国合作，标记1
    #         data.loc[idx, INTER_COOP] = 1
    for k, frame in tqdm(data.groupby(KEY)):
        country_num = len(set(frame[COUNTRY_PAPER]))
        if country_num > 1:
            data.loc[data[KEY] == k, INTER_COOP] = 1
    return data

def get_if_q1(data:pd.DataFrame, name_list:List[str]):
    '''
    查看当前行是否q1期刊论文
    :param  name_list:  q1期刊论文列表
    '''
    print('查找Q1论文')
    data[Q1] = [0] * len(data)
    for name in tqdm(name_list):
        data.loc[data[SOURCE] == name, Q1] = 1
    return data

if __name__ == '__main__':
    data = pd.read_excel('/Users/biomap/Desktop/test.xlsx')
    # data = get_international_cooperate(data)
    # data = data.loc[:,['TI', '国家', '国际合作', 'C1', 'key']]
    # data.to_excel('/Users/biomap/Documents/school wroks/situation_analysis/temp/WOS BP-土壤与肥料/WOS BP-土壤与肥料_C1_国际合作.xlsx', index=False)
    data = extract_country_institution_wos_paper(data, column = 'C1')
    data.to_excel('/Users/biomap/Desktop/test2.xlsx')



