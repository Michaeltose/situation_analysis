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
YEAR = '公开年'
COUNTRY = '申请人国别代码'
KEY = 'key'
IMPORTANT = '是否重要专利'
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

UK = {'england', 'scotland', 'north ireland', 'wales'}


def load_data(loc, key, sep = ','):
    '''
    loc为文件地址，允许的文件类型为excel或者分隔符文件
    key为唯一标识一行数据的字段名
    '''
    type = loc.split('.')[-1]
    if type == 'xls' or type == 'xlsx':
        data = pd.read_excel(loc)
    else:
        try:
            data = pd.read_csv(loc, sep=sep)
        except Exception:
            print('ERROR：文件读取失败。 请确认文件类型或文件分割符是否正确')
            return
    data[KEY] = data[key]
    data.fillna('None', inplace=True)
    return data

def load_and_merge(folder, key, sep = '\t'):
    '''
    合并WOS导出数据集
    从folder中读取文件并合并成一个大DataFrame
    '''
    folder = Path(folder)
    data = pd.DataFrame()
    for loc in tqdm(folder.iterdir()):
        data = data.append(pd.read_csv(str(loc), sep = sep, index_col= False))
    data[KEY] = data[key]
    # 处理数字类型的空值
    data[CITED_NUM] = data[CITED_NUM].fillna(0)
    data[YEAR_PAPER] = data[YEAR_PAPER].fillna(0)
    data.fillna('None', inplace=True)
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
    if ('原始名' not in mapping_data.columns) or ('标准名' not in mapping_data.columns):
        print('WARNING！！！名称映射表格式不正确，名称映射表必须包含原始名、标准名两列')
        return
    for i,r in mapping_data.iterrows():
        if pd.isna(r['标准名']):
            continue
        origin = r['原始名']
        origin_lower = origin.lower()
        if origin_lower not in mapping_dict:
            mapping_dict[origin_lower] = r['标准名'].lower()
        else:
            if mapping_dict[origin_lower] == r['标准名'].lower():
                continue
            else:
                print('原始名“%s”存在不同标准名"%s"和"%s"，后续请修改名称映射表'%(origin_lower, mapping_dict[origin_lower], r['标准名'].lower()))
    for i,r in data.iterrows():
        name = r[column_name].lower()
        if name in mapping_dict:
            data.loc[i, column_name] = mapping_dict[name]
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
    print(len(no_coop), len(coop))

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
    return data

def extract_country_institution_wos_paper(data:pd.DataFrame):
    '''
    从通讯作者字段获取wos论文数据的国家和机构，新增“国家”列、“机构”列、“二级机构”列
    '''
    data[COUNTRY_PAPER] = 'None'
    data[INSTI_PAPER] = 'None'
    data[INSTI_PAPER_II] = 'None'
    for i,r in tqdm(data.iterrows()):
        country = 'None'
        institution = 'None'
        institution2 = 'None'
        info = r['RP']
        if info != 'None':
            try:
                info = info.split('，')[1].split(',')
                if len(info) >= 3:
                    country = info[-1][0:-1].strip().lower()
                    institution = info[0].strip().lower()
                    institution2 = info[1].strip().lower()
                elif len(info) == 2:
                    country = info[-1][0:-1].strip().lower()
                    institution = info[0].strip().lower()
                if country == 'ussr':
                    country = 'russia'
                if 'usa' in country:
                    country = 'usa'
                if country in UK:
                    country = 'uk'
            except Exception:
                pass
        data.loc[i, COUNTRY_PAPER] = country
        data.loc[i, INSTI_PAPER] = institution
        data.loc[i, INSTI_PAPER_II] = institution2
    return data

def get_processed_data_patent(data:pd.DataFrame):
    '''
    获取年份、是否重要专利、专利广度
    '''
    data[YEAR] = ''
    data[IMPORTANT] = ''
    data[BREADTH] = ''
    for i,r in data.iterrows():
        year = str(r[YEAR_SOURCE]).split('-')[0]
        if_important = 0
        if r[IMPORTANCE_VALUE] == 10:
            if_important = 1
        breadth_value = len(r[GROUP_NATION].split(', '))
        data.loc[i, YEAR] = year
        data.loc[i, BREADTH] = breadth_value
        data.loc[i, IMPORTANT] = if_important
    
    return data

def get_high_cited_paper(data:pd.DataFrame, column_name = CITED_NUM, rate = 0.1):
    '''
    按照设定比例筛选高被引论文
    '''
    cited_nums = sorted(data[CITED_NUM], reverse= True)
    index = int(len(cited_nums) * rate)
    threshold = cited_nums[index]
    data[HIGH_CITED] = 0
    data.loc[data[CITED_NUM] >= threshold, HIGH_CITED] = 1
    return data
        
        



