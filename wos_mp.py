import multiprocessing
from argparse import ArgumentParser

import pandas as pd
from pathlib2 import Path
from tqdm import tqdm

from config import RESULT_FOLDER, TEMP_FOLDER, SEPTOR
from preprocess import (COLUMNS, NEW_ADD, TYPE, clean_field,extract_country_institution_wos_paper,
                        load_data_wos, split_cooperate, clean_field)
from utils.utils import cut_data
import gc
import os

if __name__ == '__main__':
    pwd = Path(str(os.getcwd()))
    parser = ArgumentParser()
    # parser.add_argument('-h', help = '帮助信息')
    parser.add_argument('-l', '--loc', help = '文件路径', type = str, default = 'd:/WOS BP-土壤与肥料.csv')
    parser.add_argument('-c', '--column', help = '待处理的列', type = str, default = 'C1')
    parser.add_argument('-n', '--num', help = '进程数量', type = int, default = 16)
    parser.add_argument('-m', '--mapping', help = '叙词表地址', type= str, default= 'd:/机构叙词表-新.xlsx')
    args = parser.parse_args()

    loc = Path(args.loc)
    column = args.column
    num = args.num
    mapping = args.mapping
    sep = SEPTOR[column]

    data = load_data_wos(loc, key = 'UT', sep = ',')
    print('筛选文献类型', len(data))
    data[TYPE] = data[TYPE].astype('str')
    data = data.loc[data[TYPE].str.contains('Article'),COLUMNS]
    gc.collect()

    print('筛选后', len(data))

    print(data.columns)
    loc = Path(loc)
    temp_folder = pwd.joinpath(TEMP_FOLDER)
    job_name = loc.name.split('.')[0]
    print('任务名称:', job_name)
    # 创建任务保存地址
    job_dir_tmp = temp_folder.joinpath(job_name)
    job_dir_tmp.mkdir(exist_ok = True, parents=True)
    
    # 开始处理

    print('处理前数据量', len(data))
    # multi_process institution extraction
    pool = multiprocessing.Pool(processes = num)
    data_list = cut_data(data, num) # 将数据分割成多分以供并行处理

    print('并行拆分有合作关系的数据')
    # 创建参数列表，以; [为分隔符拆分C1；以;为分隔符拆分RP
    split_param_list= [(data, column, sep) for data in data_list]
    
    r = []
    for args in split_param_list:
        r.append(pool.apply_async(split_cooperate,args))
    pool.close()
    pool.join()
    print('合并合作关系拆分结果')
    split_data = pd.DataFrame()
    for df in r:
        split_data = split_data.append(df.get())
    split_data.reset_index(drop = True, inplace = True)
    print('拆分合作后数据量', len(split_data))
    file = job_dir_tmp.joinpath(job_name + '_' + column + '_拆分合作.xlsx')
    split_data.loc[:, COLUMNS].to_excel(file, index=False)


    # 提取机构国家
    split_data_list = cut_data(split_data, num)
    gc.collect()
    print('并行提取机构国家')
    extract_param_list = [(data, column) for data in split_data_list]
    pool = multiprocessing.Pool(processes = num)
    r = []
    for args in extract_param_list:
        r.append(pool.apply_async(extract_country_institution_wos_paper,args))
    pool.close()
    pool.join()

    print('合并机构抽取结果')
    data_inst = pd.DataFrame()
    for df in r:
        data_inst = data_inst.append(df.get())
    # data_inst = extract_country_institution_wos_paper(split_data, column = column)
    print('提取机构后数据量', len(data_inst))
    file = job_dir_tmp.joinpath(job_name + '_' + column + '_机构国家.xlsx')
    data_inst = data_inst.loc[:, COLUMNS + NEW_ADD]
    data_inst.to_excel(file, index=False)

    # 进行机构清洗
    print('清洗机构')
    #data_inst = pd.read_excel('/Users/biomap/Documents/school wroks/situation_analysis/temp/WOS BP-土壤与肥料/WOS BP-土壤与肥料_C1_机构国家.xlsx')
    
    data_inst = clean_field(data_inst, column_name= '机构', mapping_data=mapping)
    file = job_dir_tmp.joinpath(job_name + '_' + column + '_清洗后机构.xlsx')
    data_inst.to_excel(file, index=False)

