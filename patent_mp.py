from unittest import result
from xml.etree.ElementInclude import default_loader
import pandas as pd
from utils.utils import cut_data
from preprocess import clean_field, load_data_patent, get_processed_data_patent, split_cooperate
from preprocess import extract_country_institution_wos_paper, get_high_cited_paper
from preprocess import APPLICANT, COUNTRY, COUNTRY_PAPER, INSTI_PAPER, INSTI_PAPER_II, YEAR_PAPER, HIGH_CITED, CITED_NUM, YEAR, KEY, KEY_PATENT
from analysis import annual_trends_patent, importance_analysis, annual_trends
from pathlib2 import Path
from config import TEMP_FOLDER, RESULT_FOLDER
from utils.refine_thesaurus import to_lower
import multiprocessing
from tqdm import tqdm
from argparse import ArgumentParser

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-l', '--loc', help= '文件路径', type = str, default= '/Users/biomap/Documents/school wroks/220616任务/Incopat-兽医.xls')
    parser.add_argument('-c', '--column', help = '待处理的列', type = str, default = APPLICANT)
    parser.add_argument('-n', '--num', help = '进程数量', type = int, default = 8)
    parser.add_argument('-s', '--sep', help = '拆分合作关系的分隔符；国家为"; "机构为", "', type = str, default = ', ')
    args = parser.parse_args()

    loc = args.loc
    column = args.column
    num = args.num
    sep = args.sep

    data = load_data_patent(loc, key = KEY_PATENT, sep = sep)

    print(data.columns)
    temp_folder = Path(TEMP_FOLDER)
    result_folder = Path(RESULT_FOLDER)
    loc = Path(loc)
    job_name = loc.name.split('.')[0]
    print('任务名称:', job_name)
    # 创建任务保存地址
    job_dir_tmp = temp_folder.joinpath(job_name)
    job_dir_tmp.mkdir(exist_ok = True)
    job_dir_rslt = result_folder.joinpath(job_name)
    job_dir_rslt.mkdir(exist_ok = True)


    print('处理前数据量', len(data))
    # multi_process institution extraction
    pool = multiprocessing.Pool(processes = num)
    data_list = cut_data(data, num) # 将数据分割成多分以供并行处理

    split_param_list = [(data, column, sep) for data in data_list]
    r = []

    for arg in split_param_list:
        r.append(pool.apply_async(split_cooperate, arg))
    pool.close()
    pool.join()
    print('合并合作关系拆分结果')

    split_data = pd.DataFrame()
    for df in r:
        split_data = split_data.append(df.get())
    print('拆分合作后数据量', len(split_data))
    file = job_dir_tmp.joinpath(job_name + '_' + column + '_拆分合作.xlsx')
    split_data.to_excel(file, index=False)



    