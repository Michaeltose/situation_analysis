from preprocess import get_high_cited_paper, get_international_cooperate, get_if_q1
from analysis import wos_paper_analysis, annual_trends
from argparse import ArgumentParser
from config import RESULT_FOLDER
from pathlib2 import Path

import pandas as pd
from utils.utils import load_q1_journals


if __name__ == '__main__':
    '''
    改动1：按照国家和机构两列分析，无需再单独指定分析列
    改动2：增加国家年度发文趋势
    '''
    parser = ArgumentParser()
    parser.add_argument('-l', '--loc', help = '文件路径', type = str, 
            default = '/Users/biomap/Documents/school works/situation_analysis/temp/WOS BP-土壤与肥料/WOS BP-土壤与肥料_C1_清洗后机构.xlsx')
    parser.add_argument('-f', '--field', help = '学科领域，用于筛选Q1期刊', type = str,
            default='土壤与肥料')
    parser.add_argument('-j', '--journal', help = 'Q1期刊表位置', type = str,
            default='/Users/biomap/Documents/school works/situation_analysis/utils/Q1.xlsx')

    args = parser.parse_args()
    loc = Path(args.loc)
    journal = args.journal
    field = args.field

    result_folder = Path(RESULT_FOLDER)
    job_name = loc.name.split('.')[0]
    print('任务名称:', job_name)
    # 创建任务保存地址
    job_dir_rslt = result_folder.joinpath(job_name)
    job_dir_rslt.mkdir(exist_ok = True)
    data = pd.read_excel(loc)
    print('计算中间结果')
    annual = annual_trends(data, by_column='国家')
    annual_inst = annual_trends(data, by_column='机构')
    data = get_international_cooperate(get_high_cited_paper(data))

    q1_list = load_q1_journals(journal, field)

    data = get_if_q1(data, q1_list)
    print('生成最终结果')
    column = '国家'
    result_nation = wos_paper_analysis(data, column)
    file_nation = job_dir_rslt.joinpath(job_name + '_' + column + '_分析结果.xlsx')
    column = '机构'
    result_inst = wos_paper_analysis(data, column)
    file_inst = job_dir_rslt.joinpath(job_name + '_' + column + '_分析结果.xlsx')

    
    result_nation.to_excel(file_nation, index = False)
    result_inst.to_excel(file_inst, index = False)
    annual.to_excel(job_dir_rslt.joinpath(job_name + '_国家发文量.xlsx'), index = False)
    annual_inst.to_excel(job_dir_rslt.joinpath(job_name + '_机构发文量.xlsx'), index = False)

    




    