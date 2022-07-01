from preprocess import get_high_cited_paper, get_international_cooperate, get_if_q1
from analysis import wos_paper_analysis
from argparse import ArgumentParser
from config import RESULT_FOLDER
from pathlib2 import Path

import pandas as pd
from utils.utils import load_q1_journals


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-l', '--loc', help = '文件路径', type = str, 
            default = '/Users/biomap/Documents/school wroks/situation_analysis/temp/WOS BP-土壤与肥料/WOS BP-土壤与肥料_C1_机构国家.xlsx')
    parser.add_argument('-f', '--field', help = '学科领域，用于筛选Q1期刊', type = str,
            default='土壤与肥料')
    parser.add_argument('-j', '--journal', help = 'Q1期刊表位置', type = str,
            default='/Users/biomap/Documents/school wroks/situation_analysis/utils/Q1.xlsx')
    parser.add_argument('-c', '--column', help = '分析所依据的列名', type = str,
            default='国家')

    args = parser.parse_args()
    loc = Path(args.loc)
    journal = args.journal
    column = args.column
    field = args.field

    result_folder = Path(RESULT_FOLDER)
    job_name = loc.name.split('.')[0]
    print('任务名称:', job_name)
    # 创建任务保存地址
    job_dir_rslt = result_folder.joinpath(job_name)
    job_dir_rslt.mkdir(exist_ok = True)
    print('计算中间结果')
    data = get_international_cooperate(get_high_cited_paper(pd.read_excel(loc)))

    q1_list = load_q1_journals(journal, field)

    data = get_if_q1(data, q1_list)
    print('生成最终结果')
    result = wos_paper_analysis(data, column)

    file = job_dir_rslt.joinpath(job_name + '_' + column + '_分析结果.xlsx')
    result.to_excel(file, index = False)

    




    