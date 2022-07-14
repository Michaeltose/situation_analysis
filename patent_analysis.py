import argparse
import pandas as pd
from analysis import annual_trends_patent, importance_analysis
from argparse import ArgumentParser
from config import RESULT_FOLDER
from preprocess import COUNTRY, get_processed_data_patent, APPLICANT
from pathlib2 import Path


if __name__ == '__main__':
    parser = ArgumentParser()
    # parser.add_argument('-h', help = '帮助信息')
    parser.add_argument('-l', '--loc', help = '文件路径', type = str, default = '/Users/biomap/Documents/school works/situation_analysis/temp/Incopat-土壤与肥料/Incopat-土壤与肥料_申请人_清洗后机构.xlsx')
    parser.add_argument('-c', '--column', help = '待处理的列', type = str, default = APPLICANT)
    args = parser.parse_args()

    loc = args.loc
    column = args.column

    data = pd.read_excel(loc)

    print(data.columns)
    result_folder = Path(RESULT_FOLDER)
    loc = Path(loc)
    job_name = loc.name.split('.')[0]
    print('任务名称:', job_name)
    # 创建任务保存地址
    job_dir_rslt = result_folder.joinpath(job_name)
    job_dir_rslt.mkdir(exist_ok = True)
    print('开始分析')
    
    result = importance_analysis(data, by_column=column)
    result_app = annual_trends_patent(data, column, year= '申请日')
    result_ann = annual_trends_patent(data, column, year= '公开（公告）日')
    print('分析结束，保存结果。。')

    importance_loc = job_dir_rslt.joinpath(job_name + '_' + column + '_影响力.xlsx')
    app_loc = job_dir_rslt.joinpath(job_name + '_' + column + '_申请年.xlsx')
    ann_loc = job_dir_rslt.joinpath(job_name + '_' + column + '_公开年.xlsx')
    result.to_excel(importance_loc, index = False)
    result_app.to_excel(app_loc, index = False)
    result_ann.to_excel(ann_loc)