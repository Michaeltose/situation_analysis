from operator import index
from os import sep
from unittest import result
from preprocess import clean_field, load_data, get_processed_data_patent, split_cooperate, load_and_merge
from preprocess import extract_country_institution_wos_paper, get_high_cited_paper
from preprocess import APPLICANT, COUNTRY, COUNTRY_PAPER, INSTI_PAPER, INSTI_PAPER_II, YEAR_PAPER, HIGH_CITED, CITED_NUM, YEAR
from analysis import annual_trends_patent, importance_analysis, annual_trends, high_cited_analysis
from pathlib2 import Path

KEY_COLUMN = '公开（公告）号'
KEY_COLUMN_PAPER = 'UT'
CAS_THESAURUS_LOC = '/Users/biomap/Documents/school wroks/patent_analysis/utils/中科院各所叙词表-论文.xlsx'

def run_standard_patent(data_loc, mapping_data_loc, annotation = '', co_country_split = ',', co_applicant_split = '; '):
    '''
    专利标准分析函数，生成国家、机构年度趋势，国家、机构重要专利结果
    data_loc: 待分析数据位置
    mapping_data_loc: 叙词表位置
    annotation: 用于区分结果文件名的注释，默认为空
    co_country_split: 国家合作分隔符，用于拆分多国共有专利
    co_applicant_split: 机构合作分隔符，用于拆分多机构共有专利
    '''
    data = load_data(data_loc, key=KEY_COLUMN)
    
    data_cty = get_processed_data_patent(split_cooperate(data, COUNTRY, sep = co_country_split))
    data_app = get_processed_data_patent(split_cooperate(data, APPLICANT, sep = co_applicant_split))
    data_app = clean_field(data_app, column_name= '申请人', mapping_data=mapping_data_loc)
    
    annual_trends_app = annual_trends(data_app, by_column=APPLICANT, year=YEAR)
    annual_trends_cty = annual_trends(data_cty, by_column=COUNTRY, year=YEAR)
    
    importance_result_app = importance_analysis(data_app, by_column=APPLICANT)
    importance_result_cty = importance_analysis(data_cty, by_column=COUNTRY)
    
    result_path = Path('/Users/biomap/Documents/school wroks/patent_analysis/result')
    file_name = str(Path(data_loc).name).split('.')[0]

    data_cty.to_excel('/Users/biomap/Documents/school wroks/patent_analysis/temp/'+ file_name + '.xlsx')
    result_folder = result_path.joinpath(file_name + annotation + '_结果')
    result_folder.mkdir(exist_ok = True)
    importance_result_cty_loc = result_folder.joinpath(file_name + annotation + '专利总量及重要专利-国家.xlsx')
    importance_result_app_loc = result_folder.joinpath(file_name + annotation + '专利总量及重要专利-机构.xlsx')
    annual_trends_cty_loc = result_folder.joinpath(file_name + annotation + '专利总量年度趋势-国家.xlsx')
    annual_trends_app_loc = result_folder.joinpath(file_name + annotation + '专利总量年度趋势-机构.xlsx')

    annual_trends_app.to_excel(annual_trends_app_loc, index= False)
    annual_trends_cty.to_excel(annual_trends_cty_loc, index=False)
    importance_result_cty.to_excel(importance_result_cty_loc, index=False)
    importance_result_app.to_excel(importance_result_app_loc, index=False)
    print('完成，结果已保存在 %s'%(str(result_folder)))

def run_standard_paper(dataloc, mapping_data_loc, cas_mapping_data_loc = CAS_THESAURUS_LOC, annotation = '', author_info_column = 'RP', co_operate_split=';'):
    data = load_data(loc = dataloc, key=KEY_COLUMN_PAPER)
    print('拆分合作信息')
    data = split_cooperate(data, author_info_column, sep = ';')
    print('提取国家机构')
    data = extract_country_institution_wos_paper(data)
    data = clean_field(data, INSTI_PAPER, mapping_data=mapping_data_loc)
    data_cas = data.loc[data[INSTI_PAPER] == 'chinese academy of sciences', :]
    data_cas = clean_field(data_cas, INSTI_PAPER_II, mapping_data = cas_mapping_data_loc)

    data = get_high_cited_paper(data, column_name=CITED_NUM)
    data = data.loc[:, ['key', 'TI', COUNTRY_PAPER, INSTI_PAPER, INSTI_PAPER_II, YEAR_PAPER, HIGH_CITED]]

    data_cas = get_high_cited_paper(data_cas, column_name=CITED_NUM)
    data_cas = data_cas.loc[:, ['key', 'TI', COUNTRY_PAPER, INSTI_PAPER, INSTI_PAPER_II, YEAR_PAPER, HIGH_CITED]]
    
    result_path = Path('/Users/biomap/Documents/school wroks/patent_analysis/result')
    file_name = str(Path(dataloc).name).split('.')[0]
    result_folder = result_path.joinpath(file_name + annotation + '_结果')
    result_folder.mkdir(exist_ok = True)

    temp_path = Path('/Users/biomap/Documents/school wroks/patent_analysis/temp')
    temp_folder = temp_path.joinpath(file_name + annotation + '_中间数据')
    temp_folder.mkdir(exist_ok = True)

    temp_data_loc = temp_folder.joinpath(file_name + annotation + '中间结果.xlsx')
    temp_data_loc_cas = temp_folder.joinpath(file_name + annotation + '中间结果_中科院各所.xlsx')
    data.to_excel(temp_data_loc, index=False)
    data_cas.to_excel(temp_data_loc_cas, index=False)

    high_cited_loc_it = result_folder.joinpath(file_name + annotation + '高被引统计结果-机构.xlsx')
    annual_trends_loc_it = result_folder.joinpath(file_name + annotation + '年度发文趋势-机构.xlsx')
    high_cited_loc_cas = result_folder.joinpath(file_name + annotation + '高被引统计结果-中科院各所.xlsx')

    high_cited_loc_ct = result_folder.joinpath(file_name + annotation + '高被引统计结果-国家.xlsx')
    annual_trends_loc_ct = result_folder.joinpath(file_name + annotation + '年度发文趋势-国家.xlsx')
    annual_trends_loc_cas = result_folder.joinpath(file_name + annotation + '年度发文趋势-中科院各所.xlsx')
    print('计算高被引论文')
    high_cited_result_it = high_cited_analysis(data, by_column=INSTI_PAPER)
    high_cited_result_cas = high_cited_analysis(data_cas, by_column=INSTI_PAPER_II)
    high_cited_result_ct = high_cited_analysis(data, by_column=COUNTRY_PAPER)

    high_cited_result_it.to_excel(high_cited_loc_it, index=False)
    high_cited_result_ct.to_excel(high_cited_loc_ct, index=False)
    high_cited_result_cas.to_excel(high_cited_loc_cas, index=False)
    print('分析年度趋势')
    trends_it = annual_trends(data, by_column=INSTI_PAPER, year=YEAR_PAPER)
    trends_ct = annual_trends(data, by_column=COUNTRY_PAPER, year=YEAR_PAPER)
    trends_cas = annual_trends(data_cas, by_column=INSTI_PAPER_II, year=YEAR_PAPER)

    trends_it.to_excel(annual_trends_loc_it, index=False)
    trends_ct.to_excel(annual_trends_loc_ct, index=False)
    trends_cas.to_excel(annual_trends_loc_cas, index=False)

if __name__ == '__main__':
    run_standard_paper('/Users/biomap/Documents/school wroks/patent_analysis/data/海水鱼论文.xlsx',
        mapping_data_loc='/Users/biomap/Documents/school wroks/patent_analysis/utils/机构叙词表-论文.xlsx',
        annotation='')

    run_standard_paper('/Users/biomap/Documents/school wroks/patent_analysis/data/淡水鱼论文.xlsx',
        mapping_data_loc='/Users/biomap/Documents/school wroks/patent_analysis/utils/机构叙词表-论文.xlsx',
        annotation='')

    # load_and_merge(folder='/Users/biomap/Documents/school wroks/patent_analysis/data/海水鱼10181条', 
    #     key=KEY_COLUMN_PAPER, 
    #     sep = '\t').to_excel('/Users/biomap/Documents/school wroks/patent_analysis/temp/海水鱼论文.xlsx')

    # run_standard_patent(data_loc='/Users/biomap/Documents/school wroks/patent_analysis/data/海水鱼专利.xls',
    #     mapping_data_loc='/Users/biomap/Documents/school wroks/patent_analysis/utils/专利机构清洗-不合并中科院.xlsx',
    #     annotation='不合并中科院')

    # run_standard_patent(data_loc='/Users/biomap/Documents/school wroks/patent_analysis/data/海水鱼专利.xls',
    #     mapping_data_loc='/Users/biomap/Documents/school wroks/patent_analysis/utils/专利机构清洗.xlsx',
    #     annotation='合并中科院')

    # run_standard_patent(data_loc='/Users/biomap/Documents/school wroks/patent_analysis/data/淡水鱼专利.xls',
    #     mapping_data_loc='/Users/biomap/Documents/school wroks/patent_analysis/utils/专利机构清洗-不合并中科院.xlsx',
    #     annotation='不合并中科院')
    
    # run_standard_patent(data_loc='/Users/biomap/Documents/school wroks/patent_analysis/data/淡水鱼专利.xls',
    #     mapping_data_loc='/Users/biomap/Documents/school wroks/patent_analysis/utils/专利机构清洗.xlsx',
    #     annotation='合并中科院')
    

