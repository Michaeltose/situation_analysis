import pandas as pd

def cut_data(data, num):
    length = int(len(data)/num)
    datas = []
    for i in range(num):
        if i < num -1:
            datas.append(data[i*length:(i+1)*length])
        else:
            datas.append(data[i*length:])
    return datas

def load_q1_journals(loc, field):
    q1 = pd.read_excel(loc)
    q1 = q1.loc[q1['领域'] == field, :]
    journal_list = set(q1['Journal name'].str.lower()) | set(q1['JCR Abbreviation'].str.lower()) | set(q1['Journal name']) |set(q1['JCR Abbreviation'])
    return journal_list



if __name__ =='__main__':
    loc = '/Users/biomap/Documents/school wroks/situation_analysis/utils/Q1.xlsx'
    print(load_q1_journals(loc, '农学'))