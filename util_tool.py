from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import pickle
import os

def dategen(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y%m')
    end_date = datetime.strptime(end_date_str, '%Y%m')

    date_list = []

    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y%m'))
        current_date += relativedelta(months=1)
    return date_list

def bjdongCd_extractor(target_dataframe:pd.DataFrame, ref_dataframe:pd.DataFrame) -> list:
    city_strs = list(set((target_dataframe['시군구'] + ' ' + target_dataframe['법정동'])))
    bgdongCd_list = []
    for city in city_strs:
        city = city.split()
        if len(city) == 3:
            search_frame = ref_dataframe[(ref_dataframe['시군구명'] == city[0]) & (ref_dataframe['읍면동명'] == city[1]) & (ref_dataframe['리명'] == city[2])]
            if len(search_frame) > 1:
                print(f'데이터 다중 검색으로 지역 특정 불가 -> {" ".join(city)}')
            bgdongCd_list.append(search_frame.iloc[0]['법정동코드'])
    return bgdongCd_list

def pnu_maker(target_addr:str, pre_fix_code:str, ref_dataframe:pd.DataFrame) -> str:
    """
    도,시 수준은 미리 필터링해서 ref_dataframe을 넘겨줘야함
    """
    addr_split = target_addr.split()[2:]
    if len(addr_split) != 3:
        return f'Split 데이터 이상 -> {" ".join(addr_split)}'
    
    dongcd = ref_dataframe[(ref_dataframe['읍면동명'] == addr_split[0]) & (ref_dataframe['리명'] == addr_split[1])]
    if len(dongcd) > 1:
        return(f'데이터 다중 검색으로 지역 특정 불가 -> {" ".join(addr_split)}')
    
    if '산' in addr_split[2]:
        mt = '1'
    else:
        mt = '0'
    
    nm_code = addr_split[2].replace('산','').replace('번지','').split('-')
    if len(nm_code) == 2:
        nm_code = nm_code[0].zfill(4) + nm_code[1].zfill(4)
    elif len(nm_code) == 1:
        nm_code = nm_code[0].zfill(4) + '0000'

    return str(dongcd.iloc[0]['법정동코드']) + mt + nm_code

def row_merge(data, type:str):
    match type:
        case '토지이용계획속성조회':
            return ','.join(list(data['prposAreaDstrcCodeNm']+' - ('+data['cnflcAtNm']+')'))

def addr_spliter(data):
    data = data.split()
    if data[-2] == '산':
        addr2 = ''.join(data[-2:])
        addr1 = ' '.join(data[:-2])
    else:
        addr2 = data[-1]
        addr1 = ' '.join(data[:-1])
    
    return addr1,addr2

def column_data_control(file, table_name:str|None=None, data:str|None=None):
    if data != None:
        try:
            new_dict = {table_name:{}}
            new_dict[table_name] = { eng:kor for eng, kor in zip(data.split('\n')[::5], data.split('\n')[2::5]) }
        except:
            return False
        
        try:
            if os.path.exists(file):
                with open(file, 'rb') as f:
                    column_data = pickle.load(f)
                    column_data.update(new_dict)
            else:
                column_data = new_dict
            
            with open(file,'wb') as f:
                pickle.dump(column_data,f)
            
            return True
        except:
            return False
    else:
        if os.path.exists(file):
            with open(file, 'rb') as f:
                column_data = pickle.load(f)
            return column_data
        
        return False

def column_translate(table) -> pd.DataFrame:
    column_data = column_data_control('column_trans.pkl')
    column_eng_kor_dict = {}
    try:
        for table_name in column_data.keys():
            column_eng_kor_dict.update(column_data[table_name])
    except Exception as E:
        print(E)

    table.rename(columns=column_eng_kor_dict, inplace=True)

    return table