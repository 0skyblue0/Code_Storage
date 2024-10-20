import requests
import pandas as pd
import xmltodict
import json
from util_tool import *

class APIKeyError(Exception):
    def __init__(self, message='Not Valid API Key'):
        self.message = message

    def __str__(self):
        return 'APIKeyError: ' + self.message

class Data_Controller():
    def __init__(self) -> None:
        with open('API_DataGo.key','r') as f:
            self.key = f.read()
        self.citycode_api_url = self.valied_citycode_url()
        self.citycode_baseurl = 'https://api.odcloud.kr/api'
        self.colunm_dict = self.colunm_convertor()

    def colunm_convertor(self):
        # 영문 -> 국문 변환 Dict 데이터 반환
        # 변수 선언
        file_path = '.\\ref_data\\Colunm_name_convert.csv'

        name_list = pd.read_csv(file_path, encoding='ANSI',sep='\t')
        name_list = name_list[~name_list.duplicated()].reset_index()

        colunms_dict = { name_list.loc[x,:]['영어'] : name_list.loc[x,:]['국문'] for x in range(len(name_list)) }

        return colunms_dict
    
    def getRTMdata(self, location_code, start_deal_date, end_deal_data) -> pd.DataFrame:
        main_dataframe = pd.DataFrame()
        url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcInduTrade'
        for deal_date in dategen(start_deal_date, end_deal_data):
            pagenum = 0
            data_counter = 0
            while True:
                pagenum += 1
                params ={'serviceKey' : self.key, 'LAWD_CD' : location_code, 'DEAL_YMD' : deal_date, 'numOfRows' : '1000', 'pageNo' : pagenum }
                response = requests.get(url, params=params)
                res_data = xmltodict.parse(response.content.decode())
                try:
                    if res_data['response']['header']['resultCode'] != '00':
                        return False, res_data['response']['header']['resultMsg']
                    
                    if res_data['response']['body']['items'] == None:
                        break
                    data = res_data['response']['body']['items']['item']
                except Exception as E:
                    if 'items' not in res_data['response']['body'].keys():
                        print(E)
                        raise E
                    else:
                        print(E)
                        raise E
                max_counter = int(res_data['response']['body']['totalCount'])
                if len(data) == 0:
                    break
                data_counter += len(data)
                if type(data) == dict:
                    data = [data]
                temp_dataframe = pd.DataFrame(data)
                main_dataframe = pd.concat([main_dataframe, temp_dataframe],axis=0)

                if data_counter >= max_counter:
                    break
        return main_dataframe[['년','월', '일','거래금액', '거래유형', '건물면적', '건물주용도', '건축년도', '대지면적', '매도자', '매수자',
            '법정동', '시군구', '용도지역', '유형','중개사소재지', '지번', '지역코드', '층',
            '해제사유발생일', '해제여부']].reset_index(drop=True)

    def valied_citycode_url(self) -> str:
        url = 'https://infuser.odcloud.kr/oas/docs?namespace=15063424/v1'
        response = requests.get(url)
        res_data = json.loads(response.content.decode())
        last_data_path = list(res_data['paths'].keys())[-1]

        return last_data_path
    
    def getCityCode(self) -> pd.DataFrame:
        main_dataframe = pd.DataFrame()
        pagenum = 0
        data_counter = 0
        url = self.citycode_baseurl + self.citycode_api_url
        while True:
            pagenum += 1
            params ={'serviceKey' : self.key, 'page': pagenum, 'perPage': 10000}
            response = requests.get(url, params=params)
            res_data = json.loads(response.content.decode())
            try:
                data = res_data['data']
            except Exception as E:
                if 'data' not in res_data.keys():
                    raise APIKeyError
                else:
                    raise E
            max_counter = int(res_data['totalCount'])
            data_counter += len(data)

            temp_dataframe = pd.DataFrame(data)
            main_dataframe = pd.concat([main_dataframe, temp_dataframe])

            if data_counter >= max_counter:
                break
        main_dataframe = main_dataframe[main_dataframe['삭제일자'].isna()]
        main_dataframe.reset_index(drop=True, inplace=True)
        main_dataframe[['과거법정동코드','법정동코드']] = main_dataframe[['과거법정동코드','법정동코드']].astype('str')
        return main_dataframe
    
    def getBrTitleInfo(self, location_codes:list) -> pd.DataFrame: # 표제부 다운로드
        main_dataframe = pd.DataFrame()
        url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrTitleInfo'
        for location_code in location_codes:
            pagenum = 0
            data_counter = 0
            while True:
                pagenum += 1
                params ={'serviceKey' : self.key, 'sigunguCd' : location_code[0:5], 'bjdongCd' : location_code[5:10], 'numOfRows' : '1000', 'pageNo' : pagenum }
                response = requests.get(url, params=params)
                res_data = xmltodict.parse(response.content.decode())
                try:
                    if res_data['response']['header']['resultCode'] != '00':
                        return False, res_data['response']['header']['resultMsg']
                    
                    if res_data['response']['body']['items'] == None:
                        break
                    data = res_data['response']['body']['items']['item']
                except Exception as E:
                    if 'items' not in res_data['response']['body'].keys():
                        print(E)
                        raise E
                    else:
                        print(E)
                        raise E
                max_counter = int(res_data['response']['body']['totalCount'])
                if len(data) == 0:
                    break
                data_counter += len(data)
                if type(data) == dict:
                    data = [data]
                temp_dataframe = pd.DataFrame(data)
                main_dataframe = pd.concat([main_dataframe, temp_dataframe],axis=0)

                if data_counter >= max_counter:
                    break
        return main_dataframe.reset_index(drop=True)

    def singular_getBrTitleInfo_list(self, pnu_list:list) -> pd.DataFrame:
        main_dataframe = pd.DataFrame()
        url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrTitleInfo'
        for pnu in pnu_list:
            pagenum = 0
            data_counter = 0
            while True:
                pagenum += 1
                params ={'serviceKey' : self.key, 'sigunguCd' : pnu[0:5], 'bjdongCd' : pnu[5:10], 'platGbCd' : pnu[10], 'bun' : pnu[11:15], 'ji' : pnu[15:19], 'numOfRows' : '10', 'pageNo' : pagenum }
                response = requests.get(url, params=params)
                res_data = xmltodict.parse(response.content.decode())
                try:
                    if res_data['response']['header']['resultCode'] != '00':
                        return False, res_data['response']['header']['resultMsg']
                    
                    if res_data['response']['body']['items'] == None:
                        break
                    data = res_data['response']['body']['items']['item']
                except Exception as E:
                    if 'items' not in res_data['response']['body'].keys():
                        print(E)
                        raise E
                    else:
                        print(E)
                        raise E
                max_counter = int(res_data['response']['body']['totalCount'])
                if len(data) == 0:
                    break
                data_counter += len(data)
                if type(data) == dict:
                    data = [data]
                temp_dataframe = pd.DataFrame(data)
                temp_dataframe['PNU'] = pnu
                main_dataframe = pd.concat([main_dataframe, temp_dataframe],axis=0)

                if data_counter >= max_counter:
                    break
        return main_dataframe.reset_index(drop=True)

    def getBrFlrOulnInfo(self, pnu_list:list):
        main_dataframe = pd.DataFrame()
        url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrFlrOulnInfo'
        for pnu in pnu_list:
            pagenum = 0
            data_counter = 0
            while True:
                pagenum += 1
                params ={'serviceKey' : self.key, 'sigunguCd' : pnu[0:5], 'bjdongCd' : pnu[5:10], 'platGbCd' : pnu[10], 'bun' : pnu[11:15], 'ji' : pnu[15:19], 'numOfRows' : '10', 'pageNo' : pagenum }
                response = requests.get(url, params=params)
                res_data = xmltodict.parse(response.content.decode())
                try:
                    if res_data['response']['header']['resultCode'] != '00':
                        return False, res_data['response']['header']['resultMsg']
                    
                    if res_data['response']['body']['items'] == None:
                        break
                    data = res_data['response']['body']['items']['item']
                except Exception as E:
                    if 'items' not in res_data['response']['body'].keys():
                        print(E)
                        raise E
                    else:
                        print(E)
                        raise E
                max_counter = int(res_data['response']['body']['totalCount'])
                if len(data) == 0:
                    break
                data_counter += len(data)
                if type(data) == dict:
                    data = [data]
                temp_dataframe = pd.DataFrame(data)
                temp_dataframe['PNU'] = pnu
                main_dataframe = pd.concat([main_dataframe, temp_dataframe],axis=0)

                if data_counter >= max_counter:
                    break
        return main_dataframe.reset_index(drop=True)

    def getApBasisOulnInfo(self, pnu_list:list):
            main_dataframe = pd.DataFrame()
            url = 'http://apis.data.go.kr/1613000/ArchPmsService_v2/getApBasisOulnInfo'
            count = 0
            for pnu in pnu_list:
                pagenum = 0
                count += 1
                data_counter = 0
                pnu = str(pnu)
                print(f'{count}/{len(pnu_list)}', end='\r')
                while True:
                    pagenum += 1
                    params ={'serviceKey' : self.key, 'sigunguCd' : pnu[0:5], 'bjdongCd' : pnu[5:10], 'numOfRows' : '1000', 'pageNo' : pagenum }
                    response = requests.get(url, params=params)
                    res_data = xmltodict.parse(response.content.decode())
                    try:
                        if res_data['response']['header']['resultCode'] != '00':
                            return False, res_data['response']['header']['resultMsg']
                        
                        if res_data['response']['body']['items'] == None:
                            break
                        data = res_data['response']['body']['items']['item']
                    except Exception as E:
                        if 'items' not in res_data['response']['body'].keys():
                            print(E)
                            raise E
                        else:
                            print(E)
                            raise E
                    max_counter = int(res_data['response']['body']['totalCount'])
                    if len(data) == 0:
                        break
                    data_counter += len(data)
                    if type(data) == dict:
                        data = [data]
                    temp_dataframe = pd.DataFrame(data)
                    temp_dataframe['PNU'] = pnu
                    main_dataframe = pd.concat([main_dataframe, temp_dataframe],axis=0)

                    if data_counter >= max_counter:
                        break
            return main_dataframe.reset_index(drop=True)