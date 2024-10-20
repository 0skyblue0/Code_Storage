import Query_Module_Legacy as QML
import re
import sys
import os
import Query_Module as QM

class CLI_Func():
    def __init__(self):
        DC = QML.Data_Controller()
        self.citycode_table = DC.getCityCode()
        self.citycode_table['주소명'] = self.citycode_table['시도명'] + ' ' + self.citycode_table['시군구명'] + ' ' + self.citycode_table['읍면동명']
        self.urls ={
            '건축물연령속성조회' : 'https://api.vworld.kr/ned/data/getBuildingAge',
            '용도별건물속성조회' : 'https://api.vworld.kr/ned/data/getBuildingUse',
            '토지특성속성조회': 'https://api.vworld.kr/ned/data/getLandCharacteristics',
            '토지소유정보속성조회' : 'https://api.vworld.kr/ned/data/getPossessionAttr',
            '토지이동이력속성조회' : 'https://api.vworld.kr/ned/data/getLandMoveAttr',
            '토지이용계획속성조회' : 'https://api.vworld.kr/ned/data/getLandUseAttr',
            '대지권등록목록조회' : 'https://api.vworld.kr/ned/data/ldaregList',
            '공유지연명목록조회' : 'https://api.vworld.kr/ned/data/cnrdlnList',
            '건물일련번호조회' : 'https://api.vworld.kr/ned/data/buldSnList',
            '건물동명조회' : 'https://api.vworld.kr/ned/data/buldCongNmList',
            '건물층수조회' : 'https://api.vworld.kr/ned/data/buldFloorCoList',
            '건물호수조회' : 'https://api.vworld.kr/ned/data/buldHoCoList',
            '건물실명조회' : 'https://api.vworld.kr/ned/data/buldRlnmList'
        }
        self.pnu = ''
    
    def pnu_gen(self, addr1, addr2):
        # addr1 -> ex. 경기도 화성시 진안동  // addr2 -> ex. 871-4
        """pnu_data = land_laoder Data land_name = 행정구역 ex)제주특별자치도 서귀포시 표선면 하천리 locate_detail = 상세주소 ex) 산 125-1
        pnu는 크게 행정구역코드 + 상세지번으로 구성된다. 예를 들어 ‘충청북도 청주시 오창읍 구룡리 150-6번지’의 경우 아래와 같이 pnu코드가 구성된다.
        4311425346101500006 4311425346(행정구역코드) + 1(필지구분, 일반은 1 산은 2) + 0150(본번을 0000으로 패딩) + 0006(부번을 0000으로 패딩)
        참조 : https://devlog.jwgo.kr/2020/01/20/how-to-get-pnu/"""

        locate_main_num = '0000'
        locate_detail_num = '0000'



        # 입력 데이터 ( addr1 )기준 법정 코드 정보 검색
        try:
            location_code = self.citycode_table[ self.citycode_table['주소명'] == addr1 ]['법정동코드']
            if len(location_code) == 1:
                location_code = set(location_code.to_list())
            else:
                print(f'행정구역명 재 확인 필요 : Error -> 코드번호 검색 이상 // {addr1}')
        except:
            print(f'행정구역명 재 확인 필요 : Error // {addr1}')

        # 상세 지번 데이터 ( addr2 ) 반영 ( PNU )
        try:
            if '산' in addr2:
                field_type = 2
            else:
                field_type = 1
            
            re_numbers = re.findall("\d+", addr2)
            
            locate_main_num = str(re_numbers[0]).zfill(4)

            if len(re_numbers) == 2:
                locate_detail_num = str(re_numbers[1]).zfill(4)

            pnu = f'{list(location_code)[0]}{field_type}{locate_main_num}{locate_detail_num}'

        except:
            print(f'상세 지번 변환 불가 : {addr2}')
        
        print(f'PNU 생성 완료 : {pnu}\n대상 주소 : {addr1 + addr2}\n동 주소 PNU : {list(location_code)[0]}')
        self.pnu = pnu
        return pnu

    def get_data(self, url_name, pnu=''):
        'pnu를 이미 검색한번 했으면 자동으로 입력됨'
        if self.pnu != '':
            pnu = self.pnu
        return QM.vworld_request(self.urls[url_name],{'pnu':pnu})























if __name__=="__main__":
    func = CLI_Func()

    while True:
        print('''\n
        0. 종료\n
        1. PNU 생성\n
        ''')
        select = int(input('기능 선택 : '))

        match select:
            case 0:
                break
            case 1:
                addr1 = input('동 주소를 입력하세요. ex) 경기도 화성시 진안동\n주소 : ') 
                addr2 = input('상세 주소를 입력하세요. ex) 871-4\n주소 : ') 
                func.pnu_gen(addr1, addr2)