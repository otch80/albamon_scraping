# 7.4.(일) 급여 부분 변경 

import re, time
import pandas as pd
from datetime import datetime

# Crawl
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

# DB
import sqlalchemy
from sqlalchemy import create_engine
import pymysql, MySQLdb

# multiprocessing 
from multiprocessing import Pool  # Pool import하기
import multiprocessing


class crawl_mon:
    def __init__(self):
        self.warn = "보안문자를 입력해 주시기 바랍니다"
        self.pattern = re.compile(self.warn)
        self.day = datetime.today().strftime("%Y-%m-%d")

        self.rWDate = 4  # 오늘 등록된 게시물만 보기 (get element)
        self.ps = 50  # 최대 노출 게시물 수

        self.connect_db()

        #         self.dtypesql = {
        #             src_address = Column(String(16), index=True)
        #                 'region' : sqlalchemy.types.String(),
        #                 'B_name' : sqlalchemy.types.String(),
        #                 'pay' : sqlalchemy.types.String(),
        #                 'pay_type' : sqlalchemy.types.String(),
        #                 'workspace' : sqlalchemy.types.String(),
        #                 'working_time' : sqlalchemy.types.String(),
        #                 'url' : sqlalchemy.types.String(),
        #                 'working_period' : sqlalchemy.types.String(),
        #                 'day' : sqlalchemy.types.String(),
        #                 'sub_code' : sqlalchemy.types.String(),
        #                 'enrol_date' : sqlalchemy.types.DateTime()
        #         }

        self.job_code = {
            # "외식 음료"
            "1000": {
                #                 '일반음식점':'1010',
                #                 '레스토랑':'1020',
                '패밀리레스토랑': '1030',
                #                 '패스트푸드점':'1040',
                #                 '치킨·피자전문점':'1050',
                #                 '커피전문점':'1060',
                #                 '아이스크림·디저트':'1070',
                #                 '베이커리·도넛·떡':'1080',
                #                 '호프·일반주점':'1090',
                #                 '바(bar)':'1100',
                #                 '급식·푸드시스템':'1110',
                #                 '도시락·반찬':'1120'
            },

            # "유통 판매"
            "2000": {
                '백화점·면세점': '2010',
                '복합쇼핑몰·아울렛': '2013',
                '쇼핑몰·소셜커머스·홈쇼핑': '2016',
                '유통점·마트': '2020',
                '편의점': '2030',
                '의류·잡화매장': '2050',
                '뷰티·헬스스토어': '2060',
                '휴대폰·전자기기매장': '2070',
                '가구·침구·생활소품': '2080',
                '서점·문구·팬시': '2090',
                '약국': '2100',
                '농수산·청과·축산': '2110',
                '화훼·꽃집': '2120',
                '유통·판매 기타': '2990'
            },

            # "문화 여가 생활"
            "3000": {
                '놀이공원·테마파크': '3010',
                '호텔·리조트·숙박': '3030',
                '여행·캠프·레포츠': '3040',
                '영화·공연': '3043',
                '전시·컨벤션·세미나': '3046',
                '스터디룸·독서실·고시원': '3050',
                'PC방': '3060',
                '노래방': '3070',
                '볼링·당구장': '3080',
                '스크린 골프·야구': '3090',
                'DVD·멀티방·만화카페': '3110',
                '오락실·게임장': '3120',
                '이색테마카페': '3123',
                '키즈카페': '3126',
                '찜질방·사우나·스파': '3130',
                '피트니스·스포츠': '3140',
                '공인중개': '3160',
                '골프캐디': '3170',
                '고속도로휴게소': '3180',
                '문화·여가·생활 기타': '3990',
            },

            # "서비스"
            "4000": {
                '매장관리·판매': '4010',
                'MD': '4015',
                '캐셔·카운터': '4020',
                '서빙': '4030',
                '주방장·조리사': '4040',
                '주방보조·설거지': '4050',
                '바리스타': '4060',
                '안내데스크': '4070',
                '주차관리·주차도우미': '4090',
                '보안·경비·경호': '4095',
                '주유·세차': '4100',
                '전단지배포': '4120',
                '청소·미화': '4130',
                '렌탈관리·A/S': '4135',
                '헤어·미용·네일샵': '4140',
                '피부관리·마사지': '4149',
                '반려동물케어': '4160',
                '베이비시터·가사도우미': '4170',
                '결혼·연회·장례도우미': '4180',
                '판촉도우미': '4190',
                '이벤트·행사스텝': '4200',
                '나레이터모델': '4210',
                '피팅모델': '4220',
                '서비스 기타': '4990'
            },

            # "사무직"
            "6000": {
                '사무보조': '6010',
                '문서작성·자료조사': '6015',
                '비서': '6020',
                '경리·회계보조': '6030',
                '인사·총무': '6033',
                '마케팅·광고·홍보': '6036',
                '번역·통역': '6050',
                '복사·출력·제본': '6055',
                '편집·교정·교열': '6060',
                '공공기관·공기업·협회': '6070',
                '학교·도서관·교육기관': '6080'
            },

            # "고객상담 리서치 영업"
            "7000": {
                '고객상담·인바운드': '7010',
                '텔레마케팅·아웃바운드': '7020',
                '금융·보험영업': '7025',
                '일반영업·판매': '7030',
                '설문조사·리서치': '7040',
                '영업관리·지원': '7050'
            },

            # "생산 건설 운송"
            "8000": {
                '제조·가공·조립': '8020',
                '포장·품질검사': '8040',
                '입출고·창고관리': '8050',
                '상하차·소화물 분류': '8056',
                '기계·전자·전기': '8065',
                '정비·수리·설치·A/S': '8075',
                '공사·건설현장': '8085',
                'PVC(닥트·배관설치)': '8095',
                '조선소': '8100',
                '재단·재봉': '8110',
                '생산·건설·노무 기타': '8990'
            },

            # "IT 컴퓨터"
            "9000": {
                '웹·모바일기획': '9005',
                '사이트·콘텐츠 운영': '9010',
                '바이럴·SNS마케팅': '9016',
                '프로그래머': '9060',
                'HTML코딩': '9061',
                'QA·테스터·검증': '9063',
                '시스템·네트워크·보안': '9066',
                'PC·디지털기기 설치·관리': '9070'
            },

            # "교육 강사"
            "A000": {
                '입시·보습학원': 'A010',
                '외국어·어학원': 'A020',
                '컴퓨터·정보통신': 'A030',
                '요가·필라테스 강사': 'A033',
                '피트니스 트레이너': 'A036',
                '레져스포츠 강사': 'A038',
                '예체능 강사': 'A040',
                '유아·유치원': 'A050',
                '방문·학습지': 'A060',
                '보조교사': 'A070',
                '자격증·기술학원': 'A080',
                '국비교육기관': 'A090',
                '교육·강사 기타': 'A990'
            },

            # "디자인"
            "B000": {
                '웹·모바일디자인': 'B010',
                '그래픽·편집디자인': 'B020',
                '제품·산업디자인': 'B030',
                'CAD·CAM·인테리어디자인': 'B040',
                '캐릭터·애니메이션디자인': 'B050',
                '패션·잡화디자인': 'B060',
                '디자인 기타': 'B990'
            },

            # "미디어"
            "C000": {
                '미디어 전체': 'C000',
                '보조출연·방청': 'C010',
                '방송스텝·촬영보조': 'C020',
                '동영상촬영·편집': 'C030',
                '사진촬영·편집': 'C035',
                '조명·음향': 'C040',
                '방송사·프로덕션': 'C050',
                '신문·잡지·출판': 'C060',
                '미디어 기타': 'C990'
            },

            # "운전 배달"
            "D000": {
                '운송·이사': 'D010',
                '대리운전·일반운전': 'D020',
                '택시·버스운전': 'D030',
                '수행기사': 'D040',
                '화물·중장비·특수차': 'D050',
                '택배·퀵서비스': 'D060',
                '배달': 'D070'
            },

            # "병원 간호 연구"
            "E000": {
                '병원·간호·연구 전체': 'E000',
                '간호조무사·간호사': 'E010',
                '간병·요양보호사': 'E020',
                '원무·코디네이터': 'E030',
                '외래보조·병동보조': 'E040',
                '수의테크니션·동물보건사': 'E050',
                '실험·연구보조': 'E060',
                '생동성·임상시험': 'E070'
            }
        }

    def connect_db(self):
        __qlqjs = "!whals7628rb1"  # db password
        __elql = "capstone"  # db schema

        print(">>> Connect initialize Success")

        pymysql.install_as_MySQLdb()
        self.engine = create_engine("mysql+mysqldb://root:" + __qlqjs + "@localhost/" + __elql + "", encoding='utf8')
        self.conn = self.engine.connect()

        print(">>> Connection Success")

    def insert_table(self, table_name, df):
        code_list = {"1000": "food", "2000": "sale", "3000": "cult", "4000": "serv", "6000": "desk", "7000": "rsch",
                     "8000": "buil", "9000": "comp", "A000": "edct", "B000": "desg", "C000": "medi", "D000": "deli",
                     "E000": "oper"}
        table_name = code_list[table_name]
        #         df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False, dtype=self.dtypesql)
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
        print(">>> [{}] Table Insert Complete".format(table_name))

    def make_url(self, code, page=1):
        url = "https://www.albamon.com/list/gi/mon_part_list.asp?"
        url += "rpcd=" + str(code) + "&rWDate=" + str(self.rWDate) + "&ps=" + str(self.ps) + "&page=" + str(page)
        return url

    def find_final_page(self):
        max_page = 0
        for i in crawl.bs.find(class_='pagenation').findAll("a"):
            if (i.text == "다음"):
                break
            if (max_page < int(i.text)):
                max_page = int(i.text)
        return max_page

    def open_browser(self):
        driver = webdriver.Chrome('./chromedriver.exe')

    def start(self):
        p = re.compile('\d')

        self.driver = webdriver.Chrome(executable_path='chromedriver')
        for title, code_list in self.job_code.items():  # 상위 카테고리
            self.result_df = pd.DataFrame(
                columns=['region', 'B_name', 'pay', 'pay_type', 'workspace', 'working_time', 'url', 'working_period',
                         'day', 'sub_code', 'enrol_date'])

            sub_list = list()
            for sub_title, self.code in code_list.items():  # 하위 카테고리

                page = 1
                self.url = crawl.make_url(self.code, page)
                self.driver.get(url=self.url)

                while (True):
                    repeat_time = 0.01
                    time.sleep(0.3)

                    for i in crawl.driver.find_elements_by_class_name('gListWrap > table > tbody > tr'):
                        while (True):
                            try:
                                time.sleep(repeat_time)
                                i.find_element_by_class_name('iconWrap > span > a').send_keys(Keys.ENTER)
                                time.sleep(repeat_time)
                                break
                            except:
                                repeat_time += 0.1
                        repeat_time = 0.01

                        while (True):
                            try:
                                temp = pd.Series(crawl.driver.find_element_by_class_name('preview').text.split('\n'))
                                break
                            except:
                                repeat_time += 0.1
                        repeat_time = 0.01
                        temp_list = list()

                        try:  # 지역
                            temp_list.append(temp[temp[temp.isin(["근무지"])].index[0] + 1].split(" ")[0])
                        except:
                            temp_list.append(None)
                        try:  # 상호명
                            temp_list.append(i.find_element_by_class_name('cName').text)
                        except:
                            temp_list.append(None)
                        try:  # 급여
                            temp_list.append(int(''.join(p.findall(temp[temp[temp.isin(["급여"])].index[0] + 1]))))
                        except:
                            temp_list.append(None)
                        try:  # 지급형태
                            temp_list.append(i.find_element_by_class_name('pay > p > img').get_attribute('alt'))
                        except:
                            temp_list.append(None)
                        try:  # 근무지
                            temp_list.append(temp[temp[temp.isin(["근무지"])].index[0] + 1])
                        except:
                            temp_list.append(None)
                        try:  # 근무시간
                            temp_list.append(temp[temp[temp.isin(["근무시간"])].index[0] + 1])
                        except:
                            temp_list.append(None)
                        try:  # url
                            temp_list.append(i.find_element_by_class_name('cName > a').get_attribute('href'))
                        except:
                            temp_list.append(None)
                        try:  # 기간
                            temp_list.append(temp[temp[temp.isin(["기간·요일"])].index[0] + 1].split("|")[0])
                        except:
                            temp_list.append(None)
                        try:  # 요일
                            temp_list.append(temp[temp[temp.isin(["기간·요일"])].index[0] + 1].split("|")[1])
                        except:
                            temp_list.append(None)

                        # 하위 코드명
                        temp_list.append(self.code)

                        # 공고 등록일
                        temp_list.append(self.day)

                        self.result_df = self.result_df.append(pd.Series(temp_list, index=self.result_df.columns),
                                                               ignore_index=True)

                    # 다음 페이지 이동
                    try:
                        now_page = crawl.driver.find_element_by_class_name('pagenation > ul > li > em').text
                        crawl.driver.get(url=crawl.make_url(crawl.code, int(now_page) + 1))
                    except:
                        print(crawl.code, " 마지막 페이지")
                        break

            # 상위 타이틀 테이블 저장
            self.insert_table(title, self.result_df)
            break

