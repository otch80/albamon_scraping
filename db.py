# DB
from sqlalchemy import create_engine
import pymysql, sqlalchemy
from sqlalchemy.dialects import mysql

import pandas as pd

user_info = {
    "username" : "root",
    "password" : "", # 입력
    "database" : "", # 입력
}

class DB:
    def __init__(self):
        self.connect_db()

    def connect_db(self):
        print(">>> Initializing Connection Settings")
        pymysql.install_as_MySQLdb()
        self.engine = create_engine("mysql+mysqldb://root:" + user_info['password'] + "@localhost/" + user_info['database'] + "?charset=utf8mb4", encoding = 'utf8')
        self.conn = self.engine.connect()
        print(">>> Connecting Success")


    def create_table(self):
        try:
            from sqlalchemy import Table, Column, MetaData
            metadata = MetaData()
        except:
            print(">>> sqlalchemy is not founded. Will you install sqlalchemy? [yes/no]: ")
            answer = input()
            if(answer == 'y' or answer == 'Y' or answer == 'yes' or answer == 'YES' or answer == 'Yes'):
                import os
                os.system('pip install sqlalchemy')
            else:
                raise Exception(">>> Package is not founed. Can't proceed any further.")
        try:
            # MBTI Table로 변경할 예정
            # 외식음료, 유통판매, 문화여가생활, 서비스, 사무직, 고객상담리서치영업, 생산건설노무, IT컴퓨터, 교육강사, 디자인, 미디어, 운전배달, 병원간호연구
            # table_list = ['food', 'sale', 'cult', 'serv', 'desk', 'rsch', 'buil', 'comp', 'edct', 'desg', 'medi', 'deli', 'oper']
            self.MBTI_list = ['istj', 'isfj', 'infj', 'intj', 'istp', 'isfp', 'infp', 'intp', 'estp', 'esfp', 'enfp', 'entp', 'estj', 'esfj', 'enfj', 'entj']

            for mbti in self.MBTI_list:
                temp_table = Table(mbti, metadata,
                    # Column('id', mysql.INTEGER),                            # 인덱스
                    Column('region', mysql.VARCHAR(10), nullable=False),    # 지역
                    Column('B_name', mysql.VARCHAR(80), nullable=False),    # 상호명
                    Column('pay', mysql.INTEGER, nullable=False),           # 급여
                    Column('pay_type', mysql.VARCHAR(8), nullable=False),   # 지급형태
                    Column('workspace', mysql.VARCHAR(80)),                 # 근무지
                    Column('working_time', mysql.VARCHAR(20)),              # 근무시간
                    Column('url', mysql.VARCHAR(100), nullable=False),      # url
                    Column('working_period', mysql.VARCHAR(36)),            # 기간
                    Column('day', mysql.VARCHAR(20)),                       # 요일
                    Column('sub_code', mysql.VARCHAR(8)),                   # 하위코드명
                    Column('enrol_date', mysql.DATE())                      # 공고등록일
                )
                try:
                    temp_table.create(self.engine)  # create the table
                except Exception as e:
                    print(">>> {} Table is already exists".format(mbti))

                del[[temp_table]]
            return True
        except:
            return False


    def create_log_table(self):
        from sqlalchemy import Table, Column, MetaData, DateTime
        metadata = MetaData()
        # 로그 테이블
        log_table = Table("log", metadata,
                Column('istj', mysql.INTEGER),
                Column('isfj', mysql.INTEGER),
                Column('infj', mysql.INTEGER),
                Column('intj', mysql.INTEGER),
                Column('istp', mysql.INTEGER),
                Column('isfp', mysql.INTEGER),
                Column('infp', mysql.INTEGER),
                Column('intp', mysql.INTEGER),
                Column('estp', mysql.INTEGER),
                Column('esfp', mysql.INTEGER),
                Column('enfp', mysql.INTEGER),
                Column('entp', mysql.INTEGER),
                Column('estj', mysql.INTEGER),
                Column('esfj', mysql.INTEGER),
                Column('enfj', mysql.INTEGER),
                Column('entj', mysql.INTEGER),
                Column('total_cnt', mysql.INTEGER),                         # 전체 데이터 수집 개수
                Column('run_time', mysql.INTEGER, nullable=False),      # 수집 시간
                Column('date', DateTime, nullable=False, primary_key=True)  # 수집일
        )
        try:
            log_table.create(self.engine)  # create the table
        except:
            print(">>> Log Table is already exists")

        del [[log_table]]


    def insert_table(self, ISTJ_df, ISFJ_df, INFJ_df, INTJ_df, ISTP_df, ISFP_df, INFP_df, INTP_df, ESTP_df, ESFP_df, ENFP_df, ENTP_df, ESTJ_df, ESFJ_df, ENFJ_df, ENTJ_df, rum_time, today):

        # log Table
        log_columns = ['ISTJ','ISFJ','INFJ','INTJ','ISTP','ISFP','INFP','INTP','ESTP','ESFP','ENFP','ENTP','ESTJ','ESFJ','ENFJ','ENTJ','total_cnt','run_time','date']

        sum = ISTJ_df.shape[0] + ISFJ_df.shape[0] + INFJ_df.shape[0] + INTJ_df.shape[0] + ISTP_df.shape[0] + \
              ISFP_df.shape[0] + INFP_df.shape[0] + INTP_df.shape[0] + ESTP_df.shape[0] + ESFP_df.shape[0] + \
              ENFP_df.shape[0] + ENTP_df.shape[0] + ESTJ_df.shape[0] + ESFJ_df.shape[0] + ENFJ_df.shape[0] + \
              ENTJ_df.shape[0]

        log_df = pd.DataFrame(
            [(ISTJ_df.shape[0], ISFJ_df.shape[0], INFJ_df.shape[0], INTJ_df.shape[0], ISTP_df.shape[0],
              ISFP_df.shape[0], INFP_df.shape[0], INTP_df.shape[0], ESTP_df.shape[0], ESFP_df.shape[0],
              ENFP_df.shape[0], ENTP_df.shape[0], ESTJ_df.shape[0], ESFJ_df.shape[0], ENFJ_df.shape[0],
              ENTJ_df.shape[0], sum, rum_time, today)], columns=log_columns)


        ISTJ_df.to_sql(name="istj", con=self.engine, if_exists='append', index=False)
        ISFJ_df.to_sql(name="isfj", con=self.engine, if_exists='append', index=False)
        INFJ_df.to_sql(name="infj", con=self.engine, if_exists='append', index=False)
        INTJ_df.to_sql(name="intj", con=self.engine, if_exists='append', index=False)
        ISTP_df.to_sql(name="istp", con=self.engine, if_exists='append', index=False)
        ISFP_df.to_sql(name="isfp", con=self.engine, if_exists='append', index=False)
        INFP_df.to_sql(name="infp", con=self.engine, if_exists='append', index=False)
        INTP_df.to_sql(name="intp", con=self.engine, if_exists='append', index=False)
        ESTP_df.to_sql(name="estp", con=self.engine, if_exists='append', index=False)
        ESFP_df.to_sql(name="esfp", con=self.engine, if_exists='append', index=False)
        ENFP_df.to_sql(name="enfp", con=self.engine, if_exists='append', index=False)
        ENTP_df.to_sql(name="entp", con=self.engine, if_exists='append', index=False)
        ESTJ_df.to_sql(name="estj", con=self.engine, if_exists='append', index=False)
        ESFJ_df.to_sql(name="esfj", con=self.engine, if_exists='append', index=False)
        ENFJ_df.to_sql(name="enfj", con=self.engine, if_exists='append', index=False)
        ENTJ_df.to_sql(name="entj", con=self.engine, if_exists='append', index=False)
        log_df.to_sql(name="log", con=self.engine, if_exists='append', index=False)

        print(">>> Table Insert Complete")

# if __name__=="__main__":
#     import pandas as pd
#     df = pd.read_csv("알바직종 MBTI 분류.csv")

    # db = DB()
    # db.connect_db()
    # db.create_table()
    # db.create_log_table()
