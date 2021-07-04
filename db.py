# DB
from sqlalchemy import create_engine
import pymysql, MySQLdb
from sqlalchemy.dialects import mysql

user_info = {
    "username" : "root",
    "password" : "", # 입력
    "database" : "", # 입력
}

def connect_db():
    print(">>> Connect initialize Success")
    pymysql.install_as_MySQLdb()
    engine = create_engine("mysql+mysqldb://root:" + user_info['password'] + "@localhost/" + user_info['database'] + "", encoding='utf8')
    conn = engine.connect()
    print(">>> Connection Success")


def create_table():
    from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime
    metadata = MetaData()
    engine = create_engine("mysql+mysqldb://root:" + user_info['password'] + "@localhost/" + user_info['database'] + "", encoding='utf8')
    conn = engine.connect()

    # 외식음료, 유통판매, 문화여가생호라, 서비스, 사무직, 고객상담리서치영업, 생산건설노무, IT컴퓨터, 교육강사, 디자인, 미디어, 운전배달, 병원간호연구
    table_list = ['food', 'sale', 'cult', 'serv', 'desk', 'rsch', 'buil', 'comp', 'edct', 'desg', 'medi', 'deli', 'oper']


    for table_name in table_list:
        temp_table = Table(table_name, metadata,
            Column('id', mysql.INTEGER, primary_key=True),        # 인덱스
            Column('region', mysql.VARCHAR(10), nullable=False),       # 지역
            Column('B_name', mysql.VARCHAR(80), nullable=False),       # 상호명
            Column('pay', mysql.INTEGER, nullable=False),             # 급여
            Column('pay_type', mysql.VARCHAR(8), nullable=False),     # 지급형태
            Column('workspace', mysql.VARCHAR(80)),                    # 근무지
            Column('working_time', mysql.VARCHAR(12)),                 # 근무시간
            Column('url', mysql.VARCHAR(100), nullable=False),          # url
            Column('working_period', mysql.VARCHAR(36)),               # 기간
            Column('day', mysql.VARCHAR(20)),                          # 요일
            Column('sub_code', mysql.VARCHAR(8)),                     # 하위코드명
            Column('enrol_date', mysql.DATETIME())                  # 공고등록일
        )
        temp_table.create(engine)  # create the table
        del[[temp_table]]


def create_log_table():
    from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime
    metadata = MetaData()
    engine = create_engine("mysql+mysqldb://root:" + user_info['password'] + "@localhost/" + user_info['database'] + "", encoding='utf8')
    conn = engine.connect()
    # 로그 테이블
    log_table = Table("log", metadata,
            Column('food_cnt', mysql.INTEGER),                              # 외식음료 수집 개수
            Column('sale_cnt', mysql.INTEGER),                              # 유통판매 수집 개수
            Column('cult_cnt', mysql.INTEGER),                              # 문화여가생활 수집 개수
            Column('serv_cnt', mysql.INTEGER),                              # 서비스 수집 개수
            Column('desk_cnt', mysql.INTEGER),                              # 사무직 수집 개수
            Column('rsch_cnt', mysql.INTEGER),                              # 고객상담리서치영업 수집 개수
            Column('buil_cnt', mysql.INTEGER),                              # 생산건설노무 수집 개수
            Column('comp_cnt', mysql.INTEGER),                              # IT컴퓨터 수집 개수
            Column('edct_cnt', mysql.INTEGER),                              # 교육강사 수집 개수
            Column('desg_cnt', mysql.INTEGER),                              # 디자인 수집 개수
            Column('medi_cnt', mysql.INTEGER),                              # 미디어 수집 개수
            Column('deli_cnt', mysql.INTEGER),                              # 운전배달 수집 개수
            Column('oper_cnt', mysql.INTEGER),                              # 병원간호연구 수집 개수
            Column('total_cnt', mysql.INTEGER),                             # 전체 데이터 수집 개수
            Column('run_time', mysql.VARCHAR(20), nullable=False),          # 수집 시간
            Column('date', DateTime, nullable=False, primary_key=True),     # 수집일
    )
    log_table.create(engine)  # create the table
    del [[log_table]]

if __name__=="__main__":
    connect_db()
    create_table()
    create_log_table()
    # try:
    #     create_table()
    # except:
    #     print(">>> Table is already exists")