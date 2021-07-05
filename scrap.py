# 7.4.(일) 급여 부분 변경 

import re, time
from datetime import datetime

import multy_scrap

class crawl_mon:
    def __init__(self):
        self.warn = "보안문자를 입력해 주시기 바랍니다"
        self.pattern = re.compile(self.warn)
        self.day = datetime.today().strftime("%Y-%m-%d")

        self.rWDate = 4  # 오늘 등록된 게시물만 보기 (get element)
        self.ps = 50  # 최대 노출 게시물 수

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



    def make_url(self, code, rWDate=4, ps=50, page=1):
        url = "https://www.albamon.com/list/gi/mon_part_list.asp?"
        url += "rpcd=" + str(code) + "&rWDate=" + str(rWDate) + "&ps=" + str(ps) + "&page=" + str(page)
        return url

if __name__=="__main__":

    crawl = multy_scrap.MultyScrap()
    print(crawl.df)
    day = datetime.today().strftime("%Y-%m-%d")
    crawl.df.to_csv(day+".csv",index=False,encoding="utf8")




