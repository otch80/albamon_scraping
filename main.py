from datetime import datetime
import re, time, scrap, db
import pandas as pd
import multy_scrap

class crawl_mon:
    def __init__(self):
        self.warn = "보안문자를 입력해 주시기 바랍니다"
        self.pattern = re.compile(self.warn)
        self.day = datetime.today().strftime("%Y-%m-%d")

        self.rWDate = 4  # 오늘 등록된 게시물만 보기 (get element)
        self.ps = 50  # 최대 노출 게시물 수


    def make_url(self, code, rWDate=4, ps=50, page=1):
        url = "https://www.albamon.com/list/gi/mon_part_list.asp?"
        url += "rpcd=" + str(code) + "&rWDate=" + str(rWDate) + "&ps=" + str(ps) + "&page=" + str(page)
        return url


    def divide_mbti(self, df):
        # print("df : ",df.shape)
        df.sub_code = df.sub_code.astype(str)
        mbti_df = pd.read_csv("MBTI list.csv")
        columns_list = ['region', 'B_name', 'pay', 'pay_type', 'workspace', 'working_time', 'url', 'working_period', 'day', 'sub_code', 'enrol_date']

        self.ISTJ = pd.DataFrame(columns=columns_list)
        self.ISFJ = pd.DataFrame(columns=columns_list)
        self.INFJ = pd.DataFrame(columns=columns_list)
        self.INTJ = pd.DataFrame(columns=columns_list)
        self.ISTP = pd.DataFrame(columns=columns_list)
        self.ISFP = pd.DataFrame(columns=columns_list)
        self.INFP = pd.DataFrame(columns=columns_list)
        self.INTP = pd.DataFrame(columns=columns_list)
        self.ESTP = pd.DataFrame(columns=columns_list)
        self.ESFP = pd.DataFrame(columns=columns_list)
        self.ENFP = pd.DataFrame(columns=columns_list)
        self.ENTP = pd.DataFrame(columns=columns_list)
        self.ESTJ = pd.DataFrame(columns=columns_list)
        self.ESFJ = pd.DataFrame(columns=columns_list)
        self.ENFJ = pd.DataFrame(columns=columns_list)
        self.ENTJ = pd.DataFrame(columns=columns_list)

        # sub_code별 mbti 만들기
        mbti_dict = dict()
        for group in mbti_df.groupby('직종코드'):
            mbti_dict[group[0]] = group[1]['mbti']

        # mbti별 공고 분류
        for key, value in mbti_dict.items():
            for mbti in value:
                if (mbti == "ISTJ"):
                    self.ISTJ = pd.concat([self.ISTJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ISFJ"):
                    self.ISFJ = pd.concat([self.ISFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INFJ"):
                    self.INFJ = pd.concat([self.INFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INTJ"):
                    self.INTJ = pd.concat([self.INTJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ISTP"):
                    self.ISTP = pd.concat([self.ISTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ISFP"):
                    self.ISFP = pd.concat([self.ISFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INFP"):
                    self.INFP = pd.concat([self.INFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "INTP"):
                    self.INTP = pd.concat([self.INTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESTP"):
                    self.ESTP = pd.concat([self.ESTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESFP"):
                    self.ESFP = pd.concat([self.ESFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENFP"):
                    self.ENFP = pd.concat([self.ENFP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENTP"):
                    self.ENTP = pd.concat([self.ENTP, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESTJ"):
                    self.ESTJ = pd.concat([self.ESTJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ESFJ"):
                    self.ESFJ = pd.concat([self.ESFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENFJ"):
                    self.ENFJ = pd.concat([self.ENFJ, df.loc[df['sub_code'] == key]], sort=False)
                if (mbti == "ENTJ"):
                    self.ENTJ = pd.concat([self.ENTJ, df.loc[df['sub_code'] == key]], sort=False)

if __name__=="__main__":
    start = time.time()

    crawl = multy_scrap.MultyScrap()

    day = datetime.today().strftime("%Y-%m-%d")
    crawl.df.to_csv("./log/"+day+".csv",index=False,encoding="utf-8-sig")
    end = time.time()-start
    print(">>> Scrap time : ",end)

    mon = crawl_mon()
    mon.divide_mbti(crawl.df)

    db = db.DB()
    db.create_table()
    db.create_log_table()

    db.insert_table(mon.ISTJ, mon.ISFJ, mon.INFJ, mon.INTJ, mon.ISTP, mon.ISFP, mon.INFP, mon.INTP, mon.ESTP, mon.ESFP,
                    mon.ENFP, mon.ENTP, mon.ESTJ, mon.ESFJ, mon.ENFJ, mon.ENTJ, end, day)
