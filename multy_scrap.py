# Crawl
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

# multiprocessing
from multiprocessing import Pool
import multiprocessing
from pathos.multiprocessing import ProcessingPool as Pool
# pip install pathos

from datetime import datetime
import os, time, re
import pandas as pd

import main
from job_code import job_code

class MultyScrap:
    def __init__(self, jobcode = job_code):
        self.process = multiprocessing.cpu_count() - 1 # cpu core 개수
        self.pool = Pool(processes=self.process)
        self.job_code = jobcode
        self.crawl_mon = main.crawl_mon()


        manager = multiprocessing.Manager()
        task_list = manager.list()
        start = 0
        end = int(len(self.job_code.keys()) / self.process)
        for i in range(self.process):
            task_list.append(list(job_code.items())[start:end])
            start = end
            end += end

        # self.pool.map(self.open_browser, repeat(task_list))
        self.df = pd.concat(self.pool.map(self.open_browser, task_list)).drop_duplicates()
        self.pool.close()
        self.pool.join

        """웹드라이버가 여기에 있으면 오류가 난다! 웹드라이버는 싱글스레드라서!"""
        # self.driver = webdriver.Chrome('./chromedriver.exe')

    def open_browser(self, task_list):
        day = datetime.today().strftime("%Y-%m-%d")
        p = re.compile('\d')

        result_df = pd.DataFrame(
            columns=['region', 'B_name', 'pay', 'pay_type', 'workspace', 'working_time', 'url', 'working_period', 'day',
                     'sub_code', 'enrol_date']
        )

        driver = webdriver.Chrome('./chromedriver.exe')

        # 오늘 등록된 게시물만 보기 : 4 / 최대 노출 게시물 수 : 50
        for title, title_code in dict(task_list).items():  # 상위 카테고리
            for sub_title, sub_title_code in title_code.items():  # 하위 카테고리
                url = self.crawl_mon.make_url(sub_title_code, page=1)
                driver.get(url=url)

                while (True):
                    repeat_time = 0.01
                    time.sleep(0.3)

                    no_data = False

                    for i in driver.find_elements_by_class_name('gListWrap > table > tbody > tr'):
                        while (True):
                            try:
                                time.sleep(repeat_time)
                                i.find_element_by_class_name('iconWrap > span > a').send_keys(Keys.ENTER)
                                time.sleep(repeat_time)
                                break
                            except:
                                repeat_time += 0.1
                                # 대기시간이 0.5초 이상이 경우 데이터가 없다고 판단
                                if (repeat_time > 0.5):
                                    break

                        repeat_time = 0.01

                        while (True):
                            try:
                                temp = pd.Series(driver.find_element_by_class_name('preview').text.split('\n'))
                                break
                            except:
                                repeat_time += 0.1
                                # 대기시간이 0.5초 이상이 경우 데이터가 없다고 판단
                                if (repeat_time > 0.5):
                                    no_data = True
                                    break

                        if no_data:
                            break
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
                        temp_list.append(sub_title_code)

                        # 공고 등록일
                        temp_list.append(day)

                        result_df = result_df.append(pd.Series(temp_list, index=result_df.columns),
                                                     ignore_index=True)

                    # 다음 페이지 이동
                    try:
                        now_page = driver.find_element_by_class_name('pagenation > ul > li > em').text
                        driver.get(url=self.crawl_mon.make_url(sub_title_code, page=int(now_page) + 1))
                    except:
                        print(sub_title_code, " 마지막 페이지")
                        break
        print(">>> Running PID : {}\tResult : {}".format(str(os.getpid()), result_df.shape))

        return result_df

# if __name__ == "__main__":
#     parser = MultyScrap()
#     print(parser.df)
#     parser.df.to_csv("test.csv",index=False,encoding="utf8")

