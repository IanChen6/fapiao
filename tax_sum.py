# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     
   Description :
   Author :       ianchen
   date：          
-------------------------------------------------
   Change Activity:
                   2017/11/22:
-------------------------------------------------
"""
import hashlib
import json
import redis
import re
from suds.client import Client
import suds
from selenium.webdriver import DesiredCapabilities

from log_ging.log_01 import *
import requests
from lxml import etree
import time
from selenium import webdriver
from selenium.webdriver.support import ui
from get_db import job_finish, get_db
import pymssql

logger = create_logger()


class fapiao(object):
    def __init__(self, user,pwd, batchid, companyid, customerid):
        self.headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Connection': 'keep-alive',
                        'Host': 'www.szcredit.org.cn',
                        'Cookie': 'UM_distinctid=160a1f738438cb-047baf52e99fc4-e323462-232800-160a1f73844679; ASP.NET_SessionId=4bxqhcptbvetxqintxwgshll',
                        'Origin': 'https://www.szcredit.org.cn',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://www.szcredit.org.cn/web/gspt/newGSPTList.aspx?keyword=%u534E%u88D4&codeR=28',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
                        'X-Requested-With': 'XMLHttpRequest',
                        }
        self.user=user
        self.pwd=pwd
        self.batchid = batchid
        self.companyid = companyid
        self.customerid = customerid
        self.host, self.port, self.db = get_db(companyid)

    def insert_db(self, sql, params):
        conn = pymssql.connect(host=self.host, port=self.port, user='Python', password='pl,okmPL<OKM',
                               database=self.db, charset='utf8')
        cur = conn.cursor()
        if not cur:
            raise Exception("数据库连接失败")
        len(params)
        cur.callproc(sql, params)
        conn.commit()
        cur.close()

    def tagger(self, tupian, md):
        while True:
            client = suds.client.Client(url="http://39.108.112.203:8701/SZYZService.asmx?wsdl")
            # client = suds.client.Client(url="http://192.168.18.101:1421/SZYZService.asmx?wsdl")
            auto = client.service.GetYZCodeForDll(tupian)
            if auto is not None:
                tagger = str(auto)
                flag = self.login()
                break
            result = client.service.SetYZImg(123456, "1215454545", "pyj", md, tupian)
            # flag = login("91440300MA5DRRFB45", "10284784", result)
            for i in range(30):
                result1 = client.service.GetYZCode(md)
                if result1 is not None:
                    result1 = str(result1)
                    return result1
                time.sleep(10)
            self.insert_db("[dbo].[Python_Serivce_Job_Expire]", (self.batchid, self.customerid))
            break

    def login(self):
        try_times = 0
        while try_times <= 5:
            try_times += 1
            session = requests.session()
            headers = {'Host': 'dzswj.szgs.gov.cn',
                       'Accept': 'application/json, text/javascript, */*; q=0.01',
                       'Accept-Language': 'zh-CN,zh;q=0.8',
                       'Content-Type': 'application/json; charset=UTF-8',
                       'Referer': 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/login/login.html',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                       'x-form-id': 'mobile-signin-form',
                       'X-Requested-With': 'XMLHttpRequest',
                       'Origin': 'http://dzswj.szgs.gov.cn'}
            session.get("http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/login/login.html", headers=headers)
            captcha_url = 'http://dzswj.szgs.gov.cn/tipCaptcha'
            tupian_resp = session.get(url=captcha_url, timeout=10)
            tupian_resp.encoding = 'utf8'
            tupian = tupian_resp.json()
            tupian = json.dumps(tupian, ensure_ascii=False)
            m = hashlib.md5()
            tupian1 = tupian.encode(encoding='utf8')
            m.update(tupian1)
            md = m.hexdigest()
            print(md)
            tag = self.tagger(tupian, md)
            jyjg = session.post(url='http://dzswj.szgs.gov.cn/api/checkClickTipCaptcha', data=tag)
            time_l = time.localtime(int(time.time()))
            time_l = time.strftime("%Y-%m-%d %H:%M:%S", time_l)
            tag = json.dumps(tag)
            login_data = '{"nsrsbh":"%s","nsrpwd":"%s","redirectURL":"","tagger":%s,"time":"%s"}' % (
                self.user, self.jiami(), tag, time_l)
            login_url = 'http://dzswj.szgs.gov.cn/api/auth/clientWt'
            resp = session.post(url=login_url, data=login_data)
            panduan = resp.json()['message']
            if "登录成功" in resp.json()['message']:
                print('登录成功')
                cookies = {}
                for (k, v) in zip(session.cookies.keys(), session.cookies.values()):
                    cookies[k] = v
                return cookies, session
            else:
                time.sleep(3)
        return False

    def parse_fapiao(self):
        try:
            cookies, session = self.login()
            jsoncookies = json.dumps(cookies)
            with open('cookies.json', 'w') as f:  # 将login后的cookies提取出来
                f.write(jsoncookies)
                f.close()
        except Exception as e:
            logger.warn(e)
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "登录失败")
            return False
        try:
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = (
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36')
            dcap["phantomjs.page.settings.loadImages"] = True
            browser = webdriver.PhantomJS(
                executable_path='D:/BaiduNetdiskDownload/phantomjs-2.1.1-windows/bin/phantomjs.exe',
                desired_capabilities=dcap)
            # browser = webdriver.PhantomJS(
            # executable_path='/home/tool/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
            # desired_capabilities=dcap)
            browser.implicitly_wait(10)
            browser.viewportSize = {'width': 2200, 'height': 2200}
            browser.set_window_size(1400, 1600)  # Chrome无法使用这功能
            # browser = webdriver.Chrome(executable_path='D:/BaiduNetdiskDownload/chromedriver.exe')  # 添加driver的路径
        except Exception as e:
            logger.warn(e)
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "浏览器启动失败")
            return False
        try:
            browser.get(url="http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/fp/zzszyfpdksq/fp_zzszyfpdksq.html")
            p = browser.page_source
            browser.switch_to_frame("txsqxx")
            fwpp = browser.find_element_by_css_selector('#dkfpjefw').text
            hwpp = browser.find_element_by_css_selector('#dkfpje').text
            fwzp = browser.find_element_by_css_selector('#zpdkjefw').text
            hwzp = browser.find_element_by_css_selector('#zpdkjehw').text
            gsparams = (self.companyid, self.customerid, fwpp, hwpp, fwzp, hwzp)
            self.insert_db("[dbo].[Python_Serivce_GSInvoiceSummary_Add]", gsparams)
        except Exception as e:
            print("error")
            print(e)



redis_cli = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
def run_test(user, pwd, batchid, companyid, customerid):
    print("++++++++++++++++++++++++++++++++++++")
    print('jobs[ts_id=%s] running....' % batchid)
    time.sleep(3)
    try:
        hz = fapiao(user=user,pwd=pwd , batchid=batchid, companyid=companyid,
                          customerid=customerid)
        hz.parse_fapiao()
        job_finish(sd["6"], sd["7"], sd["8"], sd["3"], sd["4"], sd["5"], '1', '成功爬取')
        logger.info("发票汇总完成")
    except Exception as e:
        logger.error(e)
        job_finish(sd["6"], sd["7"], sd["8"], sd["3"], sd["4"], sd["5"], '-1', 'error')
    print('jobs[ts_id=%s] done' % batchid)
    result = True
    return result


while True:
    # ss=redis_cli.lindex("list",0)
    ss = redis_cli.lpop("sz_credit_list")
    if ss is not None:
        # print(redis_cli.lpop("list"))
        sd = json.loads(ss)
        run_test(sd["1"], sd["2"], sd["3"], sd["4"], sd["5"])
    else:
        time.sleep(10)
        print("no task waited")
