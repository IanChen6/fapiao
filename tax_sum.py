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

import os
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
from guoshui import guoshui

class fapiao(guoshui):
    def __init__(self, user,pwd, batchid, companyid, customerid,logger):
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
        self.logger=logger

    def parse_fapiao(self):
        try:
            cookies = self.login()
            self.logger.info("customerid:{}获取cookies".format(self.customerid))
            jsoncookies = json.dumps(cookies,ensure_ascii=False)
            if "账号和密码不匹配" in jsoncookies:
                self.logger.warn("customerid:{}账号和密码不匹配".format(self.customerid))
                job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-2', "账号和密码不匹配")
                return
            with open('cookies/{}cookies.json'.format(self.customerid), 'w') as f:  # 将login后的cookies提取出来
                f.write(jsoncookies)
                f.close()
        except Exception as e:
            self.logger.warn(e)
            self.logger.warn("customerid:{}登陆失败".format(self.customerid))
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "登录失败")
            return False
        try:
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = (
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36')
            dcap["phantomjs.page.settings.loadImages"] = True
            # browser = webdriver.PhantomJS(
            #     executable_path='D:/BaiduNetdiskDownload/phantomjs-2.1.1-windows/bin/phantomjs.exe',
            #     desired_capabilities=dcap)
            browser = webdriver.PhantomJS(
            executable_path='/home/tool/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
            desired_capabilities=dcap)
            browser.implicitly_wait(10)
            browser.viewportSize = {'width': 2200, 'height': 2200}
            browser.set_window_size(1400, 1600)  # Chrome无法使用这功能
            # browser = webdriver.Chrome(executable_path='D:/BaiduNetdiskDownload/chromedriver.exe')  # 添加driver的路径
        except Exception as e:
            self.logger.warn(e)
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "浏览器启动失败")
            return False
        try:
            index_url = "http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html"
            browser.get(url=index_url)
            browser.delete_all_cookies()
            with open('cookies/{}cookies.json'.format(self.customerid), 'r', encoding='utf8') as f:
                cookielist = json.loads(f.read())
            for (k, v) in cookielist.items():
                browser.add_cookie({
                    'domain': '.szgs.gov.cn',  # 此处xxx.com前，需要带点
                    'name': k,
                    'value': v,
                    'path': '/',
                    'expires': None})
            resp=browser.get(url="http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/fp/zzszyfpdksq/fp_zzszyfpdksq.html")
            p = browser.page_source
            browser.switch_to_frame("txsqxx")
            fwpp = browser.find_element_by_css_selector('#dkfpjefw').text
            hwpp = browser.find_element_by_css_selector('#dkfpje').text
            fwzp = browser.find_element_by_css_selector('#zpdkjefw').text
            hwzp = browser.find_element_by_css_selector('#zpdkjehw').text
            gsparams = (self.companyid, self.customerid, fwpp, hwpp, fwzp, hwzp)
            self.logger.info(gsparams)
            self.logger.info("爬取完成")
            self.insert_db("[dbo].[Python_Serivce_GSInvoiceSummary_Add]", gsparams)
            job_finish(sd["6"], sd["7"], sd["8"], sd["3"], sd["4"], sd["5"], '1', '成功爬取')
            logger.info("发票汇总完成")
        except Exception as e:
            print("error")
            print(e)
            job_finish(sd["6"], sd["7"], sd["8"], sd["3"], sd["4"], sd["5"], '-1', 'error')

import sys
logger = create_logger(path=os.path.dirname(sys.argv[0]).split('/')[-1])
redis_cli = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
def run_test(user, pwd, batchid, companyid, customerid):
    print("++++++++++++++++++++++++++++++++++++")
    print('jobs[ts_id=%s] running....' % batchid)
    time.sleep(3)
    try:
        hz = fapiao(user=user,pwd=pwd , batchid=batchid, companyid=companyid,
                          customerid=customerid,logger=logger)
        hz.parse_fapiao()
    except Exception as e:
        logger.error(e)
    print('jobs[ts_id=%s] done' % batchid)
    result = True
    return result


while True:
    # ss=redis_cli.lindex("list",0)
    ss = redis_cli.lpop("fphz_list")
    if ss is not None:
        # print(redis_cli.lpop("list"))
        sd = json.loads(ss)
        run_test(sd["1"], sd["2"], sd["3"], sd["4"], sd["5"])
    else:
        time.sleep(10)
        print("no task waited")
