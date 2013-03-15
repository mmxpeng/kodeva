#!/usr/local/bin/python
#-*- coding:utf-8 -*-
# create by guojinpeng
# for m.tianya.cn
# use BeautifulSoup as the HTML parser
# use urllib to fetch url

import os
import string
import sys
import re
import urllib2

from cls_mysql import *

from BeautifulSoup import BeautifulSoup,SoupStrainer
import hashlib
import socket
from multiprocessing import Process
timeout = 30
from time import localtime,strftime,strptime
import time
socket.setdefaulttimeout(timeout)
log_path = "/home/log/ty-mt.log" 
_PRO_LIMIT_ = 3
_HTTP_ERROR_SLEEP_ = 3
lock_file = "/home/log/ty-mt.lock"
pid = "/home/log/ty-mt.pid"
_DATA_DIR_ = "/home/guojinpeng/source/tianyayidu/download"
_YIDU_DOMAIN_ = "http://www.tianyayidu.com/"
_M_TIANYA_DOMAIN = "m.tianya.cn"
_SPIDER_UA_ = """Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)"""
_FAKE_COOKIE_ = """__guid=1096598807; __guid2=1096598807; __gads=ID=01236f3f4afe7348:T=1352008647:S=ALNI_MZhTjSHm14Dds2Z-mU4Ss9iwqyX3g; Hm_lvt_b854d43c0d1cff2aae627df1f44635cc=1353850405; __utma=22245310.374392117.1356246603.1357272589.1357276190.3; __utmz=22245310.1357276190.3.3.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __cid=1; Hm_lvt_80579b57bf1b16bdf88364b13221a8bd=1361758374; __visit=wpopa%3D382; time=ct=1362729828.846; __ptime=1362729829890; __ty_tips_info=c%3D1%26t%3D57830%26d%3D8%26fc%3D0; wl_visit=u=62813539168; Hm_lpvt_5bd6030ededd64a38d1313936f792f61=1362814859; Hm_lvt_5bd6030ededd64a38d1313936f792f61=1362814859"""
def download (url):
    global logger
    if url is None:
        return None
    if len(url)<7:
        return None
    proxy_support = urllib2.ProxyHandler({"http" : "http://127.0.0.1:7168"})
    #opener = urllib2.build_opener(proxy_support)
    opener = urllib2.build_opener()  
    request = urllib2.Request(url)
    #设定爬虫的User-Agent信息
    request.add_header('User-Agent', _SPIDER_UA_)
    #设定爬虫的Cookie
    request.add_header('Cookie', _FAKE_COOKIE_)
    #request.add_header('Accept-Encoding', "gzip, deflate")
    
    #fetch the content of a page
    try:
        content = opener.open(request).read()
        #content = page.read()
    except urllib2.HTTPError,e:
        print "HTTP Error:",e.code,e.msg
        logger.LOG("[ERROR]HTTP Error: %d,%s",e.code,e.msg)
        return None
    except socket.error,e:
        print "socket error:",e
        logger.LOG("[ERROR]socket error: %d,%s", e.code, e.msg)
        return None
    except urllib2.URLError,e:
        print "URLError:",e
        logger.LOG("[ERROR]URL error: %d,%s", e.code, e.msg)
        return None
    except Exception,e:
        print "Error:",e
        logger.LOG("[ERROR]Other error: %d,%s", e.code, e.msg)
        return None
    return content
class Worker():
    def __init__(self):
        global logger
        self.dbc = DBC()
        if self.dbc.rw_connect() == -1:
            logger.LOG("[ERROR] can not connect to mysql")
            sys.exit()
        self.dbc.query("use manage")
        self.conn = self.dbc.get_conn()
        pass
    def get_author_id(self, author_name):
        global logger
        if author_name is None:
            return None
        sql = "select author_id from author_list where author_name = '%s' and author_from  = 'tianya' limit 1" % author_name
        author_id = self.dbc.getOne(sql)
        if author_id is None:
            return self.insert_author(author_name)
        return int(self.dbc.getOne(sql))
    def insert_author(self, author_name):
        global logger
        if author_name is None:
            return None
        sql = "insert into author_list set author_name = '%s', author_from = 'tianya'" % author_name
        self.dbc.query(sql)
        return self.dbc.get_insert_id()
    def article_in_box(self, art_id, floor_id):
        global logger
        if art_id is None or floor_id is None:
            return True
        sql = "select count(1) from novel where novel_id = '%d' and floor_id = '%d' " % (art_id, int(floor_id))
        if int(self.dbc.getOne(sql)) > 0:
            return True
        return False
    def loop_get_content(self, art_url, art_id, start_page):
        global logger
        while 1:
            current_url = art_url + "&p=" + str(start_page)
            (s, is_last_page) = self.get_content(current_url, art_id, start_page)
            #FIXME: DEBUG CODE HEAE
            break
            if s < 0 or is_last_page:
                if is_last_page:
                    sql = "UPDATE novel_list SET need_init = 0 where novel_id = '%d' " % (art_id)
                    self.dbc.query(sql)
                break
            start_page = start_page + 1
            
    def get_content(self, art_url,art_id,page_id):
        global logger
        if art_url is None:
            return (-1, None)
        logger.LOG("[INFO]download article %d, page %d", art_id, page_id)
        retry = 2
        while retry > 0:
            content = download(art_url)
            if content is not None:
                break
            retry = retry - 1
            time.sleep(_HTTP_ERROR_SLEEP_)
        #content = file("./article.html").read()
        if content is None:
            logger.LOG("[ERROR]download error!")
            return (-1, None)
        try:
            article_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "sp lk" }),smartQuotesTo=None)
            author_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "lk" }),smartQuotesTo=None)
            pager_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "pg" }),smartQuotesTo=None)
        except Exception,e:
            logger.LOG("[ERROR]error when parsing article content,%s",e)
            try:
                article_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "sp lk" }),smartQuotesTo=None)
                author_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "lk" }),smartQuotesTo=None)
            except Exception,e:
                logger.LOG("[ERROR]error when parsing article content ,%s",e)
                return (-1, None)
        #print len(article_tag), len(author_tag)
        #remove_adtag_re = re.compile("\<div class=\"article_adv_1\"\>.*\<\/div\>")
        remove_adtag_re = re.compile("\<a href=\"rep\.jsp.*\"\>.*\<\/a\>")
        floor_id_re = re.compile("\<a href=\"rep\.jsp.*\"\>回复第(?P<floor_id>\d+)楼\<\/a\>")
        last_page_re= re.compile("\<a href=\"art\.jsp.*\"\>下一页\<\/a\>")
        post_t = []
        article_list = []
        author_list = []
        floor_id_list = []
        # 判断是否是最后一页
        is_last_page = 0
        last_page_content = pager_tag.renderContents()
        m = last_page_re.search(last_page_content)
        if m is None:
            is_last_page = 1
        #parse the author tags
        coding = author_tag.originalEncoding
        for t in author_tag:
            tt = t.renderContents()
            print "post_time is " + t.find('span').string + "\tauthor is " + t.find('a').string
            post_timestamp = t.find('span', {"class": "gray"}).string.encode(coding)
            author_name = t.find('a').string.encode(coding)
            post_t.append(post_timestamp)
            author_list.append(author_name)
        #parse the article tags
        coding = article_tag.originalEncoding
        for c in article_tag:
            #print "---------------content is \n" + c.renderContents()
            aa = c.renderContents(coding)
            m = floor_id_re.search(aa)
            if m is not None:
                floor_id = m.group("floor_id")
            else:
                floor_id = "0"
           
            #print "------floor_id is " + floor_id
            floor_id_list.append(floor_id)
            aa = remove_adtag_re.sub("", aa) 
            
            
            article_list.append(aa)
            #print "---------------simple article content is -------------\n"
            #print aa
        new_post_download = 0    
        for i in range(0,len(post_t)):
            
            #print "第%s楼" % floor_id_list[i]
            #print "author: " + author_list[i].decode(coding) + "\tpost_time: " + post_t[i]
            #print article_list[i]
            # 检查文章是否已经下载过
            floor_id = floor_id_list[i]
            article_content = self.conn.escape_string(article_list[i])
            if self.article_in_box(art_id, floor_id):
                logger.LOG("[WARN]article already in box: %d-%s", art_id, floor_id)
                continue
            new_post_download += 1    
            author_id = self.get_author_id(author_list[i])
            sql = """INSERT INTO novel SET novel_id = '%d', floor_id = '%s', content = '%s', author_id = '%d', ctime = '%s' """ % (art_id, floor_id_list[i], article_content, author_id, post_t[i])
            self.dbc.query(sql)
        logger.LOG("[INFO]%d new post download!", new_post_download)
        if len(floor_id_list) == 0:
            return (0, None)
        last_floor_id = floor_id_list[-1]
        sql = "UPDATE novel_list SET last_floor_id = '%s', last_page_id = '%s', last_grab_time = unix_timestamp()  where novel_id = '%d' " % (last_floor_id, page_id, art_id)
        self.dbc.query(sql)
        return (0, is_last_page)

def get_task_list(task_type, task_limit):
    global logger, dbc
    #FIXME: more complex task rules
    sql = "select id, novel_id, orig_url, last_floor_id, last_page_id, last_grab_time from novel_list where last_grab_time+60 * grab_interval < unix_timestamp() and enabled = 1 and need_init = %d LIMIT %d" % (task_type, task_limit)
    return dbc.getAll(sql)
def gogogo(url, novel_id, last_page_id):
    w = Worker()
    w.loop_get_content(url, novel_id, last_page_id)
def keep_rolling():
    global logger, dbc
    logger.LOG("[INFO]start rolling...")
    task_type = 1
    task_list = get_task_list(task_type, _PRO_LIMIT_)
    if task_list is None:
        logger.LOG("[INFO]empty task...")
        return
    for t in task_list:
        p = Process(target=gogogo, args=(t['orig_url'], t['novel_id'], t['last_page_id'],))
        p.start()
    for t in task_list:
        p.join()
if __name__ == '__main__':
    #主程序入口
    #logger
    logger = FileLog(log_path)
    pid = os.getpid()

    dbc = DBC()
    dbc.rw_connect()
    dbc.query("use manage")
    conn = dbc.get_conn()
    if len(sys.argv) < 2:
        search_days = 7
    else:
        search_days = int(sys.argv[1])
    if os.path.exists(lock_file):
        logger.LOG("[WARN]find lock file...")
        old_pid = file(lock_file).read()
        mtime=os.path.getmtime(lock_file)
        time_now=time.time()
        time_diff = time_now-mtime
        if time_diff > 60*60:
            os.system("kill "+old_pid)
            logger.LOG("[INFO]killing timeout process,pid:%s",old_pid)
            os.unlink(lock_file)
            file(lock_file,'w').write(str(pid))
        else:
            logger.LOG("[INFO]find running process,running time %s seconds,exit!",time_diff)
            sys.exit('find running process, graceful exit!')

    else:
        #创建lock文件，写入pid
        logger.LOG("[INFO]creating lock file,writing pid,%s",pid)
        file(lock_file,'w').write(str(pid))
    keep_rolling()
    logger.LOG('[INFO]delete lock file!')
    if os.path.exists(lock_file):
        os.unlink(lock_file)
