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
timeout = 30
from time import localtime,strftime,strptime
import time
socket.setdefaulttimeout(timeout)
log_path = "/home/log/ty.log" 
__LIMIT__ = 1000
_HTTP_ERROR_SLEEP_ = 3
lock_file = "/home/log/ty.lock"
pid = "/home/log/ty.pid"
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
def get_author_id(author_name):
    global dbc,logger
    if author_name is None:
        return None
    sql = "select author_id from author_list where author_name = '%s' and author_from  = 'tianya' limit 1" % author_name
    author_id = dbc.getOne(sql)
    if author_id is None:
        return insert_author(author_name)
    return int(dbc.getOne(sql))
def insert_author(author_name):
    global dbc,logger
    if author_name is None:
        return None
    sql = "insert into author_list set author_name = '%s', author_from = 'tianya'" % author_name
    dbc.query(sql)
    return dbc.get_insert_id()
def article_in_box(art_id, floor_id, author_id, article_md5):
    global dbc,logger
    if art_id is None or floor_id is None:
        return True
    sql = "select count(1) from novel where novel_id = '%d' and floor_id = '%d' " % (art_id, int(floor_id))
    if int(dbc.getOne(sql)) > 0:
        return True
    sql = "select count(1) from novel where novel_id = '%d' and author_id = '%d' and md5='%s' " % (art_id, author_id, article_md5)
    if int(dbc.getOne(sql)) > 0:
        logger.LOG("[ERROR]detected duplicate article, md5 %s, author_id %d, article_id %d", article_md5, author_id, art_id)
        return True
    return False
def get_task_list():
    global dbc,logger
    #FIXME: more complex task rules
    sql = "select id, novel_id, orig_url, last_floor_id, last_page_id, last_grab_time from novel_list where last_grab_time+60 * grab_interval < unix_timestamp() and enabled = 1 and need_init = 0"
    return dbc.getAll(sql)
def keep_rolling():
    global dbc,logger
    logger.LOG("[INFO]start rolling...")
    task_list = get_task_list()
    if task_list is None:
        logger.LOG("[INFO]empty task...")
        return
    for t in task_list:
        target_url = t['orig_url']
        loop_get_content(t['orig_url'], t['novel_id'], t['last_page_id'])
        
"""
def get_content_list(aid):
    global dbc,logger

    start_url = "http://www.yidudu.com/art_%s.html" % aid
    print "download url is %s" % start_url
    #content = download(start_url)
    content = file("./art_1696.html").read()
    if content is None:
        print "download error!"
        return 
    try:
        print "download ok!"
        tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer('div',"book_article_listtext"),smartQuotesTo=None)
    except Exception,e:
        logger.LOG("error when parsing article list ,%s",e)
        try:
            tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer('div',"book_article_listtext"))
        except Exception,e:
            logger.LOG("error when parsing article list ,%s",e)
            return {}
    coding = tag.originalEncoding
    limit = 0
    for dd in tag.findAll('dd'):
        limit += 1
        if dd.find('a'):
            #encode 
            ac = dd.find('a').string.encode(coding)
            ah = dd.find('a')['href']
            print ac,ah
            article_url = "http://www.yidudu.com" + ah
            if limit > 10:
                break
            get_content(article_url,ac)
"""
def save_content(a_url, a_content):
    if a_url is None or a_content is None:
        print "a_url or a_content is None"
        return
    logger.LOG("save page %s", a_url)
    try:
        article_id = (a_url.split('.')[0]).split('-')[2]
        logger.LOG("article_id: %s", article_id)
        save_dir = _DATA_DIR_ + "/" + article_id
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)
        file(save_dir + "/" + a_url, 'w').write(a_content)
    except Exception,e:
        print "[ERR]save content failed! %s" % e
def loop_get_content(art_url, art_id, start_page):
    global dbc,logger,conn
    while 1:
        current_url = art_url + "&p=" + str(start_page)
        (s, is_last_page) = get_content(current_url, art_id, start_page)
        if s < 0 or is_last_page:
            break
        start_page = start_page + 1
        
def insert_hot_article(artname, url):
    global dbc,logger,conn
    sql = "INSERT INTO novel_queue_a (novel_title,url,ctime) values  ('%s','%s',NOW())" % (artname, "http://bbs.tianya.cn" + url)
    dbc.query(sql)

def get_hot_alist_from_page(art_url):
    global dbc,logger,conn
    if art_url is None:
        return (-1, None)
    logger.LOG("[INFO]download article %s", art_url)
    retry = 2
    """
    while retry > 0:
        content = download(art_url)
        if content is not None:
            break
        retry = retry - 1
        time.sleep(_HTTP_ERROR_SLEEP_)
    """
    content = file("./article.html").read()
    if content is None:
        logger.LOG("[ERROR]download error!")
        return (-1, None)
    #FIXME: check if content is invalid ??
    logger.LOG("[INFO]content size %d", len(content))
    try:
        article_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "mt5" }),smartQuotesTo=None)
    except Exception,e:
        logger.LOG("[ERROR]error when parsing article content,%s",e)
        try:
            article_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "mt5" }),smartQuotesTo=None)
        except Exception,e:
            logger.LOG("[ERROR]error when parsing article content ,%s",e)
            return (-1, None)
    coding = article_tag.originalEncoding
    count = 0
    remove_span_tag = re.compile("\<span .+\>.*\<\/span\>")
    print 'coding is ', coding
    for tbody in article_tag.findAll('tbody'): 
        if count == 0:
            count += 1
            continue
        #print d.renderContents()
        for d in tbody.findAll('tr'):
            dd = d.find('td')
            num = dd.find('span').string
            url = dd.find('a')['href']
            #title = dd.find('a').contents[0].string.encode(coding)
            title = dd.find('a').contents[0]
            
            #title = remove_span_tag.sub('', title)
            #print "%s\t%s\t%s" % (num, url, title)
            #title = conn.escape_string(title)
            #sql = "INSERT INTO novel_queue_a (novel_title,url,ctime) values  ('%s','%s',NOW())" % (title, "http://bbs.tianya.cn" + url)
            #dbc.query(sql)
            insert_hot_article(title, url)
        count += 1
    return (0, count)

if __name__ == '__main__':
    #主程序入口
    #logger
    logger = FileLog(log_path)
    pid = os.getpid()

    dbc = DBC()
    dbc.rw_connect()
    dbc.query("use manage")
    conn = dbc.get_conn()
    get_hot_alist_from_page("http://bbs.tianya.cn/list.jsp?item=feeling&grade=3&sub=1&order=1")

