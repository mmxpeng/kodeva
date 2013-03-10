#!/usr/local/bin/python
#-*- coding:utf-8 -*-
# create by guojinpeng
# for newsmth.net
# use BeautifulSoup as the HTML parser
# use urllib to fetch url
# download pages from newsmth.net
# $Id:$
import os
import string
import sys
import re
import urllib2

from cls_mysql import *

#from _mysql_exceptions import *
from BeautifulSoup import BeautifulSoup,SoupStrainer
# for image process
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
def article_in_box(art_id, floor_id):
    global dbc,logger
    if art_id is None or floor_id is None:
        return True
    sql = "select count(1) from novel where novel_id = '%d' and floor_id = '%d' " % (art_id, int(floor_id))
    if int(dbc.getOne(sql)) > 0:
        return True
    return False
def get_task_list():
    global dbc,logger
    #FIXME: more complex task rules
    sql = "select id, novel_id, orig_url, last_floor_id, last_page_id, last_grab_time from novel_list where last_grab_time+60 * grab_interval < unix_timestamp() and enabled = 1"
    return dbc.getAll(sql)
def keep_rolling():
    global dbc,logger
    task_list = get_task_list()
    if task_list is None:
        logger.LOG("[INFO]empty task...")
        return
    for t in task_list:
        print t
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
def get_pager_and_download(first_page):
    global dbc,logger,conn
    logger.LOG("downloading page %s", first_page)
    content = download(_YIDU_DOMAIN_ + first_page)
    if content is None:
        print "[ERR][pager_download]first page is empty"
        return
    #保存第一篇文章
    save_content(first_page, content)
    #分析文章首页的分页
    try:
        pager_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "pageNum1" }),smartQuotesTo=None)
    except Exception,e:
        logger.LOG("error when parsing pager content,%s",e)
        try:
            pager_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "pageNum1" }),smartQuotesTo=None)
        except Exception,e:
            logger.LOG("error when parsing article content ,%s",e)
            return None
    #找出所有分页，保存页面
    for tag in pager_tag.findAll('a', {"class" : "numPage"}):
        print "====@@@@===="
        a_url = tag['href']
        a_page = tag.string
        print a_url, a_page
        logger.LOG("downloading page %s", a_url)
        a_content = download(_YIDU_DOMAIN_ + a_url)
        save_content(a_url, a_content)
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
def get_article_list(index_url):
    if index_url is None:
        return
    content = download(index_url)
    #content = file('./a_list2.html').read()
    try:
        list_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("li", { "class" : "al" }),smartQuotesTo=None)
    except Exception,e:
        logger.LOG("error when parsing pager content,%s",e)
        try:
            pager_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("li", { "class" : "al" }),smartQuotesTo=None)
        except Exception,e:
            logger.LOG("error when parsing article content ,%s",e)
            return None
    for tag in list_tag.findAll('a', {"class" : "list"}):
        l_url = tag['href']
        l_a_name = tag.string
        print "article %s, article_first_url is %s" % (l_a_name, l_url)
        get_pager_and_download(l_url)
def loop_get_content(art_url, art_id, start_page):
    global dbc,logger,conn
    while 1:
        current_url = art_url + "&p=" + str(start_page)
        (s, is_last_page) = get_content(current_url, art_id, start_page)
        if s < 0 or is_last_page:
            break
        start_page = start_page + 1
        
            
def get_content(art_url,art_id,page_id):
    global dbc,logger,conn
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
        #print "post_time is " + t.find('span').string + "\tauthor is " + t.find('a').string
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
        article_content = conn.escape_string(article_list[i])
        if article_in_box(art_id, floor_id):
            logger.LOG("[WARN]article already in box: %d-%s", art_id, floor_id)
            continue
        new_post_download += 1    
        author_id = get_author_id(author_list[i])
        sql = """INSERT INTO novel SET novel_id = '%d', floor_id = '%s', content = '%s', author_id = '%d', ctime = '%s' """ % (art_id, floor_id_list[i], article_content, author_id, post_t[i])
        dbc.query(sql)
    logger.LOG("[INFO]%d new post download!", new_post_download)
    last_floor_id = floor_id_list[-1]
    sql = "UPDATE novel_list SET last_floor_id = '%s', last_page_id = '%s', last_grab_time = unix_timestamp() where novel_id = '%d' " % (last_floor_id, page_id, art_id)
    dbc.query(sql)
    return (0, is_last_page)

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
    #get_content_list(1696)
    #get_pager_and_download("./first.html")
#article_index = "http://www.tianyayidu.com/channel.php?aid=150&action=hot&page=2"
#get_article_list(article_index)
    keep_rolling()
    #get_content("http://m.tianya.cn/bbs/art.jsp?item=16&id=716391&p=3895&vu=62813539168", "asdasd")
    logger.LOG('删除lock文件！')
    if os.path.exists(lock_file):
        os.unlink(lock_file)
