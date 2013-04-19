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
import getopt
from cls_mysql import *
import json

from BeautifulSoup import BeautifulSoup,SoupStrainer
import hashlib
import socket
from multiprocessing import Process
timeout = 30
from time import localtime,strftime,strptime
import time
socket.setdefaulttimeout(timeout)
log_path = "/home/log/ty-mt.log" 
_DEBUG_ = 0
_PRO_LIMIT_ = 3
_HTTP_ERROR_SLEEP_ = 3
#lock_file = "/home/log/ty.lock"
pid = "/home/log/ty-mt.pid"
_DATA_DIR_ = "/data/backup/tianya"
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
        logger.LOG("[ERROR]URL error: %s", e)
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
    def save_content(self, content, art_id, page_id):
        if content is None:
            logger.LOG("[ERROR]save_content, content is None")
            return
        try:
            logger.LOG("[INFO]save article: %s-%s", art_id, page_id)
            save_dir = _DATA_DIR_ + "/" + str(art_id) 
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)
            file(save_dir + "/" + str(page_id) + ".txt", 'w').write(content)
        except Exception,e:
            print "[ERR]save content failed! %s" % e
            return

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
    # 检查当前的文章(文章ID，楼号，作者，文章内容是否重复)
    # 当前检查方式
    # 1. 检查是否为重复的楼: 为了避免重复爬楼
    # 2. 检查是否为重复的帖子：排除论坛返回的帖子是历史的帖子，楼号是最新的楼号
    def article_in_box(self, art_id, floor_id, author_id, article_md5, t):
        global logger
        if art_id is None or floor_id is None:
            return True
        sql = "select count(1) from novel where novel_id = '%d' and floor_id = '%d' " % (art_id, int(floor_id))
        if int(self.dbc.getOne(sql)) > 0:
            return True
        sql = "select count(1) from novel where novel_id = '%d' and author_id = '%d' and md5='%s' " % (art_id, author_id, article_md5)
        if int(self.dbc.getOne(sql)) == 0 :
            return False
        sql = "select unix_timestamp(ctime) > unix_timestamp('%s') from novel where novel_id = '%d' and floor_id < '%s' ORDER BY floor_id DESC LIMIT 1" % (t, art_id, floor_id)
        if int(self.dbc.getOne(sql)) > 0:
            logger.LOG("[ERROR]bad post detected, novel_id<%d>, floor_id<%s>, ctime<%s>", art_id, floor_id, t)
            return True
        return False
    def loop_get_content(self, art_url, art_id, start_page):
        global logger
        while 1:
            current_url = art_url + "&p=" + str(start_page)
            (s, is_last_page) = self.get_content(current_url, art_id, start_page)
            #FIXME: DEBUG CODE HEAE
            #break
            if s < 0 or is_last_page:
                if is_last_page:
                    sql = "UPDATE novel_list SET need_init = 0 where novel_id = '%d' " % (art_id)
                    self.dbc.query(sql)
                break
            start_page = start_page + 1
    def add_article(self, art_url, art_title, art_id, author_name, cat_id):        
        global logger

        author_id = self.get_author_id(author_name)
        art_title = self.conn.escape_string(art_title)
        sql = "INSERT INTO novel_list set novel_id = '%s', novel_title = '%s', cat_id = '%s', author_id ='%d', \
                orig_url = '%s', need_init = '1', enabled = 1, grab_interval = 20, ctime = NOW()" % (art_id, art_title, cat_id, author_id, art_url)
        return self.dbc.query(sql)

    # 获取给定的page_id的文章
    def get_content(self, art_url,art_id,page_id, is_roger_task = 0):
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
            logger.LOG("[ERROR]download error for article %d, page %d!", art_id, page_id)
            return (-1, None)
        logger.LOG("[INFO]content size %d", len(content))
        if len(content) < 5000:
            logger.LOG("[ERROR]maybe bad response,dumping it...")
            self.save_content(content, art_id, page_id)
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
            # get article md5
            article_md5 = hashlib.md5(article_content).hexdigest()
            author_id = self.get_author_id(author_list[i])
            ctime = post_t[i]
            if self.article_in_box(art_id, floor_id, author_id, article_md5, ctime):
                logger.LOG("[WARN]article already in box: %d-<floor_id>%s-<md5>%s", art_id, floor_id, article_md5)
                continue
            new_post_download += 1    
            sql = """INSERT INTO novel SET novel_id = '%d', floor_id = '%s', content = '%s', author_id = '%d', ctime = '%s', md5='%s',  mtime = NOW() , page_id = '%d' """ % (art_id, floor_id, article_content, author_id, ctime, article_md5, page_id)
            self.dbc.query(sql)
        logger.LOG("[INFO]%d new post download!", new_post_download)
        if not is_last_page and not is_roger_task and new_post_download == 0 and page_id != 1:
            logger.LOG("[ERROR]shit happens, dump page...")
            self.save_content(content, art_id, page_id)
        if len(floor_id_list) == 0:
            return (0, None)
        if not is_roger_task:
            last_floor_id = floor_id_list[-1]
            sql = "UPDATE novel_list SET last_floor_id = '%s', last_page_id = '%s', last_grab_time = unix_timestamp()  where novel_id = '%d' " % (last_floor_id, page_id, art_id)
            self.dbc.query(sql)
        return (0, is_last_page)

def get_task_list(task_type, task_limit):
    global logger, dbc
    #FIXME: more complex task rules
    
    sql = "select id, novel_id, orig_url, last_floor_id, last_page_id, last_grab_time from novel_list where last_grab_time+60 * grab_interval < unix_timestamp() and enabled = 1 and need_init = %d ORDER BY last_grab_time ASC LIMIT %d" % (task_type, task_limit)
    return dbc.getAll(sql)
# 获取补刀的task列表
# 补刀task的表结构设计
# id| novel_id| page_list| status
# page_list 是数组，json格式
# status 是状态，是否处理过这个task了
def get_roger_task():
    global logger, dbc
    sql = "select j.id, j.novel_id, j.lost_pages, l.orig_url from novel_ota_jobs as j left join novel_list as l ON j.novel_id = l.novel_id where j.status = 0 limit 10"
    return dbc.getAll(sql)
def gogogo(url, novel_id, last_page_id):
    w = Worker()
    w.loop_get_content(url, novel_id, last_page_id)
def roger_go(roger_id, url, novel_id, page_list):
    global logger, dbc
    w = Worker()
    for p in page_list:
        current_url = url + "&p=" + str(p)
        w.get_content(current_url, novel_id, p, 1)
    sql = "update novel_ota_jobs set status = 1, ftime=NOW() where id = '%d'" % roger_id
    dbc.query(sql)

def keep_rolling(task_type):
    global logger, dbc
    logger.LOG("[INFO]start rolling..., task type is %s", task_type)
    if task_type == 1:
        task_list = get_task_list(task_type, _PRO_LIMIT_)
        if task_list is None:
            logger.LOG("[INFO]empty task...")
            return
        for t in task_list:
            p = Process(target=gogogo, args=(t['orig_url'], t['novel_id'], t['last_page_id'],))
            p.start()
        for t in task_list:
            p.join()
    elif task_type == 2:
        task_list = get_roger_task()
        print task_list
        if task_list is None:
            logger.LOG("[INFO]empty task...")
            return
        for t in task_list:
            p = Process(target=roger_go, args=(t['id'], t['orig_url'], t['novel_id'], json.loads(t['lost_pages']),))
            p.start()
        for t in task_list:
            p.join()
        
            
    else:
        task_type = 0
        task_list = get_task_list(task_type, 100)
        if task_list is None:
            logger.LOG("[INFO]empty task...")
            return
        w = Worker()
        for t in task_list:
            w.loop_get_content(t['orig_url'], t['novel_id'], t['last_page_id'])
def check_lost_floors(article_id, start_floor_id):
    global logger, dbc
    if not article_id or not start_floor_id:
        return []
    lost_floors = []
    p_floor = 0
    n_floor = 0
        
    sql = "SELECT floor_id from novel where novel_id = '%s' and floor_id > '%s' ORDER BY floor_id ASC" % (article_id, start_floor_id)
    for f in dbc.getAll(sql):
        p_floor = f["floor_id"]
        #print "p_floor is %d" % p_floor
        #print "n_floor is %d" % n_floor
        if n_floor == 0:
            n_floor = p_floor
            continue
        while n_floor + 1 != p_floor:
            #print "@@@@floor %d lost" % n_floor
            lost_floors.append(n_floor+1)
            n_floor = n_floor + 1
        n_floor = n_floor + 1
    lost_pages = {}
    
    for c in lost_floors:
        a1 = c/20
        a2 = c%20
        
        if a2 != 0:
            a1 = a1 + 1
        if lost_pages.has_key(a1):
            lost_pages[a1].append(c) 
        else:
            lost_pages[a1] = [c]
            
        #print "lost %d, in page %d" % (c, a1)
    lost_page_final = []
    for k in lost_pages.keys():
        if len(lost_pages[k]) > 0:
            print k, lost_pages[k]
            lost_page_final.append(k)
    if len(lost_page_final) > 0:
        to_json = json.dumps(lost_page_final)
        sql = "INSERT INTO novel_ota_jobs SET novel_id = '%s', lost_pages='%s', ctime=NOW(), status=0" % (article_id, to_json)
        dbc.query(sql)
    
   
           
        
    
def extract_article_meta(art_url):
    # 给定URL，分析出其中的作者，文章标题等信息
    global logger
    if art_url is None:
        return (-1, None)
    logger.LOG("[INFO]download article %s", art_url)
    if not _DEBUG_:
        retry = 2
        while retry > 0:
            content = download(art_url)
            if content is not None:
                break
            retry = retry - 1
            time.sleep(_HTTP_ERROR_SLEEP_)
    else:
        #content = file("./index.html").read()
        pass
    if content is None:
        logger.LOG("[ERROR]download error!")
        return (-1, None)
    try:
        article_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "id" : "post_head" }),smartQuotesTo=None)
        author_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("div", { "class" : "atl-info" }),smartQuotesTo=None)
        url_tag = BeautifulSoup(content, parseOnlyThese=SoupStrainer("meta", { "http-equiv" : "mobile-agent" }),smartQuotesTo=None)
        coding = article_tag.originalEncoding
        atl_title_content = article_tag.find('span', {"class": "s_title"}).find('span').renderContents(coding)
        author_name = ""
        for t in author_tag:
            author_name = t.find('span').find('a').string.encode(coding)
            break
        #mobile_url_content = url_tag.renderContents(coding)
        art_id = ""
        art_cat = ""
        mobile_url = ""
        m_url_item_re = re.compile("item=(?P<item>.+)&id=(?P<id>[0-9]+)")
        m_url_re= re.compile("url=(?P<url>(.+))\"")
        m = m_url_re.search(url_tag.renderContents(coding))
        if m is not None:
            mobile_url = m.group("url").replace("&amp;", "&")
        m = m_url_item_re.search(mobile_url)
        if m is not None:
            art_id =  m.group("id")
            art_cat =  m.group("item")
    except Exception,e:
        print "exception,", e
        logger.LOG("[ERROR]error when parsing article content,%s",e)
        return (-1, None)
    w = Worker()
    ret = w.add_article(mobile_url, atl_title_content, art_id, author_name, art_cat)
    if ret is True:
        logger.LOG("[INFO]insert article <%s> done1", atl_title_content)
    else:
        logger.LOG("[ERROR]error when insert article <%s>", atl_title_content)
      

def usage():
    print "Usage:ty_mt.py [-t|-o] args...."
def check_lock(lock_file):
    global logger
    pid = os.getpid()
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
def on_exit(lock_file):
    global logger
    logger.LOG('[INFO]delete lock file!')
    if os.path.exists(lock_file):
        os.unlink(lock_file)
if __name__ == '__main__':
    #主程序入口
    #logger
    logger = FileLog(log_path)

    dbc = DBC()
    dbc.rw_connect()
    dbc.query("use manage")
    conn = dbc.get_conn()
    r_type = "0"
    r_task_type = 0
    # 获取参数
    try:
        opts,args = getopt.getopt(sys.argv[1:], "ht:o:", ["help", "type=", "url="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-t", "--type"):
            r_type = arg
        elif opt == "--url":
            r_url = arg
        elif opt == "-o":
            r_task_type = int(arg)

    if r_type == "0": 
        lock_file = "/home/log/tianya_lock" + "." + str(r_task_type)
        check_lock(lock_file)
        keep_rolling(r_task_type)
        on_exit(lock_file)
    elif r_type == "1":
        extract_article_meta(r_url)
    elif r_type == "2":
        ## FIXME: 这里还是写死的
        check_lost_floors("3970326", "100")

    else:
        print "bad input"
        pass
