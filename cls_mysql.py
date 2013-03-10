#!/usr/bin/python
#-*- coding:gb2312 -*-
# create by guojinpeng
# lib for use mysql
# 操作MySQL数据库的库，为代码重构提供服务
# $Id:$
import os
import string
import sys
import MySQLdb

from time import localtime,strftime
import _mysql_exceptions
db_rw_host = "localhost"
db_user = "root"
db_pass = "neverchange"
db_name = "testdb"
log_path = "/home/log/cls_mysql.log" 
#log_path = "/home/guojinpeng/proj/data/log/cls_mysql.log"
class FileLog:
    def __init__(self,file):
        self.file=file
        pass
    def LOG(self,format,*args):
        fd = open(self.file,"a+")
        thetime = strftime("%Y-%m-%d %H:%M:%S",localtime())
        fd.write("[%s] %s\n" % (thetime,format%args))
        fd.close()
class DBC:
    def __init__(self):        
        self.FL = FileLog(log_path)
        self.db_cursor = None
        self.db_conn = None
        pass
    def rw_connect(self):
        try:
            self.db_conn = MySQLdb.connect(db_rw_host,db_user,db_pass,db_name)
            self.db_cursor = self.db_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            return self.db_cursor
        except MySQLdb.Error,e: 
            self.FL.LOG("MySQL connect error on server %s, %d:%s",db_rw_host,e.args[0],e.args[1])          
            return -1  
    def get_conn(self):
        return self.db_conn
    def query (self,sql):
        #execute the sql,no data result
        try:
            #sql = unicode(sql,'latin1')
            self.db_cursor.execute('set names utf8')
            self.db_cursor.execute(sql)
            #print sql.decode('utf8')
        except MySQLdb.Error,e:
            self.FL.LOG("MySQL error on query:erron:%d,%s" ,e.args[0],e.args[1])
            self.FL.LOG("query is %s" , sql);
            #sys.exit("MySQL query error!")
            return False
               
        except MySQLdb.OperationalError,war:
            raise war
            self.FL.LOG("MySQL warning %s",war)
            self.FL.LOG("query is %s" , sql)
            return True
        return True
    def getOne (self,sql):
        try:
            #sql = unicode(sql,'latin1')
            self.db_cursor.execute('set names utf8')            
            self.db_cursor.execute(sql)
            res = self.db_cursor.fetchone()
        except MySQLdb.Error,e:
            self.FL.LOG("MySQL error on query:erron:%d,%s" ,e.args[0],e.args[1])
            self.FL.LOG("query is %s" , sql);
            #sys.exit("MySQL query error!")
            return None
               
        except _mysql_exceptions.Warning,war:
            self.FL.LOG("MySQL warning %s",war)
            self.FL.LOG("query is %s" , sql)
            return None
        
        if res is None:
            return None
        for key in res.keys():
            return res[key]
    def get_insert_id (self):
        return int(self.getOne("SELECT LAST_INSERT_ID()"))
    def getRow (self,sql):
        try:
            #sql = unicode(sql,'latin1')
            self.db_cursor.execute('set names utf8')            
            self.db_cursor.execute(sql)
            res = self.db_cursor.fetchone()
        except MySQLdb.Error,e:
            self.FL.LOG("MySQL error on query:erron:%d,%s" ,e.args[0],e.args[1])
            self.FL.LOG("query is %s" , sql);
            #sys.exit("MySQL query error!")
            return None
               
        except _mysql_exceptions.Warning,war:
            self.FL.LOG("MySQL warning %s",war)
            self.FL.LOG("query is %s" , sql)
            return None
        
        if res is None:
            return None
        return res
    def getAll (self,sql):
        try:
            #sql = unicode(sql,'latin1')
            self.db_cursor.execute('set names utf8')                
            self.db_cursor.execute(sql)
            res = self.db_cursor.fetchall()
        except MySQLdb.Error,e:
            self.FL.LOG("MySQL error on query:erron:%d,%s" ,e.args[0],e.args[1])
            self.FL.LOG("query is %s" , sql);
            #sys.exit("MySQL query error!")
            return None
               
        except _mysql_exceptions.Warning,war:
            self.FL.LOG("MySQL warning %s",war)
            self.FL.LOG("query is %s" , sql)
            return None
        if len(res) is 0:
            return None
        return res

