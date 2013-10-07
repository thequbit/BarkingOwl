import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Urls:

    __con = False

    def __connect(self):
        con = mdb.connect(host   = __dbcreds__.__server__,
                          user   = __dbcreds__.__username__,
                          passwd = __dbcreds__.__password__,
                          db     = __dbcreds__.__database__,
                         )
        return con

    def __sanitize(self,valuein):
        if type(valuein) == 'str':
            valueout = mysql.escape_string(valuein)
        else:
            valueout = valuein
        return valuein

    def add(self,orgid,url,name,description,createdatetime,creationuserid,linklevel):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO urls(orgid,url,name,description,createdatetime,creationuserid,linklevel) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                            (self.__sanitize(orgid),self.__sanitize(url),self.__sanitize(name),self.__sanitize(description),self.__sanitize(createdatetime),self.__sanitize(creationuserid),self.__sanitize(linklevel))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,urlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM urls WHERE urlid = %s",
                            (urlid)
                           )
                row = cur.fetchone()
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - get() failed with error:\n\n\t{0}".format(e))
        return row

    def getall(self):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM urls")
                rows = cur.fetchall()
                cur.close()
            _urls = []
            for row in rows:
                _urls.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _urls

    def delete(self,urlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM urls WHERE urlid = %s",
                            (self.__sanitize(urlid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,urlid,orgid,url,name,description,createdatetime,creationuserid,linklevel):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE urls SET orgid = %s,url = %s,name = %s,description = %s,createdatetime = %s,creationuserid = %s,linklevel = %s WHERE urlid = %s",
                            (self.__sanitize(orgid),self.__sanitize(url),self.__sanitize(name),self.__sanitize(description),self.__sanitize(createdatetime),self.__sanitize(creationuserid),self.__sanitize(linklevel),self.__sanitize(urlid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - update() failed with error:\n\nt{0}".format(e))

    ##### Application Specific Functions #####

#    def myfunc():
#        try:
#            con = self.__connect()
#            with con:
#                cur = son.cursor()
#                cur.execute("")
#                row = cur.fetchone()
#                cur.close()
#            con.close()
#        raise Exception("sql2api error - myfunct() failed with error:\n\n\t".format(e))
#        return row

    def byorgid(self,orgid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM urls WHERE orgid = %s",(self.__sanitize(orgid)))
                rows = cur.fetchall()
                #print "row count = {0}".format(len(rows))
                cur.close()
            con.close()
            _urls = []
            for row in rows:
                _urls.append(row)
        except Exception, e:
            Exception("sql2api error - byorgid() failed with error:\n\n\t{0}".format(e))
        return _urls

