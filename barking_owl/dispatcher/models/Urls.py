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

    def add(self,targeturl,maxlinklevel,creationdatetime,doctypeid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO urls(targeturl,maxlinklevel,creationdatetime,doctypeid) VALUES(%s,%s,%s,%s)",
                            (self.__sanitize(targeturl),self.__sanitize(maxlinklevel),self.__sanitize(creationdatetime),self.__sanitize(doctypeid))
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

    def update(self,urlid,targeturl,maxlinklevel,creationdatetime,doctypeid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE urls SET targeturl = %s,maxlinklevel = %s,creationdatetime = %s,doctypeid = %s WHERE urlid = %s",
                            (self.__sanitize(targeturl),self.__sanitize(maxlinklevel),self.__sanitize(creationdatetime),self.__sanitize(doctypeid),self.__sanitize(urlid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - update() failed with error:\n\nt{0}".format(e))

    ##### Application Specific Functions #####

    def getallurldata(self):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("""
                            SELECT
                              urls.urlid as urlid,
                              urls.targeturl as targeturl,
                              urls.maxlinklevel as maxlinklevel,
                              urls.creationdatetime as creationdatetime,
                              doctypes.title as doctypetitle,
                              doctypes.description as docdescription,
                              doctypes.doctype as doctype
                            FROM
                              urls
                            JOIN
                              doctypes
                            ON
                              urls.doctypeid = doctypes.doctypeid;
                            """)
                rows = cur.fetchall()
                cur.close()
            _urls = []
            for row in rows:
                _urls.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getallurldata() failed with error:\n\n\t{0}".format(e))
        return _urls

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


