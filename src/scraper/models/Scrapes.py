import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Scrapes:

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

    def add(self,orgid,startdatetime,enddatetime,success,urlid,linkcount):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO scrapes(orgid,startdatetime,enddatetime,success,urlid,linkcount) VALUES(%s,%s,%s,%s,%s,%s)",
                            (self.__sanitize(orgid),self.__sanitize(startdatetime),self.__sanitize(enddatetime),self.__sanitize(success),self.__sanitize(urlid),self.__sanitize(linkcount))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,scrapeid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM scrapes WHERE scrapeid = %s",
                            (scrapeid)
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
                cur.execute("SELECT * FROM scrapes")
                rows = cur.fetchall()
                cur.close()
            _scrapes = []
            for row in rows:
                _scrapes.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _scrapes

    def delete(self,scrapeid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM scrapes WHERE scrapeid = %s",
                            (self.__sanitize(scrapeid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,scrapeid,orgid,startdatetime,enddatetime,success,urlid,linkcount):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE scrapes SET orgid = %s,startdatetime = %s,enddatetime = %s,success = %s,urlid = %s,linkcount = %s WHERE scrapeid = %s",
                            (self.__sanitize(orgid),self.__sanitize(startdatetime),self.__sanitize(enddatetime),self.__sanitize(success),self.__sanitize(urlid),self.__sanitize(linkcount),self.__sanitize(scrapeid))
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


