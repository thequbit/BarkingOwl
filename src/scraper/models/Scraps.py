import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Scraps:

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

    def add(self,orgid,scrapdatetime,success,urlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO scraps(orgid,scrapdatetime,success,urlid) VALUES(%s,%s,%s,%s)",
                            (self.__sanitize(orgid),self.__sanitize(scrapdatetime),self.__sanitize(success),self.__sanitize(urlid))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,scrapid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM scraps WHERE scrapid = %s",
                            (scrapid)
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
                cur.execute("SELECT * FROM scraps")
                rows = cur.fetchall()
                cur.close()
            _scraps = []
            for row in rows:
                _scraps.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _scraps

    def delete(self,scrapid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM scraps WHERE scrapid = %s",
                            (self.__sanitize(scrapid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,scrapid,orgid,scrapdatetime,success,urlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE scraps SET orgid = %s,scrapdatetime = %s,success = %s,urlid = %s WHERE scrapid = %s",
                            (self.__sanitize(orgid),self.__sanitize(scrapdatetime),self.__sanitize(success),self.__sanitize(urlid),self.__sanitize(scrapid))
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


