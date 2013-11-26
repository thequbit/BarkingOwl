import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Doctypes:

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

    def add(self,title,description,doctype):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO doctypes(title,description,doctype) VALUES(%s,%s,%s)",
                            (self.__sanitize(title),self.__sanitize(description),self.__sanitize(doctype))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,doctypeid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM doctypes WHERE doctypeid = %s",
                            (doctypeid)
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
                cur.execute("SELECT * FROM doctypes")
                rows = cur.fetchall()
                cur.close()
            _doctypes = []
            for row in rows:
                _doctypes.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _doctypes

    def delete(self,doctypeid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM doctypes WHERE doctypeid = %s",
                            (self.__sanitize(doctypeid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,doctypeid,title,description,doctype):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE doctypes SET title = %s,description = %s,doctype = %s WHERE doctypeid = %s",
                            (self.__sanitize(title),self.__sanitize(description),self.__sanitize(doctype),self.__sanitize(doctypeid))
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


