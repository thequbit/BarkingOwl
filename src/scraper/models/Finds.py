import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Finds:

    __con = False

    def __connect(self):
        con = mdb.connect(host=__sql_server__,
                          user=__sql_username__,
                          passwd=__sql_password__,
                          db=__sql_database__,
                         )
        return con

    def __sanitize(self,valuein):
        if type(valuein) == 'str':
            valueout = mysql.escape_string(valuein)
        else:
            valueout = valuein
        return valuein

    def add(self,urlphraseid,finddatetime,docid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO finds(urlphraseid,finddatetime,docid) VALUES(%s,%s,%s)",
                            (self.__sanitize(urlphraseid),self.__sanitize(finddatetime),self.__sanitize(docid))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,findid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM finds WHERE findid = %s",
                            (findid)
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
                cur.execute("SELECT * FROM finds")
                rows = cur.fetchall()
                cur.close()
            _finds = []
            for row in rows:
                _finds.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _finds

    def delete(self,findid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM finds WHERE findid = %s",
                            (self.__sanitize(findid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,findid,urlphraseid,finddatetime,docid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE finds SET urlphraseid = %s,finddatetime = %s,docid = %s WHERE findid = %s",
                            (self.__sanitize(urlphraseid),self.__sanitize(finddatetime),self.__sanitize(docid),self.__sanitize(findid))
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


