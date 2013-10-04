import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Userinfo:

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

    def add(self,userinfoid,userid,enabled,email,twitter,phone,pushinterval):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO userinfo(userinfoid,userid,enabled,email,twitter,phone,pushinterval) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                            (self.__sanitize(userinfoid),self.__sanitize(userid),self.__sanitize(enabled),self.__sanitize(email),self.__sanitize(twitter),self.__sanitize(phone),self.__sanitize(pushinterval))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM userinfo WHERE  = %s",
                            ()
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
                cur.execute("SELECT * FROM userinfo")
                rows = cur.fetchall()
                cur.close()
            _userinfo = []
            for row in rows:
                _userinfo.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _userinfo

    def delete(self,):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM userinfo WHERE  = %s",
                            ()
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,userinfoid,userid,enabled,email,twitter,phone,pushinterval):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE userinfo SET userinfoid = %s,userid = %s,enabled = %s,email = %s,twitter = %s,phone = %s,pushinterval = %s WHERE  = %s",
                            (self.__sanitize(userinfoid),self.__sanitize(userid),self.__sanitize(enabled),self.__sanitize(email),self.__sanitize(twitter),self.__sanitize(phone),self.__sanitize(pushinterval),)
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


