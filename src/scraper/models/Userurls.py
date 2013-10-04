import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Userurls:

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

    def add(self,urlid,userid,name,notes,adddatetime):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO userurls(urlid,userid,name,notes,adddatetime) VALUES(%s,%s,%s,%s,%s)",
                            (self.__sanitize(urlid),self.__sanitize(userid),self.__sanitize(name),self.__sanitize(notes),self.__sanitize(adddatetime))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,userurlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM userurls WHERE userurlid = %s",
                            (userurlid)
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
                cur.execute("SELECT * FROM userurls")
                rows = cur.fetchall()
                cur.close()
            _userurls = []
            for row in rows:
                _userurls.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _userurls

    def delete(self,userurlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM userurls WHERE userurlid = %s",
                            (self.__sanitize(userurlid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,userurlid,urlid,userid,name,notes,adddatetime):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE userurls SET urlid = %s,userid = %s,name = %s,notes = %s,adddatetime = %s WHERE userurlid = %s",
                            (self.__sanitize(urlid),self.__sanitize(userid),self.__sanitize(name),self.__sanitize(notes),self.__sanitize(adddatetime),self.__sanitize(userurlid))
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


