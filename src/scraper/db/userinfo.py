import MySQLdb as mdb
import _mysql as mysql
import re

class userinfo:

    __settings = {}
    __con = False

    def __init__(self,host,user,passwd,db):
        self.__settings['host'] = host
        self.__settings['username'] = user
        self.__settings['password'] = passwd
        self.__settings['database'] = db
    def __connect(self):
        con = mdb.connect(host=self.__settings['host'], user=self.__settings['username'],
                          passwd=self.__settings['password'], db=self.__settings['database'])
        return con

    def __sanitize(self,valuein):
        if type(valuein) == 'str':
            valueout = mysql.escape_string(valuein)
        else:
            valueout = valuein
        return valuein

    def add(self,userinfoid,userid,enabled,email,twitter,phone,pushinterval):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO userinfo(userinfoid,userid,enabled,email,twitter,phone,pushinterval) VALUES(%s,%s,%s,%s,%s,%s,%s)",(self.__sanitize(userinfoid),self.__sanitize(userid),self.__sanitize(enabled),self.__sanitize(email),self.__sanitize(twitter),self.__sanitize(phone),self.__sanitize(pushinterval)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM userinfo WHERE  = %s",())
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
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
        return _userinfo

    def delete(self,):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM userinfo WHERE  = %s",())
            cur.close()
        con.close()

    def update(self,userinfoid,userid,enabled,email,twitter,phone,pushinterval):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE userinfo SET userinfoid = %s,userid = %s,enabled = %s,email = %s,twitter = %s,phone = %s,pushinterval = %s WHERE  = %s",(self.__sanitize(userinfoid),self.__sanitize(userid),self.__sanitize(enabled),self.__sanitize(email),self.__sanitize(twitter),self.__sanitize(phone),self.__sanitize(pushinterval),)))
            cur.close()
        con.close()

##### Application Specific Functions #####

#    def myfunc():
#        con = self.__connect()
#        with con:
#            cur = son.cursor()
#            cur.execute("")
#            row = cur.fetchone()
#            cur.close()
#        con.close()
#        return row

