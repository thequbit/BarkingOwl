import MySQLdb as mdb
import _mysql as mysql
import re

class users:

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

    def add(self,name,login,passhash,lastlogin):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO users(name,login,passhash,lastlogin) VALUES(%s,%s,%s,%s)",(self.__sanitize(name),self.__sanitize(login),self.__sanitize(passhash),self.__sanitize(lastlogin)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,userid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM users WHERE userid = %s",(userid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM users")
            rows = cur.fetchall()
            cur.close()
        _users = []
        for row in rows:
            _users.append(row)
        con.close()
        return _users

    def delete(self,userid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM users WHERE userid = %s",(userid))
            cur.close()
        con.close()

    def update(self,userid,name,login,passhash,lastlogin):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE users SET name = %s,login = %s,passhash = %s,lastlogin = %s WHERE userid = %s",(self.__sanitize(name),self.__sanitize(login),self.__sanitize(passhash),self.__sanitize(lastlogin),self.__sanitize(userid)))
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

