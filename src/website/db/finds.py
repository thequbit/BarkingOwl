import MySQLdb as mdb
import _mysql as mysql
import re

class finds:

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

    def add(self,urlphraseid,finddatetime,docid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO finds(urlphraseid,finddatetime,docid) VALUES(%s,%s,%s)",(self.__sanitize(urlphraseid),self.__sanitize(finddatetime),self.__sanitize(docid)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,findid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM finds WHERE findid = %s",(findid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
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
        return _finds

    def delete(self,findid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM finds WHERE findid = %s",(findid))
            cur.close()
        con.close()

    def update(self,findid,urlphraseid,finddatetime,docid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE finds SET urlphraseid = %s,finddatetime = %s,docid = %s WHERE findid = %s",(self.__sanitize(urlphraseid),self.__sanitize(finddatetime),self.__sanitize(docid),self.__sanitize(findid)))
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

