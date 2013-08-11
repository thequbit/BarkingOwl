import MySQLdb as mdb
import _mysql as mysql
import re

class urls:

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

    def add(self,url,name,description,createdatetime,creationuserid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO urls(url,name,description,createdatetime,creationuserid) VALUES(%s,%s,%s,%s,%s)",(self.__sanitize(url),self.__sanitize(name),self.__sanitize(description),self.__sanitize(createdatetime),self.__sanitize(creationuserid)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM urls WHERE urlid = %s",(urlid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
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
        return _urls

    def delete(self,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM urls WHERE urlid = %s",(urlid))
            cur.close()
        con.close()

    def update(self,urlid,url,name,description,createdatetime,creationuserid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE urls SET url = %s,name = %s,description = %s,createdatetime = %s,creationuserid = %s WHERE urlid = %s",(self.__sanitize(url),self.__sanitize(name),self.__sanitize(description),self.__sanitize(createdatetime),self.__sanitize(creationuserid),self.__sanitize(urlid)))
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

