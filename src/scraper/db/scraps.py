import MySQLdb as mdb
import _mysql as mysql
import re

class scraps:

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

    def add(self,scrapdatetime,success,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO scraps(scrapdatetime,success,urlid) VALUES(%s,%s,%s)",(self.__sanitize(scrapdatetime),self.__sanitize(success),self.__sanitize(urlid)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,scrapid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM scraps WHERE scrapid = %s",(scrapid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
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
        return _scraps

    def delete(self,scrapid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM scraps WHERE scrapid = %s",(scrapid))
            cur.close()
        con.close()

    def update(self,scrapid,scrapdatetime,success,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE scraps SET scrapdatetime = %s,success = %s,urlid = %s WHERE scrapid = %s",(self.__sanitize(scrapdatetime),self.__sanitize(success),self.__sanitize(urlid),self.__sanitize(scrapid)))
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

