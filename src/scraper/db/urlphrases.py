import MySQLdb as mdb
import _mysql as mysql
import re

class urlphrases:

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

    def add(self,userid,urlid,phraseid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO urlphrases(userid,urlid,phraseid) VALUES(%s,%s,%s)",(self.__sanitize(userid),self.__sanitize(urlid),self.__sanitize(phraseid)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,urlphraseid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM urlphrases WHERE urlphraseid = %s",(urlphraseid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM urlphrases")
            rows = cur.fetchall()
            cur.close()
        _urlphrases = []
        for row in rows:
            _urlphrases.append(row)
        con.close()
        return _urlphrases

    def delete(self,urlphraseid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM urlphrases WHERE urlphraseid = %s",(urlphraseid))
            cur.close()
        con.close()

    def update(self,urlphraseid,userid,urlid,phraseid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE urlphrases SET userid = %s,urlid = %s,phraseid = %s WHERE urlphraseid = %s",(self.__sanitize(userid),self.__sanitize(urlid),self.__sanitize(phraseid),self.__sanitize(urlphraseid)))
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

