import MySQLdb as mdb
import _mysql as mysql
import re

class docs:

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

    def add(self,docurl,filename,linktext,downloaddatetime,doctext,dochash,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO docs(docurl,filename,linktext,downloaddatetime,doctext,dochash,urlid) VALUES(%s,%s,%s,%s,%s,%s,%s)",(self.__sanitize(docurl),self.__sanitize(filename),self.__sanitize(linktext),self.__sanitize(downloaddatetime),self.__sanitize(doctext),self.__sanitize(dochash),self.__sanitize(urlid)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,docid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM docs WHERE docid = %s",(docid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM docs")
            rows = cur.fetchall()
            cur.close()
        _docs = []
        for row in rows:
            _docs.append(row)
        con.close()
        return _docs

    def delete(self,docid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM docs WHERE docid = %s",(docid))
            cur.close()
        con.close()

    def update(self,docid,docurl,filename,linktext,downloaddatetime,doctext,dochash,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE docs SET docurl = %s,filename = %s,linktext = %s,downloaddatetime = %s,doctext = %s,dochash = %s,urlid = %s WHERE docid = %s",(self.__sanitize(docurl),self.__sanitize(filename),self.__sanitize(linktext),self.__sanitize(downloaddatetime),self.__sanitize(doctext),self.__sanitize(dochash),self.__sanitize(urlid),self.__sanitize(docid)))
            cur.close()
        con.close()

##### Application Specific Functions #####

    def gethashs(self):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT dochash FROM docs")
            rows = cur.fetchall()
            cur.close()
        _hashs = []
        for row in rows:
            dochash, = row
            _hashs.append(dochash)
        con.close()
        return _hashs
