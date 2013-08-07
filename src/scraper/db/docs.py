import MySQLdb as mdb
import _mysql as mysql
import re

class docs:

    __settings = {}
    __con = False

    def __init__(self):
        configfile = "./db/sqlcreds.txt"
        f = open(configfile)
        for line in f:
            # skip comment lines
            m = re.search('^\s*#', line)
            if m:
                continue

            # parse key=value lines
            m = re.search('^(\w+)\s*=\s*(\S.*)$', line)
            if m is None:
                continue

            self.__settings[m.group(1)] = m.group(2)
        f.close()

    def __connect(self):
        con = mdb.connect(host=self.__settings['host'], user=self.__settings['username'], passwd=self.__settings['password'], db=self.__settings['database'])
        return con

    def __sanitize(self,valuein):
        if type(valuein) == 'str':
            valueout = mysql.escape_string(valuein)
        else:
            valueout = valuein
        return valuein

    def add(self,filename,linktext,downloaddatetime,doctext,hash,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO docs(filename,linktext,downloaddatetime,doctext,hash,urlid) VALUES(%s,%s,%s,%s,%s,%s)",(self.__sanitize(filename),self.__sanitize(linktext),self.__sanitize(downloaddatetime),self.__sanitize(doctext),self.__sanitize(hash),self.__sanitize(urlid)))
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

    def update(self,docid,filename,linktext,downloaddatetime,doctext,hash,urlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE docs SET filename = %s,linktext = %s,downloaddatetime = %s,doctext = %s,hash = %s,urlid = %s WHERE docid = %s",(self.__sanitize(filename),self.__sanitize(linktext),self.__sanitize(downloaddatetime),self.__sanitize(doctext),self.__sanitize(hash),self.__sanitize(urlid),self.__sanitize(docid)))
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

