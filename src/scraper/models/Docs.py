import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Docs:

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

    def add(self,orgid,docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,urlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO docs(orgid,docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,urlid) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (self.__sanitize(orgid),self.__sanitize(docurl),self.__sanitize(filename),self.__sanitize(linktext),self.__sanitize(downloaddatetime),self.__sanitize(creationdatetime),self.__sanitize(doctext),self.__sanitize(dochash),self.__sanitize(urlid))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,docid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM docs WHERE docid = %s",
                            (docid)
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
                cur.execute("SELECT * FROM docs")
                rows = cur.fetchall()
                cur.close()
            _docs = []
            for row in rows:
                _docs.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _docs

    def delete(self,docid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM docs WHERE docid = %s",
                            (self.__sanitize(docid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,docid,orgid,docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,urlid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE docs SET orgid = %s,docurl = %s,filename = %s,linktext = %s,downloaddatetime = %s,creationdatetime = %s,doctext = %s,dochash = %s,urlid = %s WHERE docid = %s",
                            (self.__sanitize(orgid),self.__sanitize(docurl),self.__sanitize(filename),self.__sanitize(linktext),self.__sanitize(downloaddatetime),self.__sanitize(creationdatetime),self.__sanitize(doctext),self.__sanitize(dochash),self.__sanitize(urlid),self.__sanitize(docid))
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


