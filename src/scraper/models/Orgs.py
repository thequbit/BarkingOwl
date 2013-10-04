import MySQLdb as mdb
import _mysql as mysql
import re

import __dbcreds__

class Orgs:

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

    def add(self,name,description,creationdatetime,ownerid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("INSERT INTO orgs(name,description,creationdatetime,ownerid) VALUES(%s,%s,%s,%s)",
                            (self.__sanitize(name),self.__sanitize(description),self.__sanitize(creationdatetime),self.__sanitize(ownerid))
                           )
                cur.close()
                newid = cur.lastrowid
            con.close()
        except Exception, e:
            raise Exception("sql2api error - add() failed with error:\n\n\t{0}".format(e))
        return newid

    def get(self,orgid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("SELECT * FROM orgs WHERE orgid = %s",
                            (orgid)
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
                cur.execute("SELECT * FROM orgs")
                rows = cur.fetchall()
                cur.close()
            _orgs = []
            for row in rows:
                _orgs.append(row)
            con.close()
        except Exception, e:
            raise Exception("sql2api error - getall() failed with error:\n\n\t{0}".format(e))
        return _orgs

    def delete(self,orgid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM orgs WHERE orgid = %s",
                            (self.__sanitize(orgid))
                           )
                cur.close()
            con.close()
        except Exception, e:
            raise Exception("sql2api error - delete() failed with error:\n\n\t{0}".format(e))

    def update(self,orgid,name,description,creationdatetime,ownerid):
        try:
            con = self.__connect()
            with con:
                cur = con.cursor()
                cur.execute("UPDATE orgs SET name = %s,description = %s,creationdatetime = %s,ownerid = %s WHERE orgid = %s",
                            (self.__sanitize(name),self.__sanitize(description),self.__sanitize(creationdatetime),self.__sanitize(ownerid),self.__sanitize(orgid))
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


