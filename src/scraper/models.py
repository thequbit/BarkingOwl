#from ...models import *

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    Text,
    DateTime,
    Boolean,
    )

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
Base = declarative_base()

class UserModel(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    email = Column(Text)
    name = Column(Text)
    created = Column(DateTime)
    enabled = Column(Boolean)

    def __init__(self,email,name,created,enabled):
       self.email = email
       self.name = name
       self.created = created
       self.enabled = enabled


class UrlModel(Base):
    __tablename__ = 'urls'
    id = Column(Integer, primary_key=True)
    url = Column(Text)
    name = Column(Text)
    description = Column(Text)
    created = Column(DateTime)
    userid = Column(Integer,ForeignKey('users.id'))

    def __init__(self,url,name,description,created,userid):
        self.url = url
        self.name = name
        self.description = description
        self.created = created
        self.userid = userid

class DocModel(Base):
    __tablename__ = 'docs'
    id = Column(Integer, primary_key=True)
    docurl = Column(Text)
    filename = Column(Text)
    linktext = Column(Text)
    downloaded = Column(DateTime)
    created = Column(DateTime)
    doctext = Column(Text)
    dochash = Column(Text)
    urlid = Column(Integer,ForeignKey('urls.id'))

    def __init__(self,docurl,filename,linktext,downloaded,created,doctext,dochash,urlid):
        self.docurl = docurl
        self.filename = filename
        self.linktext = linktext
        self.downloaded = downloaded
        self.created = created
        self.doctext = doctext
        self.dochash = dochash
        self.urlid = urlid


class RunModel(Base):
    __tablename__ = 'runs'
    id = Column(Integer, primary_key=True)
    start = Column(DateTime)
    end = Column(DateTime)
    success = Column(Boolean)
    urlid = Column(Integer, ForeignKey('urls.id'))

    def __init__(self,start,end,success,urlid):
        self.start = start
        self.end = end
        self.success = success
        self.urlid = urlid



#class MyModel(Base):
#    __tablename__ = 'models'
#    id = Column(Integer, primary_key=True)
#    name = Column(Text, unique=True)
#    value = Column(Integer)
#
#    def __init__(self, name, value):
#        self.name = name
#        self.value = value
