import datetime
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SeenURLs(Base):

    __tablename__ = 'seen_urls'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    doc_type = Column(String)
    #seen_datetime = Column(DateTime)

class URLCheck(object):

    def __init__(self, check_type='dict', uri='sqlite:///barkingowl-scraper.sqlite', DEBUG=False):

        self._check_type = check_type
        self._uri = uri
        self._DEBUG = DEBUG
        
        if self._check_type == 'dict':
            self._urls = []
        elif self._check_type == 'sql':
            self._engine = create_engine(self._uri, echo=self._DEBUG)
            Base.metadata.create_all(self._engine)
            Session = sessionmaker(bind=self._engine)
            self._session = Session()
        else:
            raise Exception("Invalid check_type: %s" % self._check_type)

    def clear(self):
        if self._check_type == 'dict':
            self._urls = []
        elif self._check_type == 'sql':
            self._session.query(
                SeenURLs,
            ).delete()

    def insert_url(self, url, doc_type):
        if self._check_type == 'dict':
            self._urls.append({
                'url': url,
                'type': doc_type,
            })
        elif self._check_type == 'sql':
            seen_url = SeenURLs(
                url=url,
                doc_type=doc_type,
                #seen_datetime=datetime.datetime.now()
            )
            self._session.add(seen_url)
            self._session.commit()

    def get_url(self, url):
        target = None
        if self._check_type == 'dict':
            for u in self._urls:
                if u['url'] == url:
                    target = u['url']
                    break
        elif self._check_type == 'sql':
            result = self._get_sql_url(url)
            if result != None:
                target = result.url
        return target

    def url_count(self):
        length = 0
        if self._check_type == 'dict':
            length = len(self._urls)
        elif self._check_type == 'sql':
            length = self._session.query(
                SeenURLs,
            ).count()
        return length

    def _get_sql_url(self, url):
        result = self._session.query(
            SeenURLs,
        ).filter(
            SeenURLs.url == url,
        ).first()
        return result

    def exists(self, url):
        exists = False
        if self._check_type == 'dict':
            for u in self._urls:
                if url == u['url']:
                   exists = True
                   break
        elif self._check_type == 'sql':
            if self._get_sql_url(url) != None:
                exists = True
        return exists

'''
url_check = URLCheck(check_type='dict', uri=None)
url_check.clear()
url_check.insert_url('http://timduffy.me', 'html/text')
print url_check.exists('http://google.com')
print url_check.exists('http://timduffy.me')

url_check = URLCheck(check_type='sql', uri='sqlite:///barkingowl-scraper.sqlite')
url_check.clear()
url_check.insert_url('http://timduffy.me', 'html/text')
print url_check.exists('http://google.com')
print url_check.exists('http://timduffy.me')
'''
