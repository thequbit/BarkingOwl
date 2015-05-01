from setuptools import setup

long_description = \
'''

BarkingOwl is a scalable web crawler intended to be used to
find specific document types on websites, such as PDFs, XLS, 
TXT, HTML, etc.

For documentation checkout the github wiki:

https://github.com/thequbit/BarkingOwl/wiki

For a simple web frontend, check out barkingowl-frontend:

https://github.com/thequbit/barkingowl-frontend

'''

setup(
    name="BarkingOwl",
    version="0.7.1",
    license="GPL3",
    author="Timothy Duffy",
    author_email="tim@timduffy.me",
    packages=["barking_owl", "barking_owl.scraper", "barking_owl.dispatcher"],
    zip_safe=False,
    description='Scalable web crawler.',
    long_description=long_description,
    include_package_data=True,
    platforms="any",
    install_requires=[
      "beautifulsoup4==4.3.2",
      "libmagic==1.0",
      "pika==0.9.14",
      #"python-dateutil==2.2",
      "python-magic==0.4.6",
      "six==1.5.2",
      "wsgiref==0.1.2",
      "tldextract==1.5.1",
      "requests==2.5.1",
      "pymongo==2.8",
      "daemon==1.1",
    ], 
    url="https://github.com/thequbit/BarkingOwl",
    classifiers=[
      'Development Status :: 4 - Beta',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
      #'Operating System :: POSIX',
    ],
)
