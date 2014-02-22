from setuptools import setup

long_description = \
'''YADA

YADA
'''

setup(
    name="Barking Owl",
    version="0.0.1",
    license="GPL2",
    author="Timothy Duffy",
    author_email="tim@timduffy.me",
    packages=["barking_owl"],
    zip_safe=False,
    description='YADA YADA YADA',
    long_description=long_description,
    include_package_data=True,
    platforms="any",
    install_requires=[
      "beautifulsoup4==4.3.2",
      "libmagic==1.0",
      "pika==0.9.13",
      "python-dateutil==2.2",
      "python-magic==0.4.6",
      "simplejson==3.3.3",
      "six==1.5.2",
      "wsgiref==0.1.2",
    ], 
    url="https://github.com/thequbit/BarkingOwl",
    classifiers=[],
)
