BarkingOwl
==========

scraper + pyramid web app that pulls docs off websites and produces stats and notifications about them

####Background and Description####

Barking Owl came out of the need presented at a Hacks and Hackers Rochester (#hhroc) meet-up in Syracuse, NY.
A journalist expressed his need for a tool that would assist him in looking for key words within PDFs posted
to town websites, such as meeting minutes.

BarkingOwl is a web scrapper.  A web interface to the data will be made available soon in a different repo.

####Organization####

BarkingOwl interfaces to a MySQL database via an [sql2api](https://github.com/thequbit/sql2api) access layer.
The scrapper pulls all of the organizations within the database, and then all of the urls within that org.
It then pulls down all of the pdf documents from those urls, converts them to text, and saves them back to
the database.  The documents are then searchable within the database once loaded.

####Installing and Running Barking Owl####

    TODO: this.
