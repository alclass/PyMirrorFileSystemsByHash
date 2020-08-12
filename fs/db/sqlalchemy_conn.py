#!/usr/bin/env python3
"""
In other to make mysql work with sqlalchemy, two things were done:
  1) Ubuntu's package python3-dev and libmysqlclient-dev were installed;
  2) after that, mysqlclient was installed via pip.

Because in this machine, a virtualenv is taken by the IDE (PyCharm),
 mysqlclient was installed both globally (so that app could be run without activating
 virtualenv and then also installed locally. so that PyCharm could also run app).

SqlAlchemy

=> to learn how to use Foreign Keys in SqlAlchemy
  => docs.sqlalchemy.org/en/13/orm/join_conditions.html?highlight=foreign key

this_db = config.THIS_DATABASE
user = config.DATABASE_DICT[this_db]['USER']
password = config.DATABASE_DICT[this_db]['PASSWORD']
address = config.DATABASE_DICT[this_db]['ADDRESS']
port = config.DATABASE_DICT[this_db]['PORT']
databasename = config.DATABASE_DICT[this_db]['DATABASENAME']

engine_line = this_db + '://' + user + ':' + password + '@' + address + '/' + databasename
if engine_line.startswith('mysql'):
  engine_line = engine_line + '?charset=utf8mb4'

"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import config


def get_engineline_for_sqlitefilepath(source=True):
  sqlitefilepath = config.get_datatree_sqlitefilepath(source)
  engine_line = 'sqlite:///' + sqlitefilepath
  return engine_line


def get_engine_for_sqlitefilepath(source=True):
  engine_line = get_engineline_for_sqlitefilepath(source)
  sqlalchemy_engine = create_engine(engine_line)
  return sqlalchemy_engine


def get_sessionmaker_from_sqlitefilepath(source=True):
  sqlalchemy_engine = get_engine_for_sqlitefilepath(source)
  return sessionmaker(bind=sqlalchemy_engine)  # Session


def adhoc_test():
  eline = get_engineline_for_sqlitefilepath(source=True)
  print(eline)
  eline = get_engineline_for_sqlitefilepath(source=False)
  print(eline)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
