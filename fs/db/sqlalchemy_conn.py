#!/usr/bin/env python3
"""
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# import fs.db.create_table_if_not_exists_mod as tblcreat
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


def get_session_from_sqlitefilepath(source=True):
  smaker = get_sessionmaker_from_sqlitefilepath(source)
  session = smaker()
  try:
    from models.samodels import create_table_if_not_exists
    create_table_if_not_exists(source)
  except ImportError:
    print('ImportError', ImportError)
  return session


def adhoc_test():
  eline = get_engineline_for_sqlitefilepath(source=True)
  print(eline)
  eline = get_engineline_for_sqlitefilepath(source=False)
  print(eline)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
