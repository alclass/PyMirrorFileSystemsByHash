#!/usr/bin/env python3
"""
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import config

def get_engineline_with_sqlitefilepath(sqlite_filepath):
  engine_line = 'sqlite:///' + sqlite_filepath
  return engine_line

def get_engineline_for_sqlitefilepath(source=True):
  sqlite_filepath = config.get_datatree_sqlitefilepath(source)
  return get_engineline_with_sqlitefilepath(sqlite_filepath)


def get_engine_with_sqlitefilepath(sqlite_filepath):
  engine_line = get_engineline_with_sqlitefilepath(sqlite_filepath)
  sqlalchemy_engine = create_engine(engine_line)
  return sqlalchemy_engine


def get_engine_for_sqlite_source_or_target(source=True):
  engine_line = get_engineline_for_sqlitefilepath(source)
  sqlalchemy_engine = create_engine(engine_line)
  return sqlalchemy_engine


def get_sessionmaker_for_sqlite_with_filepath(sqlite_filepath):
  sqlalchemy_engine = get_engine_with_sqlitefilepath(sqlite_filepath)
  return sessionmaker(bind=sqlalchemy_engine)  # Session


def get_sessionmaker_for_sqlite_source_or_target(source=True):
  sqlalchemy_engine = get_engine_for_sqlite_source_or_target(source)
  return sessionmaker(bind=sqlalchemy_engine)  # Session


def get_session_for_sqlite_with_filepath(sqlite_filepath):
  smaker = get_sessionmaker_for_sqlite_with_filepath(sqlite_filepath)
  session = smaker()
  try:
    import models.samodels as sam
    sam.create_table_if_not_exists_with_sqlite_abspath(sqlite_filepath)
  except ImportError:
    print('ImportError', ImportError)
  return session

def get_session_for_sqlite_source_or_target(source=True):
  smaker = get_sessionmaker_for_sqlite_source_or_target(source)
  session = smaker()
  try:
    import models.samodels as sam
    sam.create_table_if_not_exists(source)
  except ImportError:
    print('ImportError', ImportError)
  return session


def adhoc_test():
  eline = get_engineline_for_sqlitefilepath(source=True)
  print(1, eline)
  eline = get_engineline_for_sqlitefilepath(source=False)
  print(2, eline)
  abspath = config.STORE_SQLITE_IN
  eline = get_engineline_with_sqlitefilepath(abspath)
  print(3, eline)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
