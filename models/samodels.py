#!/usr/bin/env python3
"""
  docstring
"""
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, UniqueConstraint
# import fs.db.sqlalchemy_conn as saconn
import fs.db.sqlalchemy_conn as con
import config

Base = declarative_base()
dirtree_tablename = 'dirtree_entries'


def create_table_if_not_exists(source=True):
  engine = con.get_engine_for_sqlitefilepath(source)
  if not engine.dialect.has_table(engine, config.get_dirtree_tablename()):
    FSEntryInDB.__table__.create(bind=engine, checkfirst=True)
    return True
  return False


class FSEntryInDB(Base):
  """
  The following illustrates how to define a UNIQUE constraint for multiple columns:

  CREATE TABLE table_name(
      ...,
      UNIQUE(column_name1,column_name2,...)
  );
  """

  __tablename__ = 'dirtree_entries'

  id = Column(Integer, primary_key=True)
  entryname = Column(String)
  middlepath = Column(String)
  sha1hex = Column(String(40))  # , unique=True

  __table_args__ = (UniqueConstraint('entryname', 'middlepath', name='entryname_n_middlepath_uniq'),)

  def get_parentfolder_abspath(self, mountdir_abspath):
    """
    """
    return os.path.join(mountdir_abspath, self.middlepath)

  def get_entry_abspath(self, mountdir_abspath):
    """
    """
    parentfolder_abspath = self.get_parentfolder_abspath(mountdir_abspath)
    return os.path.join(parentfolder_abspath, self.entryname)

  def __str__(self):
    outstr = '<FSEntryInDB (en="%s", sh="%s")>' % (self.entryname, self.sha1hex)
    return outstr


def adhoc_test():
  ret_bool = create_table_if_not_exists(source=True)
  print(ret_bool, 'create_table_if_not_exists(source=True)')
  ret_bool = create_table_if_not_exists(source=False)
  print(ret_bool, 'create_table_if_not_exists(source=False)')


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
