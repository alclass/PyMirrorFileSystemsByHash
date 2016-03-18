#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
sqlite_create_db_mod.py


  Written on 2016-03-15 Luiz Lewis
'''
import os
import sqlite3
import sys
import sqlite_accessor_mod as sqlaccessor
import db_settings

# equal refs for textual economy
PYMIRROR_DB_PARAMS = db_settings.PYMIRROR_DB_PARAMS



conn = None
def get_connection(renew=False, dbms_to_use=None, sqlite_db_filepath=None, mysql_tuple_params=None):
  global conn
  if 1: # conn == None or renew:
    conn = set_n_get_connection(dbms_to_use, sqlite_db_filepath, mysql_tuple_params)
  return conn

def set_n_get_connection(dbms_to_use=None, sqlite_db_filepath=None, mysql_tuple_params=None):
  conn_obj = sqlaccessor.DBFactoryToConnection(dbms_to_use)
  # SQLITE takes the current path of the executing script HERE !
  conn_obj.set_file_n_folder_thru_sqlite_db_filepath(sqlite_db_filepath)
  conn = conn_obj.get_db_connection()
  return conn

sql_create_table_for_file_entries = '''
CREATE TABLE IF NOT EXISTS "%(tablename_for_file_entries)s" (
  id INT PRIMARY KEY,
  home_dir_id INT NOT NULL,
  sha1hex CHAR(40),
  filename TEXT NOT NULL,
  filesize INT,
  modified_date TEXT,
  FOREIGN KEY(home_dir_id) REFERENCES %(tablename_for_dir_entries)s(id)
);
''' % { \
  'tablename_for_file_entries' : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ENTRIES,
  'tablename_for_dir_entries'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.DIR_ENTRIES \
}

sql_create_table_for_entries_linked_list = '''
CREATE TABLE IF NOT EXISTS "%(tablename_for_entries_linked_list)s" (
  id INT PRIMARY KEY,
  parent_dir_id INT,
  FOREIGN KEY(parent_dir_id) REFERENCES %(tablename_for_entries_linked_list)s(id)
);
''' %{ 'tablename_for_entries_linked_list' : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST }

sql_create_table_for_dir_entries = '''
CREATE TABLE IF NOT EXISTS "%(tablename_for_dir_entries)s" (
  id INT PRIMARY KEY,
  foldername TEXT NOT NULL,
  FOREIGN KEY(id) REFERENCES %(tablename_for_entries_linked_list)s(id)
);
''' %{ \
  'tablename_for_dir_entries'         : PYMIRROR_DB_PARAMS.TABLE_NAMES.DIR_ENTRIES,
  'tablename_for_entries_linked_list' : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST,
}

sql_init_dir_entries_table_with_root = '''
INSERT INTO %(tablename_for_dir_entries)s (id, foldername)
  VALUES (?, ?);
''' %{ 'tablename_for_dir_entries' : PYMIRROR_DB_PARAMS.TABLE_NAMES.DIR_ENTRIES }

root_record_tuple_list_in_dir_entries_table = [( \
  PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID, \
  PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME, \
)]

sql_init_dir_linked_list_table_with_root = '''
INSERT INTO %(tablename_for_entries_linked_list)s (id, parent_dir_id)
  VALUES (?, ?);
''' %{ 'tablename_for_entries_linked_list' : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST }

root_record_tuple_list_in_linked_list_table = [( \
  PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID, \
  PYMIRROR_DB_PARAMS.CONVENTIONED_DUMMY_PARENT_OF_TOP_ROOT_FOLDER_ID, \
)]


class DBTablesCreator(object):

  def __init__(self, db_params_obj=None):
    pass
    # sqlaccessor.get_db_connection(sqlaccessor.DBMS_CONSTANTS.SQLITE, PYMIRROR_DB_PARAMS.SQLITE.SQLITE_ROOTDIR_FILENAME_DEFAULT)

  def get_connection(self):
    return get_connection()

  def create_tables(self):
    conn = self.get_connection()
    cursor = conn.cursor()
    cursor.execute(sql_create_table_for_dir_entries)
    cursor.execute(sql_create_table_for_entries_linked_list)
    cursor.execute(sql_create_table_for_file_entries)
    conn.commit()
    conn.close()

  def initialize_the_2_dir_tables_with_toproot(self):
    conn = self.get_connection()
    cursor = conn.cursor()
    try:
      cursor.executemany(sql_init_dir_entries_table_with_root, root_record_tuple_list_in_dir_entries_table)
      cursor.executemany(sql_init_dir_linked_list_table_with_root, root_record_tuple_list_in_linked_list_table)
    except sqlite3.IntegrityError:
      pass
    conn.commit()
    conn.close()

def test1():
  conn = get_connection()
  print conn

def main():
  dbcreator = DBTablesCreator()
  dbcreator.create_tables()
  dbcreator.initialize_the_2_dir_tables_with_toproot()
  # test1()

if __name__ == '__main__':
  main()
  test1()
