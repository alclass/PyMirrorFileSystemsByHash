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

# equal refs for textual economy
PYMIRROR_CONSTANTS = sqlaccessor.PYMIRROR_CONSTANTS



conn = None
def get_connection(renew=False, dbms_to_use=None, sqlite_db_filepath=None, mysql_tuple_params=None):
  global conn
  if conn == None or renew:
    conn = set_n_get_connection(dbms_to_use=None, sqlite_db_filepath=None, mysql_tuple_params=None)
  return conn

def set_n_get_connection(dbms_to_use=None, sqlite_db_filepath=None, mysql_tuple_params=None):
  conn_obj = sqlaccessor.DBFactoryToConnection(dbms_to_use)
  conn_obj.set_sqlite_dbfile_absfolder()
    sqlite_db_filepath, \
    mysql_tuple_params  \
  )
  conn = conn_obj.get_db_connection()


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
'''%{'tablename_for_file_entries':tablename_for_file_entries,'tablename_for_dir_entries':tablename_for_dir_entries}
sql_create_table_for_dir_entries = '''
CREATE TABLE IF NOT EXISTS "%(tablename_for_dir_entries)s" (
  id INT PRIMARY KEY,
  foldername TEXT NOT NULL,
);
''' %{'tablename_for_dir_entries':tablename_for_dir_entries}
sql_create_table_for_entries_linked_list = '''
CREATE TABLE IF NOT EXISTS "%(tablename_for_entries_linked_list)s" (
  id INT PRIMARY KEY,
  parent_dir_id INT,
  FOREIGN KEY(parent_dir_id) REFERENCES %(tablename_for_entries_linked_list)s(id)
);
''' %{'tablename_for_dir_entries':tablename_for_dir_entries}
def create_tables():
  conn = sqlaccessor.get_db_connection(sqlaccessor.DBMS_ID_CONSTANTS.SQLITE, PYMIRROR_CONSTANTS.SQLITE_DB_TABLENAME_DEFAULT)
  cursor = conn.cursor()
  cursor.execute(sql_create_table_for_dir_entries)
  cursor.execute(sql_create_table_for_file_entries)
  conn.commit()
  conn.close()

def init_dir_table():
  sql_init_dir_entries = '''
INSERT INTO "%(tablename_for_dir_entries)s" (id, parent_dir_id, foldername)
  VALUES (?, ?, ?);
''' %{'tablename_for_dir_entries':tablename_for_dir_entries}
  conn = sqlaccessor.get_db_connection(sqlaccessor.DBMS_ID_CONSTANTS.SQLITE, PYMIRROR_CONSTANTS.SQLITE_DB_TABLENAME_DEFAULT)
  cursor = conn.cursor()
  root_record_tuple_list = [( \
    PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID, \
    PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID, \
    PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_DIR_NAME, \
  )]
  try:
    cursor.executemany(sql_init_dir_entries, root_record_tuple_list)
  except sqlite3.IntegrityError:
    pass
  conn.commit()
  conn.close()


def main():
  create_tables()
  init_dir_table()
  # test1()

if __name__ == '__main__':
  main()
