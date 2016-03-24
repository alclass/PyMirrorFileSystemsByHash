#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
sqlite_create_db_mod.py


  Written on 2016-03-15 Luiz Lewis
'''
import os
import sqlite3
import sys
import db_connection_factory_mod as dbfact
import db_settings

# equal refs for textual economy
PYMIRROR_DB_PARAMS = db_settings.PYMIRROR_DB_PARAMS

sql_create_table_for_entries_linked_list = '''
CREATE TABLE IF NOT EXISTS %(tablename_for_entries_linked_list)s (
  id INT PRIMARY KEY NOT NULL,
  %(fieldname_for_parent_or_home_dir_id)s INT, -- same as parent_dir_id
  FOREIGN KEY(%(fieldname_for_parent_or_home_dir_id)s) REFERENCES %(tablename_for_entries_linked_list)s(id)
);
''' %{
  'tablename_for_entries_linked_list'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST,
  'fieldname_for_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID,
}

sql_create_table_for_file_n_folder_entries = '''
CREATE TABLE IF NOT EXISTS "%(tablename_for_file_n_folder_entries)s" (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  entryname TEXT NOT NULL,
  entrytype INT NOT NULL,
  FOREIGN KEY(id) REFERENCES %(tablename_for_entries_linked_list)s(id)
);
''' %{ \
  'tablename_for_file_n_folder_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES,
  'tablename_for_entries_linked_list'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST,
}

sql_create_table_for_file_attrib_values = '''
CREATE TABLE IF NOT EXISTS "%(tablename_for_file_attrib_values)s" (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  sha1hex CHAR(40),
  filesize INT,
  modified_datetime TEXT,
  FOREIGN KEY(id) REFERENCES %(tablename_for_file_n_folder_entries)s(id)
);
''' % { \
  'tablename_for_file_attrib_values'   : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES,
  'tablename_for_file_n_folder_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES \
}

sql_create_table_for_auxtab_path_id_list_per_folder = '''
CREATE TABLE IF NOT EXISTS "%(tablename_auxtab_path_id_list_per_entries)s" (
  id INT PRIMARY KEY NOT NULL,
  %(fieldname_for_entries_path_id_list_str)s TEXT NOT NULL,
  FOREIGN KEY(id) REFERENCES %(tablename_for_file_n_folder_entries)s(id)
);
''' % { \
  'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS,
  'tablename_for_file_n_folder_entries'      : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES, \
  'fieldname_for_entries_path_id_list_str'   : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
}

sql_init_dir_entries_table_with_root = '''
INSERT INTO %(tablename_for_file_n_folder_entries)s (id, entryname, entrytype)
  VALUES (?, ?, ?);
''' %{ 'tablename_for_file_n_folder_entries' : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES }

root_record_tuple_list_in_dir_entries_table = [( \
  PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID, \
  PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME, \
  PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER, \
)]

sql_init_dir_linked_list_table_with_root = '''
INSERT INTO %(tablename_for_entries_linked_list)s (id, %(fieldname_for_parent_or_home_dir_id)s)
  VALUES (?, ?);
''' %{ \
  'tablename_for_entries_linked_list' : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST, \
      'fieldname_for_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
}

root_record_tuple_list_in_linked_list_table = [( \
  PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID, \
  PYMIRROR_DB_PARAMS.CONVENTIONED_DUMMY_PARENT_OF_TOP_ROOT_FOLDER_ID, \
)]

class DBTablesCreator(object):

  def __init__(self, db_params_dict=None):
    '''
    If db_params_dict is None, the db_obj defaults to a
      standard named sqlite db file in the script's or app's executing folder [gotten by os.path.abspath('.')].
    See more about the db_params_dict in the DBFactoryToConnection class documentation

    :param db_params_dict:
    :return:
    '''
    self.db_params_dict = db_params_dict

  def get_db_connection(self):
    '''
    Even if db_params_obj is None, it can be used here and the calling function will find it and default it if needed
    :return:
    '''
    conn_obj = dbfact.DBFactoryToConnection(self.db_params_dict)
    conn = conn_obj.get_db_connection()
    return conn

  def create_tables(self):
    conn = self.get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql_create_table_for_entries_linked_list)
    cursor.execute(sql_create_table_for_file_n_folder_entries)
    cursor.execute(sql_create_table_for_file_attrib_values)
    cursor.execute(sql_create_table_for_auxtab_path_id_list_per_folder)
    conn.commit()
    conn.close()

  def initialize_the_2_dir_tables_with_toproot(self):
    conn = self.get_db_connection()
    cursor = conn.cursor()
    try:
      cursor.executemany(sql_init_dir_entries_table_with_root, root_record_tuple_list_in_dir_entries_table)
      cursor.executemany(sql_init_dir_linked_list_table_with_root, root_record_tuple_list_in_linked_list_table)
    except sqlite3.IntegrityError:
      pass
    conn.commit()
    conn.close()

  def verify_that_tables_were_created(self):
    '''

    :return:
    '''
    conn = self.get_db_connection()
    cursor = conn.cursor()
    sql = '''
      SELECT name FROM sqlite_master
        WHERE type = "table";
    '''
    result = cursor.execute(sql)
    records = result.fetchall()
    found_tablenames = []
    for record in records:
      tablename = record[0]
      found_tablenames.append(tablename)
    conn.close()
    should_be_there_tables = []
    should_be_there_tables.append(PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES)
    should_be_there_tables.append(PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES)
    should_be_there_tables.append(PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST)
    should_be_there_tables.append(PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS)
    for should_be_there_table in should_be_there_tables:
      if should_be_there_table not in found_tablenames:
        return False
    return True

  def delete_all_data_except_root(self):
    '''
    this delete rows ==>> delete from parent_dir_linked_list_table
    this delete the table itself ==>> drop table if exists fs_entries ;
    :return:
    '''
    sqlscript = '''
    DELETE FROM %(tablename_for_entries_linked_list)s;
    DELETE FROM %(tablename_for_file_attrib_values)s;
    DELETE FROM %(tablename_auxtab_path_id_list_per_entries)s;
    DELETE FROM %(tablename_for_file_n_folder_entries)s;

    ''' % { \
    'tablename_for_entries_linked_list'        : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST,
    'tablename_for_file_attrib_values'         : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES, \
    'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS, \
    'tablename_for_file_n_folder_entries'      : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES, \
  }
    conn = self.get_db_connection()
    cursor = conn.cursor()
    cursor.executescript(sqlscript)
    conn.commit()
    conn.close()
    self.initialize_the_2_dir_tables_with_toproot()


def create_tables_and_initialize_root():
  dbcreator = DBTablesCreator()
  print 'dbcreator.create_tables()'
  dbcreator.create_tables()
  print 'dbcreator.initialize_the_2_dir_tables_with_toproot()'
  dbcreator.initialize_the_2_dir_tables_with_toproot()
  print 'dbcreator.verify_that_tables_were_created()',
  bool_answer = dbcreator.verify_that_tables_were_created()
  print 'bool_answer', bool_answer

def test1():
  dbms_to_use=PYMIRROR_DB_PARAMS.SQLITE
  sqlite_db_filepath = 'test01.sqlite'
  conn_obj = dbfact.DBFactoryToConnection(dbms_params_dict)
  conn = conn_obj.get_db_connection()
  print conn

def main():
  # create_tables_and_initialize_root()
  dbcreator = DBTablesCreator()
  dbcreator.create_tables()
  print 'dbcreator.verify_that_tables_were_created()'
  dbcreator.verify_that_tables_were_created()
  # dbcreator.delete_all_data_except_root()

  # test1()

if __name__ == '__main__':
  main()
