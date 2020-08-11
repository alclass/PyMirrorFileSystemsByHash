#!/usr/bin/env python
"""
  sqlite_create_db_mod.py
  Written on 2016-03-15 Luiz Lewis
"""
import os
import sqlite3
import config

# equal refs for textual economy
PYMIRROR_DB_PARAMS = config.PYMIRROR_DB_PARAMS

tablenames = []
table_n_create_table_sql_2tuple_list = []

# table 1
tablename = PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS
tablenames.append(tablename)
create_table_sql =  '''
CREATE TABLE IF NOT EXISTS %(tablename)s (
  id INT PRIMARY KEY NOT NULL,
  %(fieldname_for_parent_or_home_dir_id)s INT NOT NULL, -- same as parent_dir_id,
  n_levels INT NOT NULL DEFAULT -1,
  %(fieldname_for_entries_path_id_list_str)s TEXT DEFAULT '-1;',
  FOREIGN KEY(%(fieldname_for_parent_or_home_dir_id)s) REFERENCES %(tablename)s(id)
);
''' %{
  'tablename': tablename,
  'fieldname_for_parent_or_home_dir_id'  : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID,
  'fieldname_for_entries_path_id_list_str':PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR,
}
table_n_create_table_sql_2tuple = (tablename, create_table_sql)
table_n_create_table_sql_2tuple_list.append(table_n_create_table_sql_2tuple)

# table 2
tablename = PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES
tablenames.append(tablename)
create_table_sql =  '''
CREATE TABLE IF NOT EXISTS "%(tablename)s" (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  entryname TEXT NOT NULL,
  entrytype INT NOT NULL,
  FOREIGN KEY(id) REFERENCES %(tablename_for_entries_parents_n_paths)s(id)
);
''' %{ \
  'tablename' : tablename,
  'tablename_for_entries_parents_n_paths' : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS,
}
table_n_create_table_sql_2tuple = (tablename, create_table_sql)
table_n_create_table_sql_2tuple_list.append(table_n_create_table_sql_2tuple)

# table 3
tablename = PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES
tablenames.append(tablename)
create_table_sql =  '''
CREATE TABLE IF NOT EXISTS "%(tablename)s" (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  sha1hex CHAR(40),
  filesize INT,
  modified_datetime TEXT,
  FOREIGN KEY(id) REFERENCES %(tablename_for_file_n_folder_entries)s(id)
);
''' % { \
  'tablename' : tablename,
  'tablename_for_file_n_folder_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES \
}
table_n_create_table_sql_2tuple = (tablename, create_table_sql)
table_n_create_table_sql_2tuple_list.append(table_n_create_table_sql_2tuple)

sqlinsert_n_explainlabel_4tuple_list = []
# 1st sql insert
sqlinsert_explainlabel = 'Inserting Root to Entries'
tablename = PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES
sqlinsert = '''
INSERT INTO %(tablename)s
  (id, entryname, entrytype)
VALUES (?, ?, ?);
''' %{ 'tablename' : tablename }
data_3tuple = ( \
  PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID, \
  PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME, \
  PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER, \
)
sqlinsert_n_explainlabel_4tuple = (sqlinsert_explainlabel, tablename, sqlinsert, data_3tuple)
sqlinsert_n_explainlabel_4tuple_list.append(sqlinsert_n_explainlabel_4tuple)

# 2nd sql insert
sqlinsert_explainlabel = 'Inserting Root Parent and Path'
tablename = PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS
sqlinsert = '''
INSERT INTO %(tablename)s
  (id, %(fieldname_for_parent_or_home_dir_id)s, n_levels, %(fieldname_for_entries_path_id_list_str)s)
VALUES (?, ?, ?, ?);
''' %{ \
  'tablename' : tablename, \
  'fieldname_for_parent_or_home_dir_id'   : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
  'fieldname_for_entries_path_id_list_str': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
}
# Notice n_levels for 'root' is 0 :: ie, the root folder is at the zeroth level
data_4tuple = ( \
  PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID, \
  PYMIRROR_DB_PARAMS.CONVENTIONED_DUMMY_PARENT_OF_TOP_ROOT_FOLDER_ID, \
  0, \
  '0', \
)
sqlinsert_n_explainlabel_4tuple = (sqlinsert_explainlabel, tablename, sqlinsert, data_4tuple)
sqlinsert_n_explainlabel_4tuple_list.append(sqlinsert_n_explainlabel_4tuple)


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
    for table_name, create_table_sql in table_n_create_table_sql_2tuple_list:
      print 'Creating table', table_name
      cursor.execute(create_table_sql)
    conn.commit()
    conn.close()

  def initialize_the_2_dir_tables_with_toproot(self):
    conn = self.get_db_connection()
    cursor = conn.cursor()
    for sqlinsert_n_explainlabel_4tuple in sqlinsert_n_explainlabel_4tuple_list:
      try:
        sqlinsert_explainlabel, tablename, sqlinsert, data_tuple_list = sqlinsert_n_explainlabel_4tuple
        print sqlinsert_explainlabel, 'in table', tablename
        cursor.execute(sqlinsert, data_tuple_list)
      except sqlite3.IntegrityError:
        print 'ATTENTION: sqlite3.IntegrityError for', tablename
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
    for tablename in tablenames:
      if tablename not in found_tablenames:
        return False
    return True

  def delete_all_data_except_root(self):
    '''
    this delete rows ==>> delete from parent_dir_linked_list_table
    this delete the table itself ==>> drop table if exists fs_entries ;
    :return:
    '''
    conn = self.get_db_connection()
    cursor = conn.cursor()
    for tablename in tablenames:
      sql = 'DELETE FROM %(tablename)s;' %{'tablename':tablename}
      cursor.execute(sql)
    conn.commit()
    conn.close()
    self.initialize_the_2_dir_tables_with_toproot()


def delete_all_data_except_root():
  pass


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
  sqlite_db_filename = 'test01.sqlite'
  dbms_params_dict = {}
  filepath = os.path.join(os.path.abspath(''), sqlite_db_filename)
  dbms_params_dict['db_sqlite_filepath'] = filepath
  conn_obj = dbfact.DBFactoryToConnection(dbms_params_dict)
  conn = conn_obj.get_db_connection()
  print conn

def main():
  # create_tables_and_initialize_root()
  dbcreator = DBTablesCreator()
  dbcreator.create_tables()
  print 'dbcreator.verify_that_tables_were_created()'
  dbcreator.verify_that_tables_were_created()
  dbcreator.initialize_the_2_dir_tables_with_toproot()
  # dbcreator.delete_all_data_except_root()

  # test1()

if __name__ == '__main__':
  main()
