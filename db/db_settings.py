#!/usr/bin/env python
#-*-encoding:utf8-*-
#import os

DATABASE_NAME = 'PYMIRROR_file_hashes_down_dir_tree'
SQLITE     = 1
MYSQL      = 2
POSTGRESQL = 3

class PYMIRROR_DB_PARAMS:

  class DBMS:
    SQLITE = SQLITE
    MYSQL  = MYSQL
    POSTGRESQL = POSTGRESQL
    PROVIDED_DBMS_LIST = [SQLITE, MYSQL, POSTGRESQL]

    SQLITE_NAME = 'SQLITE'
    MYSQL_NAME  = 'MYSQL'
    POSTGRESQL_NAME = 'POSTGRESQL'


  DATABASE_NAME = DATABASE_NAME
  CONVENTIONED_TOP_ROOT_FOLDER_ID = 1 # it's convencioned here that root's parent is itself (the only exception to parent-child pointing is 'root')
  CONVENTIONED_DUMMY_PARENT_OF_TOP_ROOT_FOLDER_ID = 0 # notice that 0 (the dummy parent id for root) will only exist in the linked list table, not in the folder entries table
  CONVENTIONED_ROOT_DIR_NAME = '/' # 'TOP_ROOT_FOLDER'

  class TABLE_NAMES:
    FILE_ENTRIES        = 'file_entries'
    DIR_ENTRIES         = 'dir_entries'
    ENTRIES_LINKED_LIST = 'parent_dir_linked_list_table'
    AUXTAB_FOR_PRE_PREPARED_PATHS = 'auxtab_path_id_list_per_folder'

  class SQLITE:
    HASHES_ETC_DATA_FILENAME = DATABASE_NAME + '.sqlite'

  class MYSQL:
    pass

  class POSTGRESQL:
    pass

def is_dbms_provided(dbms):
  if type(dbms) <> int:
    return False
  if dbms in PYMIRROR_DB_PARAMS.DBMS.PROVIDED_DBMS_LIST:
    return True
  return False

def get_dbms_name(dbms):
  if dbms == PYMIRROR_DB_PARAMS.DBMS.SQLITE:
    return PYMIRROR_DB_PARAMS.DBMS.SQLITE_NAME
  elif dbms == PYMIRROR_DB_PARAMS.DBMS.MYSQL:
    return PYMIRROR_DB_PARAMS.DBMS.MYSQL_NAME
  elif dbms == PYMIRROR_DB_PARAMS.DBMS.POSTGRESQL:
    return PYMIRROR_DB_PARAMS.DBMS.POSTGRESQL_NAME
  return 'Unknown'
