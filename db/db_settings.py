#!/usr/bin/env python
#-*-encoding:utf8-*-
#import os

DATABASE_NAME = 'PYMIRROR_file_hashes_down_dir_tree'
SQLITE     = 1
MYSQL      = 2
POSTGRESQL = 3


class PYMIRROR_DB_PARAMS:

  class ENTRY_TYPE_ID:
    FOLDER = 0
    FILE   = 1

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
    FILE_ATTRIB_VALUES    = 'file_entries' # tablename_for_file_attrib_values
    FILE_N_FOLDER_ENTRIES = 'dir_entries'  # tablename_for_file_n_folder_entries
    ENTRIES_LINKED_LIST   = 'parent_dir_linked_list_table' # tablename_for_entries_linked_list
    AUXTAB_FOR_PRE_PREPARED_PATHS = 'auxtab_path_id_list_per_folder'

  class FIELD_NAMES_ACROSS_TABLES:
    '''
    # fieldnames considered 'standard-safe', ie, don't need to be here:
      1 id, 2 entryname, 3 entrytype, 4 sha1hex, 5 filesize &  6 modified_datetime
    In a nut shell, the two here are:
      + fieldname_for_parent_or_home_dir_id because
        it might confuse at the moment of writing a SELECT/UPDATE/INSERT/DELETE whether
        it's parent_dir_id or home_dir_id
      + fieldname_for_entries_path_id_list_str
    '''
    PARENT_OR_HOME_DIR_ID    = 'home_dir_id' # fieldname_for_parent_or_home_dir_id
    ENTRIES_PATH_ID_LIST_STR = 'entries_path_id_list_str'


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
