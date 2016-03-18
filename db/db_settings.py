#!/usr/bin/env python
#-*-encoding:utf8-*-
#import os

class DBMS_CONSTANTS:
  MYSQL      = 1
  SQLITE     = 2
  POSTGRESQL = 3

DATABASE_NAME = 'PYMIRROR_file_hashes_down_dir_tree'
class PYMIRROR_DB_PARAMS:

  DATABASE_NAME = DATABASE_NAME
  CONVENTIONED_TOP_ROOT_FOLDER_ID = 1 # it's convencioned here that root's parent is itself (the only exception to parent-child pointing is 'root')
  CONVENTIONED_DUMMY_PARENT_OF_TOP_ROOT_FOLDER_ID = 0 # notice that 0 (the dummy parent id for root) will only exist in the linked list table, not in the folder entries table
  CONVENTIONED_ROOT_DIR_NAME = 'TOP_ROOT_FOLDER'

  class TABLE_NAMES:
    FILE_ENTRIES        = 'file_entries'
    DIR_ENTRIES         = 'dir_entries'
    ENTRIES_LINKED_LIST = 'parent_dir_linked_list_table'

  class SQLITE:
    HASHES_ETC_DATA_FILENAME = DATABASE_NAME + '.sqlite'

  class MYSQL:
    pass

  class POSTGRESQL:
    pass
