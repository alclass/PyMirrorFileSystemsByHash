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
  CONVENTIONED_ROOT_ENTRY_ID = 0 # it's convencioned here that root's parent is itself (the only exception to parent-child pointing is 'root')
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
