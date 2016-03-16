#!/usr/bin/env python
#-*-encoding:utf8-*-
import os

class PYMIRROR_DB_PARAMS:
  SQLITE_ROOTDIR_FILENAME_DEFAULT = 'hashed_files_thru_dir_tree.sqlite'
  CONVENTIONED_ROOT_ENTRY_ID      =  0
  CONVENTIONED_ROOT_DIR_NAME      =  'TOP_ROOT_FOLDER'
  FIRST_ENTRY_ID_FOR_FILES_WHEN_DB_EMPTY = 0
class DB_TABLE_NAMES:
  FILE_ENTRIES        = 'file_entries'
  DIR_ENTRIES         = 'dir_entries'
  ENTRIES_LINKED_LIST = 'entries_linked_list'
class DBMS_ID_CONSTANTS:
  MYSQL  = 1
  SQLITE = 2

