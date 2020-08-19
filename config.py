#!/usr/bin/env python3

import os
# import pathlib

THIS_MODULES_FOLDER_ABSPATH = os.path.dirname(__file__)
USER_HOME_DIR = os.path.expanduser('~')
USER_HOME_DATA_DIR = os.path.join(USER_HOME_DIR, '.pymirrorfsbyhash_data')
DATA_FOLDERNAME = 'dados'
PYMIRROR_DB_PARAMS = {}
COUNTER_ROTATE_SIZE_FOR_COMMITS = 1000
EMPTYFILE_SHA1HEX = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
L1DIRS_TO_AVOID_IN_MIRRORING = ['z Extra', 'z Tmp', 'z Triage', 'z-Extra', 'z-Tmp', 'z-Triage']

MOUNTPOINT_SOURCEDATADIR_DICTKEY = 'MOUNTPOINT_SOURCEDATADIR'
MOUNTPOINT_TARGETDATADIR_DICTKEY = 'MOUNTPOINT_TARGETDATADIR'

# MOUNTPOINT_DATADIRS_DICT = {}
MOUNTPOINT_DIRS_DICTFILENAME = 'data_entry_dir_source_n_target_pydict.txt'
SQLITE_UPDIRENTRIES_DEFAULT_FILENAME = '.updirentries.sqlite'


def get_dirtree_tablename():
  return SQLITE_UPDIRENTRIES_DEFAULT_FILENAME


def get_mountpoint_datadirs_dict():
  abspath = os.path.join(get_apps_root_abspath(), MOUNTPOINT_DIRS_DICTFILENAME)
  return eval(open(abspath).read())


def get_datatree_mountpoint_abspath(source=True):
  pdict = get_mountpoint_datadirs_dict()
  if source:
    return pdict[MOUNTPOINT_SOURCEDATADIR_DICTKEY]
  return pdict[MOUNTPOINT_TARGETDATADIR_DICTKEY]


def get_datatree_sqlitefilepath(source=True):
  abspath = get_datatree_mountpoint_abspath(source)
  return os.path.join(abspath, SQLITE_UPDIRENTRIES_DEFAULT_FILENAME)


def get_apps_root_abspath():
  return THIS_MODULES_FOLDER_ABSPATH
  # return pathlib.Path(THIS_MODULE_ABSPATH).parent


def get_data_abspath():
  return os.path.join(get_apps_root_abspath(), DATA_FOLDERNAME)


try:
  import local_settings
  USER_DATA_PATH_SET = local_settings.USER_DATA_PATH_SET or USER_HOME_DATA_DIR
except ModuleNotFoundError:
  USER_DATA_PATH_SET = USER_HOME_DATA_DIR


def adhoc_test():
  apps_root_abspath = get_apps_root_abspath()
  print('apps_root_abspath', apps_root_abspath)
  print('__file__', __file__)
  print(get_data_abspath())
  mountpoint_srcdir = get_datatree_sqlitefilepath(source=True)
  mountpoint_trgdir = get_datatree_sqlitefilepath(source=False)
  print('mountpoint_srcdir', mountpoint_srcdir)
  print('mountpoint_trgdir', mountpoint_trgdir)
  datatree_sqlitefilepath = get_datatree_sqlitefilepath()
  print('datatree_sqlitefilepath', datatree_sqlitefilepath)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
