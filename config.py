#!/usr/bin/env python3

import os
import pathlib

THIS_MODULES_FOLDER_ABSPATH = os.path.dirname(__file__)
USER_HOME_DIR = os.path.expanduser('~')
USER_HOME_DATA_DIR = os.path.join(USER_HOME_DIR, '.pymirrorfsbyhash_data')
DATA_FOLDERNAME = 'dados'

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


def process():
  adhoc_test()


if __name__ == '__main__':
  process()