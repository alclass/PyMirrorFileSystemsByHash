#!/usr/bin/env python
#-*-encoding:utf8-*-
import os
PYMIRROR_SYSTEM_BASE_PATH = os.path.dirname(__file__)
PYMIRROR_SYSTEM_DATA_PATH = os.path.join(PYMIRROR_SYSTEM_BASE_PATH, 'data')

USER_HOME_DIR = os.path.expanduser('~')
USER_HOME_DATA_DIR = os.path.join(USER_HOME_DIR, '.pymirrorfsbyhash_data')

try:
  import local_settings
  USER_DATA_PATH_SET = local_settings.USER_DATA_PATH_SET or USER_HOME_DATA_DIR
except ModuleNotFoundError:
  USER_DATA_PATH_SET = USER_HOME_DATA_DIR
