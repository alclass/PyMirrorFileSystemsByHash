#!/usr/bin/env python3
"""
DirTreeMirror_PrdPrjSw:
  default_settings.py

The method below was removed during a moment of deciding how to get the app's abspath:
======================================================================================
# the first constant below is not necessary if the app implements an installation or bootstrapping process
# for the time being, it's necessary for there's no installation or bootstrapping process
APPS_TOP_FOLDERNAME = 'PyMirrorFileSystemsByHashSwDv'

  @classmethod
  def normalize_to_appspath(cls, supposed_appspath):
    '''
    There are situations in which the Path(__file__) will not get the apps path correctly.
    One solution is to fix the main foldername that is the rootfolder of the app.
    Another solution might be to set the apps path in a variable in an installation process or a kind of bootstrapping.
    '''
    if supposed_appspath is None:
      return None
    while 1:
      cutpath, top = os.path.split(supposed_appspath)
      if top == APPS_TOP_FOLDERNAME:
        return supposed_appspath
      pp = cutpath.split('/')
      if len(pp) < 2 or pp == ['', '']:
        break
      supposed_appspath = cutpath
    error_msg = 'Failed to derived apps path from ' + supposed_appspath
    raise OSError(error_msg)

"""
import os
import sys
from pathlib import Path
DATA_FOLDERNAME = 'dados'
DEFAULT_DEVICEROOTDIR_SQLFILENAME = '.updirfileentries.sqlite'
DEFAULT_YTIDS_FILENAME_PREFIX = 'z_ls-R_contents-'
DEFAULT_YTIDSONLY_FILENAME = 'youtube-ids.txt'
DEFAULT_YTDIS_TABLENAME = 'ytids'
SOURCE_DIRTREE_FOLDERNAME_DEFAULT = 'src'  # this is the srcfolder default mainly for adhoc-testing
TARGET_DIRTREE_FOLDERNAME_DEFAULT = 'trg'  # this is the trgfolder default mainly for adhoc-testing
BUF_SIZE = 65536
SQL_SELECT_LIMIT_DEFAULT = 50
EXTENSIONS_TO_JUMP = ['.gif', '.htm', '.html', '.txt', '.pdf']
EXTENSIONS_IN_SHA1_VERIFICATION = [
  '.mp4', '.mp3', '.mkv', '.avi', '.webm', '.m4a'
]
RESTRICTED_DIRNAMES_FOR_WALK = ['.', 'z-del', 'z-tri', 'z-ext']  # z-tri covers z-Triage, z-ext covers z-Extra
FORBIBBEN_FIRST_LEVEL_DIRS = ['System Volume Information']
LIMIT_NUMBER_IN_WHILE_LOOP = 5000


class Paths:

  _app_abspath = None
  _datafolder_abspath = None

  @classmethod
  def get_app_abspath(cls):
    if cls._app_abspath is not None:
      return cls._app_abspath
    fpath = Path(__file__).parent
    fpath = os.path.abspath(fpath)
    # fpath = cls.normalize_to_appspath(fpath)
    cls._app_abspath = fpath
    return cls._app_abspath

  @classmethod
  def get_datafolder_abspath(cls):
    if cls._datafolder_abspath is not None:
      return cls._datafolder_abspath
    app_abspath = cls.get_app_abspath()
    cls._datafolder_abspath = os.path.join(app_abspath, DATA_FOLDERNAME)
    return cls._datafolder_abspath

  @classmethod
  def get_default_src_datafolder_abspath(cls):
    datafolder_abspath = cls.get_datafolder_abspath()
    return os.path.join(datafolder_abspath, SOURCE_DIRTREE_FOLDERNAME_DEFAULT)

  @classmethod
  def get_default_trg_datafolder_abspath(cls):
    datafolder_abspath = cls.get_datafolder_abspath()
    return os.path.join(datafolder_abspath, TARGET_DIRTREE_FOLDERNAME_DEFAULT)

  @classmethod
  def get_default_args_for_src_n_trg_mountpaths(cls):
    src_mountpath = cls.get_default_src_datafolder_abspath()
    trg_mountpath = cls.get_default_trg_datafolder_abspath()
    return src_mountpath, trg_mountpath


def get_apps_abspath_based_on_dirlevel_of_caller(caller_script_dirlevel):
  apps_abspath = Path(__file__)
  for i in range(caller_script_dirlevel):
    apps_abspath, _ = os.path.split(apps_abspath)
  return apps_abspath


def get_src_n_trg_mountpath_args_or_default():
  src_mountpath = None
  trg_mountpath = None
  try:
    src_mountpath = sys.argv[1]
    trg_mountpath = sys.argv[2]
  except IndexError:
    pass
  default_src_mountpath, default_trg_mountpath = Paths.get_default_args_for_src_n_trg_mountpaths()
  if src_mountpath is None or not os.path.isdir(src_mountpath):
    src_mountpath = default_src_mountpath
  if trg_mountpath is None or not os.path.isdir(trg_mountpath):
    trg_mountpath = default_trg_mountpath
  if not os.path.isdir(src_mountpath) or not os.path.isdir(trg_mountpath):
    pline = '''Parameter paths (either given or defaulted):
        -------------------
        src_mountpath %(src_mountpath)s
        trg_mountpath %(trg_mountpath)s
        -------------------
         => either one or both do not exist.
        ''' % {'src_mountpath': src_mountpath, 'trg_mountpath': trg_mountpath}
    print(pline)
    sys.exit(1)
  return src_mountpath, trg_mountpath


def adhoc_test1():
  ap = Paths.get_app_abspath()
  dfo = Paths.get_datafolder_abspath()
  src, trg = Paths.get_default_args_for_src_n_trg_mountpaths()
  print("app's path =", ap)
  print("data path =", dfo)
  print('src =', src)
  print('trg =', trg)
  src, trg = get_src_n_trg_mountpath_args_or_default()
  print('src =', src)
  print('trg =', trg)


def process():
  adhoc_test1()


if __name__ == '__main__':
  process()
