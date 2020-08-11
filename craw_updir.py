#!/usr/bin/env python3
"""

"""
import os

SOURCE_ABSPATH_DICTKEY = 'source_abspath'
TARGET_ABSPATH_DICTKEY = 'target_abspath'


def get_abspaths():
  pdict = eval(open('data_entry_dir_source_n_target.pydict.txt').read())
  print(pdict)
  source_abspath = pdict[SOURCE_ABSPATH_DICTKEY]
  target_abspath = pdict[TARGET_ABSPATH_DICTKEY]
  print(source_abspath, target_abspath)
  return source_abspath, target_abspath


def make_updir_sweep(p_abspath):
  for abspath, dirs, files in os.walk(os.path.abspath(p_abspath)):
    for nd, folder in enumerate(sorted(dirs)):
      print(nd+1, 'dir =>', folder, abspath)
    print('-'*30)
    for nf, eachFile in enumerate(sorted(files)):
      print(nf+1, 'eachFile =>', eachFile, abspath)


def process():
  source_abspath, target_abspath = get_abspaths()
  make_updir_sweep(source_abspath)
  print('=' * 30)
  make_updir_sweep(target_abspath)


if __name__ == '__main__':
  process()
