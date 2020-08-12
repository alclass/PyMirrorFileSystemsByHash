#!/usr/bin/env python3
"""

"""
import os
import models.samodels as sam
import models.pathpositioning.metafilemod as metaf
import config

SOURCE_ABSPATH_DICTKEY = 'source_abspath'
TARGET_ABSPATH_DICTKEY = 'target_abspath'


def get_abspaths():
  pdict = eval(open('data_entry_dir_source_n_target.pydict.txt').read())
  print(pdict)
  source_abspath = pdict[SOURCE_ABSPATH_DICTKEY]
  target_abspath = pdict[TARGET_ABSPATH_DICTKEY]
  print(source_abspath, target_abspath)
  return source_abspath, target_abspath


def make_updir_sweep(mount_abspath):
  for abspath, dirs, files in os.walk(os.path.abspath(mount_abspath)):
    if abspath == mount_abspath:
      middlepath = ''
    else:
      middlepath = abspath[len(mount_abspath)+1:]
    for nd, folder in enumerate(sorted(dirs)):
      print(nd+1, 'dir =>', folder, abspath)
    print('-'*30)
    for nf, eachFile in enumerate(sorted(files)):
      mfile = metaf.MetaFile(mount_abspath, middlepath, eachFile)  # mockmode=True
      print(nf+1, 'eachFile =>', eachFile, abspath)
      print(mfile)


def sweep_src_n_trg():
  mount_abspath = config.get_datatree_mountpoint_abspath(source=True)
  make_updir_sweep(mount_abspath)
  mount_abspath = config.get_datatree_mountpoint_abspath(source=False)
  make_updir_sweep(mount_abspath)


def process():
  sweep_src_n_trg()


if __name__ == '__main__':
  process()
