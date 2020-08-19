#!/usr/bin/env python3
"""

"""
import os
import time
import models.crawlerprocessors.dirnodes_traversal_mod as dirtrav
import fs.db.sqlalchemy_conn as con
import config


def dirtree_folders_traverse():
  session = con.get_session_from_sqlitefilepath(source=True)
  mountpoint_abspath = config.get_datatree_mountpoint_abspath(source=True)
  parent_abspath, mountdirname = os.path.split(mountpoint_abspath)
  rootdirnode = dirtrav.DirNode(mountdirname, parentpath=parent_abspath)
  dirtrav.load_tree_n_traverse_topdown_leftright(rootdirnode, session)


def process():
  # sweep_src_n_trg()
  start_time = time.time()
  dirtree_folders_traverse()
  elapsed_time = time.time() - start_time
  print('start_time', start_time)
  print('elapsed_time', elapsed_time)


if __name__ == '__main__':
  process()
