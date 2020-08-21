#!/usr/bin/env python3
"""

"""
import time
import models.crawlerprocessors.dirnodes_traversal_mod as dirmod
import fs.db.sqlalchemy_conn as con
import fs.os.prep_fs_counts_mod as prep
import config


def traverse_tree_left_to_right_recurse(dirnode, session=None):
  for dirname in dirnode.children_dirnames:
    middlepath = prep.extract_middlepath_for_files_or_subfolders_from_abspath(
      dirnode.mountpoint_abspath, dirnode.abspath
    )
    next_dirnode = dirmod.DirNode(dirname, middlepath, dirnode.mountpoint_abspath)
    print('Instantiated', next_dirnode)
    next_dirnode.parentnode = dirnode
    traverse_tree_left_to_right_recurse(next_dirnode, session)
  dirnode.concatenate_sha1hexes()
  if session is not None and not dirnode.is_root():
    print('Saving to db => dirnode.concatenate_sha1hexes() =', dirnode.sha1hex, str(dirnode))
    dirnode.save_to_db(session, docommit=True)
  else:
    print('NOT saving to db => dirnode.concatenate_sha1hexes() =', dirnode.sha1hex, str(dirnode))
  parentnode = dirnode.parentnode
  if parentnode is not None:
    dirnode.parentnode.add_sha1_calculated_child(dirnode)
  return


def traverse_tree_left_to_right_entrance():
  session = con.get_session_from_sqlitefilepath(source=True)
  mountpoint_abspath = config.get_datatree_mountpoint_abspath(source=True)
  print('Instantiating root')
  dirnoderoot = dirmod.DirNode(None, None, mountpoint_abspath)
  traverse_tree_left_to_right_recurse(dirnoderoot, session)
  print('dirnoderoot.sha1hex', dirnoderoot.sha1hex)
  session.close()


def process():
  # sweep_src_n_trg()
  # dirtree_folders_traverse()
  start_time = time.time()
  traverse_tree_left_to_right_entrance()
  elapsed_time = time.time() - start_time
  print('start_time', start_time)
  print('elapsed_time', elapsed_time)


if __name__ == '__main__':
  process()
