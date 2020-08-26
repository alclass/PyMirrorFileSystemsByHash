#!/usr/bin/env python3
"""

"""
import time
import models.crawlerprocessors.dirnodes_traversal_mod as dirmod
import fs.db.sqlalchemy_conn as con
import fs.os.middlepathmakemod as midpath
# import fs.os.prep_fs_counts_mod as prep
import config


def traverse_tree_left_to_right_recurse(dirnode, session=None):
  for dirname in dirnode.children_dirnames:
    next_dirnode = dirnode.create_childnode_with_name(dirname)
    print('Instantiated', next_dirnode)
    traverse_tree_left_to_right_recurse(next_dirnode, session)
  dirnode.concatenate_sha1hexes(session)
  if session is not None and not dirnode.is_root():
    print('Saving to db => dirnode.concatenate_sha1hexes() =', dirnode.sha1hex, str(dirnode))
    dirnode.save_to_db(session, docommit=True)
  else:
    print('NOT saving to db => dirnode.concatenate_sha1hexes() =', dirnode.sha1hex, str(dirnode))
  if dirnode.parentnode is not None:
    # if None it's root that does not have parent
    dirnode.parentnode.add_sha1_calculated_child(dirnode)
  return


def traverse_tree_left_to_right_entrance():
  session = con.get_session_from_sqlitefilepath(source=True)
  mountpoint_abspath = config.get_datatree_mountpoint_abspath(source=True)
  print('Instantiating root')
  middlepathobj = midpath.MiddlePath(mountpoint_abspath)
  dirnoderoot = dirmod.DirNode(None, '', middlepathobj)
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
