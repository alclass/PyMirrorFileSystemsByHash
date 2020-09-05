#!/usr/bin/env python3
"""

"""
import datetime
import time
import models.crawlerprocessors.dirnodes_traversal_mod as dirmod
import fs.db.sqlalchemy_conn as con
import fs.os.middlepathmakemod as midpath
# import fs.os.prep_fs_counts_mod as prep
import config

class DirTraversor:

  def __init__(self, mount_abspath=None, sqlite_abspath=None):
    self.session = None
    self.sqlite_abspath = None
    # self.mount_abspath = None
    self.mountpoint_abspath = config.get_datatree_mountpoint_abspath(source=True)
    self.total_dirs = 0

  def traverse_tree_left_to_right_recurse(self, dirnode):
    self.total_dirs += 1
    for dirname in dirnode.children_dirnames:
      next_dirnode = dirnode.create_childnode_with_name(dirname)
      print('Instantiated', next_dirnode)
      self.traverse_tree_left_to_right_recurse(next_dirnode)
    dirnode.concatenate_sha1hexes(self.session)
    if self.session is not None and not dirnode.is_root():
      print('Saving to db => dirnode.concatenate_sha1hexes() =', dirnode.sha1hex, str(dirnode))
      dirnode.save_to_db(self.session, docommit=True)
    else:
      print('NOT saving to db => dirnode.concatenate_sha1hexes() =', dirnode.sha1hex, str(dirnode))
    if dirnode.parentnode is not None:
      # if None it's root that does not have parent
      dirnode.parentnode.add_sha1_calculated_child(dirnode)
    return

  def traverse_tree_left_to_right_entrance(self):
    self.session = con.get_session_for_sqlite_source_or_target(source=True)
    print('Instantiating root')
    middlepathobj = midpath.MiddlePath(self.mountpoint_abspath)
    dirnoderoot = dirmod.DirNode(None, '', middlepathobj)
    self.traverse_tree_left_to_right_recurse(dirnoderoot)
    print('dirnoderoot.sha1hex', dirnoderoot.sha1hex)
    self.session.close()


def process():
  # sweep_src_n_trg()
  # dirtree_folders_traverse()
  start_time = datetime.datetime.now()
  traversor = DirTraversor()
  traversor.traverse_tree_left_to_right_entrance()
  finish_time = datetime.datetime.now()
  elapsed_time = finish_time - start_time
  print('start_time', start_time)
  print('finish_time', finish_time)
  print('elapsed_time', elapsed_time)
  print('total_dirs', traversor.total_dirs)


if __name__ == '__main__':
  process()
