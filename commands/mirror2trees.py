#!/usr/bin/env python3
"""
mirror2trees.py

This script does basically two things:
  1) it moves target-tree files to the relative position, in the target-tree itself, that exists in the source-tree;
  2) it copies missing files in the target-tree that exists in the source-tree;

Things this script doesn't do:
  1) This script DOESN'T do removals.
  2) This script DOESN'T do the inverse of the two operations above.

The complete mirroring effect, encompassing these two non-scoped operations above,
  is undertake by other scripts in this system / Python package.
"""
import os.path
import shutil
import sys

import models.entries.dirnode_mod as dn


class MirrorDirTree:

  def __init__(self, ori_dt, bak_dt):
    self.ori_dt = ori_dt
    self.bak_dt = bak_dt

  def fetch_row_if_sha1_exists_in_target(self, sha1):
    sql = 'SELECT * from %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = self.bak_dt.dirtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(fetched_list) == 0:
      return None
    if len(fetched_list) == 1:
      return fetched_list[0]
    error_msg = "Inconsistency Error: db has more than 1 sha1 (%s)" \
                " when it's a UNIQUE fields ie it can only contains one" % sha1
    raise ValueError(error_msg)

  def move_file_within_its_dirtree(self, trg_dirnode_to_move, src_ref_dirnode):
    print('PATH SHOULD be moved')
    print('FROM: ', trg_dirnode_to_move.path)
    print('TO: ', src_ref_dirnode.path)
    file_now_path = os.path.join(self.bak_dt.mountpath, trg_dirnode_to_move.path)
    file_new_path = os.path.join(self.ori_dt.mountpath, src_ref_dirnode.path)
    print('FROM: ', file_now_path)
    print('TO: ', file_new_path)
    print('-'*40)

  def verify_moving_files_in_target(self, src_rowlist):
    for src_row in src_rowlist:
      sha1 = src_row[4]
      trg_row = self.fetch_row_if_sha1_exists_in_target(sha1)
      if trg_row is None:
        continue
      src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.fieldnames)
      trg_dirnode = dn.DirNode.create_with_tuplerow(trg_row, self.bak_dt.fieldnames)
      if src_dirnode.is_target_in_the_same_pathposition(trg_dirnode):
        continue
      self.move_file_within_its_dirtree(trg_dirnode, src_dirnode)

  def mirror_by_moving_within_targetdirtree(self):
    print('mirror_by_moving_within_targetdirtree')
    print('='*40)
    for i, src_rowlist in enumerate(self.ori_dt.dbtree.do_select_all_w_limit_n_offset()):
      _ = len(src_rowlist)
      # print(i+1, 'n_rows', n_rows)
      self.verify_moving_files_in_target(src_rowlist)

  def verify_copying_source_files_in_target(self, src_rowlist):
    for src_row in src_rowlist:
      sha1 = src_row[4]
      trg_row = self.fetch_row_if_sha1_exists_in_target(sha1)
      if trg_row is not None:
        # no need to copy over
        continue
      src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.fieldnames)
      trg_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.bak_dt.fieldnames)
      ori_path = os.path.join(self.ori_dt.mountpath, src_dirnode.path)
      bak_path = os.path.join(self.bak_dt.mountpath, trg_dirnode.path)
      bak_dirpath, _ = os.path.split(bak_path)
      if not os.path.isdir(bak_dirpath):
        os.makedirs(bak_dirpath)
      if not os.path.isfile(bak_path):
        shutil.copy(ori_path, bak_path)
      # if it really copied over, insert it into db
      if os.path.isdir(bak_dirpath):
        trg_dirnode.insert_into_db(self.bak_dt.dbtree)
      else:
        error_msg = 'Runtime Error: Copy of %(trg_dirnode) failed.' % trg_dirnode
        raise ValueError(error_msg)

  def mirror_by_copying_across_dirtrees(self):
    print('move_files_if_ext')
    print('='*40)
    for i, src_rowlist in enumerate(self.ori_dt.dbtree.do_select_all_w_limit_n_offset()):
      _ = len(src_rowlist)
      # print(i+1, 'n_rows', n_rows)
      self.verify_copying_source_files_in_target(src_rowlist)


def get_args():
  src_mountpath = None
  trg_mountpath = None
  try:
    src_mountpath = sys.argv[1]
    trg_mountpath = sys.argv[2]
  except IndexError:
    pass
  return src_mountpath, trg_mountpath


def process():
  """
  """
  src_mountpath, trg_mountpath = get_args()
  if src_mountpath is None or trg_mountpath is None:
    print('Missing parameters src_mountpath and trg_mountpath. Please retry.')
    sys.exit(1)
  if not os.path.isdir(src_mountpath) or not os.path.isdir(trg_mountpath):
    print('Either src_mountpath or trg_mountpath does not exist as an os-path. Please look into it and retry.')
    sys.exit(1)
  mirror = MirrorDirTree(src_mountpath, trg_mountpath)
  mirror.mirror_by_moving_within_targetdirtree()
  mirror.mirror_by_copying_across_dirtrees()


if __name__ == '__main__':
  process()
