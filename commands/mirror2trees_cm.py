#!/usr/bin/env python3
"""
mirror2trees_cm.py

This script does basically two things:
  1) it moves target-tree files to the relative position, in the target-tree itself, that exists in the source-tree;
     (in case of repeats, the first one fetched will be used for moving, if possible)
  2) it copies missing files in the target-tree that exists in the source-tree;
     (in case of copy, sha1 is missing in target, so there's no worries about repeats)

Things this script doesn't do:
  1) This script DOESN'T do removals.
  2) This script DOESN'T do the inverse of the two operations above.

The complete mirroring effect, encompassing these two non-scoped operations above,
  is undertaken by other scripts in this system / Python package. (One of them is crosscopy_between_trees_cm.py).
"""
import copy
import datetime
import os.path
import shutil
import fs.db.dbdirtree_mod as dbdt
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import models.entries.dirnode_mod as dn
import default_settings as defaults


class MirrorDirTree:

  def __init__(self, ori_mountpath, bak_mountpath):
    self.start_time = datetime.datetime.now()
    self.ori_dt = dbdt.DBDirTree(ori_mountpath)
    self.bak_dt = dbdt.DBDirTree(bak_mountpath)
    self.n_moved = 0
    self.n_failed_moves = 0
    self.n_copied = 0
    self.n_failed_copies = 0
    self.n_processed_files = 0
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    self.total_srcfiles_in_os = 0
    self.total_srcdirs_in_os = 0
    self.total_trgfiles_in_os = 0
    self.total_trgdirs_in_os = 0
    self.fetch_total_files_in_src_n_trg()

  def fetch_total_files_in_src_n_trg(self):
    print('Counting totals, please wait.')
    self.total_srcfiles_in_db = self.ori_dt.count_rows_as_int()
    print('total_srcfiles_in_db', self.total_srcfiles_in_db)
    self.total_trgfiles_in_db = self.bak_dt.count_rows_as_int()
    print('total_trgfiles_in_db', self.total_trgfiles_in_db)
    self.total_unique_srcfiles = self.ori_dt.count_unique_sha1s_as_int()
    print('total_unique_srcfiles', self.total_unique_srcfiles)
    self.total_unique_trgfiles = self.bak_dt.count_unique_sha1s_as_int()
    print('total_unique_trgfiles', self.total_unique_trgfiles)
    tot_files, tot_dirs = dirf.count_total_files_n_folders_excl_root(self.ori_dt.mountpath)
    self.total_srcfiles_in_os, self.total_srcdirs_in_os = tot_files, tot_dirs
    print('total_srcfiles_in_os', self.total_srcfiles_in_os)
    print('total_srcdirs_in_os', self.total_srcdirs_in_os)
    tot_files, tot_dirs = dirf.count_total_files_n_folders_excl_root(self.bak_dt.mountpath)
    self.total_trgfiles_in_os, self.total_trgdirs_in_os = tot_files, tot_dirs
    print('total_trgfiles_in_os', self.total_trgfiles_in_os)
    print('total_trgdirs_in_os', self.total_trgdirs_in_os)

  @property
  def total_of_repeat_srcfiles(self):
    return self.total_srcfiles_in_db - self.total_unique_srcfiles

  @property
  def total_of_repeat_trgfiles(self):
    return self.total_trgfiles_in_db - self.total_unique_trgfiles

  def fetch_row_if_sha1_exists_in_target(self, sha1):
    """
    repeats are not treated here, so, if many files with the same sha1, return the first one in result_list
    """
    sql = 'SELECT * from %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = self.bak_dt.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list is None or len(fetched_list) == 0:
      return None
    return fetched_list[0]

  def verify_if_a_move_within_trg_is_needed(self, src_dirnode, trg_dirnode):
    wherefile_is_path = os.path.join(self.bak_dt.mountpath, trg_dirnode.path)
    mirrored_trg_dirnode = copy.copy(src_dirnode)
    wherefile_shouldbe_path = os.path.join(self.bak_dt.mountpath, mirrored_trg_dirnode.path)
    if wherefile_shouldbe_path == wherefile_is_path:
      print('No need to move, file is already in place')
      print(self.n_processed_files, '/', self.total_srcfiles_in_db, src_dirnode.name, '@', src_dirnode.parentpath)
      return False
    bak_dirpath, _ = os.path.split(wherefile_shouldbe_path)
    if not os.path.isdir(bak_dirpath):
      os.makedirs(bak_dirpath)
    if not os.path.isfile(wherefile_is_path):
      print('in move: file does not exist', wherefile_is_path)
      return False
    if not os.path.isfile(wherefile_shouldbe_path):
      print('in move: target file already exists', wherefile_shouldbe_path)
      return False
    print('move', self.n_moved + 1, '/', self.total_srcfiles_in_db)
    print('FROM: ', wherefile_is_path)
    print('TO: ', wherefile_shouldbe_path)
    print('-' * 40)
    try:
      shutil.move(wherefile_is_path, wherefile_shouldbe_path)
      self.n_moved += 1
    except (IOError, OSError):
      self.n_failed_moves += 1
      return False
    # if it really copied over, insert it into db
    if os.path.isdir(wherefile_shouldbe_path):
      mirrored_trg_dirnode.insert_into_db(self.bak_dt)
      return True
    return False

  def copy_src_to_trg(self, src_dirnode):
    print('PATH SHOULD be copied', src_dirnode.name, '@', src_dirnode.parentpath)
    src_filepath = os.path.join(self.ori_dt.mountpath, src_dirnode.path)
    if not os.path.isfile(src_filepath):
      print(src_filepath, 'does not exist, cannot copy')
      return False
    mirrored_trg_dirnode = copy.copy(src_dirnode)
    mirrored_filepath = os.path.join(self.bak_dt.mountpath, mirrored_trg_dirnode.path)
    if os.path.isfile(mirrored_filepath):
      print(mirrored_filepath, 'does exist as target, cannot copy')
      return False
    print('copy', self.n_copied+1, '/', self.total_srcfiles_in_db)
    print('FROM: ', src_filepath)
    print('TO: ', mirrored_filepath)
    try:
      shutil.copy2(src_filepath, mirrored_filepath)
      self.n_copied += 1
    except (IOError, OSError):
      self.n_failed_copies += 1
      return False
    if os.path.isfile(mirrored_filepath):
      return mirrored_trg_dirnode.insert_into_db(self.bak_dt)
    return False

  def verify_copy_or_move_or_none(self, src_dirnode):
    trg_row = self.fetch_row_if_sha1_exists_in_target(src_dirnode.sha1)
    if trg_row is not None:
      return self.copy_src_to_trg(src_dirnode)
    trg_dirnode = dn.DirNode.create_with_tuplerow(trg_row, self.bak_dt.fieldnames)
    return self.verify_if_a_move_within_trg_is_needed(src_dirnode, trg_dirnode)

  def process_mirroring_by_copying_or_moving(self, src_rowlist):
    for src_row in src_rowlist:
      self.n_processed_files += 1
      src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.fieldnames)
      print(
        'proc', self.n_processed_files, '/', self.total_srcfiles_in_db,
        src_dirnode.name, '@', src_dirnode.parentpath
      )
      bool_ret = self.verify_copy_or_move_or_none(src_dirnode)
      print('bool_ret', bool_ret)

  def processing_dirtrees_mirroring(self):
    print('mirror_by_moving_within_targetdirtree')
    print('='*40)
    for src_rowlist in self.ori_dt.do_select_all_w_limit_n_offset():
      self.process_mirroring_by_copying_or_moving(src_rowlist)
    self.report()

  def report(self):
    print('='*50)
    print('Report MirrorDirTree: ')
    print('='*50)
    print('ori', self.ori_dt.mountpath)
    print('bak', self.bak_dt.mountpath)
    print('n_moved', self.n_moved)
    print('n_failed_moves', self.n_failed_moves)
    print('n_copied', self.n_copied)
    print('n_failed_copies', self.n_failed_copies)
    print('n_processed_files', self.n_processed_files)
    print('total_srcfiles_in_db', self.total_srcfiles_in_db)
    print('total_trgfiles_in_db', self.total_trgfiles_in_db)
    print('total_unique_srcfiles', self.total_unique_srcfiles)
    print('total_unique_trgfiles', self.total_unique_trgfiles)
    print('total_srcfiles_in_os', self.total_srcfiles_in_os)
    print('total_srcdirs_in_os', self.total_srcdirs_in_os)
    print('total_trgfiles_in_os', self.total_trgfiles_in_os)
    print('total_trgdirs_in_os', self.total_trgdirs_in_os)
    print('total_of_repeat_srcfiles', self.total_of_repeat_srcfiles)
    print('total_of_repeat_trgfiles', self.total_of_repeat_trgfiles)
    finished_time = datetime.datetime.now()
    process_duration = finished_time - self.start_time
    print('process_duration', process_duration)


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  mirror = MirrorDirTree(src_mountpath, trg_mountpath)
  mirror.processing_dirtrees_mirroring()


if __name__ == '__main__':
  process()
