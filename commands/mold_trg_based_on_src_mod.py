#!/usr/bin/env python3
"""
mold_trg_based_on_src_mod.py
"""
import copy
import os.path
import shutil
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
# import commands.dbentry_updater_by_filemove_based_on_size_n_mdt_mod as dbentry_upd
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirf


class TrgBasedOnSrcMolder:

  def __init__(self, src_mountpath, trg_mountpath):
    self.ori_dt = dt.DirTree('ori', src_mountpath)
    self.bak_dt = dt.DirTree('bak', trg_mountpath)
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    self.total_srcfiles_in_os = 0
    self.total_trgfiles_in_os = 0
    self.total_srcdirs_in_os = 0
    self.total_trgdirs_in_os = 0
    self.n_copied_files = 0
    self.n_moved_files = 0
    self.calc_totals()

  def calc_totals(self):
    print('Counting files and dirs in db and os. Please wait.')
    self.total_unique_srcfiles = self.ori_dt.dbtree.count_unique_sha1s_as_int()
    self.total_unique_trgfiles = self.bak_dt.dbtree.count_unique_sha1s_as_int()
    self.total_srcfiles_in_db = self.ori_dt.dbtree.count_rows_as_int()
    self.total_trgfiles_in_db = self.bak_dt.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders(self.ori_dt.mountpath)
    self.total_srcfiles_in_os = total_files
    self.total_srcdirs_in_os = total_dirs
    total_files, total_dirs = dirf.count_total_files_n_folders(self.bak_dt.mountpath)
    self.total_trgfiles_in_os = total_files
    self.total_trgdirs_in_os = total_dirs

  def print_counts(self):
    print('total_unique_srcfiles:', self.total_unique_srcfiles)
    print('total_unique_trgfiles:', self.total_unique_trgfiles)
    print('total_srcfiles_in_db:', self.total_srcfiles_in_db)
    print('total_trgfiles_in_db:', self.total_trgfiles_in_db)
    print('total_srcfiles_in_os:', self.total_srcfiles_in_os)
    print('total_srcdirs_in_os:', self.total_srcdirs_in_os)
    print('total_trgfiles_in_os:', self.total_trgfiles_in_os)
    print('total_trgdirs_in_os:', self.total_trgdirs_in_os)

  def find_sha1_in_trg_n_return_trg_dirnode(self, sha1, src_name, src_parentpath):
    if sha1 is None:
      return None
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1, )
    fetched_list = self.bak_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list is None:
      return None
    fieldnames = self.bak_dt.dbtree.fieldnames
    if len(fetched_list) == 1:
      row = fetched_list[0]
      trg_dirnode = dn.DirNode.create_with_tuplerow(row, fieldnames)
      return trg_dirnode
    # at this point len(fetched_list) > 1
    trg_dirnode = None
    for row in fetched_list:
      idx = fieldnames.index('name')
      name = row[idx]
      idx = fieldnames.index('parentpath')
      parentpath = row[idx]
      if parentpath == src_parentpath:
        trg_dirnode = dn.DirNode.create_with_tuplerow(row, fieldnames)
        if name == src_name:
          return trg_dirnode
    if trg_dirnode is not None:
      return trg_dirnode
    # ok, at least a coincidence with parentpath was not found, take the first one and return
    # the returning point will move it to its equivalent source parentpath and name
    if len(fetched_list) > 0:
      row = fetched_list[0]
      trg_dirnode = dn.DirNode.create_with_tuplerow(row, fieldnames)
    return trg_dirnode

  def move_file_within_trg_to_its_src_relative_position_if_vacant(self, src_dirnode, trg_dirnode):
    new_trg_dirnode = copy.copy(src_dirnode)
    new_trg_dirnode.name = src_dirnode.name
    new_trg_dirnode.parentpath = src_dirnode.parentpath
    oldfile = trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    newfile = new_trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    if oldfile == newfile:
      return False
    if not os.path.isfile(oldfile):
      return False
    if os.path.isfile(newfile):
      return False
    # now a move can be tried
    basepath, _ = os.path.split(newfile)
    if not os.path.isdir(basepath):
      os.makedirs(basepath)
    self.n_moved_files += 1
    print(
      self.n_moved_files, 'of', self.total_srcfiles_in_os,
      'moving within trg', new_trg_dirnode.name, new_trg_dirnode.parentpath
    )
    shutil.move(oldfile, newfile)
    return True

  def copy_over_src_to_trg(self, src_dirnode):
    src_filepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mountpath)
    trg_filepath = src_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    if not os.path.isfile(src_filepath):
      # file is not there, it can't be copied
      return False
    if os.path.isfile(trg_filepath):
      # another option is to autorename (with integers at name's end) the one in target
      return False
    basepath, _ = os.path.split(trg_filepath)
    if not os.path.isdir(basepath):
      os.makedirs(basepath)
    self.n_copied_files += 1
    print(self.n_copied_files, 'of', self.total_srcfiles_in_os, 'copying', src_dirnode.name, src_dirnode.parentpath)
    shutil.copy2(src_filepath, trg_filepath)
    return True

  def move_trg_file_if_applicable(self, src_row):
    src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.dbtree.fieldnames)
    src_filepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mountpath)
    if not os.path.isfile(src_filepath):
      return False
    trg_dirnode = self.find_sha1_in_trg_n_return_trg_dirnode(src_dirnode.sha1, src_dirnode.name, src_dirnode.parentpath)
    if trg_dirnode is None:
      return False
    trg_filepath = trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    if not os.path.isfile(trg_filepath):
      return self.copy_over_src_to_trg(src_dirnode)
    if trg_dirnode.name == src_dirnode.name and trg_dirnode.parentpath == src_dirnode.parentpath:
      # no need to move target
      return False
    return self.move_file_within_trg_to_its_src_relative_position_if_vacant(src_dirnode, trg_dirnode)

  def process_src_rows(self, rows):
    for row in rows:
      print(row[0], 'Processing:', row)
      _ = self.move_trg_file_if_applicable(row)

  def sweep_src_files_in_db(self):
    for i, generated_rows in enumerate(self.ori_dt.dbtree.do_select_all_w_limit_n_offset()):
      for row in generated_rows:
        # self.process_src_rows(rows)
        self.move_trg_file_if_applicable(row)

  def report(self):
    print('=_+_+_='*3, 'TrgBasedOnSrcMolder Report', '=_+_+_='*3)
    self.print_counts()
    print('n_moved_files:', self.n_moved_files)
    print('n_copied_files:', self.n_copied_files)

  def process(self):
    self.sweep_src_files_in_db()
    dirf.prune_dirtree_deleting_empty_folders(self.bak_dt.mountpath)
    self.report()


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()

  molder = TrgBasedOnSrcMolder(src_mountpath, trg_mountpath)
  molder.process()


if __name__ == '__main__':
  process()
