#!/usr/bin/env python3
"""
mold_trg_based_on_src_mod.py

This script does the following:

"""
import copy
import os.path
import shutil
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import lib.dirfilefs.dir_n_file_fs_mod as dirf
import lib.strnlistfs.strfunctions_mod as strf
# import commands.dbentry_deleter_those_without_corresponding_osentry_mod as dbentry_del


class ReportSha1Lister:

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
    self.n_src_processed_files = 0
    self.n_copied_files = 0
    self.n_moved_files = 0
    self.n_move_not_needed = 0
    self.n_failed_moves = 0
    self.n_failed_copies = 0
    self.n_empty_dirs_removed = 0
    self.n_empty_dirs_fail_rm = 0
    self.calc_totals()

  def calc_totals(self):
    print('Counting files and dirs in db and os. Please wait.')
    self.total_unique_srcfiles = self.ori_dt.dbtree.count_unique_sha1s_as_int()
    self.total_unique_trgfiles = self.bak_dt.dbtree.count_unique_sha1s_as_int()
    self.total_srcfiles_in_db = self.ori_dt.dbtree.count_rows_as_int()
    self.total_trgfiles_in_db = self.bak_dt.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.ori_dt.mountpath)
    self.total_srcfiles_in_os = total_files
    self.total_srcdirs_in_os = total_dirs
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.bak_dt.mountpath)
    self.total_trgfiles_in_os = total_files
    self.total_trgdirs_in_os = total_dirs

  def fetch_n_process_unique_sha1s_in_scr(self):
    sql = 'select DISTINCT sha1, count(sha1) as c from %(tablename)s group by sha1;'  #  having c = 1
    fetched_list = self.ori_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    histogram_dict = {}
    for i, shorter_row in enumerate(fetched_list):
      sha1 = shorter_row[0]
      qtd = shorter_row[1]
      if qtd in histogram_dict:
        histogram_dict[qtd] += 1
      else:
        histogram_dict[qtd] = 1
      # src_dirnode = dn.DirNode.create_with_tuplerow(row, self.ori_dt.dbtree.fieldnames)
      # src_filepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mountpath)
      print(
        i+1, sha1, qtd
      )
    print(histogram_dict)

  def process(self):
    self.fetch_n_process_unique_sha1s_in_scr()
    self.prune_empty_folders()
    self.report()

  def prune_empty_folders(self):
    n_visited, n_removed, n_failed = dirf.prune_dirtree_deleting_empty_folders(self.bak_dt.mountpath)
    self.n_empty_dirs_removed = n_removed
    self.n_empty_dirs_fail_rm = n_failed

  def print_counters(self):
    print('total_srcfiles_in_db:', self.total_srcfiles_in_db)
    print('total_trgfiles_in_db:', self.total_trgfiles_in_db)
    print('total_srcfiles_in_os:', self.total_srcfiles_in_os)
    print('total_srcdirs_in_os:', self.total_srcdirs_in_os)
    print('total_trgfiles_in_os:', self.total_trgfiles_in_os)
    print('total_trgdirs_in_os:', self.total_trgdirs_in_os)
    print('total_unique_srcfiles:', self.total_unique_srcfiles)
    print('total_unique_trgfiles:', self.total_unique_trgfiles)

  def report(self):
    print('=_+_+_='*3, 'TrgBasedOnSrcMolder Report', '=_+_+_='*3)
    self.print_counters()
    print('n_src_processed_files:', self.n_src_processed_files)
    print('n_moved_files:', self.n_moved_files)
    print('n_move_not_needed:', self.n_move_not_needed)
    print('n_copied_files:', self.n_copied_files)
    print('n_failed_moves:', self.n_failed_moves)
    print('n_failed_copies:', self.n_failed_copies)
    print('n_empty_dirs_removed:', self.n_empty_dirs_removed)
    print('n_empty_dirs_fail_rm:', self.n_empty_dirs_fail_rm)


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  lister = ReportSha1Lister(src_mountpath, trg_mountpath)
  lister.process()


if __name__ == '__main__':
  process()
