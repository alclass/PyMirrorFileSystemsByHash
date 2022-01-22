#!/usr/bin/env python3
"""
delete_excess_sha1_trgfiles_that_dont_exist_in_src_cm.py
Description:
===========
This scripts reads ori and bak db's (*) and takes files by sha1 that exist in bak but don't in ori.
These files (in bak not in ori) are called here excess files and are listed for deletion upon user confirmation.

(*) db-os syncronization, by the user, is pressuposed

IMPORTANT OBS: 
  the files to be deleted do not have a copy (or backup copy) elsewhere, so this script is useful 
  when a cleaning-up operation in source has already been taken and now the cleaning-up needs to be
  completed in target. Caution: take care with this cleaning operation, it deletes files that do not have back-up!

Usage:
=====
$delete_excess_sha1_trgfiles_that_dont_exist_in_src_cm.py <abspath-to-ori> <abspath-to-bak>

Example:
=======
$delete_excess_sha1_trgfiles_that_dont_exist_in_src_cm.py "/media/user/disk_science_1" "/media/user/disk_science_2"
"""
import os.path
import sys
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirf


def print_sha1_set(missing_set, dbtree_opposite, mountpath):
  for i, sha1 in enumerate(missing_set):
    print('-' * 70)
    print(i + 1, sha1.hex() + ' in ' + mountpath)
    print('-' * 70)
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = dbtree_opposite.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    for row in fetched_list:
      dirnode = dn.DirNode.create_with_tuplerow(row, dbtree_opposite.fieldnames)
      filepath = dirnode.get_abspath_with_mountpath(mountpath)
      print(filepath)
      print(dirnode)


class FilesMissingFinderBySha1:

  def __init__(self, src_mountpath, trg_mountpath):
    self.ori_dt = dt.DirTree('ori', src_mountpath)
    self.bak_dt = dt.DirTree('bak', trg_mountpath)
    self.sha1_in_bak_missing_in_ori = None
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    self.total_srcfiles_in_os = 0
    self.total_trgfiles_in_os = 0
    self.total_srcdirs_in_os = 0
    self.total_trgdirs_in_os = 0
    self.n_src_processed_files = 0
    self.n_deletes = 0
    self.n_failed_deletes = 0
    self.n_empty_dirs_removed = 0
    self.n_empty_dirs_fail_rm = 0
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

  @property
  def total_sha1s_in_bak_missing_in_ori(self):
    if self.sha1_in_bak_missing_in_ori is None:
      return 0
    return len(self.sha1_in_bak_missing_in_ori)

  def delete_file(self, dirnode):
    if dirnode is None:
      self.n_failed_deletes += 1
      print(self.n_failed_deletes, self.total_sha1s_in_bak_missing_in_ori, 'dirnode is None')
      return False
    del_trg_filepath = dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    total_to_del = self.total_sha1s_in_bak_missing_in_ori
    if not os.path.isfile(del_trg_filepath):
      print('cannot delete, trg file does exist', dirnode)
      print(del_trg_filepath)
      return False
    try:
      print('Initiating delete of', dirnode.name)
      os.remove(del_trg_filepath)
      self.n_deletes += 1
      print(self.n_deletes, '/', total_to_del, 'DELETED in os')
      print(del_trg_filepath)
    except (IOError, OSError):
      print('failed copy IOError|OSError', dirnode)
      self.n_failed_deletes += 1
      return False
    return dirnode.delete_from_db(self.bak_dt.dbtree)

  def fetch_n_delete_trg_files(self):
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    for i, sha1 in enumerate(self.sha1_in_bak_missing_in_ori):
      tuplevalues = (sha1,)
      fetched_list = self.bak_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for row in fetched_list:
        dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dt.dbtree.fieldnames)
        self.delete_file(dirnode)

  def confirm_delete(self):
    print('total_sha1s_in_bak_missing_in_ori', self.total_sha1s_in_bak_missing_in_ori)
    screen_msg = 'Confirm the deletes above? (*Y/n) ([ENTER] also means yes) '
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def find_missing_files_in_src_that_exist_in_trg(self, sha1s_ori, sha1s_bak):
    self.sha1_in_bak_missing_in_ori = set(filter(lambda x: x not in sha1s_ori, sha1s_bak))
    # sorted(self.sha1_in_bak_missing_in_ori)
    print_sha1_set(self.sha1_in_bak_missing_in_ori, self.bak_dt.dbtree, self.bak_dt.mountpath)

  def fetch_all_sha1s_in_src_n_trg_n_find_trg_excess(self):
    sql = 'SELECT DISTINCT sha1 FROM %(tablename)s ORDER BY parentpath;'
    fetched_list = self.ori_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    sha1s = [tupl[0] for tupl in fetched_list]
    sha1s_ori = set(sha1s)
    fetched_list = self.bak_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    sha1s = [tupl[0] for tupl in fetched_list]
    sha1s_bak = set(sha1s)
    print('ori qtd', self.total_unique_srcfiles)
    print('bak qtd', self.total_unique_trgfiles)
    print('Please wait. Processing finding missing files either in ori or in bak.')
    self.find_missing_files_in_src_that_exist_in_trg(sha1s_ori, sha1s_bak)

  def process(self):
    self.fetch_all_sha1s_in_src_n_trg_n_find_trg_excess()
    if self.total_sha1s_in_bak_missing_in_ori > 0:
      if self.confirm_delete():
        self.fetch_n_delete_trg_files()
        self.prune_empty_folders()
    self.report()

  def prune_empty_folders(self):
    """
    The method calls a function to erase empty directories (it is automatic, no confirmation is asked).
    """
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
    print('=_+_+_='*3, 'TrgBasedByrcSha1sMolder Report', '=_+_+_='*3)
    self.print_counters()
    print('total sha1 in bak missing in ori:', self.total_sha1s_in_bak_missing_in_ori)
    print('n_deletes:', self.n_deletes)
    print('n_failed_deletes:', self.n_failed_deletes)
    print('n_empty_dirs_removed:', self.n_empty_dirs_removed)
    print('n_empty_dirs_fail_rm:', self.n_empty_dirs_fail_rm)


def check_args_for_cli_help():
  for arg in sys.argv:
    if arg.startswith('-h') or arg.startswith('--help'):
      print(__doc__)
      sys.exit(0)


def process():
  """
  """
  check_args_for_cli_help()
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  deleter = FilesMissingFinderBySha1(src_mountpath, trg_mountpath)
  deleter.process()


if __name__ == '__main__':
  process()
