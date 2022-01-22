#!/usr/bin/env python3
"""

"""
import copy
import os.path
import shutil
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirf


def print_sha1_set(missing_set, dbtree_opposite, direction_str):
  for i, sha1 in enumerate(missing_set):
    print('-' * 70)
    print(i + 1, sha1.hex() + ' ' + direction_str)
    print('-' * 70)
    sql = 'select * from %(tablename)s where sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = dbtree_opposite.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    for row in fetched_list:
      dirnode = dn.DirNode.create_with_tuplerow(row, dbtree_opposite.fieldnames)
      print(dirnode)


class FilesMissingFinderBySha1:

  def __init__(self, src_mountpath, trg_mountpath):
    self.ori_dt = dt.DirTree('ori', src_mountpath)
    self.bak_dt = dt.DirTree('bak', trg_mountpath)
    self.sha1_in_bak_missing_in_ori = None
    self.sha1_in_ori_missing_in_bak = None
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
    self.n_failed_copies = 0
    self.n_names_not_liberated = 0
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

  def copy_over(self, dirnode, from_dirtree, to_dirtree, total_to_copy):
    src_filepath = dirnode.get_abspath_with_mountpath(from_dirtree.mountpath)
    if not os.path.isfile(src_filepath):
      print('cannot copy, src file does exist', dirnode)
      return False
    to_be_trg_dirnode = copy.copy(dirnode)
    trg_filepath = to_be_trg_dirnode.get_abspath_with_mountpath(to_dirtree.mountpath)
    if not os.path.isfile(src_filepath):
      print('cannot copy, trg file exists', dirnode)
      return False
    try:
      p, filename = os.path.split(trg_filepath)
      if not os.path.isdir(p):
        print('Creating missing directory', p)
        os.makedirs(p)
      print('Initiating copy of', filename)
      shutil.copy2(src_filepath, trg_filepath)
      self.n_copied_files += 1
      print(self.n_copied_files, '/', total_to_copy, 'COPIED')
      print('FROM', src_filepath)
      print('TO', trg_filepath)
    except (IOError, OSError):
      print('failed copy IOError|OSError', dirnode)
      self.n_failed_copies += 1
      return False
    return to_be_trg_dirnode.insert_into_db(to_dirtree.dbtree)

  def copy_over_missing_either_way(self):
    sql = 'select * from %(tablename)s where sha1=?;'
    for i, sha1 in enumerate(self.sha1_in_bak_missing_in_ori):
      tuplevalues = (sha1,)
      fetched_list = self.bak_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for row in fetched_list:
        dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dt.dbtree.fieldnames)
        total_to_copy = self.total_sha1s_in_bak_missing_in_ori
        self.copy_over(dirnode, self.bak_dt, self.ori_dt, total_to_copy)
    for i, sha1 in enumerate(self.sha1_in_ori_missing_in_bak):
      tuplevalues = (sha1,)
      fetched_list = self.ori_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for row in fetched_list:
        dirnode = dn.DirNode.create_with_tuplerow(row, self.ori_dt.dbtree.fieldnames)
        print(dirnode)
        total_to_copy = self.total_sha1s_in_bak_missing_in_ori
        self.copy_over(dirnode, self.ori_dt, self.bak_dt, total_to_copy)

  @property
  def total_sha1s_in_ori_missing_in_bak(self):
    if self.sha1_in_ori_missing_in_bak is None:
      return 0
    return len(self.sha1_in_ori_missing_in_bak)

  @property
  def total_sha1s_in_bak_missing_in_ori(self):
    if self.sha1_in_bak_missing_in_ori is None:
      return 0
    return len(self.sha1_in_bak_missing_in_ori)

  def confirm_copy(self):
    print('ori to bak number of copies', self.total_sha1s_in_ori_missing_in_bak)
    print('bak to ori number of copies', self.total_sha1s_in_bak_missing_in_ori)
    screen_msg = 'Confirm the copies above? (*Y/n) ([ENTER] also means yes) '
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def find_missing_files_either_way(self, sha1s_ori, sha1s_bak):
    self.sha1_in_bak_missing_in_ori = set(filter(lambda x: x not in sha1s_ori, sha1s_bak))
    direction_str = 'present in bak, missing in ori'
    print_sha1_set(self.sha1_in_bak_missing_in_ori, self.bak_dt.dbtree, direction_str)
    self.sha1_in_ori_missing_in_bak = set(filter(lambda x: x not in sha1s_bak, sha1s_ori))
    direction_str = 'present in ori, missing in bak'
    print_sha1_set(self.sha1_in_ori_missing_in_bak, self.ori_dt.dbtree, direction_str)

  def find_sha1s_missing_either_way(self):
    sql = 'select DISTINCT sha1 from %(tablename)s;'
    fetched_list = self.ori_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    sha1s = [tupl[0] for tupl in fetched_list]
    sha1s_ori = set(sha1s)
    fetched_list = self.bak_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    sha1s = [tupl[0] for tupl in fetched_list]
    sha1s_bak = set(sha1s)
    print('ori qtd', self.total_unique_srcfiles)
    print('bak qtd', self.total_unique_trgfiles)
    print('Please wait. Processing finding missing files either in ori or in bak.')
    self.find_missing_files_either_way(sha1s_ori, sha1s_bak)

  def process(self):
    self.find_sha1s_missing_either_way()
    if self.total_sha1s_in_bak_missing_in_ori > 0 or self.total_sha1s_in_ori_missing_in_bak > 0:
      if self.confirm_copy():
        self.copy_over_missing_either_way()
    self.report()

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
    print('total sha1 in ori missing in bak:', self.total_sha1s_in_ori_missing_in_bak)
    print('total sha1 in bak missing in ori:', self.total_sha1s_in_bak_missing_in_ori)
    print('n_copied_files:', self.n_copied_files)
    print('n_failed_copies:', self.n_failed_copies)


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  finder = FilesMissingFinderBySha1(src_mountpath, trg_mountpath)
  finder.process()


if __name__ == '__main__':
  process()
