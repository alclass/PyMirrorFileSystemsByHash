#!/usr/bin/env python3
"""
mold_trg_based_on_src_mod.py

This script does the following:

"""
import os.path
import models.entries.dirtree_mod as dt
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import models.entries.dirnode_mod as dn


class ReportFilenameEndingWithNumberLister:

  def __init__(self, src_mountpath):
    self.dirtree = dt.DirTree('ori', src_mountpath)
    self.n_number_found = 0
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    self.total_srcfiles_in_os = 0
    self.total_trgfiles_in_os = 0
    self.total_srcdirs_in_os = 0
    self.total_trgdirs_in_os = 0
    self.n_processed_files = 0
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
    self.total_unique_srcfiles = self.dirtree.dbtree.count_unique_sha1s_as_int()
    self.total_srcfiles_in_db = self.dirtree.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.dirtree.mountpath)
    self.total_srcfiles_in_os = total_files
    self.total_srcdirs_in_os = total_dirs
    self.total_trgfiles_in_os = total_files
    self.total_trgdirs_in_os = total_dirs

  def fetch_n_process_files_ending_with_numbers(self):
    total = self.total_unique_srcfiles
    yielded_rows = self.dirtree.dbtree.do_select_all_w_limit_n_offset()
    for rows in yielded_rows:
      for i, row in enumerate(rows):
        dirnode = dn.DirNode.create_with_tuplerow(row, self.dirtree.dbtree.fieldnames)
        if not dirnode.does_dirnode_exist_in_disk(self.dirtree.mountpath):
          print('### file does not exist in disk', dirnode.name, dirnode.parentpath)
          continue
        self.n_processed_files += 1
        middlepath = dirnode.parentpath
        filename = dirnode.name
        extlessname, _ = os.path.splitext(filename)
        pp = extlessname.split(' ')
        if len(pp) > 0:
          try:
            supposed_number = int(pp[-1])
            self.n_number_found += 1
            seq = i + 1
            print('-'*50)
            print(
              'found', self.n_number_found, seq, 'tot', total,
              'number', supposed_number, 'for file'
            )
            print(filename)
            print(middlepath)
          except ValueError:
            continue

  def process(self):
    self.fetch_n_process_files_ending_with_numbers()
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
    print(
      '=_+_+_='*3,
      'Obs: this script searches for space plus a number at the end of an extensionless filename',
      '=_+_+_='*3
    )
    self.print_counters()
    print('n_processed_files_in_trg:', self.n_processed_files)
    print('n_number_found:', self.n_number_found)


def process():
  """
  """
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  lister = ReportFilenameEndingWithNumberLister(src_mountpath)
  lister.process()


if __name__ == '__main__':
  process()
