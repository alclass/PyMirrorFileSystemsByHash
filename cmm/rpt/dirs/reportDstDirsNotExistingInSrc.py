#!/usr/bin/env python3
"""
cmm/rpt/dirs/reportDstDirsNotExistingInSrc.py

Reports destination middlepaths that are not refleted
  in the source dirtree.

Example:
  <src_dirtree>/
    A/B/C/Folder1
    A/B/C/Folder2
    A/X/XPTO/FolderX

  <src_dirtree>/
    A/B/C/Folder1
    A/Bla/C/Folder2
    A/X/XPTO/FolderX

In the example above, notice that middlepaths:
    A/B/C/Folder1
    A/X/XPTO/FolderX
  both exist in source and destination, but:
    A/Bla/C/Folder2
  does not.
  Then, this script lists, for the example, A/Bla/C/Folder2
"""
import sys
from pathlib import Path
import os
import models.entries.dirtree_mod as dt
import default_settings as defaults
import llib.dirfilefs.dir_n_file_fs_mod as dirf
import models.entries.dirnode_mod as dn


class Reporter:

  def __init__(self, src_dtpath, dst_dtpath, dst_starts_at=None):
    self.scr_dt = dt.DirTree('ori', src_dtpath)
    self.dst_dt = dt.DirTree('dst', dst_dtpath)
    self.dst_starts_at = dst_starts_at or self.dst_dt.mountpath
    self.verify_dst_starts_at_validity()
    self.src_middlepath = None
    # self.dst_middlepath = None  # this is derivable (a property)
    self.report_text = ""
    self.cur_walk_dstpath = None
    self.n_number_found = 0
    self.n_missing_mirrordir = 0
    self.n_processed_dirs = 0
    self.tot_srcfiles_in_db = 0
    self.tot_dstfiles_in_db = 0
    self.tot_uniq_srcfiles_in_db = 0
    self.tot_uniq_dstfiles_in_db = 0
    self.tot_uniq_srcfiles_in_os = 0
    self.tot_uniq_dstfiles_in_os = 0
    self.tot_srcdirs_in_os = 0
    self.tot_dstdirs_in_os = 0
    self.n_dstdirs_missing_in_src = 0
    self.n_srcdirs_missing_in_dst = 0
    self.calc_totals()

  def verify_dst_starts_at_validity(self):
    if self.dst_starts_at == self.dst_dt.mountpath:
      return
    if self.dst_dt.mountpath in self.dst_starts_at:
      return
    errmsg = f"dst_starts_at path is not a subdirectory of dst_dt.mountpath\n"
    errmsg += f"starts_at = {self.dst_starts_at}\n"
    errmsg += f"dst_dirpath = {self.dst_dt.mountpath}\n"
    errmsg += f"scr_dirpath = {self.scr_dt.mountpath}\n"
    raise OSError(errmsg)

  def calc_totals(self):
    print('Counting files and dirs in db and os. Please wait.')
    self.tot_uniq_srcfiles_in_db = self.scr_dt.dbtree.count_unique_sha1s_as_int()
    self.tot_srcfiles_in_db = self.scr_dt.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.scr_dt.mountpath)
    self.tot_uniq_srcfiles_in_os = total_files
    self.tot_srcdirs_in_os = total_dirs
    self.tot_uniq_dstfiles_in_os = total_files
    self.tot_dstdirs_in_os = total_dirs

  @property
  def curr_srcpath(self):
    baserootpath = self.scr_dt.dbtree.mountpath
    baserootpath = Path(baserootpath)
    return baserootpath / self.dst_middlepath

  @property
  def dst_middlepath(self):
    rootpath = self.dst_dt.dbtree.mountpath
    middlepath = self.cur_walk_dstpath[len(rootpath):]
    middlepath = middlepath.lstrip('/')
    return middlepath

  def does_scr_mirrorpath_exist_for_dst(self):
    return self.curr_srcpath.is_dir()

  def verify_mirrorfolder_at_src(self):
    if not self.does_scr_mirrorpath_exist_for_dst():
      self.reports_this_curwalkdstpath()

  def walkup_dst_dirtree(self):
    print('scr dt', self.scr_dt.mountpath)
    print('dst dt', self.dst_dt.mountpath)
    print('starts at', self.dst_starts_at)
    for self.cur_walk_dstpath, foldernames, _ in os.walk(self.dst_starts_at):
      self.n_processed_dirs += 1
      seq = self.n_processed_dirs
      # scrmsg = f"{seq} Looking up {self.dst_middlepath}"
      # print(scrmsg)
      self.verify_mirrorfolder_at_src()

  def process(self):
    self.walkup_dst_dirtree()
    self.reports_this_curwalkdstpath()
    self.report()

  def reports_this_curwalkdstpath(self):
    self.n_missing_mirrordir += 1
    seq = self.n_missing_mirrordir
    scrmsg = f"{seq} missing: {self.curr_srcpath}\n"
    self.report_text += scrmsg

  def report(self):
    print(self.report_text)

  def print_counters(self):
    print('total_srcfiles_in_db:', self.tot_srcfiles_in_db)
    print('total_trgfiles_in_db:', self.tot_dstfiles_in_db)
    print('total_srcfiles_in_os:', self.tot_uniq_srcfiles_in_os)
    print('total_srcdirs_in_os:', self.tot_srcdirs_in_os)
    print('total_trgfiles_in_os:', self.tot_uniq_dstfiles_in_os)
    print('total_trgdirs_in_os:', self.tot_dstdirs_in_os)
    print('total_unique_srcfiles:', self.tot_uniq_srcfiles_in_db)
    print('total_unique_trgfiles:', self.tot_uniq_dstfiles_in_db)


def get_args():
  for arg in sys.argv[1:]:
    if arg.startswith('-startsat='):
      starts_at = arg[len('-startsat='):]
      return starts_at
  return None


def process():
  """
  """
  src_mountpath, dst_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  starts_at = get_args()
  print('src =', src_mountpath)
  print('dst = ', dst_mountpath)
  print('starts_at = ', starts_at)
  lister = Reporter(src_mountpath, dst_mountpath, starts_at)
  lister.process()


if __name__ == '__main__':
  process()
