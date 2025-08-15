#!/usr/bin/env python3
"""
delete_files_in_trg_that_exist_in_source_cm.py

This script finds files that exist both in source and target and
  enlist the copies in target for deletion upon user confirmation.
Obs:
  1) the deletion (one or mosre files) will only be applied to target (not source)
     source files are unaffected
  2) the deletion (in target) will happen only if the user confirms it
  3) disks here are synonymous of "directory trees" (or dirtrees),
     so the operation may happen in the same disk reaching different areas
     (look up script "delete_repeatfiles_lookingup_targetdirtree_cm.py" when target may be inside source)

Script's main applicability:
  When keeping external disks (not considering their back-ups),
    some content may be repeated across disks using up disk space redundantly (duplicates).
  In these cases and instances, this script is useful to free up byte-space in the target disk.

Example:
=======
  1) suppose source [Disk1-HD] has:
  /dirA/dirA2/file1.txt
  /dirA/dirA2/file-in-both-places.doc
  /dirX/folder/only-here-file.dat

  2) suppose further target [Disk2-HD]  has:
  /fileA.txt (with the same content as /dirA/dirA2/file1.txt in Disk1-HD)
  /dirA/dirA2/file-in-both-places.doc
  /dirX/only-here-dir/not-the-one-above.dat

Considering the two dirtrees shown above, if the following command
  $delete_files_in_trg_that_exist_in_source_cm "/media/user/Disk1-HD" "/media/user/Disk2-HD"
is issued, then confirmation will be prompted for deleting the following files:

1 => /media/user/Disk2-HD/fileA.txt
2 => /media/user/Disk2-HD/dirA/dirA2/file-in-both-places.doc
  Confirm deletion of the 2 files above? (*Y/n) [ENTER] means yes => [prompt-waiting-answer]

In this example, if deletion is confirmed, the final result will be:
  1) [Disk1-HD] (same as before, intact, deletion does not apply to it):
  /dirA/dirA2/file1.txt
  /dirA/dirA2/file-in-both-places.doc
  /dirX/only-here-dir/not-the-one-above.dat

  2) [Disk2-HD] (after deletion confirmed):
  /dirX/only-here-dir/not-the-one-above.dat

Deletion ones, if confirmed, are:
  /media/user/Disk2-HD/fileA.txt
  /media/user/Disk2-HD/dirA/dirA2/file-in-both-places.doc

Caution:
  1) do not use this script when target is inside source (ie target is a subfolder of source)
  2) for the case above, see script delete_repeatfiles_lookingup_targetdirtree_cm.py
"""
import os
import sys
# import fs.db.dbdirtree_mod as dbdt
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import default_settings as defaults
# import fs.strnlistfs.strfunctions_mod as strf
import lib.dirfilefs.dir_n_file_fs_mod as dirf


def print_sha1_set(missing_set, dbtree_opposite, mountpath):
  """
  TO-DO:
    1) this function may be, in the future, refactored/reorganized to a function-module
    2) another point of attention is to study whether or not the loop-inside-loop below is too costy
       or, thinking of alternatives, whether a prefetching scheme (avoiding double-loops) is possible
  """
  n_sha_is_none = 0
  for i, sha1 in enumerate(missing_set):
    print('-' * 70)
    if sha1 is None:  # this is to treat some old versions of the database where NOT NULL where missing for column sha1
      # TO-DO thie if may be removed later on because SQL-schema table has already been updated
      n_sha_is_none += 1
      print('sha1 is None (', n_sha_is_none, '): missing_set size is ', len(missing_set))
      continue
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


class FilesRepeatedFinderBySha1:

  def __init__(self, src_mountpath, trg_mountpath):
    self.ori_dt = dt.DirTree('ori', src_mountpath)
    self.bak_dt = dt.DirTree('bak', trg_mountpath)
    self.sha1_in_trg_existing_in_ori = None
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
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.ori_dt.mountpath)
    self.total_srcfiles_in_os = total_files
    self.total_srcdirs_in_os = total_dirs
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.bak_dt.mountpath)
    self.total_trgfiles_in_os = total_files
    self.total_trgdirs_in_os = total_dirs

  @property
  def total_sha1_in_trg_existing_in_ori(self):
    if self.sha1_in_trg_existing_in_ori is None:
      return 0
    return len(self.sha1_in_trg_existing_in_ori)

  def delete_file(self, dirnode):
    if dirnode is None:
      self.n_failed_deletes += 1
      print(self.n_failed_deletes, self.total_sha1_in_trg_existing_in_ori, 'dirnode is None')
      return False
    del_trg_filepath = dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    total_to_del = self.total_sha1_in_trg_existing_in_ori
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
    for i, sha1 in enumerate(self.sha1_in_trg_existing_in_ori):
      tuplevalues = (sha1,)
      fetched_list = self.bak_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for row in fetched_list:
        dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dt.dbtree.fieldnames)
        self.delete_file(dirnode)

  def confirm_delete(self):
    print('total_sha1_in_trg_existing_in_ori', self.total_sha1_in_trg_existing_in_ori)
    screen_msg = 'Confirm the deletes above? (*Y/n) ([ENTER] also means yes) '
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def find_files_in_trg_that_exist_in_src(self, sha1s_ori, sha1s_bak):
    self.sha1_in_trg_existing_in_ori = set(filter(lambda x: x in sha1s_bak, sha1s_ori))
    # sorted(self.sha1_in_bak_missing_in_ori)
    print_sha1_set(self.sha1_in_trg_existing_in_ori, self.bak_dt.dbtree, self.bak_dt.mountpath)

  def fetch_all_sha1s_in_src_n_trg_n_find_trg_repeats(self):
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
    self.find_files_in_trg_that_exist_in_src(sha1s_ori, sha1s_bak)

  def process(self):
    self.fetch_all_sha1s_in_src_n_trg_n_find_trg_repeats()
    if self.total_sha1_in_trg_existing_in_ori > 0:
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
    print('total sha1 in bak missing in ori:', self.total_sha1_in_trg_existing_in_ori)
    print('n_deletes:', self.n_deletes)
    print('n_failed_deletes:', self.n_failed_deletes)
    print('n_empty_dirs_removed:', self.n_empty_dirs_removed)
    print('n_empty_dirs_fail_rm:', self.n_empty_dirs_fail_rm)


def show_help_cli_msg_if_asked():
  for arg in sys.argv:
    if arg in ['-h', '--help']:
      print(__doc__)
      sys.exit(0)


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
  deleter = FilesRepeatedFinderBySha1(src_mountpath, trg_mountpath)
  deleter.process()


if __name__ == '__main__':
  process()
