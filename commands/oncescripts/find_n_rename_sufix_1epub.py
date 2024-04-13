#!/usr/bin/env python3
"""
commands/oncescripts/find_n_rename_sufix_1epub.py
"""
import datetime
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import fs.db.dbfailed_fileread_mod as freadfail
import models.entries.dirtree_mod as dt
import os
DEFAULT_SUFIX_FOR_FIND_N_RENAME = ' 1.epub'


class FilesUpDirTreeWalker:

  def __init__(self, mountpath):
    """
    treename is generally 'ori' (source) or 'bak' (back-up)
    source and target are generally 'src' (source) or 'trg' (back-up)
    some operations may occur in the same dirtree,
      in such cases 'bak' may refer to a subdirectory in the same dirtree as 'ori'
    """
    self.sufix_for_find_n_rename = DEFAULT_SUFIX_FOR_FIND_N_RENAME
    self.total_dirs_in_os = 0
    self.total_files_in_os = 0
    self.total_processed_files = 0
    self.total_files_in_db = 0
    self.total_unique_files_in_db = 0
    self.src_ren_filepaths_queue = []
    self.rename_pairs = []
    self.delete_queue_if_ren_not_poss = []
    self.n_found_files_name_n_parent_in_db = 0
    self.n_found_files_size_n_date_in_db = 0
    self.n_processed_files = 0
    self.n_restricted_dirs = 0
    self.n_updated_dbentries = 0
    self.n_inserted = 0
    self.n_files_empty_sha1 = 0
    self.n_failed_filestat = 0
    self.n_failed_sha1s = 0
    self.n_empty_dirs_removed = 0
    self.n_empty_dirs_fail_rm = 0
    self.n_dbentries_ins_upd = 0
    self.n_dbentries_failed_ins_upd = 0
    self.all_nodes_with_osread_problem = []
    self.ongoingfolder_abspath = None
    self.mountpath = mountpath
    if not os.path.isdir(self.mountpath):
      error_msg = 'Missing file errror mount_abspath (%s) does not exist.'
      raise OSError(error_msg)
    treename = 'ori'
    self.dirtree = dt.DirTree(treename, self.mountpath)
    self.dbtree = self.dirtree.dbtree  # dbu.DBDirTree(mount_abspath)
    self.calc_totals()
    # freadfailer will record the sha1 fileread failed attempts
    self.freadfailer = freadfail.DBFailFileReadReporter(self.dbtree.mountpath)

  def calc_totals(self):
    """
    count_total_files_n_folders_with_restriction(mountpath, restricted_dirnames, forbidden_first_level_dirs)
    """
    print('Counting files and dirs in db and os. Please wait.')
    self.total_unique_files_in_db = self.dbtree.count_unique_sha1s_as_int()
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_with_restriction(self.dirtree.mountpath)
    self.total_files_in_os = total_files
    self.total_dirs_in_os = total_dirs

  def find_n_rename(self, files):
    """

    """
    for filename in files:
      self.total_processed_files += 1
      if filename.endswith(self.sufix_for_find_n_rename):
        filepath = os.path.join(self.ongoingfolder_abspath, filename)
        self.src_ren_filepaths_queue.append(filepath)

  def walkup_dirtree_files(self):
    """

    """
    for self.ongoingfolder_abspath, dirs, files in os.walk(self.mountpath):
      if self.ongoingfolder_abspath == self.mountpath:  # this means not to process the mount_abspath folder itself
        continue
      if dirf.is_forbidden_dirpass(self.ongoingfolder_abspath):
        continue
      self.find_n_rename(files)

  def derive_rename_tuples(self):
    for i, src_filepath in enumerate(self.src_ren_filepaths_queue):
      print(i, src_filepath)
      folderpath, src_filename = os.path.split(src_filepath)
      name, dotext = os.path.splitext(src_filename)
      if not name.endswith(' 1'):
        continue
      newname = name[:-2]
      trg_filename = newname + dotext
      trg_filepath = os.path.join(folderpath, trg_filename)
      if not os.path.isfile(trg_filepath):
        continue
      if os.path.isfile(src_filepath):
        self.delete_queue_if_ren_not_poss.append(src_filepath)
        continue
      rename_pair = (src_filepath, trg_filepath)
      self.rename_pairs.append(rename_pair)

  def confirm_rename_pairs(self):
    for i, rename_pair in enumerate(self.rename_pairs):
      src_filepath, trg_filepath = rename_pair
      print('-'*30)
      print(i, 'to rename FROM / TO')
      print(' FROM ', src_filepath)
      print(' TO   ', trg_filepath)
    total_files_to_rename = len(self.rename_pairs)
    if total_files_to_rename == 0:
      print('Nothing to rename: total_files_to_rename', total_files_to_rename)
      return False
    print('='*40)
    scrmsg = f'Confirm the {total_files_to_rename} files above to rename? (*Y/n) ([ENTER] means yes)'
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def do_rename(self):
    for i, rename_pair in enumerate(self.rename_pairs):
      src_filepath, trg_filepath = rename_pair
      print(i, 'Renaming now FROM / TO')
      print('\tFROM ', src_filepath)
      print('\tTO   ', trg_filepath)
      os.rename(src_filepath, trg_filepath)

  def delete_files_impeding_rename(self):
    n_deleted = 0
    for i, src_filepath in enumerate(self.delete_queue_if_ren_not_poss):
      print(i, 'deleting', src_filepath)
      os.remove(src_filepath)
      n_deleted += 1
    return n_deleted

  def confirm_n_delete_files_if_any(self):
    for i, src_filepath in enumerate(self.delete_queue_if_ren_not_poss):
      print(i, 'to delete: ', src_filepath)
    total_files_to_del = len(self.delete_queue_if_ren_not_poss)
    if total_files_to_del == 0:
      print('Nothing to delete: total_files_to_del', total_files_to_del)
      return 0
    scrmsg = f'Confirm the {total_files_to_del} files above to delete? (*Y/n) ([ENTER] means yes)'
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      return self.delete_files_impeding_rename()
    return 0

  def process(self):
    self.walkup_dirtree_files()
    self.derive_rename_tuples()
    if self.confirm_rename_pairs():
      self.do_rename()
    n_deleted = self.confirm_n_delete_files_if_any()
    print('n_deleted', n_deleted)


def adhoc_test():
  pass


def process():
  start_time = datetime.datetime.now()
  print('Start Time', start_time)
  # ------------------
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  walker = FilesUpDirTreeWalker(src_mountpath)
  walker.process()
  # ------------------
  finish_time = datetime.datetime.now()
  elapsed_time = finish_time - start_time
  print('-'*50)
  print('Finish Time:', finish_time)
  print('Run Time:', elapsed_time)


if __name__ == '__main__':
  process()

