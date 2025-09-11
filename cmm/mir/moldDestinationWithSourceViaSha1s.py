#!/usr/bin/env python3
"""
DirTreeMirror_PrdPrjSw:
  cmm/mir/moldDestinationWithSourceViaSha1s.py
Description below.

This script does the following:

  1) it looks up both sqlite-db dirtree contents
  2) determine what exists in source and is missing in destination
  3) proceeds to copy or move these contents from source to destination

These operations simulate a dirtree mirroring excepting that it doesn't do 'deleting',
  operation that is done by another script in this system.
"""
import copy
import os.path
import shutil
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import lib.dirfilefs.dir_n_file_fs_mod as dirf
import lib.strnlistfs.strfunctions_mod as strf


class TrgBasedByrcSha1sMolder:

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
    self.n_failed_copies = 0
    self.n_moved_files = 0
    self.n_names_not_liberated = 0
    self.n_move_not_needed = 0
    self.n_failed_moves = 0
    self.n_renames = 0
    self.failed_renames = 0
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

  def find_sha1_in_trg_n_return_trg_dirnode(self, src_dirnode):
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (src_dirnode.sha1, )
    fetched_list = self.bak_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list is None or len(fetched_list) == 0:
      return None
    fieldnames = self.bak_dt.dbtree.fieldnames
    if len(fetched_list) == 1:
      row = fetched_list[0]
      trg_dirnode = dn.DirNode.create_with_tuplerow(row, fieldnames)
      return trg_dirnode
    # at this point len(fetched_list) > 1
    for row in fetched_list:
      trg_dirnode = dn.DirNode.create_with_tuplerow(row, fieldnames)
      if trg_dirnode.parentpath == src_dirnode.parentpath and trg_dirnode.name == src_dirnode.name:
        if trg_dirnode.does_dirnode_exist_in_disk(self.bak_dt.mountpath):
          return trg_dirnode
    # rerun for-loop and return the first that exists in disk
    for row in fetched_list:
      trg_dirnode = dn.DirNode.create_with_tuplerow(row, fieldnames)
      if trg_dirnode.does_dirnode_exist_in_disk(self.bak_dt.mountpath):
        return trg_dirnode
    return None

  def rename_file_as_parentpaths_are_same(self, old_trg_dirnode, new_trg_dirnode):
    """
    At this point, if another file with a different sha1 but same name were in folder,
      that file would have been renamed by liberate_filename_by_renaming_with_incremental_int_sufixes()
    Consider this method private, it should only be called by
      move_file_within_trg_to_its_src_relative_position_if_vacant()
    """
    old_filepath = old_trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    new_filepath = new_trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    try:
      os.rename(old_filepath, new_filepath)
      self.n_renames += 1
      print(
        'Renamed', self.n_renames, 'of', self.total_srcfiles_in_os,
        '@', strf.put_ellipsis_in_str_middle(new_trg_dirnode.parentpath, 50)
      )
      print('previous name [', old_trg_dirnode.name, ']')
      print('current name [', new_trg_dirnode.name, ']')
    except (IOError, OSError):
      self.failed_renames += 1
      return False
    name = new_trg_dirnode.name
    parentpath = new_trg_dirnode.parentpath
    return old_trg_dirnode.update_db_name_n_parentpath(name, parentpath, self.bak_dt.dbtree)

  def move_file_within_trg_to_its_src_relative_position_if_vacant(self, src_dirnode, old_trg_dirnode):
    """
    if program flow gets up to here, srcfile exists.
    Also, trgfile exists (at this point, trgfile is out of its relative position).
    That is trgfile is not in the "projected position" ie having the same name and parentpath.
      Though it's not guaranteed another file [with another sha1] is occupying that place/position (name & parentpath).
      If so, a rename will be tried on that other file, liberating the corresponding mirror-filename for the orig sha1.)
    """
    new_trg_dirnode = copy.copy(src_dirnode)  # new trg name and parentpath will be the ones from src
    old_filepath = old_trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    new_filepath = new_trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    is_liberated, _ = dirf.liberate_filename_by_renaming_with_incremental_int_sufixes(new_filepath)
    if not is_liberated:
      self.n_names_not_liberated += 1
      return False
    # if whichname is not None:
    #   pass
    if old_trg_dirnode.parentpath == new_trg_dirnode.parentpath:
      return self.rename_file_as_parentpaths_are_same(old_trg_dirnode, new_trg_dirnode)
    basepath, _ = os.path.split(new_filepath)
    if not os.path.isdir(basepath):
      # create target base dir when it does not exist
      os.makedirs(basepath)
    self.n_moved_files += 1
    print(
      'Moving', self.n_moved_files, 'of', self.total_srcfiles_in_os,
      'within target', new_trg_dirnode.name, '@', strf.put_ellipsis_in_str_middle(new_trg_dirnode.parentpath, 50)
    )
    try:
      shutil.move(old_filepath, new_filepath)
    except OSError:
      self.n_failed_moves += 1
      return False
    name = new_trg_dirnode.name
    parentpath = new_trg_dirnode.parentpath
    boolres = old_trg_dirnode.update_db_name_n_parentpath(name, parentpath, self.bak_dt.dbtree)
    if boolres:
      return True
    return new_trg_dirnode.insert_into_db(self.bak_dt.dbtree)

  def copy_over_src_to_trg(self, src_dirnode):
    """
    If program flow gets up to here, srcfile exists.
    """
    src_filepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mountpath)
    trg_filepath = src_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    if os.path.isfile(trg_filepath):
      # TO-DO implement a db-rename if a rename happened (this is known by the second returned variable)
      boolres, _ = dirf.liberate_filename_by_renaming_with_incremental_int_sufixes(trg_filepath)
      if not boolres:
        # original filename in target cannot be got, copy is not possible
        return False
    basepath, _ = os.path.split(trg_filepath)
    if not os.path.isdir(basepath):
      try:
        os.makedirs(basepath)
      except (OSError, IOError):
        self.n_failed_copies += 1
        return False
    self.n_copied_files += 1
    print(
      'Copying', self.n_copied_files, 'of', self.total_srcfiles_in_os,
      'src', src_dirnode.name, '@', strf.put_ellipsis_in_str_middle(src_dirnode.parentpath, 50)
    )
    try:
      shutil.copy2(src_filepath, trg_filepath)
    except (OSError, IOError):
      self.n_failed_copies += 1
      return False
    return src_dirnode.insert_into_db(self.bak_dt.dbtree)

  def move_trg_file_based_on_src_if_applicable(self, src_dirnode, trg_dirnode):
    if trg_dirnode.name == src_dirnode.name and trg_dirnode.parentpath == src_dirnode.parentpath:
      # no need to move target, it's already there where it should be
      self.n_move_not_needed += 1
      print(
        'no need to move', self.n_move_not_needed, self.n_src_processed_files, '/', self.total_srcfiles_in_db,
        src_dirnode.name, '@', strf.put_ellipsis_in_str_middle(src_dirnode.parentpath, 50)
      )
      return True
    return self.move_file_within_trg_to_its_src_relative_position_if_vacant(src_dirnode, trg_dirnode)

  def fetch_trg_dirnode_or_none_based_on_src(self, src_dirnode):
    """
    if program flow got up to here, srcfile exist.
    """
    trg_dirnode = self.find_sha1_in_trg_n_return_trg_dirnode(src_dirnode)
    if trg_dirnode is None:
      print(
        self.n_src_processed_files, '/', self.total_srcfiles_in_db,
        src_dirnode.name, 'WAS NOT FOUND by sha1 in target @',
        strf.put_ellipsis_in_str_middle(src_dirnode.parentpath, 50)
      )
      return None  # it will be copied over on return
    print(
      self.n_src_processed_files, '/', self.total_srcfiles_in_db,
      src_dirnode.name, 'WAS FOUND in trg db by sha1 @',
      strf.put_ellipsis_in_str_middle(src_dirnode.parentpath, 50)
    )
    return trg_dirnode

  def treat_unique_src_dirnode_with_copy_move_or_none(self, src_dirnode):
    """
    if program flow got up to here, srcfile exist.
    """
    self.n_src_processed_files += 1
    print(
      self.n_src_processed_files, '/', self.total_srcfiles_in_db,
      'Processing srcfile', src_dirnode.name, '@', strf.put_ellipsis_in_str_middle(src_dirnode.parentpath, 50)
    )
    trg_dirnode = self.fetch_trg_dirnode_or_none_based_on_src(src_dirnode)
    if trg_dirnode:
      self.move_trg_file_based_on_src_if_applicable(src_dirnode, trg_dirnode)
    else:
      self.copy_over_src_to_trg(src_dirnode)

  def fetch_n_process_unique_sha1s_in_scr(self):
    sql = 'select DISTINCT sha1, count(sha1) as c, * from %(tablename)s group by sha1 having c = 1;'
    fetched_list = self.ori_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    for larger_row in fetched_list:
      row = larger_row[2:]
      src_dirnode = dn.DirNode.create_with_tuplerow(row, self.ori_dt.dbtree.fieldnames)
      src_filepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mountpath)
      if not os.path.isfile(src_filepath):
        continue
      self.treat_unique_src_dirnode_with_copy_move_or_none(src_dirnode)

  def process(self):
    self.fetch_n_process_unique_sha1s_in_scr()
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
    print('n_src_processed_files:', self.n_src_processed_files)
    print('n_moved_files:', self.n_moved_files)
    print('n_failed_moves:', self.n_failed_moves)
    print('n_move_not_needed:', self.n_move_not_needed)
    print('n_names_not_liberated:', self.n_names_not_liberated)
    print('n_renames:', self.n_renames)
    print('failed_renames:', self.failed_renames)
    print('n_copied_files:', self.n_copied_files)
    print('n_failed_copies:', self.n_failed_copies)
    print('n_empty_dirs_removed:', self.n_empty_dirs_removed)
    print('n_empty_dirs_fail_rm:', self.n_empty_dirs_fail_rm)


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  molder = TrgBasedByrcSha1sMolder(src_mountpath, trg_mountpath)
  molder.process()


if __name__ == '__main__':
  process()
