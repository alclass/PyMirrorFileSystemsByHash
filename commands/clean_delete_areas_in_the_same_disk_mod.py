#!/usr/bin/env python3
"""
clean_delete_areas_in_the_same_disk_mod.py

This script does the following:
  1) its main class accepts two parameters, they are: src_mountpath & dirpath_to_cleandel
  2) it looks up sha1's that are repeats inside dirpath_to_cleandel, ie files that exist somewhere else in the disk;
  3) if found theses repeats in 2), it checks/assures originals are not missing in disk
     and then deletes the ones in excess, ie the one inside dirpath_to_cleandel.

IMPORTANT:
  1) this script does not involve a second disk (or dirtree);
  2) this script does not RECALCULATE the origin sha1's,
     it trusts that script walk_dirtree (or another with similar tasks)
     has been previously run and generated good results.
  3) DO NOT use this script without having a copied disk (origin or back-up)
     (this system provides this copying with other scripts, so use them first).
"""
import os.path
import sys

import fs.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirfil
import fs.strfs.strfunctions_mod as strf


class CleanDeleterThruSubtreeInSameDisk:
  """
  This class implements the deletion of all repeat-sha1's in target that exist in source.
  The files to be deleted are gathered first and will only be deleted with the user's confirmation.
  IMPORTANT: if confirmed, the deletion operation here cannot be undone.
  """

  def __init__(self, mountpath, dirpath_to_cleandel):
    self.deletion_confirmed = False
    self.n_processed_files = 0
    self.n_processed_deletes = 0
    self.clean_del_ids = []
    self.n_deleted = 0
    self.n_failed_deletes = 0
    self.total_files_in_db = 0
    self.total_files_os = 0
    self.total_dirs_os = 0
    self.total_files_cleandeldirpath_os = 0
    self.total_dirs_cleandeldirpath_os = 0
    self.total_files_cleandeldirpath_db = 0
    self.n_rmdirs_visited = 0
    self.n_dirs_deleted = 0
    self.n_rmdirs_failed = 0
    self.dbtree = dbdt.DBDirTree(mountpath)
    self.curr_cleandel_dirpath = None
    self.dirpath_to_cleandel = dirpath_to_cleandel
    self.treat_dirpath_to_cleandel()
    self.calc_totals()

  def treat_dirpath_to_cleandel(self):
    if not os.path.isdir(self.dirpath_to_cleandel):
      error_msg = 'dirpath_to_cleandel (%s) does not exist.' % self.dirpath_to_cleandel
      raise OSError(error_msg)
    if not self.dirpath_to_cleandel.startswith(self.dbtree.mountpath):
      error_msg = 'dirpath_to_cleandel (%s) should be INSIDE mountpath (%s).' %\
                  (self.dirpath_to_cleandel, self.dbtree.mountpath)
      raise OSError(error_msg)

  @property
  def cleandel_middlepath(self):
    middlepath = self.dirpath_to_cleandel[len(self.dbtree.mountpath):]
    middlepath = middlepath.lstrip('./')
    return middlepath

  @property
  def cleandel_parentpath(self):
    parentpath = '/' + self.cleandel_middlepath
    return parentpath

  def derive_parentpath_from_currentcleandelpath(self):
    """
    self.curr_cleandel_dirpath
    """
    currpath = self.curr_cleandel_dirpath
    middlepath = currpath[len(self.dbtree.mountpath):]
    middlepath = middlepath.lstrip('./')
    parentpath = '/' + middlepath
    return parentpath

  def count_files_in_db(self):
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    # count db-entries in "separate area" (the one under self.dirpath_to_cleandel)
    n_chars = 1 + len(self.cleandel_parentpath)
    where_clause = ' WHERE substr(parentpath, 0, %d)=?;' % n_chars
    sql = 'SELECT count(*) FROM %(tablename)s' + where_clause
    tuplevalues = (self.cleandel_parentpath,)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) == 1:
      self.total_files_cleandeldirpath_db = int(fetched_list[0][0])

  def count_files_in_dirtree(self):
    self.total_files_os = 0
    self.total_dirs_os = 0
    print('Counting src_total_files and folders. Please wait.')
    self.total_files_os, self.total_dirs_os = dirfil.count_total_files_n_folders(self.dbtree.mountpath)
    self.total_files_cleandeldirpath_os, self.total_dirs_cleandeldirpath_os = \
        dirfil.count_total_files_n_folders_inc_root(self.dirpath_to_cleandel)

  def calc_totals(self):
    self.count_files_in_db()
    self.count_files_in_dirtree()

  def does_sha1_exist_in_src(self, trg_sha1):
    sql = 'SELECT id FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (trg_sha1, )
    fetched_rows = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(fetched_rows) > 0:
      return True
    return False

  def delete_entry_in_os_n_in_db(self, _id):
    """
    IMPORTANT: only method do_batch_deletion_if_confirmed() can call this one,
      for it checks the user's confirmation before calling here.
    """
    if not self.deletion_confirmed:
      error_msg = 'Program Error: erroneous logical call for delete_entry_in_os_n_in_db()' \
                  ' when delete depends on confirmation'
      raise ValueError(error_msg)
    dirnode = self.dbtree.fetch_dirnode_by_id(_id)
    if dirnode is None:
      return
    fpath = dirnode.get_abspath_with_mountpath(self.dbtree.mountpath)
    if os.path.isfile(fpath):
      trg_total_to_del = len(self.clean_del_ids)
      self.n_processed_deletes += 1
      print(self.n_processed_deletes, '/', trg_total_to_del, ' >>> DELETING', _id, dirnode.name)
      print(fpath)
      try:
        os.remove(fpath)
        self.n_deleted += 1
        print(
          'Deleted', self.n_deleted, 'proc', self.n_processed_deletes,
          '/', trg_total_to_del, '/', self.total_files_os, '>>> DELETING', _id,
          dirnode.name, '@', strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50)
        )
        return True
      except (OSError, IOError):
        self.n_failed_deletes += 1
        print(
          'Failed del', self.n_failed_deletes, 'proc', self.n_processed_deletes,
          '/', trg_total_to_del, '/', self.total_files_os, '>>> DELETING', _id, dirnode.name
        )
        return False
    print(self.n_processed_files, '/', self.total_files_os, 'file to delete does not exist.')
    return False

  def do_batch_deletion_if_confirmed(self):
    """
      if os.path.isfile(fpath):
        os.remove(fpath)
        self.bak_dbtree.delete_row_by_id(_id)
        self.n_deletes += 1
        print(self.n_deletes, 'deleted', dirnode.name)
        print(fpath)
    """
    if not self.deletion_confirmed:
      return 0
    for _id in self.clean_del_ids:
      self.delete_entry_in_os_n_in_db(_id)

  def print_out_all_files_to_delete(self):
    trg_del_total = len(self.clean_del_ids)
    for i, _id in enumerate(self.clean_del_ids):
      fetched_rows = self.dbtree.fetch_rowlist_by_id(_id)
      if fetched_rows is None or len(fetched_rows) == 0:
        print('id', _id, 'is empty. Continuing')
        continue
      row = fetched_rows[0]
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      print(i+1, 'of', trg_del_total, '/ id', _id, '[', dirnode.name, ']')
      fpath = dirnode.get_abspath_with_mountpath(self.dbtree.mountpath)
      ppath = strf.put_ellipsis_in_str_middle(fpath, 150)
      print(ppath)
      print('-'*50)

  def confirm_deletion(self):
    self.deletion_confirmed = False
    if len(self.clean_del_ids) == 0:
      print('Empty list. No trg deletes to confirm.')
      print('=' * 40)
      return
    print('='*40)
    print('List of File Deletions to Confirm:')
    print('='*40)
    self.print_out_all_files_to_delete()
    print('='*40)
    screen_msg = 'Confirm the deletion of the %d ids above? (*Y/n) ' % len(self.clean_del_ids)
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      self.deletion_confirmed = True

  def remove_empty_folders_in_trg(self):
    self.n_rmdirs_visited, self.n_dirs_deleted, self.n_rmdirs_failed = \
      dirfil.prune_dirtree_deleting_empty_folders(self.dbtree.mountpath)

  def fetch_dirnode_from_file_n_path(self, filename):
    parentpath = self.derive_parentpath_from_currentcleandelpath()
    dirnode = self.dbtree.fetch_dirnode_with_name_n_parent(filename, parentpath)
    return dirnode

  def does_sha1_exist_elsewhere_in_the_non_clean_area(self, dirnode):
    """
    Notice that the underlying select uses a second NESTED select
      restricting the search "area" to the non cleandir paths.

    SELECT * FROM %(tablename)s WHERE sha1=?
      AND parentpath NOT IN
      (SELECT parentpath FROM %(tablename)s' WHERE substr(parentpath, %d)=?);

    A simpler alternative (to be tested) is the use the != (different operator) directly in the 2nd where-clause:

    SELECT * FROM %(tablename)s WHERE sha1=? AND parentpath substr(parentpath, %d)!=?;
    """
    sha1 = dirnode.sha1
    n_chars = 1 + len(self.cleandel_parentpath)
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=? '
    and_where = 'AND substr(parentpath, 0, %d)!=?;' % n_chars
    sql += and_where
    tuplevalues = (sha1, self.cleandel_parentpath)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) > 0:
      return True
    return False

  def process_cleandel_files(self, files):
    for filename in files:
      self.n_processed_files += 1
      filepath = os.path.join(self.curr_cleandel_dirpath, filename)
      if not os.path.isfile(filepath):
        print(self.n_processed_files, '/', self.total_files_os, filename, 'does not exist in dir. Continuing.')
        continue
      # middlepath = self.dirpath_to_cleandel
      dirnode = self.fetch_dirnode_from_file_n_path(filename)
      if dirnode is None:
        print(self.n_processed_files, '/', self.total_files_os, filename, 'does not exist in db. Continuing.')
        continue
      if self.does_sha1_exist_elsewhere_in_the_non_clean_area(dirnode):
        self.clean_del_ids.append(dirnode.get_db_id())

  def walk_thru_toclean_dirtree_db_entries(self):
    for self.curr_cleandel_dirpath, folders, files in os.walk(self.dirpath_to_cleandel):
      self.process_cleandel_files(files)

  def process(self):
    self.walk_thru_toclean_dirtree_db_entries()
    self.confirm_deletion()
    if self.deletion_confirmed:
      self.do_batch_deletion_if_confirmed()
      self.remove_empty_folders_in_trg()
    self.report()

  def report(self):
    print('Report:')
    print('=======')
    print('dirtrees:', self.dbtree.mountpath, self.dbtree.mountpath)
    print('len(delete_ids)', len(self.clean_del_ids))
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_os', self.total_files_os)
    print('total_dirs_os', self.total_dirs_os)
    print('total_files_cleandeldirpath_os', self.total_files_cleandeldirpath_os)
    print('total_dirs_cleandeldirpath_os', self.total_dirs_cleandeldirpath_os)
    print('total_files_cleandeldirpath_db', self.total_files_cleandeldirpath_db)
    self.calc_totals()
    print('After process:')
    print('='*40)
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_os', self.total_files_os)
    print('total_dirs_os', self.total_dirs_os)
    # --------------------
    print('total_files_cleandeldirpath_os', self.total_files_cleandeldirpath_os)
    print('total_dirs_cleandeldirpath_os', self.total_dirs_cleandeldirpath_os)
    print('total_files_cleandeldirpath_db', self.total_files_cleandeldirpath_db)
    print('n_rmdirs_visited', self.n_rmdirs_visited)
    print('n_dirs_deleted', self.n_dirs_deleted)
    print('n_rmdirs_failed', self.n_rmdirs_failed)


def get_dirpath_to_cleandel_arg():
  dirpath_to_cleandel = None
  for arg in sys.argv:
    if arg.startswith('-d='):
      dirpath_to_cleandel = arg[len('-d='):]
      break
  return dirpath_to_cleandel


def process():
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  dirpath_to_cleandel = get_dirpath_to_cleandel_arg()
  cleandeleter = CleanDeleterThruSubtreeInSameDisk(src_mountpath, dirpath_to_cleandel)
  cleandeleter.process()


if __name__ == '__main__':
  process()
