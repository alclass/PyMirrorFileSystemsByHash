#!/usr/bin/env python3
"""
dbentry_updater_by_filemove_based_on_size_n_mdt_cm.py

This script looks up all files thru a dirtree and checks if each has a corresponding dbentry.
If a file doesn't have a corresponding dbentry, three conditions must be met for an updating against the dbentry, ie:
  1) the file in os must not have its correspondent entry in db (the dbentry);
  2) one sole dbentry must exist with the same bytesize and mdatetime as file's (ie no ambiguity in files);
  3) the above dbentry must not have its own correspondent in os (dirtree);
If the 3 conditions above are met, update the dbentry with its new location, the file's location.

This supposition is based on the following reasoning:
1) the file was moved from its previous location to another;
2) the bytesize and mdatetime are reasonably stable, ie the file will keep its two attributes across a move;
3) there is no further files with the same bytesize and mdatetime;
4) the file was not modified (though it could be modified still keeping its original size) seen by its mdatetime.
"""
import os
import fs.db.dbdirtree_mod as dbdt
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import fs.strnlistfs.strfunctions_mod as strf
import models.entries.dirnode_mod as dn


class DBEntryUpdater:

  def __init__(self, mountpath):
    self.dbtree = dbdt.DBDirTree(mountpath)
    self.current_abspath = None
    self.n_dbupdates = 0
    self.n_failed_filestats = 0
    self.n_processed_files = 0
    self.n_processed_dirs = 0
    self.total_files_in_db = 0
    self.total_files_in_os = 0
    self.total_dirs_in_os = 0
    self.total_unique_sha1s = 0
    self.count_total_files_n_dirs_in_os()

  def count_total_files_n_dirs_in_os(self):
    print('@ count_total_files_n_dirs_in_os(). Please, wait.')
    total_files, total_dirs = dirf.count_total_files_n_folders_with_restriction(self.dbtree.mountpath)
    self.total_files_in_os = total_files
    self.total_dirs_in_os = total_dirs
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    self.total_unique_sha1s = self.dbtree.count_unique_sha1s_as_int()

  @property
  def total_repeats_in_db(self):
    return self.total_files_in_db - self.total_unique_sha1s

  def verify_if_an_update_can_happen(
      self,
      row_found, filename, parentpath_to_be
    ):
    """
    bytesize, mdatetime are not used here but row_found had these two equal
    If this point is reached in program flow, the following TWO condtions are met:
      1) a file in os does not have a correspondent entry in db (a dbentry);
      2) one sole dbentry exists with the same bytesize and mdatetime as file's (ie no ambiguity in files);
    The third one will be checked/tested here:
      3) that dbentry in 2) does not have its correspondent in os (dirtree);
    With the 3 above conditions met, it's reasonable to update dbentry
      with the new file location (hkey, name, parentpath)
    """
    # check if dbentry has its correspondent osentry:
    _id = row_found[0]
    idx = self.dbtree.fieldnames.index('name')
    name = row_found[idx]
    idx = self.dbtree.fieldnames.index('parentpath')
    parentpath = row_found[idx]
    middlepath = parentpath.lstrip('/')
    folderpath = os.path.join(self.dbtree.mountpath, middlepath)
    filepath = os.path.join(folderpath, name)
    if os.path.isfile(filepath):
      return False
    # now it is false to update dbentry to an osentry with the supposition of same bytesize and mdatetime
    sql = '''
    UPDATE %(tablename)s
    SET
      name=?,
      parentpath=?
    WHERE
      id=?;
    '''
    print(sql)
    tuplevalues = (filename, parentpath_to_be, _id)
    update_result = self.dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)
    if update_result:
      self.n_dbupdates += 1
      return True
    return False

  def verify_if_many_exist_if_dbfound_exists_in_os_or_update_location(
      self, fetched_list, filename, parentpath
    ):
    """
    middlepath is dirnode.parentpath (with the starting '/')
    """
    if fetched_list is None or len(fetched_list) == 0:
      return False
    if len(fetched_list) > 1:
      # ambiguity problem: give up updating db
      return False
    if len(fetched_list) == 1:
      row_found = fetched_list[0]
      return self.verify_if_an_update_can_happen(
        row_found, filename, parentpath
      )
    return False

  def verify_file_by_its_bysize_n_mdatetime(self, filename, parentpath):
    filepath = os.path.join(self.current_abspath, filename)
    try:
      filestat = os.stat(filepath)
    except OSError:
      self.n_failed_filestats += 1
      return False
    bytesize = filestat.st_size
    mdatetime = filestat.st_mtime
    print(self.n_processed_files, '/', self.total_files_in_os, 'bytesize', bytesize, 'mdatetime', mdatetime)
    sql = 'SELECT * FROM %(tablename)s WHERE bytesize=? AND mdatetime=?;'
    tuplevalues = (bytesize, mdatetime)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(fetched_list) > 0:
      return self.verify_if_many_exist_if_dbfound_exists_in_os_or_update_location(
        fetched_list, filename, parentpath
      )
    else:
      print(self.n_processed_files, '/', self.total_files_in_os, filename, 'was not found by bytesize and mdatetime')
    return False

  def get_dirnode_or_none_with_name_n_parent(self, filename, parentpath):
    """
    """
    sql = 'SELECT * FROM %(tablename)s WHERE name=? AND parentpath=?;'
    tuplevalues = (filename, parentpath)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list or len(fetched_list) == 1:
      row = fetched_list[0]
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      print(
        self.n_processed_files, '/', self.total_files_in_os,
        'file EXISTS in db:', filename, '@', strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50),
        ' Continuing.'
      )
      return dirnode
    return None

  def verify_files_possibly_moved_n_dbupdate(self, files):
    middlepath = self.current_abspath[len(self.dbtree.mountpath):]
    if middlepath.startswith('/'):
      middlepath = middlepath.lstrip('/')
    parentpath = '/' + middlepath
    for filename in files:
      self.n_processed_files += 1
      dirnode = self.get_dirnode_or_none_with_name_n_parent(filename, parentpath)
      if dirnode is not None:
        continue
      # file may have been moved, try find it via bytesize and mdatetime
      self.verify_file_by_its_bysize_n_mdatetime(filename, parentpath)

  def walkup_dirtree_to_verify_possible_moveupdates(self):
    print('total os-files', self.total_files_in_db, '@ walkup_dirtree_to_verify_possible_moveupdates()')
    for self.current_abspath, folders, files in os.walk(self.dbtree.mountpath):
      if self.current_abspath == self.dbtree.mountpath:
        continue
      if dirf.is_forbidden_dirpass(self.current_abspath):
        continue
      self.n_processed_dirs += 1
      print(
        'dir', self.n_processed_dirs, '/', self.total_dirs_in_os, self.current_abspath
      )
      self.verify_files_possibly_moved_n_dbupdate(files)

  def report(self):
    print('='*40)
    print('DBEntryUpdater Report:')
    print('='*40)
    print('dirtree:', self.dbtree.mountpath)
    print('total_files_in_db', self.total_files_in_db)
    print('total_unique_sha1s', self.total_unique_sha1s)
    print('total_repeats_in_db', self.total_repeats_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os, '(obs: rootdir files do not count.)')
    print('n_processed_dirs', self.n_processed_dirs)
    print('n_processed_files_in_trg', self.n_processed_files)
    print('n_failed_filestats', self.n_failed_filestats)
    print('n_dbupdates', self.n_dbupdates, "(meaning files that were moved before and got unsync'd, now db-sync'd)")

  def set_totals_in_db(self, n_files_in_db=None, n_unique_sha1s=None):
    if n_files_in_db is not None:
      self.total_files_in_db = n_files_in_db
    else:
      self.total_files_in_db = self.dbtree.count_rows_as_int()
    if n_unique_sha1s is not None:
      self.total_unique_sha1s = n_unique_sha1s
    else:
      self.total_unique_sha1s = self.dbtree.count_unique_sha1s_as_int()

  def process(self):
    self.walkup_dirtree_to_verify_possible_moveupdates()
    self.report()


def process():
  """
  """
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  dbentryupdater = DBEntryUpdater(src_mountpath)
  dbentryupdater.process()


if __name__ == '__main__':
  process()
