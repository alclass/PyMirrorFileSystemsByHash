#!/usr/bin/env python3
"""
dbentry_updater_by_filemove_based_on_size_n_mdt_mod.py

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
import fs.hashfunctions.hash_mod as hm
import default_settings as defaults


class DBEntryUpdater:

  def __init__(self, mountpath):
    self.dbtree = dbdt.DBDirTree(mountpath)
    self.current_abspath = None
    self.n_dbupdates = 0
    self.n_processed_files = 0
    self.n_files_in_db = 0
    self.n_files_in_dirtree = 0
    self.n_unique_sha1s = 0
    self.n_dirs = 0

  def verify_if_an_update_can_happen(
      self,
      row_found, filename, middlepath, bytesize, mdatetime
    ):
    """
    If this point is reached in program flow, the following TWO condtions are met:
      1) a file in os does not have a correspondent entry in db (a dbentry);
      2) one sole dbentry exists with the same bytesize and mdatetime as file's (ie no ambiguity in files);
    The third one will be checked/tested here:
      3) that dbentry in 2) does not have its correspondent in os (dirtree);
    With the 3 above conditions met, it's reasonable to update dbentry
      with the new file location (hkey, name, parentpath)
    """
    # check if dbentry has its correspondent osentry:
    name = row_found[2]
    parentpath = row_found[3]
    folderpath = os.path.join(self.dbtree.mountpath, parentpath.lstrip('/'))
    filepath = os.path.join(folderpath, name)
    if os.path.isfile(filepath):
      return False
    # now it is false to update dbentry to an osentry with the supposition of same bytesize and mdatetime
    sql = '''
    UPDATE %(tablename)s
    SET
      hkey=?,
      name=?,
      parentpath=?
    WHERE
      bytesize=? and 
      mdatetime=?;
    '''
    print(sql)
    hkey = hm.HashSimple(middlepath + filename).num
    tuplevalues = (hkey, filename, middlepath, bytesize, mdatetime)
    update_result = self.dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)
    if update_result:
      self.n_dbupdates += 1
      return True
    return False

  def verify_if_many_exist_if_dbfound_exists_in_os_or_update_location(
      self, fetched_list, filename, middlepath, bytesize, mdatetime
    ):
    """
    middlepath is dirnode.parentpath
    """
    if fetched_list is None or len(fetched_list) == 0:
      return False
    if len(fetched_list) > 1:
      # ambiguity problem: give up updating db
      return False
    if len(fetched_list) == 1:
      row_found = fetched_list[0]
      return self.verify_if_an_update_can_happen(
        row_found, filename, middlepath, bytesize, mdatetime
      )

  def verify_file_by_its_bysize_n_mdatetime(self, filename, middlepath):
    filepath = os.path.join(self.current_abspath, filename)
    filestat = os.stat(filepath)
    bytesize = filestat.st_size
    mdatetime = filestat.st_mtime
    print(self.n_processed_files, '/', self.n_files_in_dirtree, 'bytesize', bytesize, 'mdatetime', mdatetime)
    sql = 'select * from %(tablename)s where bytesize=? and mdatetime=?;'
    tuplevalues = (bytesize, mdatetime)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(fetched_list) > 0:
      return self.verify_if_many_exist_if_dbfound_exists_in_os_or_update_location(
        fetched_list, filename, middlepath, bytesize, mdatetime
      )
    else:
      print(self.n_processed_files, '/', self.n_files_in_dirtree, filename, 'was not found by bytesize and mdatetime')

  def does_osfile_exist_in_db(self, filename, parentpath):
    """
    parentpath is middlepath which is fullpath minus mountpath
    """
    sql = 'select * from %(tablename)s where name=? and parentpath=?;'
    tuplevalues = (filename, parentpath)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(fetched_list) > 0:
      print(self.n_processed_files, '/', self.n_files_in_dirtree, 'file:', filename, 'is in db. Continuing.')
      return True
    return False

  def verify_files_possibly_moved_n_dbupdate(self, files):
    for filename in files:
      self.n_processed_files += 1
      middlepath = self.current_abspath[len(self.dbtree.mountpath):]
      if not middlepath.startswith('/'):
        middlepath = '/' + middlepath
      if self.does_osfile_exist_in_db(filename, middlepath):
        continue
      # file may have been moved, try find it via bytesize and mdatetime
      self.verify_file_by_its_bysize_n_mdatetime(filename, middlepath)

  def walkup_to_count_files(self):
    self.n_files_in_dirtree = 0
    self.n_dirs = 0
    is_root_dir_looppass = True
    for self.current_abspath, folders, files in os.walk(self.dbtree.mountpath):
      if is_root_dir_looppass:
        # do not count files in rootdir
        is_root_dir_looppass = False
        continue
      self.n_files_in_dirtree += len(files)
      self.n_dirs += 1

  def walkup_dirtree(self):
    n_seq_dir = 0
    is_root_dir_looppass = True
    for self.current_abspath, folders, files in os.walk(self.dbtree.mountpath):
      if is_root_dir_looppass:
        # do not count files in rootdir
        is_root_dir_looppass = False
        continue
      n_seq_dir += 1
      print(n_seq_dir, self.current_abspath)
      self.verify_files_possibly_moved_n_dbupdate(files)

  def report(self):
    print('='*40)
    print('DBEntryUpdater Report:')
    print('='*40)
    print('dirtree:', self.dbtree.mountpath)
    print('n_dirs', self.n_dirs)
    print('n_files_in_db', self.n_files_in_db)
    print('n_files_in_dirtree', self.n_files_in_dirtree, '(obs: rootdir files do not count.)')
    print('n_processed_files', self.n_processed_files)
    print('n_unique_sha1s', self.n_unique_sha1s)
    print('n_dbupdates', self.n_dbupdates, "(meaning files that were moved before and got unsync'd, now db-sync'd)")

  def process(self):
    self.n_files_in_db = self.dbtree.count_rows_as_int()
    self.n_unique_sha1s = self.dbtree.count_unique_sha1s_as_int()
    self.walkup_to_count_files()
    self.walkup_dirtree()
    self.report()


def process():
  """
  """
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  dbentryupdater = DBEntryUpdater(src_mountpath)
  dbentryupdater.process()


if __name__ == '__main__':
  process()
