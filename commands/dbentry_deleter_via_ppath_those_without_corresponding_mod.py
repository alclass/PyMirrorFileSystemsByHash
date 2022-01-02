#!/usr/bin/env python3
"""
dbentry_deleter_those_without_corresponding_osentry_mod.py
This script should be used after dbentry_updater_by_filemove_based_on_size_n_mdt_mod,
  ie, before deleting a dbentry, an attempt should be made to check whether an osentry was moved.

This script reads all files in db and checks whether or not each one is in its folder.

A bit of history
================

When the strategy of this system discontinued the self-reference id field (which linked childnode to parentnode),
it also discontinued the db-recording of foldernodes leaving only filenodes in db.
  (That old strategy was done with the help of SqlAlchemy,
    instead of direct sqlite which became the implemented approach nowadays. This was a simplying initiative.)

At that moment, field is_file, though introduced, was not really used. After a while, it was thought that
is_file could be changed into is_present_on_folder for transient use, ie for use in a step-by-step process chain.

For example: suppose a back-up operation is started. An operation in this chain process
  might check whether or not the on-going db-registered file is present on folder.
   If it's not present, before concluding it's been erased/removed,
     the system might try to find it elsewhere by size and mdatetime (name itself might have be changed).

  It's expected that this might help resync files that were moved and if name, size and mdate are the same,
    it's reasonable to expect this file was previously moved.
  So this script does this checking recording True (1) of False (0) in the is_present field.
"""
import datetime
import os.path
import fs.db.dbdirtree_mod as dbt
import fs.dirfilefs.dir_n_file_fs_mod as dirfil
import fs.strfs.strfunctions_mod as strf
import default_settings as defaults
import models.entries.dirnode_mod as dn
SQL_SELECT_LIMIT_DEFAULT = 50


class DBEntryViaPPathWithoutCorrespondingOsDeleter:
  """
  This class looks for db-entries without their respective os-entries and deleted the db-ones.
  This functionality should be used after
  """

  def __init__(self, mountpath):
    self.mountpath = mountpath
    self.dbtree = dbt.DBDirTree(self.mountpath)
    self.total_files_os = 0
    self.total_dirs_os = 0
    self.total_files_in_db = 0
    self.n_deleted_dbentries = 0
    self.n_loop_deletes = 0
    self.n_processed_paths = 0
    self.count_totals()
    # this below is zeroed at every pass thru outside loop (the one with sql-limit/offset)
    # its counting ajusts the db-select-offset (it's here for the IDE to reflect it)

  def count_totals(self):
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    self.total_files_os, self.total_dirs_os = dirfil.count_total_files_n_folders_with_restriction(self.mountpath)

  def delete_dbentry_if_theres_no_equivalent_dir_entry(self, parentpath):
    middlepath = parentpath
    middlepath = middlepath.lstrip('/')
    fpath = os.path.join(self.mountpath, middlepath)
    if not os.path.isdir(fpath):
      n_chars = 1 + len(parentpath)
      where_clause = ' WHERE SUBSTR(parentpath, 0, %d)=?;' % n_chars
      sql = 'DELETE FROM %(tablename)s' + where_clause
      tuplevalues = (parentpath, )
      n_deletes = self.dbtree.delete_with_sql_n_tuplevalues(sql, tuplevalues)  # db_del_result
      self.n_loop_deletes += 1
      self.n_deleted_dbentries += n_deletes
      print(self.n_loop_deletes, 'dirs', self.total_dirs_os, 'deleting entries thru path:', parentpath)
      print('n_deletes', n_deletes, 'up til now total deleted_dbentries', self.n_deleted_dbentries)
      return True
    return False

  def fetch_dbentries_via_ppath_n_check_path_exists(self):
    sql = 'SELECT DISTINCT parentpath FROM %(tablename)s ORDER BY parentpath;'
    generated_rows = self.dbtree.do_select_with_sql_wo_tuplevalues_w_limit_n_offset(sql)
    for rows in generated_rows:
      for row in rows:
        self.n_processed_paths += 1
        parentpath = row[0]
        print(self.n_processed_paths, '/', self.total_dirs_os, 'processing path:', parentpath)
        has_deleted = self.delete_dbentry_if_theres_no_equivalent_dir_entry(parentpath)
        if has_deleted:
          # restart generated_rows
          return self.fetch_dbentries_via_ppath_n_check_path_exists()

  def process(self):
    dirfil.prune_dirtree_deleting_empty_folders(self.mountpath)
    self.fetch_dbentries_via_ppath_n_check_path_exists()
    # self.delete_empty_dirs()
    self.report()

  def report(self):
    print('='*40)
    print('DBEntryViaPPathWithoutCorrespondingOsDeleter Report')
    print('='*40)
    print('Mountpath is', self.mountpath)
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_os', self.total_files_os)
    print('total_dirs_os', self.total_dirs_os)
    print('n_loop_deletes', self.n_loop_deletes)
    print('n_deleted_dbentries', self.n_deleted_dbentries)
    print('n_processed_paths', self.n_processed_paths)
    print('End of Processing')


def process():
  start_time = datetime.datetime.now()
  mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  print('start_time', start_time)
  dbentry_eraser = DBEntryViaPPathWithoutCorrespondingOsDeleter(mountpath)
  dbentry_eraser.process()
  finish_time = datetime.datetime.now()
  print('finish_time', finish_time)
  elapsed_time = finish_time - start_time
  print('elapsed_time', elapsed_time, finish_time)


if __name__ == '__main__':
  process()
