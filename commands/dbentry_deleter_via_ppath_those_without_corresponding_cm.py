#!/usr/bin/env python3
"""
dbentry_deleter_via_ppath_those_without_corresponding_cm

This script takes a different strategy to delete db-entries that do not have corresponding os-entries.

First, let us comment how script:
  dbentry_deleter_those_without_corresponding_osentry_cm.py
treats the problem.  It visits each db-entry and checks, one by one, existence of its corresponding os-entry.

This script doesn't do that, it uses a completely different strategy, it looks each parentpath,
   if it doesn't exist, it chdirs down (it drills down to check if parent folders are also non-existence)
     until a parent folder exists. (Notice this drilling-down must stop at least in the root folder.)
   Once the most-based non-existent folder is determined, a SQL-DELETE will apply
     to every record covered by the WHERE clause encompassing the folders.

  So instead of going one record by one record, it may take various at one shot.

This script is useful for deleting remainders when folders were moved or renamed (outside of this system)
  and a resync still left "loose" records on db.
Prefer this script for deleting db-entries to folders that were moved, renamed or deleted outside of this system.
Prefer script dbentry_deleter_those_without_corresponding_osentry_cm.py otherwise (when changes are mix).
"""
import datetime
import os.path
import sys

import fs.db.dbdirtree_mod as dbt
import fs.dirfilefs.dir_n_file_fs_mod as dirfil
# import fs.strnlistfs.strfunctions_mod as strf
import default_settings as defaults
# import models.entries.dirnode_mod as dn
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
    self.acc_parentpaths = []
    self.count_totals()
    # this below is zeroed at every pass thru outside loop (the one with sql-limit/offset)
    # its counting ajusts the db-select-offset (it's here for the IDE to reflect it)

  def count_totals(self):
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    self.total_files_os, self.total_dirs_os = dirfil.count_total_files_n_folders_with_restriction(self.mountpath)

  def do_delete_path_substr(self, parentpath):
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

  def drill_down_parentpath_checking_nonexisting_folders(self, parentpath, last_dirname=None):
    middlepath = parentpath
    middlepath = middlepath.lstrip('/')
    fpath = os.path.join(self.mountpath, middlepath)
    if os.path.isdir(fpath):
      if last_dirname is None:
        return False
      # last_dirname exists, recompose non-existing path/parentpath and return calling do_delete_path_substr()
      backparentpath = os.path.join(parentpath, last_dirname)
      return self.do_delete_path_substr(backparentpath)
    # path does not exist, drilldown to see further down
    downparentpath, topdirname = os.path.split(parentpath)
    if (parentpath, topdirname) != ('/', ''):  # TO-DO: think about this case from which drilldown cannot continue
      return self.drill_down_parentpath_checking_nonexisting_folders(downparentpath, topdirname)
    return False

  def fetch_dbentries_via_ppath_n_check_path_exists(self):
    sql = 'SELECT DISTINCT parentpath FROM %(tablename)s ORDER BY parentpath;'
    # generated_rows = self.dbtree.do_select_with_sql_wo_tuplevalues_w_limit_n_offset(sql)
    # for rows in generated_rows:
    rows = self.dbtree.do_select_with_sql_without_tuplevalues(sql)
    for row in rows:
      self.n_processed_paths += 1
      parentpath = row[0]
      print(self.n_processed_paths, '/', self.total_dirs_os, 'processing path:', parentpath)
      has_deleted = self.drill_down_parentpath_checking_nonexisting_folders(parentpath)
      if has_deleted:
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


def show_help_cli_msg_if_asked():
  for arg in sys.argv:
    if arg in ['-h', '--help']:
      print(__doc__)
      sys.exit(0)


def process():
  start_time = datetime.datetime.now()
  show_help_cli_msg_if_asked()
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
