#!/usr/bin/env python3
"""
dbentry_deleter_those_without_corresponding_osentry_cm.py
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
# import commands.dbentry_deleter_via_ppath_those_without_corresponding_mod as dbdelviapath
import default_settings as defaults
import fs.db.dbdirtree_mod as dbt
import fs.dirfilefs.dir_n_file_fs_mod as dirfil
import fs.strnlistfs.strfunctions_mod as strf
import models.entries.dirnode_mod as dn
SQL_SELECT_LIMIT_DEFAULT = 50


class DBEntryWithoutCorrespondingOsEntryDeleter:
  """
  This class looks for db-entries without their respective os-entries and deleted the db-ones.
  This functionality should be used after
  """

  def __init__(self, mountpath):
    self.n_deleted_dbentries = 0
    self.n_processed_in_db = 0
    self.total_files_os = 0
    self.total_dirs_os = 0
    self.total_files_in_db = 0
    self.total_sha1s_in_db = 0
    self.n_updates = 0
    self.mountpath = mountpath
    self.dbtree = dbt.DBDirTree(self.mountpath)
    self.count_totals()
    # this below is zeroed at every pass thru outside loop (the one with sql-limit/offset)
    # its counting ajusts the db-select-offset (it's here for the IDE to reflect it)
    self.n_deleted_in_loop = 0

  def count_totals(self):
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    self.total_sha1s_in_db = self.dbtree.count_unique_sha1s_as_int()
    self.total_files_os, self.total_dirs_os = dirfil.count_total_files_n_folders_with_restriction(self.mountpath)

  def delete_dbentry_if_theres_no_equivalent_os_entry(self, row):
    self.n_processed_in_db += 1
    dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
    filepath = dirnode.get_abspath_with_mountpath(self.mountpath)
    if not os.path.isfile(filepath):
      _ = self.dbtree.delete_row_by_id(dirnode.get_db_id())  # del_result
      self.n_deleted_dbentries += 1
      self.n_deleted_in_loop += 1
      print(' *-=-' * 4, 'DELETE DBENTRY', ' *-=-' * 4)
      print(
        self.n_deleted_dbentries, '/', self.n_processed_in_db, '/', self.total_files_in_db,
        'deleted dbentry for', dirnode.get_db_id(),
        dirnode.name, strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50)
      )

  def fetch_dbentries_n_check_their_osentries(self):
    offset = 0
    k_limit = 50
    while 1:  # see below the condition for interrupting this while-infinite-loop
      limit_clause = ' LIMIT %(limit)d OFFSET %(offset)d ;' \
        % {'limit': k_limit, 'offset': offset}
      sql = 'SELECT * FROM %(tablename)s' + limit_clause
      rows = self.dbtree.do_select_with_sql_without_tuplevalues(sql)
      for i, row in enumerate(rows):
        if self.n_processed_in_db > self.total_files_in_db:
          error_msg = \
            'self.n_processed_in_db (%d) > self.total_files_in_db (%d) ' \
            'dbentry_deleter_those->in fetch_dbentries_n_check_their_osentries()' \
            % (self.n_processed_in_db, self.total_files_in_db)
          raise ValueError(error_msg)
        print(
          i + 1, '/', self.n_processed_in_db, '/', self.total_files_in_db
        )
        self.delete_dbentry_if_theres_no_equivalent_os_entry(row)
      offset = offset + k_limit - self.n_deleted_in_loop
      if len(rows) < k_limit:  # this is the condition for interrupting the while-infinite-loop "WHILE 1" above
        print('Interrupting while-1 loop: len(rows)', len(rows), ' < k_limit', k_limit, 'offset', offset)
        break

  def report(self):
    print('='*40)
    print('DBEntryWithoutCorrespondingOsEntryDeleter Report')
    print('='*40)
    print('Mountpath is', self.mountpath)
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_os', self.total_files_os)
    print('total_dirs_os', self.total_dirs_os)
    print('total_sha1s_in_db', self.total_sha1s_in_db)
    print('n_processed_in_db', self.n_processed_in_db)
    print('n_deleted_dbentries', self.n_deleted_dbentries)
    print('End of Processing')

  def delete_empty_dirs(self):
    screen_msg = 'Do you want to delete empty folders in [%s] ? (/Y/n) ' % self.mountpath
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      dirfil.prune_dirtree_deleting_empty_folders(self.mountpath)

  def process(self):
    self.fetch_dbentries_n_check_their_osentries()
    # self.delete_empty_dirs()
    self.report()


class PresentInDBNotInDirTreeReporter:
  """
  The field is_present was DEPRECATED/removed from db.
  The presence occurrence must be done on-the-fly, ie as needed.
  Because of this DEPRECATION this class now just fills in an id list with files missing in dirtree.
  The order of fields in the dirtree table is derived from list dbtree.fieldnames
  """

  sha1_repeat_dict = {}

  def __init__(self, mountpath):
    """
    """
    self.n_processed_files = 0
    self.n_updates = 0
    self.ids_present_in_db_not_in_os = []
    self.mountpath = mountpath
    self.dbtree = dbt.DBDirTree(self.mountpath)
    self.total_files_in_db = self.dbtree.count_unique_sha1s_as_int()

  def printline(self, dirnode):
    filepath = dirnode.get_abspath_with_mountpath(self.dbtree.mount_abspath)
    print(self.n_processed_files, '/', self.total_files_in_db, dirnode.name)
    print(' => filepath =', filepath)

  def gather_ids_not_present_in_os(self, rows):
    for row in rows:
      self.n_processed_files += 1
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      filepath = dirnode.get_abspath_with_mountpath(self.dbtree.mount_abspath)
      if not os.path.isfile(filepath):
        self.printline(dirnode)
        self.ids_present_in_db_not_in_os.append(dirnode.get_db_id())
        screen_line = ' ==>>> this not in dirtree (%d)' % len(self.ids_present_in_db_not_in_os)
        print(screen_line)

  def loop_thru_files_in_db(self):
    for generated_rows in self.dbtree.do_select_all_w_limit_n_offset():
      for rows in generated_rows:
        self.gather_ids_not_present_in_os(rows)

  def report_files_in_db_not_in_os(self):
    for _id in self.ids_present_in_db_not_in_os:
      fetched_list = self.dbtree.fetch_rowlist_by_id(_id)
      if fetched_list is None or len(fetched_list) == 0:
        continue
      row = fetched_list[0]
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      self.printline(dirnode)

  def process(self):
    print('Process verify files present in db not in dirtree')
    print('='*40)
    self.loop_thru_files_in_db()
    self.report_files_in_db_not_in_os()
    print('Finished with', self.n_processed_files, 'files processed.')


def process():
  start_time = datetime.datetime.now()
  mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  print('start_time', start_time)
  # the first object tries to optimize performance, deleting "bulkly" if possible
  # dbviapath_deleter = dbdelviapath.DBEntryViaPPathWithoutCorrespondingOsDeleter(mountpath)
  # dbviapath_deleter.process()
  # this second object will delete one by one (hopeful the above delete will have saved a lot of "performance")
  dbentry_eraser = DBEntryWithoutCorrespondingOsEntryDeleter(mountpath)
  dbentry_eraser.process()
  finish_time = datetime.datetime.now()
  print('finish_time', finish_time)
  elapsed_time = finish_time - start_time
  print('elapsed_time', elapsed_time, finish_time)


if __name__ == '__main__':
  process()
