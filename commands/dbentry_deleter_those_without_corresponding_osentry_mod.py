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
import default_settings as defaults
SQL_SELECT_LIMIT_DEFAULT = 50


class DBEntryWithoutCorrespondingOsEntryDeleter:
  """
  This class looks for db-entries without their respective os-entries and deleted the db-ones.
  This functionality should be used after
  """

  def __init__(self, mountpath):
    """
    """
    self.n_deleted_dbentries = 0
    self.n_processed = 0
    self.n_updates = 0
    self.mountpath = mountpath
    self.dbtree = dbt.DBDirTree(self.mountpath)
    self.n_files = self.dbtree.count_rows_as_int()

  def fetch_dbentries_n_check_their_osentries(self):
    offset = 0
    k_limit = 50
    while 1:  # see below the condition for interrupting this while-infinite-loop
      limit_clause = ' LIMIT %(limit)d OFFSET %(offset)d ;' \
        % {'limit': k_limit, 'offset': offset}
      sql = 'SELECT * FROM %(tablename)s' + limit_clause
      rows = self.dbtree.do_select_with_sql_without_tuplevalues(sql)
      n_deleted_in_loop = 0
      for row in rows:
        self.n_processed += 1
        _id = row[0]
        idx = self.dbtree.fieldnames.index('name')
        name = row[idx]
        idx = self.dbtree.fieldnames.index('parentpath')
        parentpath = row[idx]
        print(_id, name, parentpath)
        middlepath = os.path.join(parentpath, name)
        middlepath = middlepath.lstrip('/')
        filepath = os.path.join(self.mountpath, middlepath)
        if not os.path.isfile(filepath):
          del_result = self.dbtree.delete_row_by_id(_id)
          self.n_deleted_dbentries += 1
          n_deleted_in_loop += 1
          print(' *-=-'*4, 'DELETE DBENTRY', ' *-=-'*4)
          print(del_result, self.n_deleted_dbentries, 'deleted dbentry for', _id, name)
      offset = offset + k_limit - n_deleted_in_loop
      if len(rows) < k_limit:  # this is the condition for interrupting the while-infinite-loop "WHILE 1" above
        print('Interrupting while-1 loop: len(rows)', len(rows), ' < k_limit', k_limit, 'offset', offset)
        break

  def report(self):
    print('='*40)
    print('DBEntryWithoutCorrespondingOsEntryDeleter Report')
    print('='*40)
    print('Mountpath is', self.mountpath)
    print('n_files', self.n_files)
    print('n_processed', self.n_processed)
    print('n_deleted_dbentries', self.n_deleted_dbentries)
    print('End of Processing')

  def process(self):
    self.fetch_dbentries_n_check_their_osentries()
    self.report()


class PresentOnFolderVerifier:
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
    self.n_files = 0
    self.n_updates = 0
    self.not_present_ids = []
    self.mountpath = mountpath
    self.dbtree = dbt.DBDirTree(self.mountpath)

  def verify_files_presence(self, rowlist):
    """
    """
    for row in rowlist:
      self.n_files += 1
      idx = self.dbtree.fieldnames.index('name')
      name = row[idx]
      idx = self.dbtree.fieldnames.index('parentpath')
      parentpath = row[idx]
      if parentpath is None:
        parentpath = ''
      if parentpath.startswith('/'):
        parentpath = parentpath.lstrip('/')
      folderpath = os.path.join(self.dbtree.mount_abspath, parentpath)
      filepath = os.path.join(folderpath, name)
      print(self.n_files, 'filepath', filepath)
      if not os.path.isfile(filepath):
        _id = row[0]
        self.not_present_ids.append(_id)

  def process(self):
    print('Process verify files presence on folder')
    print('='*40)
    for i, rowlist in enumerate(self.dbtree.do_select_all_w_limit_n_offset()):
      self.verify_files_presence(rowlist)
    print('Finished with', self.n_files, 'files processed.')


def process():
  start_time = datetime.datetime.now()
  mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  print('start_time', start_time)
  # verifier = PresentOnFolderVerifier(mountpath)
  # verifier.process()
  dbentry_eraser = DBEntryWithoutCorrespondingOsEntryDeleter(mountpath)
  dbentry_eraser.process()
  finish_time = datetime.datetime.now()
  print('finish_time', finish_time)
  elapsed_time = finish_time - start_time
  print('elapsed_time', elapsed_time)


if __name__ == '__main__':
  process()
