#!/usr/bin/env python
import os
import string

from classes import sqlite_accessor_mod as sqlite_mod
PYMIRROR_CONSTANTS = sqlite_mod.PYMIRROR_CONSTANTS

class SomeTests1(sqlite_mod.DBAccessorBase):

  def __init__(self, DEVICE_PREFIX_ABSPATH):
    super(SomeTests1, self).__init__(DEVICE_PREFIX_ABSPATH)
    # in Python 3, it's just: super().__init__()
    dbinit = sqlite_mod.DBInit(self.DEVICE_PREFIX_ABSPATH)
    dbinit.verify_and_create_fs_entries_sqlite_db_table()

  def insert_root_record_on_db_table(self):
    '''
    This root record is:
      'conventioned_root_entry_id': CONVENTIONED_ROOT_entry_id, = 0
      'conventioned_root_entry_id': CONVENTIONED_ROOT_entry_id, = 0
      'conventioned_root_name'  : CONVENTIONED_ROOT_DIR_NAME, = 'ROOT'

    :return:
    '''
    # first, check if it's already there
    return

    # refactor here!

    conn = self.get_db_connection_handle()
    sql = '''
      INSERT INTO %(tablename)s
        (parent_dir_id, entry_id, entryname)
      VALUES
        ("%(conventioned_root_entry_id)d", "%(conventioned_root_entry_id)d", "%(conventioned_root_name)s");''' \
      %{
        'tablename'                 : PYMIRROR_CONSTANTS.SQLITE_DB_TABLENAME_DEFAULT,
        'conventioned_root_entry_id': PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID,
        'conventioned_root_entry_id': PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID,
        'conventioned_root_name'    : PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_DIR_NAME,
      }
    conn.execute(sql)

  def is_there_the_root_record(self):
    '''

    :return:
    '''
    sql = ''' \
    SELECT entry_id, parent_dir_id, entryname FROM %(tablename)s
      WHERE
        entry_id      = "%(conventioned_root_entry_id)d" AND
        parent_dir_id = "%(conventioned_root_entry_id)d" AND
        entryname     = "%(conventioned_root_name)s" '''
    conn = self.get_db_connection_handle()
    curr = conn.execute(sql)
    record = curr.fetchone()
    if record != None:
      return True
    return False

  def delete_root_record_on_db_table(self):
    '''

    :return:
    '''
    sql = '''
      DELETE FROM %(tablename)s
        WHERE
          entry_id      = "%(conventioned_root_entry_id)d" AND
          parent_dir_id = "%(conventioned_root_entry_id)d" AND
          entryname     = "%(conventioned_root_name)s" '''
    conn = self.get_db_connection_handle()
    conn.execute(sql)
    conn.close()
    if self.is_there_the_root_record():
      error_msg = 'Could not delete the root record'
      raise Exception(error_msg)

  def insert_a_sample_file_on_db_table(self):
    '''

    :return:
    '''
    sql = '''
    SELECT * FROM %(tablename)s
      WHERE
        entry_id = "-1";''' %{ 'tablename' : self.get_dbtable_name() }
    conn = self.get_db_connection_handle()
    curr = conn.execute(sql)
    if curr != None: # curr.rowcount > 0:
      conn.close()
      return
    sha1hex = string.digits*4
    sql = '''
    INSERT INTO %(tablename)s
      (entry_id, entryname, parent_dir_id, sha1hex)
    VALUES
      ("%(entry_id)d", "%(entryname)s", "%(parent_dir_id)d", "%(sha1hex)s"); ''' \
    %{ \
      'tablename'     : self.get_dbtable_name(), \
      'entry_id'      : -1, \
      'parent_dir_id' : 0, \
      'entryname'    : 'test_file.txt', \
      'sha1hex'       : sha1hex, \
    }
    print sql
    retval = conn.execute(sql)
    conn.commit()
    '''
    if retVal <> 0:
      print 'retVal <> 0 ', retVal, 'on', sql
    else:
      print 'OK\n', sql, '\nOK'
    '''
    conn.close()

  def list_files_and_folders_contents(self):
    db_acessor = sqlite_mod.DBAccessor(self.DEVICE_PREFIX_ABSPATH)
    print db_acessor.get_up_tree_contents_as_text()

def test1():
  DEVICE_PREFIX_ABSPATH = os.path.abspath('.')
  test1 = SomeTests1(DEVICE_PREFIX_ABSPATH)
  test1.insert_root_record_on_db_table()
  test1.insert_a_sample_file_on_db_table()
  test1.list_files_and_folders_contents()

def main():
  test1()

if __name__ == '__main__':
  main()
