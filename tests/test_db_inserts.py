#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import string
import random

from db import sqlite_accessor_mod as sqlite_mod
from util import util_mod as um
# sha1hex, filename, relative_parent_path, device_and_middle_path, filesize, modified_datetime


sha1hexes=[];filesizes=[];modified_datimes=[]
for i in xrange(4):
  sha1hexes.append(um.take_random_sha1hex())
  filesizes.append(random.randint(100000))
  modified_datimes.append(um.take_random_datetime())


entries=[]
FOLDER = 1
FILE   = 2
e=(FOLDER,'/abc')
entries.append(e)
e=(FOLDER,'/abc/ab')
entries.append(e)
e=(FOLDER,'/ab')
entries.append(e)
e=(FOLDER,'/abc/abc')
entries.append(e)
seq=0
e=(FILE,'/abc/abc/file1',sha1hexes[seq],filesizes[seq],modified_datimes[seq])
entries.append(e)
seq+=1
e=(FOLDER,'/abc/abc/file2',sha1hexes[seq],filesizes[seq],modified_datimes[seq])
entries.append(e)
e=(FOLDER,'/z')
entries.append(e)
e=(FOLDER,'/z/z')
entries.append(e)
seq+=1
e=(FILE,'/z/z/filez',sha1hexes[seq],filesizes[seq],modified_datimes[seq])
entries.append(e)


def insert_file(tuple_file_values):
  filepath = tuple_file_values[0]
  sha1hex  = tuple_file_values[1]
  filesize = tuple_file_values[2]
  modified_datime = tuple_file_values[3]
  parent_path, filename = os.path.split(filepath)
  home_dir_id = find_folder_path_id_on_db_or_enter_it(parent_path)
  data_tuple_record = (home_dir_id, sha1hex, filename, filesize, modified_datime)
  sql = '''
  INSERT INTO "%(tablename_for_file_entries)s" (home_dir_id, sha1hex, filename, filesize, modified_date)
                                       VALUES  (     ?     ,    ?   ,    ?    ,    ?    ,       ?      );
  '''
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute(sql, data_tuple_record)


def insert_folder(folder_path):
  parent_path, foldername = os.path.split(folder_path)
  home_dir_id = find_folder_path_id_on_db_or_enter_it(parent_path)
  # first, insert folder name, return its id
  
  data_tuple_record = (home_dir_id, sha1hex, filename, filesize, modified_datime)
  sql = '''
  INSERT INTO "%(tablename_for_file_entries)s" (home_dir_id, sha1hex, filename, filesize, modified_date)
                                       VALUES  (     ?     ,    ?   ,    ?    ,    ?    ,       ?      );
  '''
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute(sql, data_tuple_record)

for e in entries:
  entry_type = e[0]
  if entry_type == FOLDER:
    insert_folder(e[1])
  elif entry_type == FILE:
    insert_file(e[1:])




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


  def insert_multiple_files_and_folders_on_db_table(self):
    '''

    :return:
    '''
    db_accessor = sqlite_mod.DBAccessor(self.DEVICE_PREFIX_ABSPATH)
    dir1 = os.path.join(self.DEVICE_PREFIX_ABSPATH, 'testdir/')
    db_accessor.db_insert_filename_and_its_sha1hex_with_its_folder_abspath('test2.txt', dir1, take_random_sha1hex())
    db_accessor.db_insert_filename_and_its_sha1hex_with_its_folder_abspath('test3.txt', dir1, take_random_sha1hex())
    dir2 = os.path.join(self.DEVICE_PREFIX_ABSPATH, 'test2dir/')
    db_accessor.db_insert_filename_and_its_sha1hex_with_its_folder_abspath('test4.txt', dir2, take_random_sha1hex())

  def list_files_and_folders_contents(self):
    db_acessor = sqlite_mod.DBAccessor(self.DEVICE_PREFIX_ABSPATH)
    print db_acessor.get_up_tree_contents_as_text()

def test1():
  DEVICE_PREFIX_ABSPATH = os.path.abspath('.')
  test1 = SomeTests1(DEVICE_PREFIX_ABSPATH)
  test1.insert_root_record_on_db_table()
  test1.insert_a_sample_file_on_db_table()
  test1.insert_multiple_files_and_folders_on_db_table()
  test1.list_files_and_folders_contents()

def main():
  test1()

if __name__ == '__main__':
  main()
