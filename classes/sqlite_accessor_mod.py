#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
sqlite_accessor_mod.py

  This script contains class DBAccessor
    which is the class that does the actual reads and writes
    to the sqlite database that stores a meta representation and files and folders
    mapping to files their sha1hex hash word.

  Some refactoring will occur in the future
    to improve db-functionality and code readability, for example:
    the table creation script may be ported to a different script,
    to better organize code and functionalities.

  Written on 2015-01-13 Luiz Lewis
'''
import hashlib
import os
import sqlite3
import sys
import time
import string
class SHA1_NOT_OBTAINED(Exception):
  pass

SQLITE_DB_TABLENAME_DEFAULT     = 'hashes_of_uptree_files'
SQLITE_ROOTDIR_FILENAME_DEFAULT = 'hashed_files_thru_dir_tree.sqlite'
CONVENTIONED_ROOT_ENTRY_ID      =  0
CONVENTIONED_ROOT_DIR_NAME      =  'ROOT'

next_entry_id_for_files = 0 # the dir that is 0 is the ROOT dir; the first next_entry_id_for_dirs will +1 and it increases one by one
next_entry_id_for_dirs  = 0  # these is the staging point; the first next_entry_id_for_dirs will -1 and it decreases one by one
device_root_abspath = None

def get_sqlite_connection():
  '''

  :return:
  '''
  conn = sqlite3.connect(SQLITE_ROOTDIR_FILENAME_DEFAULT)
  return conn


class DBAccessor(object):

  def __init__(self, DEVICE_PREFIX_ABSPATH):
    '''

    :param DEVICE_PREFIX_ABSPATH:
    :return:
    '''
    if not os.path.isdir(DEVICE_PREFIX_ABSPATH):
      error_msg = 'DEVICE_PREFIX_ABSPATH [%s] is not valid.' %DEVICE_PREFIX_ABSPATH
      raise Exception(error_msg)
    self.DEVICE_PREFIX_ABSPATH = DEVICE_PREFIX_ABSPATH
    self.init_entry_ids_for_files_and_dirs()

  def init_entry_ids_for_files_and_dirs(self):
    '''

    :return:
    '''
    self.entry_id_for_dirs  = 0
    self.entry_id_for_files = 0
    conn = get_sqlite_connection()
    sql = 'SELECT max(entry_id) FROM %(tablename)s'
    curr = conn.execute(sql)
    record = curr.fetchone()
    if record:
      self.entry_id_for_dirs = record[0]
    return False
    sql = 'SELECT min(entry_id) FROM %(tablename)s'
    curr = conn.execute(sql)
    record = curr.fetchone()
    if record:
      self.entry_id_for_files = record[0]
    conn.close()

  def get_positive_max_entry_id_for_dirs(self):
    '''

    :return:
    '''
    max_entry_id = None
    sql = 'SELECT max(entry_id) FROM %(tablename)s' \
          %{'tablename': SQLITE_DB_TABLENAME_DEFAULT}
    try:
      conn = get_sqlite_connection()
      curr = conn.execute(sql)
      result = curr.fetchone()
      if result:
        max_entry_id = int(result[0])
      conn.close()
    except sqlite3.OperationalError:
      return None
    return max_entry_id

  def get_negative_min_entry_id_for_files(self):
    '''

    :return:
    '''
    min_entry_id = 0 # the first file to be db-inserted will have entry_id -1
    sql = 'SELECT min(entry_id) FROM %(tablename)s' \
          %{'tablename': SQLITE_DB_TABLENAME_DEFAULT}
    try:
      conn = get_sqlite_connection()
      curr = conn.execute(sql)
      result = curr.fetchone()
      if result:
        min_entry_id = int(result[0])
      conn.close()
    except sqlite3.OperationalError:
      return None
    return min_entry_id


  def get_dirnames_on_db_with_same_parent_id(self, parent_dir_id):
    '''

    :param parent_dir_id:
    :return:
    '''
    sql = 'SELECT entryname FROM %(tablename)s WHERE '  \
          'parent_dir_id = "%(parent_dir_id)d" AND '  \
          'entry_id > -1 '  \
          %{ \
            'tablename'                       : SQLITE_DB_TABLENAME_DEFAULT,   \
            'parent_dir_id'                   : parent_dir_id,                 \
          } # files have negative entry_id's, so condition (entry_id > -1) restricts SELECT to fetch only folders
    conn = get_sqlite_connection()
    curr = conn.execute(sql)
    dirnames = []
    for record in curr.fetchall():
      dirname = record[0] # ['entryname']
      dirnames.append(dirname)
    conn.close()
    return dirnames

  def next_entry_id_for_dirs(self):
    '''

    :return:
    '''
    self.entry_id_for_dirs += 1
    return self.entry_id_for_dirs

  def next_entry_id_for_files(self):
    '''

    :return:
    '''
    self.entry_id_for_files += 1
    return self.entry_id_for_files

  def insert_dirnames_with_parent_dir_id(self, dirnames, parent_dir_id):
    '''

    :param dirnames:
    :param parent_dir_id:
    :return:
    '''
    if dirnames == None or len(dirnames) == 0:
      return
    conn = get_sqlite_connection()
    dirnames_on_db = self.get_dirnames_on_db_with_same_parent_id(parent_dir_id)
    for dirname in dirnames:
      if dirname in dirnames_on_db:
        continue
      sql = 'INSERT INTO %(tablename)s ' \
            '(entry_id, entryname, parent_dir_id) VALUES ' \
            '(%(entry_id)d, %(entryname)s, %(parent_dir_id)d)' \
            %{
              'tablename'     : SQLITE_DB_TABLENAME_DEFAULT, \
              'entry_id'      : self.next_entry_id_for_dirs(),
              'entryname'     : dirname,
              'parent_dir_id' : parent_dir_id,
              }
      retVal = conn.execute(sql)
      '''
      if retVal <> 0:
        print 'retVal <> 0 ', retVal, 'on', sql
      else:
        print 'OK\n', sql, '\nOK
      '''
    conn.commit()
    conn.close()


  def db_insert_dirnames_with_dirpath(self, dirnames, dirpath):
    '''

    :param dirnames:
    :param dirpath:
    :return:
    '''
    global next_entry_id
    parent_dir_id = self.find_entry_id_for_dirpath(dirpath)
    return self.insert_dirnames_with_parent_dir_id(dirnames, parent_dir_id)

  def is_path_good_in_relation_to_device_prefix_abspath(self, current_abspath):
    '''
    The logic is this: the device prefix path should start the current_abs_path
    If it's not so, current_abs_path is not good and an exception should be raised.

    :return:
    '''
    # 1st check: is it an OS path?
    if not os.path.isdir(current_abspath):
      error_msg = "Path [%s] does not exist or it's a file." %current_abspath
      raise Exception(error_msg)
    # 2nd check: does the device prefix path start it?
    if self.DEVICE_PREFIX_ABSPATH != current_abspath[ : len( self.DEVICE_PREFIX_ABSPATH ) ]
      error_msg = "Abspath [%s] does not start with the device prefix path [%s]" %(current_abspath, self.DEVICE_PREFIX_ABSPATH)
      raise Exception(error_msg)

  def extract_current_abspath_minus_device_prefix(self, current_abspath):
    current_abspath_minus_device_prefix = current_abspath[ len( self.DEVICE_PREFIX_ABSPATH ) : ]
    if not current_abspath_minus_device_prefix.startswith('/'):
      current_abspath_minus_device_prefix = '/' + current_abspath_minus_device_prefix
    return current_abspath_minus_device_prefix

  def are_split_pieces_good_in_relation_to_minus_path(self, pp):
    '''

    :param pp:
    :return:
    '''
    if len(pp) < 2:
      error_msg = '''Inconsistency in internal program list manipulation
      for finding root abs dir.  The process of finding the id of a directory
      is a recursive one, starting on ROOT, the / symbolized first folder.
      Somehow, this ROOT was lost. It may be a logical error.
      To help find further:
        1) '/'.split('/') is ['',''] AND
        2) '/a'.split('/') is ['','a']
      The condition that triggered this error is that list is smaller than 2 items.'''
      raise Exception(error_msg)


  def find_entry_id_for_dirpath(self, current_abspath):
    '''
    :param current_abs_path:
    :return:
    '''
    self.is_path_good_in_relation_to_device_prefix_abspath(current_abspath)
    current_abspath_minus_device_prefix = self.extract_current_abspath_minus_device_prefix(current_abspath)
    pp = current_abspath_minus_device_prefix.split('/')
    self.are_split_pieces_good_in_relation_to_minus_path(pp)
    if pp == ['','']:
      return CONVENTIONED_ROOT_ENTRY_ID
    return self.loop_on_to_find_entry_id_for_dirpath(pp)

  def loop_on_to_find_entry_id_for_dirpath(self, pp):
    '''

    :param pp:
    :return:
    '''
    conn = get_sqlite_connection()
    parent_dir_id = CONVENTIONED_ROOT_ENTRY_ID  # it starts its traversal at 'root'
    pp = pp[1:] # shift left 1 position
    for dirname in pp[1:]:
      sql = 'SELECT entry_id FROM %(tablename)s WHERE ' \
            'entryname     = "%(dirname)s" AND'        \
            'parent_dir_id = "%(parent_dir_id)s"' \
            %{ \
              'tablename'     : SQLITE_DB_TABLENAME_DEFAULT, \
              'dirname'       : dirname,
              'parent_dir_id' : parent_dir_id,
            }
      curr = conn.execute(sql)
      record = curr.fetchone()
      if record:
        entry_id = record[0] #['entry_id']
        parent_dir_id = entry_id # in case it loops on from here
      else: # must record it!
        self.db_insert_a_dirname_with_parent_dir(dirname, parent_dir_id)
        entry_id = self.entry_id_for_dirs
    conn.close()
    return entry_id

  def db_insert_a_dirname_with_parent_dir(self, dirname, parent_dir_id):
    '''

    :param dirname:
    :param parent_dir_id:
    :return:
    '''
    conn = get_sqlite_connection()
    sql = 'INSERT INTO %(tablename)s ' \
          '(entry_id, entryname, parent_dir_id) VALUES ' \
          '(%(entry_id)d, %(entryname)s, %(parent_dir_id)d)' \
          %{
            'tablename'     : SQLITE_DB_TABLENAME_DEFAULT, \
            'entry_id'      : self.next_entry_id_for_dirs(),
            'entryname'     : dirname,
            'parent_dir_id' : parent_dir_id,
            }
    retVal = conn.execute(sql)
    conn.commit()
    conn.close()

  def delete_file_entry(self, next_entry_id_to_delete):
    '''

    :param next_entry_id_to_delete:
    :return:
    '''
    pass

  def delete_a_dir_entry_removing_everything_belonging_to_it(self, parent_dir_id_to_delete):
    '''

    :return:
    '''
    sql = '''

    SELECT entry_id FROM table
     WHERE
     parent_dir_id = "%(parent_dir_id_to_delete)d"
    '''

    entry_ids_to_delete = []
    if len(entry_ids_to_delete) == 0:
      # delete itself and return
      sql = '''DELETE FROM table
      where entry_id =
      '''
    for next_entry_id_to_delete in entry_ids_to_delete:
      if next_entry_id_to_delete < 0:
        self.delete_file_entry(next_entry_id_to_delete)
      return self.delete_a_dir_entry_removing_everything_belonging_to_it()


  def rename_or_move_entry_to_a_different_folder(self, entry_id, target_entryname, target_parent_dir_id, sha1hex=None):
    '''

    :param target_entryname:
    :param target_parent_dir_id:
    :param sha1hex:
    :return:
    '''
    sql = '''UPDATE %(tablename)s
          entryname     = "%(target_entryname)s"
          parent_dir_id = "%(target_parent_dir_id)d"
            WHERE
          entry_id = "%(entry_id)d" '''
    interpolate_dict = { \
            'tablename'     : SQLITE_DB_TABLENAME_DEFAULT, \
            'entryname'     : target_entryname, \
            'parent_dir_id' : target_parent_dir_id, \
            'entry_id'      : entry_id, \
    }
    if sha1hex != None:
      sql += ''' AND sha1hex = "%(sha1hex)s" '''
      interpolate_dict['sha1hex'] = sha1hex
    sql = sql %interpolate_dict
    conn = sqlite3.connect()
    retVal = conn.execute(sql)
    '''
    if retVal <> 0:
      print 'retVal NOT ZERO', retVal, 'for', sql
    '''
    conn.commit()
    conn.close()

  def db_insert_filename_and_its_sha1hex_with_parent_dir_id(self, filename, parent_dir_id, sha1hex):
    '''

    :param filename:
    :param parent_dir_id:
    :return:
    '''
    conn = sqlite3.connect()
    sql = 'INSERT INTO %(tablename)s ' \
          '(entry_id, entryname, parent_dir_id, sha1hex) VALUES ' \
          '(%(entry_id)d, %(entryname)s, %(parent_dir_id)d, %(sha1hex)s)' \
          %{
            'tablename'     : SQLITE_DB_TABLENAME_DEFAULT, \
            'entry_id'      : self.next_entry_id_for_files(),
            'entryname'     : filename,
            'parent_dir_id' : parent_dir_id,
            'sha1hex'       : sha1hex,
            }
    retVal = conn.execute(sql)
    '''
    if retVal <> 0:
      print 'retVal NOT ZERO', retVal, 'for', sql
    '''
    conn.commit()
    conn.close()

def is_there_the_root_record():
  sql = '''SELECT entry_id, parent_dir_id, entryname FROM %(tablename)s WHERE
           entry_id        = "%(conventioned_root_entry_id)d" AND
           parent_dir_id = "%(conventioned_root_entry_id)d" AND
           entryname     = "%(conventioned_root_name)s" '''
  conn = get_sqlite_connection()
  curr = conn.execute(sql)
  record = curr.fetchone()
  if record <> None:
    return True
  return False

def delete_root_record_on_db_table():
  sql = '''DELETE FROM %(tablename)s WHERE
           entry_id      = "%(conventioned_root_entry_id)d" AND
           parent_dir_id = "%(conventioned_root_entry_id)d" AND
           entryname     = "%(conventioned_root_name)s" '''
  conn = get_sqlite_connection()
  conn.execute(sql)
  if is_there_the_root_record():
    error_msg = 'Could not delete the root record'
    raise Exception(error_msg)

def insert_root_record_on_db_table():
  '''
  This root record is:
    'conventioned_root_entry_id': CONVENTIONED_ROOT_entry_id, = 0
    'conventioned_root_entry_id': CONVENTIONED_ROOT_entry_id, = 0
    'conventioned_root_name'  : CONVENTIONED_ROOT_DIR_NAME, = 'ROOT'

  :return:
  '''
  # first, check if it's already there
  conn = get_sqlite_connection()
  sql = '''INSERT INTO %(tablename)s (parent_dir_id, entry_id, entryname)
    VALUES ("%(conventioned_root_entry_id)d", "%(conventioned_root_entry_id)d", "%(conventioned_root_name)s");
  ''' %{
    'tablename'               : SQLITE_DB_TABLENAME_DEFAULT,
    'conventioned_root_entry_id': CONVENTIONED_ROOT_entry_id,
    'conventioned_root_entry_id': CONVENTIONED_ROOT_entry_id,
    'conventioned_root_name'  : CONVENTIONED_ROOT_DIR_NAME,
  }
  conn.execute(sql)
  sha1hex = string.digits*4
  sql = '''INSERT INTO %(tablename)s (entry_id, entryname, parent_dir_id, sha1hex)
    VALUES ("%(entry_id)d", "%(entryname)s", "%(parent_dir_id)d", "%(sha1hex)s");
  ''' %{
    'tablename'     : SQLITE_DB_TABLENAME_DEFAULT,
    'entry_id'      : -1,
    'parent_dir_id' : 0,
    'entryname'    : 'test_file.txt',
    'sha1hex'       : sha1hex,
  }
  conn.execute(sql)
  conn.commit()
  conn.close()
  '''
  if retVal <> 0:
    print 'retVal <> 0 ', retVal, 'on', sql
  else:
    print 'OK\n', sql, '\nOK'
  '''

def create_sqlite_db_file_on_root_folder():
  '''
  Convention:
  Root Folder has entry_id = 0 and name is ROOT
    if a ROOT named dir exists on ROOT, it will have the same name,
    but not the same entry_id
  Another special case is that ROOT's parent_id is also 0 (code will have to "see" it)
  A file has entry_id equals to -1
  :return:
  '''
  sql = '''
  CREATE TABLE %(tablename)s (
    entry_id      INT  PRIMARY KEY   NOT NULL,
    entryname     TEXT NOT NULL,
    parent_dir_id INT  NOT NULL,
    sha1hex CHAR(40) );''' \
      %{'tablename' : SQLITE_DB_TABLENAME_DEFAULT}
  conn = get_sqlite_connection()
  retVal = conn.execute(sql)
  if retVal <> 0:
    print 'retVal <> 0 ', retVal, 'on', sql
  else:
    print 'OK\n', sql, '\nOK'
  conn.close()

def test():
  #create_sqlite_db_file_on_root_folder()
  insert_root_record_on_db_table()

next_entry_id = 0
def main():
  global next_entry_id
  next_entry_id = get_biggest_entry_id() + 1
  # walk_on_up_tree_to_grab_sha1hex()


if __name__ == '__main__':
  # main()
  test()
