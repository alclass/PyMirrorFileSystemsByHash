#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
batchWalkHashingFilesToRootFolderSqliteDB.py
Explanation:
  This script walks all files and folders up the directory tree.
  As it encounters files it sha1-hashes them and store sha1hex on a database.
  As for now, this database is a SQLite file staged on the device's root folder.

  In a nutshell, it keeps a database of hashes so that it can be used
    for external back-up programs that need to know
    whether or not a file exists and, if so, where it is located.

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


class Sha1FileSystemComplementer(object):

  def __init__(self, DEVICE_PREFIX_ABSPATH):
    '''

    :param DEVICE_PREFIX_ABSPATH:
    :return:
    '''
    if not os.path.isdir(DEVICE_PREFIX_ABSPATH):
      raise Exception, 'DEVICE_PREFIX_ABSPATH [%s] is not valid.' %DEVICE_PREFIX_ABSPATH
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

  def db_insert_dirnames(self, dirnames, dirpath):
    '''

    :param dirnames:
    :param dirpath:
    :return:
    '''
    global next_entry_id
    parent_dir_id = self.find_entry_id_for_dirpath(dirpath)
    dirnames_on_db = self.get_dirnames_on_db_with_same_parent_id(parent_dir_id)
    if len(dirnames) > 0:
      conn = get_sqlite_connection()
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

  def find_entry_id_for_dirpath(self, current_abs_path):
    '''
    :param current_abs_path:
    :return:
    '''
    if not os.path.isdir(current_abs_path):
      raise Exception, 'path does not exist : ' + current_abs_path
    minus_device_abspath = current_abs_path[ len( self.DEVICE_PREFIX_ABSPATH ) : ]
    if minus_device_abspath.startswith('/'):
      raise Exception, 'abs path not starting with / : '+ minus_device_abspath
    pp = minus_device_abspath.split('/')
    if pp == ['','']:
      return CONVENTIONED_ROOT_ENTRY_ID
    if len(pp) < 2:
      raise Exception, 'inconsistency in internal list manipulation for finding root abs dir'
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
        self.db_insert_a_dirname_on_parent_dir(dirname, parent_dir_id)
        entry_id = self.entry_id_for_dirs
    conn.close()
    return entry_id

  def db_insert_a_dirname_on_parent_dir(self, dirname, parent_dir_id):
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

  def process_filenames_on_folder(self, current_abs_dirpath, filenames):
    '''

    :param current_abs_dirpath:
    :param filenames:
    :return:
    '''
    print 'process_filenames_on_folder:', current_abs_dirpath
    parent_dir_id = self.find_entry_id_for_dirpath(current_abs_dirpath)
    if parent_dir_id == None:

    conn = sqlite3.connect()
    for filename in filenames:
      file_abspath = os.path.join(current_abs_dirpath, filename)
      sha1obj = hashlib.sha1()
      try:
        f = open(file_abspath, 'r')
        sha1obj.update(f.read())
        sha1hex = sha1obj.hexdigest()
      except Exception:
        raise SHA1_NOT_OBTAINED, 'SHA1_NOT_OBTAINED'
      # verify previous record existence and check equality
      sql = 'SELECT entryname, parent_dir_id FROM %(tablename)s WHERE ' \
            'sha1hex = "%(sha1hex)s"'      \
            %{ \
              'tablename': SQLITE_DB_TABLENAME_DEFAULT, \
              'sha1hex'  : sha1hex, \
            }
      curr = conn.execute(sql)
      record = curr.fetchone()
      if record:
        other_entryname = record[0] #['entryname']
        other_parent_id = record[1] #['parent_dir_id']
        if other_parent_id == parent_dir_id and other_entryname == filename:
          # nothing needs be done
          continue
        else: # well, the file exists either having a different name or it's somewhere else having or not the same name
          sql = '''UPDATE %(tablename)s
                entryname     = "%(entryname)s"
                parent_dir_id = "%(parent_dir_id)d"
                  WHERE
                sha1hex =  %(sha1hex)s ''' \
                %{
                  'tablename'     : SQLITE_DB_TABLENAME_DEFAULT, \
                  'entryname'     : filename,
                  'parent_dir_id' : parent_dir_id,
                  'sha1hex'       : sha1hex,
                  }

      else: # ie, above SELECT did not find an equal SHA1 record, it's time to insert it
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


def walk_on_up_tree_to_grab_sha1hex():
  '''

  :return:
  '''
  global device_root_abspath
  device_root_abspath = os.path.abspath('.')
  fs_complementer = Sha1FileSystemComplementer(device_root_abspath)
  if not os.path.isfile(SQLITE_ROOTDIR_FILENAME_DEFAULT):
    create_sqlite_db_file_on_root_folder()
  sqlite3.connect(SQLITE_ROOTDIR_FILENAME_DEFAULT)
  walk_counter = 0
  for dirpath, dirnames, filenames in os.walk(device_root_abspath):
    fs_complementer.db_insert_dirnames(dirnames, dirpath)
    complement_path = dirpath
    if complement_path.startswith('./'):
      complement_path = complement_path[2:]
    current_abs_dirpath = os.path.join(device_root_abspath, complement_path)
    walk_counter += 1
    print walk_counter, 'current path:', current_abs_dirpath
    print 'Files found @', complement_path
    for filename in filenames:
      print filename
    os.chdir(current_abs_dirpath)
    process_folder(current_abs_dirpath, filenames)
    os.chdir(device_root_abspath)
    print '-'*40
    print 'Voltei!', time.ctime(), dirpath, dirnames, current_abs_dirpath
    print '='*40

def get_biggest_entry_id():
  sql = 'SELECT max(entry_id) FROM %(tablename)s' \
        %{'tablename': SQLITE_DB_TABLENAME_DEFAULT}
  try:
    conn = get_sqlite_connection()
    curr = conn.execute(sql)
    result = curr.fetchone()
    print result[0], result
    max_entry_id = int(result[0])
    conn.close()
  except sqlite3.OperationalError:
    create_sqlite_db_file_on_root_folder()
    return get_biggest_entry_id()
  return max_entry_id

next_entry_id = 0
def main():
  global next_entry_id
  next_entry_id = get_biggest_entry_id() + 1
  walk_on_up_tree_to_grab_sha1hex()


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
    raise Exception, 'could not delete the root record'

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

if __name__ == '__main__':
  # main()
  test()
