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
import os
import sqlite3
import sys
class SHA1_NOT_OBTAINED(Exception):
  pass

class PYMIRROR_CONSTANTS:
  SQLITE_DB_TABLENAME_DEFAULT     = 'hashes_of_uptree_files'
  SQLITE_ROOTDIR_FILENAME_DEFAULT = 'hashed_files_thru_dir_tree.sqlite'
  CONVENTIONED_ROOT_ENTRY_ID      =  0
  CONVENTIONED_ROOT_DIR_NAME      =  'ROOT'
  FIRST_ENTRY_ID_FOR_FILES_WHEN_DB_EMPTY = 0
# the first entry_id for dirs must exist and be the ROOT conventioned "0".
#   This setting belongs to the creation scripts. If it's not there, an exception must be raised.
# the dir that is 0 is the ROOT dir; the first next_entry_id_for_dirs will +1 and it increases one by one
# these is the staging point; the first next_entry_id_for_dirs will -1 and it decreases one by one

def get_sqlite_connection(DEVICE_PREFIX_ABSPATH):
  '''

  :return:
  '''
  sqlite_dbfile_abspath = os.path.join(DEVICE_PREFIX_ABSPATH, PYMIRROR_CONSTANTS.SQLITE_ROOTDIR_FILENAME_DEFAULT)
  '''
  if not os.path.isfile(sqlite_dbfile_abspath):
    error_msg = 'sqlite_dbfile_abspath [%] does not exist.' %sqlite_dbfile_abspath
    raise Exception()
  '''
  conn = sqlite3.connect(sqlite_dbfile_abspath)
  return conn

class DBAccessorBase(object):

  def __init__(self, DEVICE_PREFIX_ABSPATH):
    '''

    :param DEVICE_PREFIX_ABSPATH:
    :return:
    '''
    if not os.path.isdir(DEVICE_PREFIX_ABSPATH):
      error_msg = 'DEVICE_PREFIX_ABSPATH [%s] is not valid.' %DEVICE_PREFIX_ABSPATH
      raise Exception(error_msg)
    self.DEVICE_PREFIX_ABSPATH = DEVICE_PREFIX_ABSPATH

  def get_db_connection_handle(self):
    return get_sqlite_connection(self.DEVICE_PREFIX_ABSPATH)

  def get_dbtable_name(self):
    return PYMIRROR_CONSTANTS.SQLITE_DB_TABLENAME_DEFAULT


class DBAccessor(DBAccessorBase):

  def __init__(self, DEVICE_PREFIX_ABSPATH):
    '''

    :param DEVICE_PREFIX_ABSPATH:
    :return:
    '''
    super(DBAccessor, self).__init__(DEVICE_PREFIX_ABSPATH)
    # in Python 3, it's just: super().__init__()
    self.verify_dbsqlitefile_existence()
    self.init_entry_ids_for_files_and_dirs()

  def verify_dbsqlitefile_existence(self):
    dbsqlitefile_abspath = os.path.join(self.DEVICE_PREFIX_ABSPATH, PYMIRROR_CONSTANTS.SQLITE_DB_TABLENAME_DEFAULT)
    if not os.path.isfile(dbsqlitefile_abspath):
      dbinit = DBInit(self.DEVICE_PREFIX_ABSPATH)
      dbinit.verify_and_create_fs_entries_sqlite_db_table()

  def init_entry_ids_for_files_and_dirs(self):
    '''

    :return:
    '''
    self.entry_id_for_dirs  = 0
    self.entry_id_for_files = 0
    conn = self.get_db_connection_handle()
    sql = 'SELECT max(entry_id) FROM "%(tablename)s";' \
      %{'tablename': self.get_dbtable_name()}
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
    max_entry_id = PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID
    sql = 'SELECT max(entry_id) FROM %(tablename)s' \
          %{'tablename': self.get_dbtable_name()}
    conn = self.get_db_connection_handle()
    try:
      curr = conn.execute(sql)
      result = curr.fetchone()
      if result:
        max_entry_id = int(result[0])
      conn.close()
    except sqlite3.OperationalError:
      pass
    return max_entry_id

  def get_negative_min_entry_id_for_files(self):
    '''
    The first file to be db-inserted will have entry_id -1
    If no files have entered db, the entry_id_for_files will be positioned at 0
     and decreased by one, resulting -1 when retrieved.

    :return:
    '''
    min_entry_id = PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID
    sql = 'SELECT min(entry_id) FROM %(tablename)s' \
          %{'tablename': self.get_dbtable_name()}
    conn = self.get_db_connection_handle()
    try:
      curr = conn.execute(sql)
      result = curr.fetchone()
      if result:
        min_entry_id = int(result[0])
      conn.close()
    except sqlite3.OperationalError:
      pass
    return min_entry_id

  def get_dirnames_on_db_with_same_parent_id(self, parent_dir_id):
    '''

    :param parent_dir_id:
    :return:
    '''
    sql = '''
      SELECT
        entryname FROM %(tablename)s
      WHERE
        parent_dir_id = "%(parent_dir_id)d" AND
        entry_id > -1 '''  \
      %{ \
        'tablename'     : self.get_dbtable_name(), \
        'parent_dir_id' : parent_dir_id,           \
      } # files have negative entry_id's, so condition (entry_id > -1) restricts SELECT to fetch only folders
    conn = self.get_db_connection_handle()
    curr = conn.execute(sql)
    dirnames = []
    for record in curr.fetchall():
      dirname = record[0] # ['entryname']
      dirnames.append(dirname)
    conn.close()
    return dirnames

  def increment_and_get_entry_id_for_dirs(self):
    '''

    :return:
    '''
    self.entry_id_for_dirs += 1  # dirs increment their entry_id's
    return self.entry_id_for_dirs

  def increment_and_get_entry_id_for_files(self):
    '''

    :return:
    '''
    self.entry_id_for_files -= 1  # files decrement their entry_id's
    return self.entry_id_for_files

  def db_insert_dirnames_bulk_with_parent_dir_id(self, dirnames, parent_dir_id):
    '''

    :param dirnames:
    :param parent_dir_id:
    :return:
    '''
    if dirnames == None or len(dirnames) == 0:
      return

    entry_id_for_dirs_position_to_undo_to_if_needed = self.entry_id_for_dirs
    conn = self.get_db_connection_handle()
    dirnames_on_db = self.get_dirnames_on_db_with_same_parent_id(parent_dir_id)
    tuples_list_for_executemany = []
    for dirname in dirnames:
      if dirname in dirnames_on_db:
        continue
      tuple_record = ("%(entry_id)d", "%(entryname)s", "%(parent_dir_id)d") \
        %{
          'entry_id'      : self.increment_and_get_entry_id_for_dirs(),
          'entryname'     : dirname,
          'parent_dir_id' : parent_dir_id,
        }
      tuples_list_for_executemany.append(tuple_record)

    sql = '''
      INSERT INTO %(tablename)s
        (entry_id, entryname, parent_dir_id)
      VALUES
        (?, ?, ?) ''' \
      %{
        'tablename' : self.get_dbtable_name(), \
      }
    try:
      retVal = conn.executemany(sql, tuples_list_for_executemany)
      '''
      if retVal <> 0:
        print 'retVal <> 0 ', retVal, 'on', sql
      else:
        print 'OK\n', sql, '\nOK
      '''
      conn.commit()
    except sqlite3.Error:
      self.entry_id_for_dirs = entry_id_for_dirs_position_to_undo_to_if_needed
    conn.close()

  def db_insert_dirnames_bulk_with_dirpath(self, dirnames, dirpath):
    '''

    :param dirnames:
    :param dirpath:
    :return:
    '''
    parent_dir_id = self.find_entry_id_for_dirpath(dirpath)
    for dirname in dirnames:
      self.db_insert_dirname_with_parent_dir_id(dirname, parent_dir_id)

  def db_insert_all_subfolders_within_root_minus_path(self, root_minus_path):
    '''

    :param root_minus_path:
    :return:
    '''
    pp = root_minus_path.split('/')
    parent_dir_id = PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID
    for foldername in pp:
      if foldername == '':
        continue
      entry_id = self.db_insert_dirname_with_parent_dir_id(foldername, parent_dir_id)
      if entry_id == None:
        error_msg = 'Could not db_insert_dirname_with_parent_dir_id in root_minus_path = [%s]' %root_minus_path
        raise Exception(error_msg)
      parent_dir_id = entry_id

  def db_insert_dirname_with_parent_dir_id(self, dirname, parent_dir_id):
    '''

    :param dirname:
    :param parent_dir_id:
    :return:
    '''
    if parent_dir_id == None:
      error_msg = 'parent_dir_id is None in db_insert_dirname_with_parent_dir_id()'
      raise Exception(error_msg)
    ok_dir_id_to_return = None
    entry_id_for_dirs_position_to_undo_to_if_needed = self.entry_id_for_dirs
    dir_id = self.increment_and_get_entry_id_for_dirs()
    data_dict = { \
        'tablename'     : self.get_dbtable_name(), \
        'entry_id'      : dir_id,
        'entryname'     : dirname,
        'parent_dir_id' : parent_dir_id,
        }
    sql = '''INSERT INTO %(tablename)s
        (entry_id, entryname, parent_dir_id)
      VALUES
        ("%(entry_id)d", "%(entryname)s", "%(parent_dir_id)d") ''' %data_dict
    print 'db_insert_dirname_with_parent_dir_id %s' %data_dict
    conn = self.get_db_connection_handle()
    try:
      retVal = conn.execute(sql)
      conn.commit()
      ok_dir_id_to_return = dir_id
    except sqlite3.Error:
      pass
    conn.close()
    if ok_dir_id_to_return == None:
      self.entry_id_for_dirs = entry_id_for_dirs_position_to_undo_to_if_needed
    return ok_dir_id_to_return

  def db_insert_filename_and_its_sha1hex_with_parent_dir_id(self, filename, parent_dir_id, sha1hex):
    '''

    :param filename:
    :param parent_dir_id:
    :return:
    '''
    entry_id_for_files_position_to_undo_to_if_needed = self.entry_id_for_files
    sql = '''
      INSERT INTO %(tablename)s
        (entry_id, entryname, parent_dir_id, sha1hex)
      VALUES
        ("%(entry_id)d", "%(entryname)s", "%(parent_dir_id)d", "%(sha1hex)s") ''' \
      %{
        'tablename'     : self.get_dbtable_name(), \
        'entry_id'      : self.increment_and_get_entry_id_for_files(),
        'entryname'     : filename,
        'parent_dir_id' : parent_dir_id,
        'sha1hex'       : sha1hex,
        }
    conn = self.get_db_connection_handle()
    try:
      retVal = conn.execute(sql)
      conn.commit()
    except sqlite3.Error:
      self.entry_id_for_files = entry_id_for_files_position_to_undo_to_if_needed
    '''
    if retVal <> 0:
      print 'retVal NOT ZERO', retVal, 'for', sql
    '''
    conn.close()

  def db_insert_filename_and_its_sha1hex_with_its_folder_abspath(self, filename, its_folder_abspath, sha1hex):
    '''

    :param filename:
    :param folder_abspath:
    :param sha1hex:
    :return:
    '''
    #root_minus_path = self.prepare_root_minus_path(its_folder_abspath)
    parent_dir_id = self.find_entry_id_for_dirpath(its_folder_abspath)
    self.db_insert_filename_and_its_sha1hex_with_parent_dir_id(filename, parent_dir_id, sha1hex)

  def db_insert_filename_and_its_sha1hex_with_file_abspath(self, file_abspath, sha1hex):
    '''

    :param file_abspath:
    :param sha1hex:
    :return:
    '''
    its_folder_abspath, filename = os.path.split(file_abspath)
    self.db_insert_filename_and_its_sha1hex_with_its_folder_abspath(filename, its_folder_abspath, sha1hex)

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
    if self.DEVICE_PREFIX_ABSPATH != current_abspath[ : len( self.DEVICE_PREFIX_ABSPATH ) ]:
      error_msg = "Abspath [%s] does not start with the device prefix path [%s]" %(current_abspath, self.DEVICE_PREFIX_ABSPATH)
      raise Exception(error_msg)

  def extract_current_abspath_minus_device_prefix(self, current_abspath):
    '''

    :param current_abspath:
    :return:
    '''
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

  def prepare_root_minus_path(self, target_abspath):
    '''

    :param target_abspath:
    :return:
    '''
    root_minus_path = self.extract_current_abspath_minus_device_prefix(target_abspath)
    if not root_minus_path.startswith('/'):
      root_minus_path = '/' + root_minus_path
    return root_minus_path

  def find_entry_id_for_root_minus_path(self, root_minus_path):
    pp = root_minus_path.split('/')
    self.are_split_pieces_good_in_relation_to_minus_path(pp)
    if pp == ['','']:
      return PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID
    return self.loop_on_to_find_entry_id_for_dirpath(pp, root_minus_path)

  def find_entry_id_for_dirpath(self, target_abspath):
    '''
    :param current_abs_path:
    :return:
    '''
    root_minus_path = self.prepare_root_minus_path(target_abspath)
    return self.find_entry_id_for_root_minus_path(root_minus_path)

  def loop_on_to_find_entry_id_for_dirpath(self, pp, root_minus_path, second_pass=False):
    '''
    PRIVATE METHOD! Only find_entry_id_for_dirpath() can call this.
    :param pp:
    :return:
    '''
    conn = self.get_db_connection_handle()
    parent_dir_id = PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID  # it starts its traversal at 'root'
    pp = pp[1:] # shift left 1 position
    run_insert_dirs = False
    entry_id = PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID
    for dirname in pp[1:]:
      if dirname == '':
        continue
      datadict =         { \
        'tablename'     : self.get_dbtable_name(), \
        'dirname'       : dirname,
        'parent_dir_id' : parent_dir_id,
      }
      sql = '''
      SELECT entry_id FROM %(tablename)s
        WHERE
          entryname     = "%(dirname)s" AND
          parent_dir_id = "%(parent_dir_id)s" ''' \
        %{ \
          'tablename'     : self.get_dbtable_name(), \
          'dirname'       : dirname,
          'parent_dir_id' : parent_dir_id,
        } %datadict
      curr = conn.execute(sql)
      record = curr.fetchone()
      print datadict
      if record:
        entry_id = record[0] #['entry_id']
        parent_dir_id = entry_id # in case it loops on from here
      else: # must record it!
        run_insert_dirs = True
    conn.close()
    if run_insert_dirs:
      if second_pass:
        error_msg = 'Could not find and/or record the abspath to file or folder. root_minus_path = [%s]' %root_minus_path
        raise Exception(error_msg)
      self.db_insert_all_subfolders_within_root_minus_path(root_minus_path)
      return self.loop_on_to_find_entry_id_for_dirpath(pp, root_minus_path, True)
    return entry_id

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
    sql = '''
      UPDATE %(tablename)s
        entryname     = "%(target_entryname)s"
        parent_dir_id = "%(target_parent_dir_id)d"
      WHERE
        entry_id = "%(entry_id)d" '''
    interpolate_dict = { \
      'tablename'     : self.get_dbtable_name(), \
      'entryname'     : target_entryname, \
      'parent_dir_id' : target_parent_dir_id, \
      'entry_id'      : entry_id, \
    }
    if sha1hex != None:
      sql += ''' AND sha1hex = "%(sha1hex)s" '''
      interpolate_dict['sha1hex'] = sha1hex
    sql = sql %interpolate_dict
    conn = self.get_db_connection_handle()
    retVal = conn.execute(sql)
    '''
    if retVal <> 0:
      print 'retVal NOT ZERO', retVal, 'for', sql
    '''
    conn.commit()
    conn.close()

  def transform_dir_ids_to_fullpath(self, trailed_dir_ids):
    '''

    :return:
    '''
    fullpath = '/'
    for dir_id in trailed_dir_ids[1:]:
      fullpath += self.get_entryname_by_entry_id(dir_id) + '/'
    return fullpath

  def search_fullpath_for_dir_id(self, dir_id_to_search_for, trailed_dir_ids=[0]):
    '''

    :param parent_dir_id_to_search_for:
    :param trailed_dir_ids:
    :return:
    '''
    on_going_search_dir_id = trailed_dir_ids[-1]
    dir_ids = self.retrieve_dir_ids_of(on_going_search_dir_id)
    for dir_id in dir_ids:
      if dir_id_to_search_for == dir_id:
        trailed_dir_ids.append(dir_id)
        fullpath = self.transform_dir_ids_to_fullpath(trailed_dir_ids)
        return fullpath
      else:
        new_trailed_dir_ids = trailed_dir_ids[:]
        new_trailed_dir_ids.append(dir_id)
        return self.search_fullpath_for_dir_id(dir_id_to_search_for, new_trailed_dir_ids)
    return None

  def retrieve_dir_ids_of(self, parent_dir_id):
    '''

    :param entry_id:
    :return:
    '''
    sql = '''
      SELECT entry_id
        FROM
         %(tablename)s
        WHERE
          parent_dir_id = "%(parent_dir_id)s" AND
          entry_id > -1 ''' \
      %{ \
        'tablename'     : self.get_dbtable_name(), \
        'parent_dir_id' : parent_dir_id,
      }
    conn = self.get_db_connection_handle()
    curr = conn.execute(sql)
    records = curr.fetchall()
    dir_ids = []
    for record in records:
      dir_id = record[0]
      dir_ids.append(dir_id)
    return dir_ids

  def get_entryname_by_entry_id(self, entry_id):
    '''

    :param entry_id:
    :return:
    '''
    sql = '''
      SELECT entryname
        FROM
         %(tablename)s
        WHERE
          entry_id = "%(entry_id)s" ''' \
      %{ \
        'tablename' : self.get_dbtable_name(), \
        'entry_id'  : entry_id,
      }
    conn = self.get_db_connection_handle()
    curr = conn.execute(sql)
    record = curr.fetchone()
    if record:
      entryname = record[0]
      return entryname
    return None

  def list_up_tree_contents_as_text(self, up_tree_contents_text='', parent_dir_id=None, parent_entryname=None):
    '''

    :return:
    '''

    if parent_dir_id==None:
      parent_dir_id = PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID
    if parent_entryname==None:
      parent_entryname = PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_DIR_NAME

    #print 'parent_dir_id, contents_text ==>> ', parent_dir_id
    sql = '''
      SELECT entry_id, entryname, sha1hex
        FROM
         %(tablename)s
        WHERE
          parent_dir_id = "%(parent_dir_id)s" AND
          entry_id <> "%(conventioned_root_entry_id)s" ''' \
      %{ \
        'tablename'     : self.get_dbtable_name(), \
        'parent_dir_id' : parent_dir_id,
        'conventioned_root_entry_id' : PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID,
      }
    conn = self.get_db_connection_handle()
    curr = conn.execute(sql)
    records = curr.fetchall()
    tuple_list_dir_id_and_name = []
    dir_context_text = '\nThe contents of ' + parent_entryname
    if records ==None:
      return dir_context_text + ' is empty'
    for record in records:
      entry_id      = record[0]
      entryname     = record[1]
      sha1hex       = record[2]
      line = '%s | %s | %s | %s' %(entry_id, entryname, parent_dir_id, sha1hex)
      dir_context_text += '\n' + line
      if entry_id > -1:
        tuple_list_dir_id_and_name.append((entry_id, entryname))
    for tuple_dir_id_and_name in tuple_list_dir_id_and_name:
      entry_id, entryname = tuple_dir_id_and_name
      dir_context_text += self.list_up_tree_contents_as_text('', entry_id, entryname)
    up_tree_contents_text = up_tree_contents_text + dir_context_text
    return up_tree_contents_text

class DBInit(DBAccessorBase):


  def __init__(self, DEVICE_PREFIX_ABSPATH):
    '''

    :param DEVICE_PREFIX_ABSPATH:
    :return:
    '''
    super(DBInit, self).__init__(DEVICE_PREFIX_ABSPATH)
    # in Python 3, it's just: super().__init__()


  def create_fs_entries_sqlite_db_table(self):
    '''

    :return:
    '''
    conn = self.get_db_connection_handle()
    curr = conn.cursor()
    sql_script = '''
    CREATE TABLE "%(tablename)s" (
      entry_id      INT  PRIMARY KEY NOT NULL,
      entryname     TEXT,
      filesize      INT,
      parent_dir_id INT,
      sha1hex       CHAR(40),
      FOREIGN KEY (parent_dir_id) on "%(tablename)s".entry_id
    );
    INSERT INTO "%(tablename)s"
      (entry_id, entryname, parent_dir_id)
    VALUES
      ("%(entry_id)d", "%(entryname)s", "%(parent_dir_id)d");''' \
    %{
      'tablename'     : self.get_dbtable_name(),
      'entry_id'      : PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID,
      'entryname'     : PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_DIR_NAME,
      'parent_dir_id' : PYMIRROR_CONSTANTS.CONVENTIONED_ROOT_ENTRY_ID,
    }
    retval = curr.executescript(sql_script)
    conn.commit()
    conn.close()

  def verify_fs_entries_sqlite_db_table_exists(self):
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
      SELECT * FROM sqlite_master
        WHERE
          name = "%(tablename)s" AND type = "table";''' %{'tablename' : self.get_dbtable_name()}
    conn = self.get_db_connection_handle()
    curr = conn.execute(sql)
    record = curr.fetchone()
    conn.close()
    if record == None or len(record) == 0:
      return False
    return True

  def verify_and_create_fs_entries_sqlite_db_table(self):
    if not self.verify_fs_entries_sqlite_db_table_exists():
      self.create_fs_entries_sqlite_db_table()

def get_args_to_dict():
  args_dict = {}
  for arg in sys.argv:
    if arg.startswith('-p='):
      device_root_abspath = arg [ len( '-p=') : ]
      args_dict['device_root_abspath'] = device_root_abspath
  return args_dict

def test1():
  #create_sqlite_db_file_on_root_folder()
  args_dict = get_args_to_dict()
  try:
    device_root_abspath = args_dict['device_root_abspath']
    sometests = SomeTests1(device_root_abspath)
    sometests.insert_root_record_on_db_table()
    sometests.insert_a_sample_file_on_db_table()
    sometests.list_files_and_folders_contents()
  except IndexError:
    print ('Parameter -p for device root abspath is missing.')

def main():
  test1()

if __name__ == '__main__':
  main()
