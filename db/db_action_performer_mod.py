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

import db_settings as dbsetts
PYMIRROR_DB_PARAMS = dbsetts.PYMIRROR_DB_PARAMS
import db_connection_factory_mod as dbfact
import sqlite_create_db_mod as sqlcreate

class FolderPathAjuster(object):
  '''
  This class has functionality to help ajust the abspath to the relpath, ie, the os-absolute path must
    be stripped of the device_and_middle_path, remaining the relative path (ie, the relpath)
  '''

  def fetch_folderpath_id(self, folderpath):
    '''
    The idea here is following: having a slash-separated path (or other 'sep'),
      find the folder_id of the ending folder
    :param folderpath:
    :return:
    '''
    relative_folder = folderpath[ len(self.device_and_middle_abspath) : ]
    foldernames = relative_folder.split(os.sep)
    if len(foldernames) == 1:
      return PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID
    ids_starting_trace_path = [PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID]
    return self.fetch_folderpath_id_recursive(foldernames[1:], ids_starting_trace_path)


class DBProxyFetcher(object):

  def __init__(self, dbms_params_dict=None):
    self.dbfact_obj = dbfact.DBFactoryToConnection(dbms_params_dict)

  def fetch_children_folder_ids_by_node_id_from_db(self, node_id):
    '''
    This is an encapsulated function that is called from the recursive function
      that is used by a bootstrap class to get all tree fs paths at the application's init time
    :param _id:
    :return:
    '''
    sql = '''SELECT id FROM %(tablename_for_entries_linked_list)s
    WHERE
      %(fieldname_for_parent_or_home_dir_id)s=%(node_id)d
    ORDER BY id;
    ''' %{ \
      'tablename_for_entries_linked_list'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST, \
      'fieldname_for_parent_or_home_dir_id':PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
      'node_id' : node_id, \
    }
    # n_of_selects += 1 # global module var
    conn = self.dbfact_obj.get_db_connection()
    cursor = conn.cursor()
    result = cursor.execute(sql)
    traversal_ids = []
    for row in result.fetchall():
      traversal_ids.append(row[0])
    conn.close()
    return traversal_ids

  def get_folder_id_to_name_tuple_list_from_db(self):
    '''

    :param _id:
    :return:
    '''
    sql = '''SELECT id, entryname FROM %(tablename_for_file_n_folder_entries)s;
      WHERE
        entrytype = %(FOLDER_ENTRY_TYPE_ID)s
      ORDER BY id
    ''' %{ \
      'tablename_for_file_n_folder_entries':PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES, \
      'FOLDER_ENTRY_TYPE_ID'               :PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER, \
    }
    conn = self.dbfact_obj.get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    folder_id_to_name_tuple_list = cursor.fetchall()[:]
    conn.close()
    # folder_id_to_name_tuple_list.sort(key=lambda e:e[0])
    return folder_id_to_name_tuple_list

  def get_folder_id_to_name_dict_from_db(self):
    '''

    :return:
    '''
    folder_id_to_name_tuple_list = self.get_folder_id_to_name_tuple_list_from_db()
    if folder_id_to_name_tuple_list == None or len(folder_id_to_name_tuple_list) == 0:
      return {}
    folder_id_to_name_dict = {}
    for folder_id_to_name_tuple in folder_id_to_name_tuple_list:
      folder_id_to_name_dict[folder_id_to_name_tuple[0]] = folder_id_to_name_tuple[1]
    return folder_id_to_name_dict

  def update_folder_path_list_str_for(self, folder_id, folder_path_id_list_str):
    sql = '''
    UPDATE %(auxtab_path_id_list_per_folder)s
      SET %(fieldname_for_entries_path_id_list_str)s = ?
    WHERE
      id = ? ;
    ''' %{ \
      'auxtab_path_id_list_per_folder'        : PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS, \
      'fieldname_for_entries_path_id_list_str': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
    }
    conn = dbfact.get_db_connection()
    cursor = conn.cursor()
    auxtab_path_id_list_per_folder_tuple_record = (folder_id, folder_path_id_list_str)
    cursor.execute(sql, auxtab_path_id_list_per_folder_tuple_record)
    conn.commit()
    conn.close()

  def insert_folder_path_list_str_for(self, folder_id, folder_path_id_list_str):
    '''

    :param folder_id:
    :param folder_path_id_list_str:
    :return:
    '''
    sql = '''
    INSERT INTO %(auxtab_path_id_list_per_folder)s
           (id, %(fieldname_for_entries_path_id_list_str)s)
    VALUES ( ? ,                  ?           );
    ''' %{ \
      'auxtab_path_id_list_per_folder'        : PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS, \
      'fieldname_for_entries_path_id_list_str': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
    }
    bool_return = False
    conn = dbfact.get_db_connection()
    cursor = conn.cursor()
    auxtab_path_id_list_per_folder_tuple_record = (folder_id, folder_path_id_list_str)
    try:
      cursor.execute(sql, auxtab_path_id_list_per_folder_tuple_record)
      conn.commit()
      bool_return = True
    except sqlite3.IntegrityError:
      bool_return = False
    finally:
      conn.close()
    return bool_return

  def insert_or_update_folder_path_list_str_for(self, folder_id, folder_path_id_list_str):
    '''
    If the insert method returns False, the update method will be issued
    :param folder_id:
    :param folder_id_path_list_str:
    :return:
    '''
    if not self.insert_folder_path_list_str_for(folder_id, folder_path_id_list_str):
      self.update_folder_path_list_str_for(folder_id, folder_path_id_list_str)

class CannotInsertFolderWithSameNameAsAFile(OSError):
  # For me: not yet used or check usage or decide whether a None returned is better
  pass

class DBActionPerformer(object):


  def __init__(self, dbms_params_dict=None):
    '''

    :param device_and_middle_abspath:
    :param sqlite_db_filename:
    :return:
    '''
    # super(DBAccessor, self).__init__(device_and_middle_abspath)
    # in Python 3, it's just: super().__init__()

    self.conn_obj = dbfact.DBFactoryToConnection(dbms_params_dict)

  def insert_entryname_n_get_id_with_parent_dir_id_for(self, entryname, entrytype, parent_or_home_dir_id):
    '''

    :param foldername:
    :param parent_dir_id:
    :return:
    '''
    sql = '''
    INSERT INTO %(tablename_for_file_n_folder_entries)s
           (entryname, entrytype)
    VALUES (    ?    ,     ?    ) ; '''  %{ \
      'tablename_for_file_n_folder_entries' : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES,
    }
    entry_2tuple = (entryname, entrytype)
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql, entry_2tuple)
    inserted_entry_id = cursor.lastrowid
    self.insert_corresponding_entries_linked_list_for(inserted_entry_id, parent_or_home_dir_id, cursor)
    self.insert_new_entry_to_entries_path_id_list_str_for(inserted_entry_id, cursor)
    conn.commit()
    return inserted_entry_id

  def insert_corresponding_entries_linked_list_for(self, entry_id, parent_dir_id, cursor):
    '''
    This insert is a cascading one, notice [[cursor]] as a parameter.
    :param entry_id:
    :param cursor:
    :return:
    '''
    sql = '''
    INSERT INTO "%(tablename_for_entries_linked_list)s"
            (id, %(fieldname_for_parent_or_home_dir_id)s)
    VALUES  (?, ?) ; '''  %{ \
      'tablename_for_entries_linked_list'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST, \
      'fieldname_for_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
    }
    tuple_id_to_parent_dir_id = (entry_id, parent_dir_id)
    cursor.execute(sql, tuple_id_to_parent_dir_id)


  def insert_new_entry_to_entries_path_id_list_str_for(self, entry_id, cursor):
    '''
    This insert is a cascading one, notice [[cursor]] as a parameter.
    :param entry_id:
    :param cursor:
    :return:
    '''
    sql = '''
    SELECT %(fieldname_for_entries_path_id_list_str)s from "%(tablename_auxtab_path_id_list_per_entries)s"
    WHERE id = %(entry_id)d ;
    '''  %{ \
      'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS,
      'fieldname_for_entries_path_id_list_str'   : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
      'entry_id' : entry_id, \
    }
    result = cursor.execute(sql)
    row = result.fetchone()
    entries_path_id_list_str = None
    if row:
      entries_path_id_list_str = row[0]
    else:
      return False
    sql = '''
    INSERT INTO "%(tablename_auxtab_path_id_list_per_entries)s"
            (id, %(fieldname_for_entries_path_id_list_str)s)
    VALUES  (?, ?) ; '''  %{ \
      'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS,
      'fieldname_for_entries_path_id_list_str'   : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
    }
    tuple_id_to_parent_dir_id = (entry_id, entries_path_id_list_str)
    cursor.execute(sql, tuple_id_to_parent_dir_id)
    return True


  def retrieve_or_insert_entryname_n_get_id_with_parent_dir_id_for(self, entryname, entrytype, parent_or_home_dir_id):
    '''

    :param entryname:
    :param parent_or_home_dir_id:
    :return:
    '''
    sql = '''
    SELECT d.id, d.entrytype FROM %(tablename_for_file_n_folder_entries)s d, %(tablename_for_entries_linked_list)s p
      WHERE
        d.entryname    = '%(foldername)s'   AND
        p.%(fieldname_for_parent_or_home_dir_id)s = %(home_dir_id)d  AND
        d.id = p.id ; ''' %{ \
      'tablename_for_file_n_folder_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES,
      'tablename_for_entries_linked_list'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST,
      'fieldname_for_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID,
      'foldername'                         : entryname,
      'home_dir_id'                        : parent_or_home_dir_id,
    }
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    result = cursor.execute(sql)
    row = result.fetchone()
    entry_id_found = None
    if row <> None and len(row) > 0:
      entry_id_found = row[0]
      entrytype      = row[1]
    conn.close()
    if entry_id_found <> None:
      #if entrytype == PYMIRROR_DB_PARAMS.NONDBTYPES.ID_FOLDER_TYPE:
        #raise CannotInsertFolderWithSameNameAsAFile('CannotInsertFolderWithSameNameAsAFile')
      return entry_id_found, entrytype
    # Nothing found at the SELECT above, good to go insert foldername
    entry_id_found = self.insert_entryname_n_get_id_with_parent_dir_id_for(entryname, entrytype, parent_or_home_dir_id)
    if entry_id_found == None:
      return None, None
    return entry_id_found, entrytype

  def check_file_on_folder_existence_n_get_file_id(self, filename, home_dir_id):
    pass

  def insert_n_get_folder_id_for_foldernamed_path(self, foldernamed_path):
    '''

    :param foldernamed_path:
    :return:
    '''
    foldernamed_path.rstrip('/')
    if not foldernamed_path.startswith('/'):
      foldernamed_path = '/' + foldernamed_path
    foldernames = foldernamed_path.split('/')
    if foldernames == ['',''] or foldernamed_path == '/':
      return PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID
    del foldernames[0] # the first / produces an empty '' first element
    parent_dir_id = PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID
    id_path_list = [parent_dir_id]
    for foldername in foldernames: # 1st foldername is a 1st level entry below ROOT
      try:
        next_parent_dir_id = self.retrieve_or_insert_foldername_n_get_id_with_parent_dir_id_for(foldername, parent_dir_id)
      except CannotInsertFolderWithSameNameAsAFile:
        print 'CannotInsertFolderWithSameNameAsAFile'
        return None
      id_path_list.append(next_parent_dir_id)
      parent_dir_id = next_parent_dir_id
    foldernamed_path_folder_id = id_path_list[-1]
    return foldernamed_path_folder_id

  def insert_update_or_pass_thru_file_field_values(self, file_id, sha1hex, filesize, modified_datetime):
    '''

    :param file_id:
    :param sha1hex:
    :param filesize:
    :param modified_datime:
    :return:
    '''
    is_found, found_sha1hex, found_filesize, found_modified_datetime = self.retrieve_if_exists_file_attrib_values_entry_for_id(file_id)
    if is_found:
      if found_sha1hex == sha1hex and found_filesize == filesize and found_modified_datetime == modified_datetime:
        # record is the same, no SQL-UPDATE is needed
        return False, 'ENTRY ALREADY EXISTS AND DOES NOT NEED CHANGE'
      else:
        self.update_file_field_values_with_file_id(file_id, sha1hex, filesize, modified_datetime)
        return True, 'ENTRY EXISTS AND HAS BEEN UPDATED'
    self.insert_file_field_values(file_id, sha1hex, filesize, modified_datetime)
    return True, 'ENTRY HAS BEEN INSERTED'

  def insert_file_field_values(self, file_id, sha1hex, filesize, modified_datetime):
    '''
    Notice this method shoud be considered PRIVATE and only callable by insert_update_or_pass_thru_file_field_values(self, file_id, sha1hex, filesize, modified_datetime)
    :param file_id:
    :param sha1hex:
    :param filesize:
    :param modified_datetime:
    :return:
    '''

    sql = '''
      INSERT INTO %(tablename_for_file_attrib_values)s
              (id, sha1hex, filesize, modified_datetime)
      VALUES  ( ?,    ?,       ?,            ?         ) ''' \
      %{
        'tablename_for_file_attrib_values'   :PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES, \
        'fieldname_for_parent_or_home_dir_id':PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID,
      }
    data_tuple_in_order = (file_id, sha1hex, filesize, modified_datetime,)
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql, data_tuple_in_order)
    conn.commit()
    conn.close()

  def update_file_field_values_with_file_id(self,file_id,sha1hex,filesize,modified_datime):
    '''
    parent_or_home_dir_id is not used here, for it's in the table file_n_folder_entries
    :param file_id:
    :param sha1hex:
    :param filesize:
    :param modified_datime:
    :return:
    '''
    sql = '''
    UPDATE %(tablename_for_file_attrib_values)s
      SET
        sha1hex  = ? ,
        filesize = ? ,
        modified_datetime = ?
      WHERE
        id = %(file_id)d ;
    ''' %{
      'tablename_for_file_attrib_values': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES, \
      'file_id' : file_id,
    }
    data_tuple_field_values = (sha1hex, filesize, modified_datime)
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql, data_tuple_field_values)
    conn.commit()
    conn.close()
    return

  def does_samenamed_entry_exist_in_db(self, entryname, parent_or_home_dir_id):
    sql = '''
      SELECT id, entrytype FROM %(tablename_for_file_attrib_values)s e, %(tablename_for_entries_linked_list)s p
        WHERE
          entryname    = "%(entryname)s" AND
          %(fieldname_for_parent_or_home_dir_id)s = %(parent_or_home_dir_id)d
    ''' %{
      'tablename_for_file_attrib_values': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES, \
      'entryname'            : entryname, \
      'parent_or_home_dir_id': parent_or_home_dir_id, \
      }
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    result = cursor.execute(sql)
    row = result.fetchone()
    file_id   = None
    entrytype = None
    if row <> None:
      file_id  = row[0]
      entrytype  = row[1]
    conn.close()
    return file_id, entrytype

  def get_file_attr_values_by_id(self, file_id):
    sql = '''
      SELECT sha1hex, filesize, modified_datetime FROM %(tablename_for_file_attrib_values)s
        WHERE
          id = %(file_id)d ;''' \
      %{
        'tablename_for_file_attrib_values': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_ATTRIB_VALUES, \
        'file_id'                         : file_id, \
      }
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    result = cursor.execute(sql)
    row = result.fetchone()
    sha1hex           = None
    filesize          = None
    modified_datetime = None
    if row <> None:
      sha1hex           = row[0]
      filesize          = row[1]
      modified_datetime = row[2]
    conn.close()
    return sha1hex, filesize, modified_datetime

  def insert_dirnames_on_one_home_dir_id(self, dirnames, parent_or_home_dir_id):
    '''

    :param dirnames:
    :param parent_dir_id:
    :return:
    '''
    if dirnames == None or len(dirnames) == 0:
      return
    FOLDER_ENTRY_TYPE_ID = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER
    for entryname in dirnames:
      self.insert_entryname_n_get_id_with_parent_dir_id_for(entryname, FOLDER_ENTRY_TYPE_ID, parent_or_home_dir_id)

  def insert_dirnames_on_one_home_dir_id_with_folderpath(self, dirnames, folderpath):
    '''

    :param dirnames:
    :param folderpath:
    :return:
    '''

    parent_or_home_dir_id = self.fetch_folderpath_id(folderpath)
    return self.insert_dirnames_on_one_home_dir_id(dirnames, parent_or_home_dir_id)




  def insert_a_file(self, filename, folderpath, sha1hex, modified_date=None):
    '''

    :param filename:
    :param folder_abspath:
    :param sha1hex:
    :return:
    '''
    # 1st retrieval: fetch the folderpath id
    _id = self.fetch_folderpath_id(folderpath)
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
      return PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID
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
    parent_dir_id = PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID  # it starts its traversal at 'root'
    pp = pp[1:] # shift left 1 position
    run_insert_dirs = False
    entry_id = PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID
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
      parent_dir_id = PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID
    if parent_entryname==None:
      parent_entryname = PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME

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
        'conventioned_root_entry_id' : PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID,
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


def get_db_connection(dbms_params_dict=None):
  '''

  :param dbms_params_dict:
  :return:
  '''
  db_obj = DBFactoryToConnection(dbms_params_dict)
  return  db_obj.get_db_connection()

def get_sqlite_connection_by_filepath(sqlite_db_filepath=None):
  '''
  This is a wrapper function. It used the DBFactoryToConnection class
    to get the sqlite db connection either via the
    filepath given or by the default one
  :return:
  '''
  dbms_params_dict = {}
  dbms_params_dict['dbms'] = PYMIRROR_DB_PARAMS.DBMS.SQLITE
  dbms_params_dict['sqlite_db_filepath'] = sqlite_db_filepath
  return get_db_connection(dbms_params_dict)


def get_sqlite_connection_by_folderpath_n_filename(sqlite_db_folderpath=None, sqlite_db_filename=None):
  '''
  Use this function only in tests. In the app, use rather the class DBFactoryToConnection
  :param sqlite_db_folderpath:
  :param sqlite_db_filename:
  :return:
  '''

  if sqlite_db_folderpath == None or not os.path.isdir(sqlite_db_folderpath):
    sqlite_db_folderpath = os.path.abspath('.')

  if sqlite_db_filename == None or type(sqlite_db_filename) not in [str, unicode]:
    sqlite_db_filename = PYMIRROR_DB_PARAMS.SQLITE.HASHES_ETC_DATA_FILENAME

  # well, hopefully the if's above will avoid an os-join error below
  sqlite_db_filepath = os.path.join(sqlite_db_folderpath, sqlite_db_filename)
  return get_sqlite_connection_by_filepath(sqlite_db_filepath)

class DeviceAndMiddlePathSetter(object):


  def set_default_sqlite_db_filename(self):
    self.sqlite_db_filename = PYMIRROR_DB_PARAMS.SQLITE.HASHES_ETC_DATA_FILENAME

  def set_sqlite_db_filename(self, sqlite_db_filename=None):
    if sqlite_db_filename == None or sqlite_db_filename not in [str, unicode]:
      self.set_default_sqlite_db_filename()
      return
    self.sqlite_db_filename = sqlite_db_filename

  def set_default_device_and_middle_abspath(self):
    '''

    :return:
    '''
    self.device_and_middle_abspath = os.path.abspath('.')
    if not os.path.isdir(self.device_and_middle_abspath):
      raise OSError('Cannot establish DEFAULT device_and_middle_abspath as %s' %self.device_and_middle_abspath)

  def set_device_and_middle_abspath(self, device_and_middle_abspath=None):
    if device_and_middle_abspath == None or not os.path.isdir(device_and_middle_abspath):
      self.set_default_device_and_middle_abspath()
      return
    self.device_and_middle_abspath = device_and_middle_abspath

  def get_device_and_middle_abspath(self):
    if not os.path.isdir(self.device_and_middle_abspath):
      self.set_default_device_and_middle_abspath()
    return self.device_and_middle_abspath

  def set_sqlite_db_filepath(self, sqlite_db_filename):
    self.sqlite_db_filepath = os.path.join(self.get_device_and_middle_abspath, self.sqlite_db_filename)

  def verify_dbsqlitefile_existence(self):
    dbsqlitefile_abspath = os.path.join(self.DEVICE_PREFIX_ABSPATH, PYMIRROR_DB_PARAMS.SQLITE.HASHES_ETC_DATA_FILENAME)
    if not os.path.isfile(dbsqlitefile_abspath):
      dbinit = DBInit(self.DEVICE_PREFIX_ABSPATH)
      dbinit.verify_and_create_fs_entries_sqlite_db_table()

class DBSearch(object):

  def __init__(self, dbms_params_dict=None):
    self.dbfact_obj = dbfact.DBFactoryToConnection(dbms_params_dict)

  def find_in_auxdb_the_folder_path_id_list_for(self, folder_id):
    '''

    :param folder_id:
    :return:
    '''
    sql = '''SELECT folder_path_id_list_str
    FROM %(tablename_for_auxtab_path_id_list_per_folder)s
    WHERE id = %(folder_id)d ;
    ''' %{ \
      'tablename_for_auxtab_path_id_list_per_folder':PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS, \
      'folder_id':folder_id, \
    }
    conn = self.dbfact_obj.get_db_connection()





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

def test2():
  db_performer = DBActionPerformer()
  print "db_performer.insert_foldername_n_get_id_with_parent_dir_id_for('testinsertdir', 1)'"
  inserted_folder_id = db_performer.retrieve_or_insert_foldername_n_get_id_with_parent_dir_id_for('testinsertdir', 1)
  print 'inserted_folder_id', inserted_folder_id

def main():
  # test1()
  # sqlcreate.create_tables_and_initialize_root()
  test2()

if __name__ == '__main__':
  main()


'''
  def find_foldername_in_path_traces(self, foldername, _index, ids_starting_trace_path):
    compare_paths = get_all_paths_starting_with_trace(ids_starting_trace_path)
    for _path in compare_paths:
      corresponding_folder_id = _path[_index]
      if foldername == foldername_dict[corresponding_folder_id]:
        return corresponding_folder_id
    return None

  def fetch_folderpath_id_recursive(self, foldernames, ids_starting_trace_path):
    '''

    : return:
    '''
    for i, foldername in enumerate(foldernames):
      _index = i + 1 # this index is the one corresponding in the trace path
      corresponding_folder_id = self.find_foldername_in_path_traces(foldername, _index, ids_starting_trace_path)
      if corresponding_folder_id == None:
        return None
      break
    ids_starting_trace_path.append(corresponding_folder_id)
    return ids_starting_trace_path[-1]

'''