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
import db_fetcher_mod as dbfetch
import sqlite_create_db_mod as sqlcreate

class CannotInsertFolderWithSameNameAsAFile(OSError):
  # For me: not yet used or check usage or decide whether a None returned is better
  pass

class DBModificationQueryPerformer(object):

  class DBActionPerformerError(OSError):
    pass

  def __init__(self, dbms_params_dict=None):
    '''

    :param device_and_middle_abspath:
    :param sqlite_db_filename:
    :return:
    '''
    # super(DBAccessor, self).__init__(device_and_middle_abspath)
    # in Python 3, it's just: super().__init__()

    self.conn_obj  = dbfact.DBFactoryToConnection(dbms_params_dict)
    self.dbfetcher = dbfetch.DBFetcher(dbms_params_dict)

  def update_entryname_after_existscheck_with_id_parent_dir_id_entrytype_n_cursor_for(self, entry_id, entryname, entrytype, parent_or_home_dir_id, cursor):
    '''

    :param entry_id:
    :param entryname:
    :param entrytype:
    :param parent_or_home_dir_id:
    :param cursor:
    :return:
    '''
    sql = '''
    UPDATE %(tablename_for_file_n_folder_entries)s
      SET
        entryname = ?
        entrytype = ?
      WHERE
       id = %(entry_id)d
       %(fieldname_for_parent_or_home_dir_id)s = %(parent_or_home_dir_id)d ;
    '''  %{ \
      'tablename_for_file_n_folder_entries' : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES, \
      'fieldname_for_parent_or_home_dir_id' : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
      'parent_or_home_dir_id' : parent_or_home_dir_id, \
    }
    has_insert_or_update_happened = False
    try:
      cursor.execute(sql, (entryname, entrytype))
      has_insert_or_update_happened = True
    except sqlite3.IntegrityError:
      return False

    # OBS: the path_id_list_str AND the linked_list_table BOTH do not change here
    #      for both the parent_id and the path_trace_to_root continue to be the same

    # do not conn.close() here, let the caller do it at the end of its 'pipeline'
    return has_insert_or_update_happened

  def insert_entryname_entrytype_after_existscheck_n_get_id_with_cursor(self, entryname, entrytype, cursor):
    '''

    :param entryname:
    :param entrytype:
    :param cursor:
    :return:
    '''
    inserted_entry_id = None
    sql = '''
    INSERT INTO %(tablename)s
           (entryname, entrytype)
    VALUES (    ?    ,     ?    ) ; '''  %{ \
      'tablename' : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES,
    }
    try:
      cursor.execute(sql, (entryname, entrytype))
      inserted_entry_id = cursor.lastrowid
    except sqlite3.IntegrityError:
      pass
    return inserted_entry_id

  def insert_entryname_n_parentlink_after_existscheck_n_get_id_with_parent_dir_id_n_cursor_for(self, entryname, entrytype, parent_or_home_dir_id, cursor, file_values_dict=None):
    '''

    :param foldername:
    :param parent_dir_id:
    :return: a2tuple (inserted_entry_id, dbchange_has_happened)
    '''
    dbchange_has_happened = False

    inserted_entry_id  = self.insert_entryname_entrytype_after_existscheck_n_get_id_with_cursor(entryname, entrytype, cursor)
    if inserted_entry_id == None:
      return None, False
    dbchange_has_happened = True

    parentlink_inserted  = False
    parentlink_inserted = self.insert_into_parententries_entryid_entrytype_n_idpathlist_ifany_with_cursor(inserted_entry_id, parent_or_home_dir_id, entrytype, cursor, n_levels=None, id_path_list=None )
    if not dbchange_has_happened and parentlink_inserted:
      dbchange_has_happened = True

    return inserted_entry_id, dbchange_has_happened

  def insert_into_parententries_fileid_with_cursor(self, entry_id, parent_dir_id, cursor):
    '''

    :param entry_id:
    :param parent_dir_id:
    :param cursor:
    :return:
    '''
    dbchange_happened = False
    sql = '''
    INSERT INTO "%(tablename)s"
            (id, %(parent_or_home_dir_id), n_levels, %(id_path_list_str)s)
    VALUES  (?, ?, ?, ?) ; '''  %{ \
      'tablename'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS, \
      'parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
      'id_path_list_str': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
    }
    data_4tuple = (entry_id, parent_dir_id, -1, '')
    try:
      cursor.execute(sql, data_4tuple)
      dbchange_happened = False
    except sqlite3.IntegrityError:
      pass
    return dbchange_happened

  def insert_into_parententries_folderid_with_cursor(self, entry_id, parent_dir_id, cursor, n_levels=None, id_path_list=None):
    '''

    :param entry_id:
    :param parent_dir_id:
    :param cursor:
    :return:
    '''
    dbchange_happened = False
    if parent_dir_id == PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID:
      id_path_list = []

    if id_path_list==None:
      id_path_list, has_dbchange_happened = self.dbfetcher.fetch_or_build_idpathlist_with_cursor_for(parent_dir_id, cursor)
      if id_path_list == None or type(id_path_list) <> list or id_path_list == []: # at this point, it can't be [] for ROOT has been tested above
        # can't decide, something wrong happened
        error_msg = 'Could not find the id_path_list of entryid=%d with parentid=%d.' %(entry_id, parent_dir_id)
        raise ValueError(error_msg)

    # It's guaranteed id_path_list is not None at the point in code (see above)
    id_path_list += [parent_dir_id]
    id_path_strelem_list = map(str, id_path_list)
    id_path_list_str = ';'.join(id_path_strelem_list)
    n_levels = len(id_path_list)

    sql = '''
    INSERT INTO "%(tablename)s"
            (id, %(parent_or_home_dir_id)s, n_levels, %(id_path_list_str)s)
    VALUES  (?, ?, ?, ?) ; '''  %{ \
      'tablename'            : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS, \
      'parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
      'id_path_list_str'     : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
    }
    try:
      cursor.execute(sql, (entry_id, parent_dir_id, n_levels, id_path_list_str))
      dbchange_happened = True
    except sqlite3.IntegrityError:
      pass
    return dbchange_happened

  def insert_into_parententries_entryid_entrytype_n_idpathlist_ifany_with_cursor(self, entry_id, parent_dir_id, entrytype, cursor, n_levels=None, id_path_list=None):
    '''
    # This insert is a cascading one, notice [[cursor]] as a parameter.
    :param entry_id:
    :param parent_dir_id:
    :param n_levels:
    :param id_path_list_str:
    :param cursor:
    :return:
    '''
    dbchange_happened = False
    # if entrytype is FILE, no id_path_list is necessary (for it's '' for files), so it's simpler, we can solve it first in code
    if entrytype == PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FILE:
      fileparent_inserted = self.insert_into_parententries_fileid_with_cursor(self, entry_id, parent_dir_id, cursor)
      if not dbchange_happened and fileparent_inserted:
        dbchange_happened = True
      return dbchange_happened

    # From here, entrytype is FOLDER, now we'll need id_path_list
    folderparent_inserted = self.insert_into_parententries_folderid_with_cursor(entry_id, parent_dir_id, cursor, n_levels, id_path_list)
    if not dbchange_happened and folderparent_inserted:
      dbchange_happened = True

    return dbchange_happened

  def fetch_entries_path_id_list_str_with_cursor_for(self, entry_id, cursor):
    '''

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
    # entries_path_id_list_str += ',' + str(parent_or_home_dir_id)
    return entries_path_id_list_str

  def fetch_entries_path_id_list_str_for(self, entry_id):
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    entries_path_id_list_str = self.fetch_entries_path_id_list_str_with_cursor_for(entry_id, cursor)
    conn.close()
    return entries_path_id_list_str

  def insert_entryid_to_entries_path_id_list_with_parent_id_n_cursor_for(self, entry_id, parent_or_home_dir_id, cursor):
    '''
    This insert is a cascading one, notice [[cursor]] as a parameter.

    The process:
    1) First, find the path_str of the parent_or_home_dir_id. This must exist.
       Keep the path_str, adding the entry_id to it.
    2) Second, find whether or not the entry_id already exists.
       If so, do an UPDATE to it, if not, do an INSERT.

    :param entry_id:
    :param cursor:
    :return:
    '''

    if entry_id == None:
      raise OSError('entry_id param entered as None for updating/inserting entries_path_id_list_str_for_parent.  This could not happen.')

    should_be_entries_path_id_list_str = None  # to be known
    entries_path_id_list_str = None # to be db-fetched if it exists

    if parent_or_home_dir_id == PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID:
      should_be_entries_path_id_list_str = str(PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID)
    else:
      entries_path_id_list_str_for_parent = self.fetch_entries_path_id_list_str_with_cursor_for(parent_or_home_dir_id, cursor)
      if entries_path_id_list_str_for_parent == None:
        raise OSError('entries_path_id_list_str_for_parent is None below (or above if one chooses) root, which could not happen. Below root, there must be at least a second id.')
      should_be_entries_path_id_list_str = entries_path_id_list_str_for_parent + ';' + str(parent_or_home_dir_id)
    # okay, now let's check whether or not the entry_id already exists

    entries_path_id_list_str = self.fetch_entries_path_id_list_str_with_cursor_for(entry_id, cursor)

    action = None
    DO_INSERT = 1
    DO_UPDATE = 2

    if entries_path_id_list_str == None:
      action = DO_INSERT
    elif entries_path_id_list_str <> should_be_entries_path_id_list_str:
      action = DO_UPDATE

    has_insert_or_update_happened = False
    if action == DO_INSERT:
      self.insert_after_existscheck_entry_to_entries_path_id_list_str_with_cursor_for(entry_id, should_be_entries_path_id_list_str, cursor)
      has_insert_or_update_happened = True
    elif action == DO_UPDATE:
      self.update_after_existscheck_entry_to_entries_path_id_list_str_with_cursor_for(entry_id, should_be_entries_path_id_list_str, cursor)
      has_insert_or_update_happened = True

      # cursor & conn will be finished in the caller()
    return has_insert_or_update_happened, should_be_entries_path_id_list_str

  def insert_after_existscheck_entry_to_entries_path_id_list_str_with_cursor_for(self, entry_id, entries_path_id_list_str, cursor):
    '''

    :param entry_id:
    :param entries_path_id_list_str:
    :param cursor:
    :return:
    '''
    sql = '''
    INSERT INTO "%(tablename_auxtab_path_id_list_per_entries)s"
            (id, %(fieldname_for_entries_path_id_list_str)s)
    VALUES  (?, ?) ; '''  %{ \
      'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS,
      'fieldname_for_entries_path_id_list_str'   : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
    }
    cursor.execute(sql, (entry_id, entries_path_id_list_str))
    return True

  def update_after_existscheck_entry_to_entries_path_id_list_str_with_cursor_for(self, entry_id, entries_path_id_list_str, cursor):
    '''
    :param entry_id:
    :param entries_path_id_list_str:
    :param cursor:
    :return:
    '''
    sql = '''
    UPDATE "%(tablename_auxtab_path_id_list_per_entries)s"
      SET %(fieldname_for_entries_path_id_list_str)s = ?
      WHERE id = %(entry_id)d ; ''' %{ \
      'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS, \
      'fieldname_for_entries_path_id_list_str'   : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
      'entry_id' : entry_id, \
    }
    cursor.execute(sql, (entries_path_id_list_str,))
    return True

  def update_entry_path_list_str_for(self, entry_id, new_entry_path_id_list_str):
    '''

    :param entry_id:
    :param new_entry_path_id_list_str:
    :return:
    '''
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    found_one = self.fetch_entries_path_id_list_str_with_cursor_for(entry_id, cursor)
    if found_one:
      self.update_after_existscheck_entry_to_entries_path_id_list_str_with_cursor_for(entry_id, new_entry_path_id_list_str, cursor)
    conn.commit()
    conn.close()
    if found_one == None:
      error_msg = 'Tried to update new_entry_path_id_list_str for a non-existing entry_id (=%d) in table.' %entry_id
      raise self.DBActionPerformerError(error_msg)

  def insert_or_update_entries_path_list_str_for(self, entry_id, force_this_entries_path_id_list_str):
    '''
    There is a 2nd option for this insertion,
      that is when the list_str is based upon the parent entry

    :param folder_id:
    :param folder_path_id_list_str:
    :return:
    '''
    has_insert_or_update_happened = False
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    found_one = entries_path_id_list_str = self.fetch_entries_path_id_list_str_with_cursor_for(entry_id, cursor)
    if found_one == None:
      self.insert_after_existscheck_entry_to_entries_path_id_list_str_with_cursor_for(entry_id, entries_path_id_list_str, cursor)
      has_insert_or_update_happened = True
    else:
      self.update_after_existscheck_entry_to_entries_path_id_list_str_with_cursor_for(entry_id, entries_path_id_list_str, cursor)
      has_insert_or_update_happened = True
    if has_insert_or_update_happened:
      conn.commit()
    conn.close()
    return has_insert_or_update_happened

  def does_entryname_entrytype_exist_in_parent_dir_id_with_cursor(self, entryname, entrytype, parent_or_home_dir_id, cursor):
    '''

    :param entryname:
    :param entrytype:
    :param parent_or_home_dir_id:
    :return:
    '''
    sql = '''
    SELECT d.id, d.entrytype FROM %(tablename)s d, %(tablename_for_parent_entries)s p
      WHERE
        d.entryname    = "%(foldername)s"   AND
        p.%(fieldname_for_parent_or_home_dir_id)s = %(home_dir_id)d  AND
        d.id = p.id ; ''' %{ \
      'tablename'                          : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES,
      'tablename_for_parent_entries'       : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS,
      'fieldname_for_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID,
      'foldername'                         : entryname,
      'home_dir_id'                        : parent_or_home_dir_id,
    }
    result = cursor.execute(sql)
    row = result.fetchone()
    entry_id_found = None
    entrytype_found = None
    if row <> None and len(row) > 0:
      entry_id_found  = row[0]
      entrytype_found = row[1]
    return entry_id_found, entrytype_found

  def does_entryname_entrytype_exist_in_parent_dir_id(self, entryname, entrytype, parent_or_home_dir_id):
    '''

    :param entryname:
    :param entrytype:
    :param parent_or_home_dir_id:
    :return:
    '''
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    entry_id_found, entrytype_found = self.does_entryname_entrytype_exist_in_parent_dir_id_with_cursor(entryname, entrytype, parent_or_home_dir_id, cursor)
    conn.close()
    return entry_id_found, entrytype_found

  def insert_update_or_pass_thru_entryname_n_get_id_with_parent_dir_id_n_entrytype_for(self, entryname, entrytype, parent_or_home_dir_id):
    '''

    :param entryname:
    :param parent_or_home_dir_id:
    :return:
    '''
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    entry_id, entrytype_found = self.does_entryname_entrytype_exist_in_parent_dir_id_with_cursor(entryname, entrytype, parent_or_home_dir_id, cursor)

    db_action = None; has_insert_or_update_happened = False
    DO_INSERT  = 1
    DO_UPDATE  = 2
    DO_NOTHING = 3

    if entry_id == None:
      db_action = DO_INSERT
    elif entrytype_found <> entrytype:
      db_action = DO_UPDATE
    else:
      db_action = DO_NOTHING

    if db_action == DO_INSERT:
      entry_id, has_insert_or_update_happened = self.insert_entryname_n_parentlink_after_existscheck_n_get_id_with_parent_dir_id_n_cursor_for( \
        entryname, entrytype, parent_or_home_dir_id, cursor \
      )
      if entry_id <> None:
        has_insert_or_update_happened = True
    elif db_action == DO_UPDATE:
      has_insert_or_update_happened = self.update_entryname_after_existscheck_with_id_parent_dir_id_entrytype_n_cursor_for(entry_id, entryname, entrytype, parent_or_home_dir_id, cursor)
    if has_insert_or_update_happened:
      conn.commit()
    conn.close()
    return entry_id

  def check_file_on_folder_existence_n_get_file_id(self, filename, home_dir_id):
    pass

  def insert_foldername_n_get_id_having_ossepfullpath_afternotexist(self, ossepfullpath):
    '''

    :param ossepfullpath:
    :param cursor:
    :return:
    '''
    entrytype = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER
    normalizedfoldernamespathlist = self.dbfetcher.normalize_foldernamespathlist_with_ossepfullpath(ossepfullpath)
    parent_dir_id = PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID
    for foldername in normalizedfoldernamespathlist:
      passing_folder_id = self.insert_update_or_pass_thru_entryname_n_get_id_with_parent_dir_id_n_entrytype_for(foldername, entrytype, parent_dir_id)
      if passing_folder_id == None:
        error_msg = 'Could not insert foldername %s in path %s.' %(foldername, ossepfullpath)
        raise ValueError(error_msg)
      parent_dir_id = passing_folder_id
    folder_id = passing_folder_id
    return folder_id

  def insert_foldername_n_get_id_having_ossepfullpath(self, ossepfullpath):
    '''
    ossepfullpath = foldernamed_path
    :param ossepfullpath:
    :return: folder_id
    '''

    folder_id = self.dbfetcher.find_edgefolderid_of_the_ossepfullpath_via_auxtable(ossepfullpath)
    if folder_id == None:
      folder_id = self.insert_foldername_n_get_id_having_ossepfullpath_afternotexist(ossepfullpath)
    return folder_id

  def insert_update_or_pass_thru_file_field_values(self, file_id, sha1hex, filesize, modified_datetime):
    '''

    :param file_id:
    :param sha1hex:
    :param filesize:
    :param modified_datime:
    :return:
    '''
    found_sha1hex, found_filesize, found_modified_datetime = self.\
      get_file_attr_values_by_id(file_id)
    if found_sha1hex <> None:
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

  def does_samenamed_entry_exist_in_parent_dir_id(self, entryname, parent_or_home_dir_id):
    '''

    :param entryname:
    :param parent_or_home_dir_id:
    :return:
    '''
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
    if type(file_id) <> int:
      raise OSError('Passed wrongly typed arg in get_file_attr_values_by_id(self, file_id=%s)' %file_id)
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
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    at_least_one_insert_happened = False
    for entryname in dirnames:
      next_parent_or_home_dir_id, dbchange_happened = self.insert_entryname_n_parentlink_after_existscheck_n_get_id_with_parent_dir_id_n_cursor_for(entryname, FOLDER_ENTRY_TYPE_ID, parent_or_home_dir_id)
      if not at_least_one_insert_happened and dbchange_happened:
        at_least_one_insert_happened = True
      if next_parent_or_home_dir_id <> None:
        at_least_one_insert_happened = False
      parent_or_home_dir_id = next_parent_or_home_dir_id
    if at_least_one_insert_happened:
      conn.commit()
    conn.close()

  def insert_dirnames_on_one_home_dir_id_with_folderpath(self, dirnames, folderpath):
    '''

    :param dirnames:
    :param folderpath:
    :return:
    '''

    parent_or_home_dir_id = self.fetch_folderpath_id(folderpath)
    return self.insert_dirnames_on_one_home_dir_id(dirnames, parent_or_home_dir_id)

  def fetch_entryid_n_entrytype_by_foldername_n_parent_dir_id_with_cursor(self, foldername, parent_or_home_dir_id, cursor):
    '''

    :return:
    '''
    entrytype = None
    sql = '''
    -- e for 'entries', p for 'parent'
    SELECT e.id, e.entrytype FROM %(tablename_for_file_n_folder_entries)s e, %(tablename_for_entries_linked_list)s p
    WHERE
      e.entryname = "%(foldername)s" AND
      p.%(fieldname_for_parent_or_home_dir_id)s = %(parent_or_home_dir_id)d AND
      e.id = p.id ;
    ''' %{ \

      'tablename_for_entries_linked_list'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST,   \
      'tablename_for_file_n_folder_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES, \
      'fieldname_for_parent_or_home_dir_id' : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
      'foldername' : foldername, \
      'parent_or_home_dir_id' : parent_or_home_dir_id, \
    }
    result = cursor.execute(sql)
    entry_id = None
    row = result.fetchone()
    if row <> None and len(row) == 1:
      entry_id  = row[0]
      entrytype = row[1]
    # a dilemma happens here, whether or not an EXCEPTION should be raised if entrytype is not FOLDER
    return entry_id, entrytype

  def fetch_entryid_by_its_foldernamed_path_or_create_namedpath(self, foldernamed_path):
    '''

    :param foldernamed_path:
    :return:
    '''
    foldernames = self.dbfetcher.normalize_foldernames_pathlist_from_osfolderpath(foldernamed_path)
    if foldernames == [PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME]:
      return PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID

    parent_dir_id = PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID
    has_insert_happened = False
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    for foldername in foldernames:
      next_parent_dir_id, entrytype = self.fetch_entryid_n_entrytype_by_foldername_n_parent_dir_id_with_cursor(foldername, parent_dir_id, cursor)
      if next_parent_dir_id == None:
        next_parent_dir_id, has_insert_happened = self.insert_entryname_n_parentlink_after_existscheck_n_get_id_with_parent_dir_id_n_cursor_for(foldername, PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER, parent_dir_id, cursor)
        if next_parent_dir_id == None:
          error_msg = 'Could not create foldername %s for path %s' %(foldername, foldernamed_path)
          raise OSError(error_msg)
        has_insert_happened = True
      elif entrytype <> PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER:
        raise OSError('Cannot create folder %s in path %s' %(foldername, foldernamed_path))
      parent_dir_id = next_parent_dir_id
    if has_insert_happened:
      conn.commit()
    conn.close()
    return parent_dir_id


  def insert_a_file_with_conventioned_filedict(self, file_values_dict):
    '''

    The conventioned filedict is:
    filepath = file_values_dict['filepath'] # needs os.path.split() -->> path, filename
    sha1hex  = file_values_dict['sha1hex']
    filesize = file_values_dict['filesize']
    modified_datetime = file_values_dict['modified_datetime']

    :param filedict:
    :return:
    '''
    filepath = file_values_dict['filepath']
    sha1hex  = file_values_dict['sha1hex']
    filesize = file_values_dict['filesize']
    modified_datetime = file_values_dict['modified_datetime']

    foldernamed_path, filename = os.path.split(filepath)

    parent_or_home_dir_id = self.fetch_entryid_by_its_foldernamed_path_or_create_namedpath(foldernamed_path)
    file_id = self.insert_update_or_pass_thru_entryname_n_get_id_with_parent_dir_id_n_entrytype_for(filename, PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FILE, parent_or_home_dir_id)
    self.insert_update_or_pass_thru_file_field_values(file_id, sha1hex, filesize, modified_datetime)


  def db_insert_filename_and_its_sha1hex_with_file_abspath(self, file_abspath, sha1hex):
    '''

    :param file_abspath:
    :param sha1hex:
    :return:
    '''
    its_folder_abspath, filename = os.path.split(file_abspath)
    self.db_insert_filename_and_its_sha1hex_with_its_folder_abspath(filename, its_folder_abspath, sha1hex)


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
      SELECT entry_id, entryname FROM %(tablename)s
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

def test1():
  db_performer = DBModificationQueryPerformer()
  print "db_performer.insert_foldername_n_get_id_with_parent_dir_id_for('testinsertdir', 1)'"
  inserted_folder_id = db_performer.retrieve_or_insert_foldername_n_get_id_with_parent_dir_id_for('testinsertdir', 1)
  print 'inserted_folder_id', inserted_folder_id

def main():
  # sqlcreate.create_tables_and_initialize_root()
  test1()

if __name__ == '__main__':
  main()
