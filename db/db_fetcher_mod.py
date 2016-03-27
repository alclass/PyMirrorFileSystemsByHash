#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''


  Written on 2015-01-23 Luiz Lewis
'''
import os
import sqlite3
import db_settings as dbsetts
import sqlite_create_db_mod as sqlcreate
PYMIRROR_DB_PARAMS = dbsetts.PYMIRROR_DB_PARAMS
import db_connection_factory_mod as dbfact
import db_modification_query_performer_mod as dbquery


class DBFetcher(object):

  def __init__(self, dbms_params_dict=None):
    self.conn_obj = dbfact.DBFactoryToConnection(dbms_params_dict)
    self.entries_path_id_auxdict = {}

  def fetch_entryname_with_cursor_of(self, folderid, cursor):
    '''
    '''
    sql = '''
    SELECT entryname FROM %(tablename_for_file_n_folder_entries)s
      WHERE
        id = %(folderid)d ;
    ''' % { \
      'tablename_for_file_n_folder_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES, \
      'folderid': folderid, \
      }
    result = cursor.execute(sql)
    one_record = result.fetchone()
    entryname = None
    if one_record:
      entryname = one_record[0]
    return entryname

  def update_correct_n_levels_in_parent_entries(self, entry_id, n_levels, cursor):
    '''

    :param n_levels:
    :return:
    '''
    sql = '''
    UPDATE %(tablename)d
      SET n_levels = ?
    WHERE
      id = %(entry_id)d ;
    ''' %{ \
      'tablename': PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS, \
      'entry_id' : entry_id, \
    }
    cursor.execute(sql)
    # a conn.commit() is delay from here


  def fetch_or_build_idpathlist_with_cursor_for(self, entry_id, cursor, at_least_one_dbchange_happened=False):
    '''

    :param entry_id:
    :param cursor:
    :param at_least_one_dbchange_happened:
    :return: a2tuple (id_path_list, at_least_one_dbchange_happened)
    '''

    if entry_id == PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID:
      return [], at_least_one_dbchange_happened

    sql = '''
    SELECT %(fieldname_parent_or_home_dir_id)s, n_levels, %(fieldname_id_path_list_str)s FROM %(tablename)s
      WHERE
        id = %(entry_id)d ;
    '''  %{ \
      'tablename'                      : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS, \
      'fieldname_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
      'fieldname_id_path_list_str'     : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
      'entry_id'                       : entry_id, \
    }
    parent_or_home_dir_id = None
    n_levels              = None
    id_path_list_str      = None
    id_path_list          = None

    result = cursor.execute(sql)
    record = result.fetchone()
    if record == None:
      return None, at_least_one_dbchange_happened

    parent_or_home_dir_id = record[0]
    n_levels              = record[1]
    id_path_list_str      = record[2]

    if parent_or_home_dir_id == None:
      return None, at_least_one_dbchange_happened

    if id_path_list_str <> None and id_path_list_str <> '':
      try:
        id_path_strelem_list = id_path_list_str.split(';')
        id_path_list = map(int, id_path_strelem_list)
        n_levels_in_list = len(id_path_list)
        if n_levels == None or n_levels <> n_levels_in_list:
          has_change_happened = self.update_correct_n_levels_in_parent_entries(entry_id, n_levels, cursor)
          if not at_least_one_dbchange_happened and has_change_happened:
            at_least_one_dbchange_happened = True

        if parent_or_home_dir_id == id_path_list[-1]:
          return id_path_list, at_least_one_dbchange_happened
      except IndexError:
        pass

    if parent_or_home_dir_id == PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID:
      return [PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID], at_least_one_dbchange_happened

    idpathlist_of_parent, change_boolean = self.fetch_or_build_idpathlist_with_cursor_for(parent_or_home_dir_id, cursor, at_least_one_dbchange_happened)
    if not at_least_one_dbchange_happened and change_boolean:
      at_least_one_dbchange_happened = True

    id_path_list = idpathlist_of_parent + [parent_or_home_dir_id]
    change_boolean = self.insert_into_parent_entries_with_cursor_for(entry_id, parent_or_home_dir_id, n_levels, id_path_list, cursor)

    if not at_least_one_dbchange_happened and change_boolean:
      at_least_one_dbchange_happened = True

    return id_path_list, at_least_one_dbchange_happened

  def insert_into_parent_entries_with_cursor_for(self, entry_id, parent_or_home_dir_id, n_levels, id_path_list, cursor):
    '''

    :param entry_id:
    :param parent_or_home_dir_id:
    :param n_levels:
    :param id_path_list:
    :return:
    '''
    db_change_happened = False

    # Perhaps this piece may be improved later on for if it's guaranteed that it has one specific caller, this previous caller has already checked this
    if id_path_list == None or type(id_path_list) <> list:
      error_msg = 'id_path_list (=%s) is either None or is not a list when trying to insert parententries for entryid=%d and parentid=%d.' %(str(id_path_list), entry_id, parent_or_home_dir_id)
      raise ValueError(error_msg)
    if id_path_list == []:
      if parent_or_home_dir_id == PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID:
        id_path_list = [parent_or_home_dir_id]
      else:
        error_msg = 'Inconsistent empty id_path_list not having the root parentid (entryid=%d and parentid=%d).' %(entry_id, parent_or_home_dir_id)
        raise ValueError(error_msg)

    id_path_strelem_list = map(str, id_path_list)
    id_path_list_str = ';'.join(id_path_strelem_list)
    n_levels = len(id_path_list)
    sql = '''
    INSERT INTO %(tablename)s
        (id, %(fieldname_parent_or_home_dir_id)s, n_levels, %(fieldname_id_path_list_str)s)
    VALUES
        (?,?,?,?);
    '''  %{ \
      'tablename'                      : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS, \
      'fieldname_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
      'fieldname_id_path_list_str'     : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
    }
    data_4tuple = (entry_id, parent_or_home_dir_id, n_levels, id_path_list_str)
    try:
      cursor.execute(sql, data_4tuple)
    except sqlite3.IntegrityError:
      return db_change_happened
    db_change_happened = True
    return db_change_happened


  def normalize_foldernamespathlist_with_ossepfullpath(self, ossepfullpath):
    '''
    This should be noticed (or remembered)
    Detail 1:
    '/'.split('/') produces ['','']
       : this is protected by the first 'if' for CONVENTIONED_ROOT_DIR_NAME is equal to the os.path.sep parameter
    Detail 1:
    '/a'.split('/') produces ['','a']
       : the first empty '' is erased at the 'del normalized_foldernames[0]' command
    '''
    if ossepfullpath == None or ossepfullpath == '':
      return None
    normalized_foldernames = []
    if ossepfullpath == PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME:
      return [PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID] # same as os.path.sep
    ossepfullpath = ossepfullpath.rstrip(os.path.sep)
    if not ossepfullpath.startswith(os.path.sep):
      ossepfullpath = os.path.sep + ossepfullpath
    normalized_foldernames = ossepfullpath.split(os.path.sep)
    del normalized_foldernames[0]  # the first / produces an empty '' first element
    return normalized_foldernames

  def searchdeeprecursive_edgefolderid_for_the_normalizedfoldernamelist_n_cursor(self, normalizedfoldernamelist, candidate_folder_ids_w_paths_dict, cursor):
    '''
    This is a recursive method that tries to find the edge folder id associated to a path.
    '''
    # Protect against a 'bad' normalizedfoldernamelist
    if normalizedfoldernamelist == None or len(normalizedfoldernamelist) == 0:
      # if this happens, this is weird, it may be that the database is not good, see also the last return in this method
      return None
    new_candidate_folder_ids_w_paths_dict = {}
    edgefoldername_at_this_recursive_level = normalizedfoldernamelist.pop() # edge is the last one; this pop() also prepares for a later recurse, if needed
    original_ambiguous_edge_folder_id_list = candidate_folder_ids_w_paths_dict.keys()
    n_collisions = 0
    for original_ambiguous_edge_folder_id in original_ambiguous_edge_folder_id_list:
      id_path_list = candidate_folder_ids_w_paths_dict[original_ambiguous_edge_folder_id]
      candidate_folder_id_in_level = id_path_list.pop() # notice also that id_path_list is also diminished here
      candidate_foldername_in_level = self.fetch_entryname_with_cursor_of(candidate_folder_id_in_level, cursor)
      if candidate_foldername_in_level == edgefoldername_at_this_recursive_level:
        n_collisions += 1
        collidededgefolderid = original_ambiguous_edge_folder_id
        new_candidate_folder_ids_w_paths_dict[original_ambiguous_edge_folder_id] = id_path_list  # the one popped above, ie, with minus one element

    if n_collisions == 1: # this means that recursion ends here, for the dict is not ambiguous anymore, see explanation in the caller-before-recursion's __doc__
      return collidededgefolderid
    elif n_collisions > 1:
      return self.searchdeeprecursive_edgefolderid_for_the_normalizedfoldernamelist_n_cursor(normalizedfoldernamelist, new_candidate_folder_ids_w_paths_dict, cursor)
    # From here n_collisions = 0
    # It's a weird thing, having no collision, nothing was found; it's probably a data inconsistency
    #   of some kind, ie, the database is not good. Solution is to return None, not raise an exception.
    return None

  def find_edgefolderid_of_the_ossepfullpath_via_auxtable_n_cursor(self, ossepfullpath, cursor):
    '''
    This method tries to find the edge folder id associated to a path.
    (Notice that ossepfullpath is sometimes referred to foldernamed_path.)

    The edge folder id associated to a path is the following. Suppose path p is:

     /a/abc/xpto

    The folder xpto has an id and this is the one that is searched for.


    Let's see this search via an example:

    Suppose the 3 following aux paths:

    id-to-name | prefixing named path
    ---------------------------------
         d     |  a/b/c
         x     |  a/e/f/bla/blah
         d     |  x/xpto/c

    The letters above are names. However, all id's are numbers and they are all different,
      for they are PRIMARY KEY there.

    Remind also that auxtable has only id's, not names.  The names above are just to explain why
      we need the logic here.

    Suppose we search for the folderid of path x/xpto/c/d

    This path is the last row above in the example, ie:

         d     |  x/xpto/c

    However, after having passed thru this method, the following 2 rows:

    id-to-name | named path
    -----------------------
         d     |  a/b/c/
         d     |  x/xpto/c/

    will have to go into a recursive method, because both have a 3-level depth
      and both have the same name, ie, 'd'.

    The folderid of the first 'd' is ambiguous with the folderid of the second 'd'.

    In the first passage thru the recursive search, the table above will become:

    keptid | id-to-name | named path | corresponding original foldername in level 3
    -------------------------------------------------------------------------------
     {id1} |     c      |  a/b/      |                c (still ambiguous)
     {id2} |     c      |  x/xpto/   |                c (still ambiguous)

     {id1} is still ambiguous with {id2}, for both related names are 'c'. It needs to go recurse again:

    keptid | id-to-name | named path | corresponding original foldername in level 2
    -------------------------------------------------------------------------------
     {id1} |     b      |  a/        |              xpto (no longer ambiguous, {id1} is out)
     {id2} |     xpto   |  x/        |              xpto (no longer ambiguous, {id2} is in)

     At this step, {id2} is not ambiguous with {id2} for only {id2} relates to 'xpto',
       recursion is finished and answer is found, ie, the sought-after folder id is {id2}.

     ***

     This system has a second search algorithm. The first one is this above explained.

     The second algorithm goes top to bottom, search the folder id from root all below to the last name.
     Let's briefly compare the 2 algorithms:
       + This second algorithm needs n-level SELECTs for names.
       + The first algorithm does one first SELECT and some bifurcating name-SELECTs
         (or no further SELECTs if bootstrap for id-to-names is used).
         The bifurcating name-SELECTs will equal the n-1- level SELECTs
           in the worst case scenario of identical names up to the 1st level.
           In the best case scenario, the folder id may be found right away even before any recursion.
           However the case/scenario, with bootstrap on no further SELECTs will happen.
           But the bootstrap on, there exists an option of having all paths in memory, so that would be
             much simpler, for a single list element match would do.
             (This is not planned to be done due to memory size economy.)
    '''


    # Because this method may be called by anyone with a cursor, this protection is repeated from the caller in this class
    if ossepfullpath == None or ossepfullpath == '':
      return None

    # Because this method may be called by anyone with a cursor, this protection is repeated from the caller in this class
    if ossepfullpath == PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME:
      return PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID

    normalizedfoldernamelist = self.normalize_foldernamespathlist_with_ossepfullpath(ossepfullpath)
    # Protect against a 'bad' normalizedfoldernamelist
    if normalizedfoldernamelist == None or len(normalizedfoldernamelist) == 0:
      # weird thing, return None for edgefolderid
      return None

    n_levels = len(normalizedfoldernamelist) # because 'root' is also inside the normalized list, the TWO end up the same
    edgefoldername = normalizedfoldernamelist.pop() # edge is the last element (an entryname here); this pop() also prepares for a later recurse, if needed (see below)
    sql = '''
    SELECT id, %(fieldname_for_entries_path_id_list_str)s
      FROM %(tablename)s
      WHERE
        n_levels = %(n_levels)d
    ''' % { \
      'tablename': PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_PARENTS_N_PATHS, \
      'fieldname_for_entries_path_id_list_str': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
      'n_levels': n_levels, \
      }
    result = cursor.execute(sql)
    #if result == None:
      # return None  # ie, edge folder id not found, it must either be inserted in sequence or a message propagate to user
    aux_records = result.fetchall()
    if aux_records == None or len(aux_records) == 0:
      return None  # ie, edge folder id not found, it must either be inserted in sequence or a message propagate to user
    if len(aux_records) == 1:
      # this is probably a rare case, because if levels are equal, the folderid should be the edge one there
      aux_record = aux_records[0]  # first record
      edgefolderid = aux_record[0]  # first field value
      candidate_foldername = self.fetch_entryname_with_cursor_of(edgefolderid, cursor)
      if candidate_foldername == edgefoldername:
        return edgefolderid  # ok, found it, return it
      else:
        # only one record found, but this record doesn't have the edgefolderid-to-the-edgefoldername
        # then, return None (meaning not found)
        return None

    # At this point ==>> len(aux_records) > 1 ie: there are more than one n-or-above-level path, all of them are candidates
    # Prepare the dict and go recurse to find edge folder id
    # During this preparation, there's a chance to get the edgefolderid first time and not recurse at all
    n_collisions = 0
    candidate_folder_ids_w_paths_dict = {}
    # Gather all records that, having the same n-level path, have that same edgefoldername
    for record in aux_records:
      folder_id = record[0]
      candidate_foldername = self.fetch_entryname_with_cursor_of(folder_id, cursor)
      if candidate_foldername == edgefoldername:
        n_collisions += 1
        edgefolderid = folder_id
        id_path_list_str = record[1]
        id_path_strelem_list = id_path_list_str.split(';')
        id_path_list = map(int, id_path_strelem_list)
        # Cut it down to the correct level if needed
        if len(id_path_list) > n_levels:
          id_path_list = id_path_list[:n_levels]
        if len(id_path_list) <> n_levels:
          # a data error, a ';' separated string is wrongly inside the database, raise ValueError
          error_msg = "The ';'-separated string (=%s) did not convert to the expected sized folder id list. The expected size is %d, the split size is %d." % (id_path_list_str, n_levels, len(id_path_list))
          raise ValueError(error_msg)
        candidate_folder_ids_w_paths_dict[folder_id] = id_path_list
    if n_collisions == 0:
      # weird thing, there's probably some bad data in database
      return None
    # if only one collision happened, good luck, the edgefolderid is that one, no recursion needed, return it
    if n_collisions == 1:
      return edgefolderid
    # From here n_collisions > 1; recursion is needed
    # Remind that a normalizedfoldernamelist.pop() happened at the entrance; the list's good to go recurse
    edgefolderid = self.searchdeeprecursive_edgefolderid_for_the_normalizedfoldernamelist_n_cursor( \
      normalizedfoldernamelist, \
      candidate_folder_ids_w_paths_dict, \
      cursor
    )
    # let's take the chance to find perhaps a bug here, ie, the edgefolderid returned must be in the candidate_folder_ids list
    if edgefolderid == None or edgefolderid not in candidate_folder_ids_w_paths_dict.keys():
      error_msg = 'Logic error in the program. The edgefolderid (%d) was chosen outside candidates (=%s).' %( \
        edgefolderid, candidate_folder_ids_w_paths_dict.keys() \
      )
      raise ValueError(error_msg)
    # edgefolderid is good, return it
    return edgefolderid

  def find_edgefolderid_of_the_ossepfullpath_via_auxtable(self, ossepfullpath):
    '''
    This method is a wrapper for method find_edgefolderid_of_the_ossepfullpath_via_auxtable_n_cursor()
    It picks up the 'cursor' and calls the above wrapped method.

    The __doc__ for the above method is much more explanatory, please look that up in case it's needed.
    '''

    # This 'heading-check' is to avoid opening a db-connection. The cursor method to call below also has this checking
    if ossepfullpath == None or ossepfullpath == '':
      return None

    # This 'heading-check' is to avoid opening a db-connection. The cursor method to call below also has this checking
    if ossepfullpath == PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_DIR_NAME:
      return PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID

    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    edgefolderid = self.find_edgefolderid_of_the_ossepfullpath_via_auxtable_n_cursor(ossepfullpath, cursor)
    conn.close()
    return edgefolderid

  def find_edge_folderid_with_ossepfullpath_via_linktable(self, ossepfullpath):

    parent_dir_id = PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID
    id_path_list = [parent_dir_id]
    for foldername in foldernames:  # 1st foldername is a 1st level entry below ROOT
      try:
        next_parent_dir_id = self.insert_update_or_pass_thru_entryname_n_get_id_with_parent_dir_id_n_entrytype_for( \
          foldername, \
          PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER, \
          parent_dir_id \
          )
      except CannotInsertFolderWithSameNameAsAFile:
        print 'CannotInsertFolderWithSameNameAsAFile'
        return None
      id_path_list.append(next_parent_dir_id)
      parent_dir_id = next_parent_dir_id
    folder_id = id_path_list[-1]
    return folder_id

  def fetch_children_folder_ids_by_node_id(self, node_id):
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
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    result = cursor.execute(sql)
    traversal_ids = []
    for row in result.fetchall():
      traversal_ids.append(row[0])
    conn.close()
    return traversal_ids

  def get_all_folder_id_to_name_2Dtuples_list(self):
    '''

    :param _id:
    :return:
    '''
    sql = '''SELECT id, entryname FROM %(tablename_for_file_n_folder_entries)s
      WHERE
        entrytype = %(FOLDER_ENTRY_TYPE_ID)d
      ORDER BY id ;
    ''' %{ \
      'tablename_for_file_n_folder_entries':PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES, \
      'FOLDER_ENTRY_TYPE_ID'               :PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER, \
    }
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    folder_id_to_name_tuple_list = cursor.execute(sql).fetchall()[:]
    # folder_id_to_name_tuple_list = result.fetchall()[:]
    '''
    for row in result.fetchall():
      entry_id  = row[0]
      entryname = row[1]
      folder_id_to_name_tuple_list.append((entry_id, entryname))
    '''
    conn.close()
    # folder_id_to_name_tuple_list.sort(key=lambda e:e[0])
    return folder_id_to_name_tuple_list

  def get_all_folder_id_to_name_pairs_dict(self):
    '''

    :return:
    '''
    folder_id_to_name_tuple_list = self.get_all_folder_id_to_name_2Dtuples_list()
    if folder_id_to_name_tuple_list == None or len(folder_id_to_name_tuple_list) == 0:
      return {}
    folder_id_to_name_dict = {}
    for folder_id_to_name_tuple in folder_id_to_name_tuple_list:
      folder_id_to_name_dict[folder_id_to_name_tuple[0]] = folder_id_to_name_tuple[1]
    return folder_id_to_name_dict

  def find_in_mem_folder_path_id_list_in_auxtable_for(self, folder_id):
    if self.entries_path_id_auxdict == {}:
      self.rebuild_tree_from_auxtable_into_auxdict()
    if self.entries_path_id_auxdict.has_key(folder_id):
      return self.entries_path_id_auxdict[folder_id]
    return None

  def find_in_db_folder_path_id_list_in_auxtable_for(self, folder_id):
    '''

    :param folder_id:
    :return:
    '''
    sql = '''SELECT %(fieldname_for_folder_path_id_list_str)s
    FROM %(tablename_for_auxtab_path_id_list_per_folder)s
    WHERE id = %(folder_id)d ;
    ''' %{ \
      'tablename_for_auxtab_path_id_list_per_folder':PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS, \
      'fieldname_for_folder_path_id_list_str' : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
      'folder_id':folder_id, \
    }
    conn = self.dbfact_obj.get_db_connection()
    cursor = conn.cursor()
    result = cursor.execute(sql)
    row = result.fetchone()
    folder_path_id_list_str = None
    if row:
      folder_path_id_list_str = row[0]
    conn.close()
    return folder_path_id_list_str

  def rebuild_tree_from_auxtable_into_auxdict(self):
    sql = '''
    SELECT * FROM %(tablename_auxtab_path_id_list_per_entries)s ORDER BY id;
    ''' % { \
    'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS, \
  }
    # clean up dict
    self.entries_path_id_auxdict = {}
    conn = self.conn_obj.get_db_connection()
    cursor = conn.cursor()
    result = cursor.execute(sql)
    for row in result.fetchall():
      entry_id = row[0]
      entries_path_id_list_str = row[1]
      entries_path_id_strelem_list = entries_path_id_list_str.split(';')
      entries_path_id_list = map(int, entries_path_id_strelem_list)
      self.entries_path_id_auxdict[entry_id] = entries_path_id_list
    conn.close()

  def str_tree(self):
    if self.entries_path_id_auxdict == {}:
      self.rebuild_tree_from_auxtable_into_auxdict()
    outstr = 'Tree:\n'
    entry_ids = self.entries_path_id_auxdict.keys()
    entry_ids.sort()
    for entry_id in entry_ids:
      outstr += '%d -> %s\n' %(entry_id, self.entries_path_id_auxdict[entry_id])
    return outstr

FILE   = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FILE
FOLDER = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER
def prep_data_01():
  modquerier = dbquery.DBModificationQueryPerformer()
  entries = []
  e=(FILE,'/hierarq/a/abcd/secret.js')
  entries.append(e)
  e=(FILE,'/hierarq/z/xpto/treasure.txt')
  entries.append(e)
  for entry_tuple in entries:
    entrytype = entry_tuple[0]
    if entrytype == PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FILE:
      filedict = {}
      filedict['filepath'] = entry_tuple[1]
      filedict['sha1hex'] =  '01234'*10
      filedict['filesize'] =  1000
      filedict['modified_datetime'] = '2010-03-12 12:12:12'
      modquerier.insert_a_file_with_conventioned_filedict(filedict)

def test1():
  prep_data_01()
  fetcher = DBFetcher()
  print 'fetcher.fetch_children_folder_ids_by_node_id()'
  print fetcher.fetch_children_folder_ids_by_node_id(0)
  print 'fetcher.get_all_folder_id_to_name_2Dtuples_list()'
  print fetcher.get_all_folder_id_to_name_2Dtuples_list()
  print 'fetcher.get_all_folder_id_to_name_pairs_dict()'
  print fetcher.get_all_folder_id_to_name_pairs_dict()
  print fetcher.str_tree()

def force_insert_test_mass():
  '''
  Test mass is:

  [foldernamed paths]
  d   a/b/c/
  x   a/e/f/
  d   a/z/c/

  [folder id's to names]
  root is 1
  a = 2    d = 5    x = 8       2nd d = 11
  b = 3    e = 6    z = 9
  c = 4    f = 7    2nd c = 10

  [folder id's to prefixing paths]
  5   2;3;4
  8   2;6;7
  11  2;9;10
  '''
  conn_obj = dbfact.DBFactoryToConnection()
  conn = conn_obj.get_db_connection()
  cursor = conn.cursor()
  sql = '''
  INSERT INTO %(tablename_for_entries_linked_list)s
   (id,%(fieldname_for_parent_or_home_dir_id)s)
  VALUES
   (?,?);
  ''' % { \
    'tablename_for_entries_linked_list'  : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST, \
    'fieldname_for_parent_or_home_dir_id': PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.PARENT_OR_HOME_DIR_ID, \
  }
  data = ((2,1),(3,2),(4,3),(5,4),(6,2),(7,6),(8,7),(9,2),(10,9),(11,10),)
  cursor.executemany(sql, data)
  # for r in data:
  sql = '''
  INSERT INTO %(tablename_for_file_n_folder_entries)s
    (id, entryname, entrytype)
  VALUES
    (?,?,?);
  '''%{ 'tablename_for_file_n_folder_entries' : PYMIRROR_DB_PARAMS.TABLE_NAMES.FILE_N_FOLDER_ENTRIES }
  '''
  [folder id's to names]
  root is 1
  a = 2    d = 5    x = 8       2nd d = 11
  b = 3    e = 6    z = 9
  c = 4    f = 7    2nd c = 10
  '''
  data = ((2, 'a', 0), (3, 'b', 0), (4, 'c', 0), (5, 'd', 0), (6, 'e', 0), (7, 'f', 0), (8, 'x', 0), (9, 'z', 0), (10, 'c', 0), (11, 'd', 0),)
  cursor.executemany(sql, data)
  sql = '''
  INSERT INTO %(tablename_auxtab_path_id_list_per_entries)s
    (id, n_levels, %(fieldname_for_entries_path_id_list_str)s)
  VALUES
    (?,?,?);
  ''' % { \
    'tablename_auxtab_path_id_list_per_entries': PYMIRROR_DB_PARAMS.TABLE_NAMES.AUXTAB_FOR_PRE_PREPARED_PATHS,
    'fieldname_for_entries_path_id_list_str'   : PYMIRROR_DB_PARAMS.FIELD_NAMES_ACROSS_TABLES.ENTRIES_PATH_ID_LIST_STR, \
  }
  data = [ \
    (2, 1, '1'), (3, 2, '1;2'), (4,  3, '1;2;3'),  (5, 4, '1;2;3;4'),  \
                 (6, 2, '1;2'), (7,  3, '1;2;6'),  (8, 4, '1;2;6;7'),  \
                 (9, 2, '1;2'), (10, 3, '1;2;9'), (11, 4, '1;2;9;10'), \
  ]
  cursor.executemany(sql, data)
  # all 3 tables received data
  conn.commit()
  conn.close()


def prepare_tables():
  sqlcreator = sqlcreate.DBTablesCreator()
  sqlcreator.create_tables()
  sqlcreator.initialize_the_2_dir_tables_with_toproot()
  sqlcreator.delete_all_data_except_root()
  force_insert_test_mass()

def test2():
  dbfetcher = DBFetcher()
  paths = ['/a/z/c/d/', 'bla/blah/', 'a/e/f/x', '/a/z/', 'a', '/', '//']
  paths = ['a/e/f/x',]
  for ossepfullpath_to_find_edgefolderid in paths:
    print 'ossepfullpath_to_find_edgefolderid', ossepfullpath_to_find_edgefolderid
    edgefolderid = dbfetcher.find_edgefolderid_of_the_ossepfullpath_via_auxtable(ossepfullpath_to_find_edgefolderid)
    print 'Found:', edgefolderid

def main():
  # test1()
  # prepare_tables()
  test2()

if __name__ == '__main__':
  main()
