#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''


  Written on 2015-01-23 Luiz Lewis
'''
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


def main():
  sqlcreate.create_tables_and_initialize_root()
  test1()

if __name__ == '__main__':
  main()
