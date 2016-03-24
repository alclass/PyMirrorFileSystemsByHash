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


class DBFetcher(object):

  def __init__(self, dbms_params_dict=None):
    self.conn_obj = dbfact.DBFactoryToConnection(dbms_params_dict)

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


def test1():
  fetcher = DBFetcher()
  print 'fetcher.fetch_children_folder_ids_by_node_id()'
  print fetcher.fetch_children_folder_ids_by_node_id(0)
  print 'fetcher.get_all_folder_id_to_name_2Dtuples_list()'
  print fetcher.get_all_folder_id_to_name_2Dtuples_list()
  print 'fetcher.get_all_folder_id_to_name_pairs_dict()'
  print fetcher.get_all_folder_id_to_name_pairs_dict()


def main():
  sqlcreate.create_tables_and_initialize_root()
  test1()

if __name__ == '__main__':
  main()
