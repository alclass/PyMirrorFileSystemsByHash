#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''

  This module contains the FolderPathsBootStrapper class which, as its main function,
    loads and prepares all folder full paths in database.
  This data is kept as a linked-list and needs a recursive procedure to derive all paths.
  This recursive procedure is relatively expensive (it costs 1 select per node, and a node models/equals a directory)
    and it's been designed that it should derived the whole set only once (ie, in a bootstrap/init routine).

  Example:
    Suppose all paths are (actually, these are found after bootstrap, see doc of method fetch_folder_paths_via_recursive_traversal()):
  [1, 2, 3, 7]
  [1, 2, 8]
  [1, 5, 99, 101]

  Suppose further that:
  1 = / (the root dir)
  2 = 'Finance'
  3 = 'Banks'
  7 = 'Bank ABC'
  8 = 'Accounting'
  5 = 'Science'
  99 = 'Physics'
  101 = 'Astrophysics'

  Then, after bootstrap, the full paths will be:
  [1, 2, 3, 7]    = '/Finance/Banks/Bank ABC/' (the ending slash / is optional)
  [1, 2, 8]       = '/Finance/Accounting'
  [1, 5, 99, 101] = '/Science/Physics/Astrophysics'

  Written on 2015-01-18 Luiz Lewis
'''
import os

import db.db_settings as dbsetts
import db.sqlite_accessor_mod as sqlaccessor


PYMIRROR_DB_PARAMS = dbsetts.PYMIRROR_DB_PARAMS


def get_traversal_ids_from_db(_id):
  sql = '''SELECT id FROM %(tablename_for_entries_linked_list)s WHERE parent_dir_id=%(_id)d ORDER BY id;
  ''' %{ \
    'tablename_for_entries_linked_list' : PYMIRROR_DB_PARAMS.TABLE_NAMES.ENTRIES_LINKED_LIST, \
    '_id' : _id, \
  }
  # n_of_selects += 1 # global module var
  conn = sqlaccessor.get_sqlite_connection_by_folderpath_n_filename()
  cursor = conn.cursor()
  result = cursor.execute(sql)
  traversal_ids = []
  for row in result.fetchall():
    traversal_ids.append(row[0])
  return traversal_ids

def get_id_dirname_tuple_list_from_db():
  '''

  :param _id:
  :return:
  '''
  sql = '''SELECT id, name FROM %(tablename_for_dir_entries);
  ''' %{'tablename_for_dir_entries':PYMIRROR_DB_PARAMS.TABLE_NAMES.DIR_ENTRIES}
  conn = sqlaccessor.get_sqlite_connection_by_folderpath_n_filename()
  cursor = conn.cursor()
  cursor.execute(sql)
  return cursor.fetchall()

class FolderPathsBootStrapper(object):

  def __init__(self):
    self.folder_names_dict = {}
    self.folder_ids_paths_list = []
    self.n_of_found_paths = 0
    self.bootstrap_folder_paths()

  def fetch_folder_paths_via_recursive_traversal(self, prefix_list=[], traversal_ids=[PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID]):
    '''
    This is the recursive method that finds all folder index paths.
    The trick, so to say, is seen in the parameters to the method. It starts with an empty prefix_list
      and a traversal_ids list containing initially the root conventioned index.

    :param prefix_list:
    :param traversal_ids:
    :return:
    '''
    for current_id in traversal_ids:
      current_prefix_list = prefix_list + [current_id]
      current_traversal_ids = get_traversal_ids_from_db(current_id)
      if len(current_traversal_ids) > 0:
        # print 'current_prefix_list, current_traversal_ids', current_prefix_list, current_traversal_ids
        self.fetch_folder_paths_via_recursive_traversal(current_prefix_list, current_traversal_ids)
      else: # nó folha, caminho encontra-se completo
        self.n_of_found_paths += 1
        # print n_of_found_path, 'º caminho encontrado (ocorrência de nó folha):', current_prefix_list
        self.folder_ids_paths_list.append(current_prefix_list)

  def bootstrap_folder_paths(self):
    '''
    The idea here is to pre-fetch all folder paths at the application's boot time.
    This pre-fetch demands ONE select per node, each node a directory.
    A 100-directory structure will call for 100 selects.

    The fetched paths are a list of lists of integers.  Each list is a path to a leaf folder, ie, a folder without children (subdirectories).

    :return:
    '''
    self.fetch_folder_paths_via_recursive_traversal()
    if self.n_of_found_paths <> len(self.folder_ids_paths_list):
      raise ValueError('n_of_found_paths=%d <> len(folder_ids_paths_list)=%d' %(self.n_of_found_paths, len(self.folder_ids_paths_list))

  def init_folder_names_dict(self):
    '''

    :return:
    '''
    fetchall_tuple_list = get_id_dirname_tuple_list_from_db()
    for row in fetchall_tuple_list:
      _id  = row[0]
      name = row[1]
      self.folder_names_dict[_id] = name
    ids = self.folder_names_dict.keys()
    ids.sort()
    for _id in ids:
      print _id, self.folder_names_dict[_id]
    #return self.folder_names_dict

  def fetch_all_folder_paths(self):
    ospaths = []
    for path_as_list in self.folder_ids_paths_list:
      ospath = '/'
      for _id in path_as_list:
        ospath += self.folder_ids_paths_list[_id] + '/'
      ospaths.append(ospath)
    return ospaths

  def print_all_folder_paths(self):
    ospaths = self.fetch_all_folder_paths()
    for ospath in ospaths:
      print ospath


def test1():
  bootstrapper = FolderPathsBootStrapper()
  bootstrapper.print_all_folder_paths()

def main():
  FolderPathsBootStrapper()
  test1()

if __name__ == '__main__':
  main()
