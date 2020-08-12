#!/usr/bin/env python
"""

  This module contains the FolderPathsBootStrapper class which, as its main function,
    loads and prepares all folder full paths in database.
  This data is kept as a linked-list and needs a recursive procedure to derive all paths.
  This recursive procedure is relatively expensive (it costs 1 select per node, and
    a node models/equals a directory)
    and it's been designed that it should derived the whole set only once
    (ie, in a bootstrap/init routine).

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
"""
import os
import config
import fs.db.db_proxy_mod as dbprox
import fs.db.sqlite_create_db_mod as sqlcreate

PYMIRROR_DB_PARAMS = config.PYMIRROR_DB_PARAMS


class FolderPath(object):

  def __init__(self, folder_id, bootstrapper):
    # self.db_params_dict = db_params_dict # to tell the db factory which db and 'how' to db-connect
    # self.dbproxy = dbact.DBProxyFetcher(self.db_params_dict)
    self.folder_id = folder_id
    self.folder_path_id_list = []
    self.folder_path_names_dict = {}
    # self.folder_path is an dynamically formed by a @property annotated method
    if bootstrapper == None:
      bootstrapper = FolderPathsBootStrapper()
    self.bootstrapper = bootstrapper
    self.init_folder_id()

  def init_folder_id(self):
    self.folder_path_id_list = self.bootstrapper.find_folder_path_id_list_for(self.folder_id)
    self.folder_path_each_name_list = self.get_folder_name_list_for_ids(self.folder_path_id_list)

  def form_n_get_folder_path(self):
    folder_path = '/'
    for folder_id in self.folder_path_ids:
      passing_folder_name = self.folder_path_names_dict[folder_id]
      folder_path += passing_folder_name + '/'
    return folder_path

  @property
  def folder_path(self):
    return self.form_n_get_folder_path()

  def __str__(self):
    outstr = '''Folder Id %(folder_id)d
    FolderPath %(folder_path)''' %{'folder_id':self.folder_id, 'folder_path':self.folder_path}
    return outstr

class FolderPathsBootStrapper(object):

  def __init__(self, dbms_params_dict=None):
    """

    :rtype: FolderPathsBootStrapper
    """
    self.folder_id_to_dirname_dict = {}
    self.all_id_path_list_list = []
    self.reversed_id_path_list_list = []
    # self.n_of_found_paths # This is dynamically gotten from len(self.all_id_path_list_list)
    self.n_of_found_paths_audit = 0 # This must equal the above n_of_found_paths
    self.dbms_params_dict = dbms_params_dict
    self.dbproxy = dbprox.DBProxyFetcher(self.dbms_params_dict)
    self.bootstrap_folder_paths()
    self.init_folder_id_to_dirname_dict()
    self.reversed_id_path_list_list = []
    self.build_reversed_id_path_list_list()
    self.update_dbauxtable_for_path_searches()

  def set_db_params_dict(self, db_params_dict):
    self.db_params_dict = db_params_dict

  def build_reversed_id_path_list_list(self):
    self.reversed_id_path_list_list = []
    for id_path_list in self.all_id_path_list_list:
      reversed_id_path_list = id_path_list[:]
      reversed_id_path_list.reverse()
      self.reversed_id_path_list_list.append(reversed_id_path_list)
    self.reversed_id_path_list_list.sort(key = lambda e: e[0])


  def update_dbauxtable_for_path_searches(self):
    '''

    :return:
    '''
    for reversed_folder_path_id_list in self.reversed_id_path_list_list:
      folder_id = reversed_folder_path_id_list[0]
      folder_path_id_list = reversed_folder_path_id_list[:]
      folder_path_id_list.reverse()
      folder_path_id_list_str = str(folder_path_id_list)
      folder_path_id_list_str = folder_path_id_list_str.replace('[','')
      folder_path_id_list_str = folder_path_id_list_str.replace(']','')
      folder_path_id_list_str = folder_path_id_list_str.replace(' ','')

      self.dbproxy.insert_or_update_folder_path_list_str_for(folder_id, folder_path_id_list_str)

  def include_a_new_id_path_list_to_bootstrapper_for(self, folder_id, id_path_list):
    '''

    :param id_path_list:
    :return:
    '''
    self.all_id_path_list_list.append(id_path_list)
    foldername = self.dbproxy.fetch_folder_name_for(folder_id)
    self.folder_id_to_dirname_dict[folder_id] = foldername
    self.build_reversed_id_path_list_list()

  def find_folder_path_id_list_for(self, folder_id, index_ini=0, index_fim=None):
    '''
    See if there's another and well proved way to do this search
    For the time being, the 'find' is a simple "binary search algorithm"
    :param folder_id:
    :return:
    '''
    index_mid = (index_fim - index_ini) / 2
    comp_id = self.reversed_id_path_list_list[index_mid][0]
    if index_fim - index_ini > 1:
      if folder_id < comp_id:
        index_fim = index_mid
        return self.find_folder_path_id_list_for(folder_id, index_ini, index_fim)
      elif folder_id > comp_id:
        index_ini = index_mid
        return self.find_folder_path_id_list_for(folder_id, index_ini, index_fim)
      else:
        # Found !
        return self.reversed_id_path_list_list[index_mid]
    # Have to look both index_ini and index_fim
    if folder_id == self.reversed_id_path_list_list[index_ini][0]:
      return self.reversed_id_path_list_list[index_ini]
    if folder_id == self.reversed_id_path_list_list[index_fim][0]:
      return self.reversed_id_path_list_list[index_fim]
    # Nothing found here, try the db aux table
    folder_id_path_list_str = self.dbproxy.fetch_folder_path_list_str_for(folder_id)
    if folder_id_path_list_str != None:
      str_ids = folder_id_path_list_str.split(',')
      id_path_list = map(int, str_ids)
      self.include_a_new_id_path_list_to_bootstrapper_for(folder_id, id_path_list)
      return id_path_list
    return None

  def get_folder_name_list_for_ids(self, id_list):
    folder_name_list = []
    for folder_id in id_list:
      folder_name_list.append(self.folder_id_to_dirname_dict[folder_id])
    return folder_name_list

  def fetch_folder_paths_via_recursive_traversal(
      self,
      prefix_list=None,
      traversal_ids=[PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID]
  ):
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
      current_traversal_ids = self.dbproxy.fetch_children_folder_ids_by_node_id(current_id)
      if len(current_traversal_ids) > 0:
        # print 'current_prefix_list, current_traversal_ids', current_prefix_list, current_traversal_ids
        self.fetch_folder_paths_via_recursive_traversal(current_prefix_list, current_traversal_ids)
      else: # nó folha, caminho encontra-se completo
        self.n_of_found_paths_audit += 1
        # print n_of_found_path, 'º caminho encontrado (ocorrência de nó folha):', current_prefix_list
        self.all_id_path_list_list.append(current_prefix_list)

  def bootstrap_folder_paths(self):
    '''
    The idea here is to pre-fetch all folder paths at the application's boot time.
    This pre-fetch demands ONE select per node, each node a directory.
    A 100-directory structure will call for 100 selects.

    The fetched paths are a list of lists of integers.  Each list is a path to a leaf folder, ie, a folder without children (subdirectories).

    :return:
    '''
    self.fetch_folder_paths_via_recursive_traversal()
    if self.n_of_found_paths_audit != len(self.all_id_path_list_list):
      raise ValueError('n_of_found_paths=%d <> len(folder_ids_paths_list)=%d' %(self.n_of_found_paths, len(self.folder_ids_paths_list)))

  def init_folder_id_to_dirname_dict(self):
    '''

    :return:
    '''
    self.folder_id_to_dirname_dict = self.dbproxy.get_all_folder_id_to_name_pairs_dict()

  def find_ids_paths_starting_with(self, ids_starting_path_trace):
    '''

    :param ids_starting_path_trace:
    :return:
    '''
    str_trace = str(ids_starting_path_trace)
    # text = get_str_crescent_ordered_paths()


  def fetch_all_paths_as_ossep_names_as_list_of_str(self):
    ossep_named_paths_as_list_of_str = []
    for path_as_id_list in self.all_id_path_list_list:
      ossep_named_path = os.sep # '/'
      if len(path_as_id_list) > 1:
        for folder_id in path_as_id_list[1:]:
          try:
            folder_name = self.folder_id_to_dirname_dict[folder_id]
          except KeyError:
            folder_name = '[NOT FOUND]'
          ossep_named_path += folder_name + os.sep # '/'
      ossep_named_paths_as_list_of_str.append(ossep_named_path)
    return ossep_named_paths_as_list_of_str

  def print_all_folder_paths(self):
    ossep_named_paths_as_list_of_str = self.fetch_all_paths_as_ossep_names_as_list_of_str()
    for ossep_named_path in ossep_named_paths_as_list_of_str:
      print(ossep_named_path)

  def print_folder_id_to_dirname_dict(self):
    print(self.folder_id_to_dirname_dict)
    print(self.all_id_path_list_list)
    for folder_id in self.folder_id_to_dirname_dict.keys():
      print('Folder ID', folder_id,'==>>', self.folder_id_to_dirname_dict[folder_id])

def test1():
  bootstrapper = FolderPathsBootStrapper()
  # db_params_dict = ...
  # bootstrapper.set_db_params_dict(db_params_dict)
  bootstrapper.print_all_folder_paths()
  bootstrapper.print_folder_id_to_dirname_dict()

def main():
  sqlcreate.create_tables_and_initialize_root()
  bootstrapper = FolderPathsBootStrapper()
  print('bootstrapper.print_folder_id_to_dirname_dict()')
  bootstrapper.print_folder_id_to_dirname_dict()
  print('bootstrapper.print_all_folder_paths()')
  bootstrapper.print_all_folder_paths()
  print('bootstrapper.all_id_path_list_list =',bootstrapper.all_id_path_list_list)
  dbproxy = dbprox.DBProxyFetcher()
  print('dbproxy.get_all_folder_id_to_name_2Dtuples_list()')
  print(dbproxy.get_all_folder_id_to_name_2Dtuples_list())

  # test1()

if __name__ == '__main__':
  main()
