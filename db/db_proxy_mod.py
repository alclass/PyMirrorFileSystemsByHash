#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''


  Written on 2015-01-23 Luiz Lewis
'''
import db_connection_factory_mod as dbfact
import db_modification_query_performer_mod as dbquery

class DBProxier(object):

  def __init__(self, dbms_params_dict=None):
    self.conn_obj = dbfact.DBFactoryToConnection(dbms_params_dict)
    self.entries_path_id_auxdict = {}
    self.dbquerier = dbquery.DBModificationQueryPerformer(dbms_params_dict)

  def insert_file(self, conventioned_filedict):
    '''
    '''
    self.dbquerier.insert_a_file_with_conventioned_filedict(conventioned_filedict)

  def insert_folder(self, top_bottom_relative_folderpath):
    '''
    '''
    self.dbquerier.insert_foldername_n_get_id_having_ossepfullpath(top_bottom_relative_folderpath)


  def delete_folder(self, top_bottom_relative_folderpath):
    '''
    '''
    pass

  def delete_file(self, top_bottom_relative_filepath):
    '''
    '''
    pass


  def fetch_filenames_in_reporelativehomedirpath(self, reporelativehomedirpath):
    pass


  def fetch_foldernames_in_reporelativehomedirpath(self, reporelativehomedirpath):
    pass


def main():
  # test1()
  # prepare_tables()
  pass

if __name__ == '__main__':
  main()
