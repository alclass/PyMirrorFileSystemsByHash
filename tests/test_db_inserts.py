#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import string
import random

import db.db_connection_factory_mod as dbfact
from util import util_mod as um
# sha1hex, filename, relative_parent_path, device_and_middle_path, filesize, modified_datetime

import db.db_connection_factory_mod as dbfact
import db.db_action_performer_mod as dbperf
import db.sqlite_create_db_mod as dbcreat

FOLDER = 1
FILE = 2

sha1hexes=[];filesizes=[];modified_datetimes=[]
for i in xrange(4):
  sha1hexes.append(um.take_random_sha1hex())
  filesizes.append(random.randint(1,100001))
  modified_datetimes.append(um.take_random_datetime())

def make_tuple_list_data_for_dbinsert():
  entries=[]
  FOLDER = 1
  FILE   = 2
  e=(FOLDER,'/abc')
  entries.append(e)
  e=(FOLDER,'/abc/ab')
  entries.append(e)
  e=(FOLDER,'/ab')
  entries.append(e)
  e=(FOLDER,'/abc/abc')
  entries.append(e)
  seq=0
  e=(FILE,'/abc/abc/file1',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  seq+=1
  e=(FOLDER,'/abc/abc/file2',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  e=(FOLDER,'/z')
  entries.append(e)
  e=(FOLDER,'/z/z')
  entries.append(e)
  seq+=1
  e=(FILE,'/z/z/filez',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  seq+=1
  e=(FILE,'/abc/abc/file2',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  return entries

class TestDataFiller(object):

  def __init__(self, dbms_params_dict=None):
    self.conn_obj    = dbfact.DBFactoryToConnection(dbms_params_dict)
    self.dbperformer = dbperf.DBActionPerformer(dbms_params_dict)
    self.make_tuple_list_data_for_dbinsert()

  def insert_file_n_get_file_id(self, file_values_dict):
    '''

    :param file_values_dict:
    :return:
    '''
    filepath = file_values_dict['filepath']
    parent_path, filename = os.path.split(filepath)

    file_values_dict['filename']=filename
    home_dir_id = self.dbperformer.insert_n_get_folder_id_for_foldernamed_path(parent_path)
    if home_dir_id == None:
      print 'Cannot continue, a folder has the same name of a file in db. Please, look up and correct it if possible, then rerun this script.'
      return

    file_id     = self.dbperformer.insert_file_field_values_n_get_file_id( \
      filename        = file_values_dict['filename'],
      sha1hex         = file_values_dict['sha1hex'],
      home_dir_id     = home_dir_id,
      filesize        = file_values_dict['filesize'],
      modified_datetime = file_values_dict['modified_datetime'],
    )
    return file_id

  def insert_folder_n_get_folder_id(self, folderpath):
    '''

    :param folderpath:
    :return:
    '''
    folder_id = self.dbperformer.insert_n_get_folder_id_for_foldernamed_path(folderpath)

  def make_tuple_list_data_for_dbinsert(self):
    '''

    :return:
    '''
    tuple_list = make_tuple_list_data_for_dbinsert()
    for tuple_record in tuple_list:
      entry_type = tuple_record[0]
      if entry_type == FOLDER:
        folderpath = tuple_record[1]
        self.insert_folder_n_get_folder_id(folderpath)
      elif entry_type == FILE:
        # '/abc/abc/file1',sha1hexes[seq],filesizes[seq],modified_datetimes[seq]
        file_values_dict = {}
        tuple_values = tuple_record[1:]
        file_values_dict['filepath'] = tuple_values[0]
        file_values_dict['sha1hex']  = tuple_values[1]
        file_values_dict['filesize'] = tuple_values[2]
        file_values_dict['modified_datetime'] = tuple_values[3]
        self.insert_file_n_get_file_id(file_values_dict)

def test1():
  dbcreat.create_tables_and_initialize_root()
  filler = TestDataFiller()
  file_data_dict = { \
    'filepath' : '/abc/test_dir/file1.txt', \
    'sha1hex'  : sha1hexes[0], \
    'filesize' : 1000, \
    'modified_datetime' : modified_datetimes[0], \
  }
  filler.insert_file_n_get_file_id(file_data_dict)
  pass

def main():
  test1()

if __name__ == '__main__':
  main()
