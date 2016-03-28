#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import string
import random

import db.db_connection_factory_mod as dbfact
from util import util_mod as um
# sha1hex, filename, relative_parent_path, device_and_middle_path, filesize, modified_datetime

import db.db_connection_factory_mod as dbfact
import db.db_modification_query_performer_mod as dbperf
import db.db_fetcher_mod as dbfetch
import db.sqlite_create_db_mod as dbcreat
import db.db_settings as dbsetts
PYMIRROR_DB_PARAMS = dbsetts.PYMIRROR_DB_PARAMS
FOLDER = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER
FILE   = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FILE

sha1hexes=[];filesizes=[];modified_datetimes=[]

class Node(object):

  def __init__(self, folderid, parentid):
    self.folderid = folderid
    self.children = []
    self.parentid = parentid

  def add_child(self, entryid):
    self.children.append(entryid)
    self.children.sort()

  def get_longer_path_node_has(self):
    pass


class Nodes(dict):

  def __init__(self):
    pass
    # self.all_node_ids = []

  def __setitem__(self, entryid, node):
    if type(node) <> Node:
      raise TypeError()


class TestDataReader(object):

  def __init__(self, dbms_params_dict=None):
    self.data_records_tuple_list = []
    self.conn_obj    = dbfact.DBFactoryToConnection(dbms_params_dict)
    self.modquerier = dbperf.DBModificationQueryPerformer(dbms_params_dict)
    self.dbfetcher =  dbfetch.DBFetcher()
    self.dbfetcher.fetch_or_build_idpathlist_with_cursor_for()
    # self.make_tuple_list_data_for_dbinsert()

  def rebuild_tree_and_paths(self):
    pass



  def __str__(self):
    outstr = '''Instance of TestDataFiller:
    %s ''' %self.data_records_tuple_list


def test1():
  pass


def main():
  test1()

if __name__ == '__main__':
  main()
