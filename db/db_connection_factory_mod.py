#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
db_connection_factory_mod.py

  This script contains class DBFactoryToConnection


  Written initially on 2015-01-13 Luiz Lewis
  Refactored on 2015-03-20 Luiz Lewis
'''
import os
import sqlite3
import sys

import db_settings as dbsetts
PYMIRROR_DB_PARAMS = dbsetts.PYMIRROR_DB_PARAMS

class DBFactoryToConnection(object):

  def __init__(self, dbms_params_dict=None):

    self.dbms = None
    # the sqlite_db_filepath is not keep as an instance variable,
    # it is, rather, joined by an annotated @property method the joins folder with filename
    self.sqlite_db_folderpath = None
    self.sqlite_db_filename   = None
    self.mysql_tuple_params      = None
    self.postgresql_tuple_params = None
    self.set_dbms_params_dict(dbms_params_dict)

  def set_default_sqlite_db_folderpath(self):
    # the default is the folder from where the script is executed
    self.sqlite_db_folderpath = os.path.abspath('.')

  def set_default_sqlite_db_filename(self):
    self.sqlite_db_filename = PYMIRROR_DB_PARAMS.SQLITE.HASHES_ETC_DATA_FILENAME

  def set_default_db_sqlite_folderpath_n_filename(self):
    self.set_default_sqlite_db_folderpath()
    self.set_default_sqlite_db_filename()

  def set_default_dbms(self):
    self.dbms = PYMIRROR_DB_PARAMS.DBMS.SQLITE

  def set_default_db_sqlite_dbms_n_file(self):
    self.set_default_dbms()
    self.set_default_db_sqlite_folderpath_n_filename()

  def set_dbms_params_dict(self, dbms_params_dict):
    '''

    :param db_params_obj:
    :return:
    '''
    if dbms_params_dict == None or type(dbms_params_dict) != dict:
      self.set_default_db_sqlite_dbms_n_file()
      return

    dbms = None
    db_sqlite_filepath = None
    mysql_tuple_params = None
    postgresql_tuple_params = None

    try:
      dbms = dbms_params_dict['dbms']
    except IndexError:
      pass
    try:
      db_sqlite_filepath = dbms_params_dict['db_sqlite_filepath']
    except IndexError:
      pass
    try:
      mysql_tuple_params = dbms_params_dict['mysql_tuple_params']
    except IndexError:
      pass
    try:
      postgresql_tuple_params = dbms_params_dict['postgresql_tuple_params']
    except IndexError:
      pass

    self.set_dbms(dbms)
    self.set_db_sqlite_folderpath_n_filename_from_filepath(db_sqlite_filepath)
    self.set_mysql_tuple_params(mysql_tuple_params)
    self.set_postgresql_tuple_params(postgresql_tuple_params)


  def dynamically_rebuild_dbms_params_dict(self):
    '''
    Rebuild dbms_params_dict
    :return:
    '''
    dbms_params_dict = {}
    dbms_params_dict['dbms'] = self.dbms
    dbms_params_dict['sqlite_db_filepath'] = self.sqlite_db_filepath
    dbms_params_dict['mysql_tuple_params'] = self.mysql_tuple_params
    dbms_params_dict['postgresql_tuple_params'] = self.postgresql_tuple_params

  @property
  def dbms_params_dict(self):
    return self.dynamically_rebuild_dbms_params_dict()

  def set_db_sqlite_folderpath_n_filename_from_filepath(self, p_sqlite_db_filepath):
    '''

    :return:
    '''
    if p_sqlite_db_filepath == None:
      self.set_default_db_sqlite_folderpath_n_filename()
      return
    try:
      folderpath, filename = os.path.split(p_sqlite_db_filepath)
      self.set_sqlite_db_filename(filename)
      self.set_sqlite_db_folderpath(folderpath)
      return
    except Exception:
      self.set_default_db_sqlite_folderpath_n_filename()
      return

  def set_dbms(self, dbms):
    '''
    The provided DBMS (DataBase Management Systems are given in the db_settings.py)
    :param dbms:
    :return:
    '''
    if dbms == None or not dbsetts.is_dbms_provided(dbms):
      self.set_default_dbms()
      return
    self.dbms == dbms

  def set_sqlite_db_filename(self, p_sqlite_db_filename=None):
    if p_sqlite_db_filename <> None and type(p_sqlite_db_filename) in ['str', 'unicode']:
      self.sqlite_db_filename = p_sqlite_db_filename
      return
    else:
      self.set_default_sqlite_db_filename()

  def set_sqlite_db_folderpath(self, p_sqlite_db_folderpath=None):
    if p_sqlite_db_folderpath != None and os.path.isdir(p_sqlite_db_folderpath):
        self.sqlite_db_folderpath = p_sqlite_db_folderpath
        return
    else:
      self.set_default_sqlite_db_folderpath()
      return

  def join_n_get_db_sqlite_filepath_with_folderpath_n_filename(self):
    '''
    db_sqlite_filepath is a DERIVED attribute, it's recomposed on-the-fly as asked
    As a composition, it's the os-join of folderpath and filename of the sqlite data file
    :return:
    '''
    if not os.path.isdir(self.sqlite_db_folderpath):
      self.set_default_sqlite_db_folderpath()
      # do not [[[return]]] from here, there's still the 'filename step' to go
    if self.sqlite_db_filename == None:
      self.set_default_sqlite_db_filename()
    return os.path.join(self.sqlite_db_folderpath, self.sqlite_db_filename)

  @property
  def sqlite_db_filepath(self):
    return self.join_n_get_db_sqlite_filepath_with_folderpath_n_filename()

  def get_mysql_connection(self):
    '''
    Not yet implemented
    :return:
    '''
    return None

  def get_postgresql_connection(self):
    '''
    Not yet implemented
    :return:
    '''
    return None

  def get_sqlite_connection(self):
    '''
    Remember that attribute self.sqlite_db_filepath is, in fact, an annotated @property method
      ie, it will check filepath is okay and if not, it will default it
    :return:
    '''
    return sqlite3.connect(self.sqlite_db_filepath)

  def get_db_connection(self):
    if self.dbms == PYMIRROR_DB_PARAMS.DBMS.SQLITE:
      return self.get_sqlite_connection()
    elif self.dbms == PYMIRROR_DB_PARAMS.DBMS.MYSQL:
      return self.get_mysql_connection()
    elif self.dbms == PYMIRROR_DB_PARAMS.DBMS.POSTGRESQL:
      return self.get_postgresql_connection()
    # fall back to sqlite if self.dbms is none of the above
    return self.get_sqlite_connection()

  def __str__(self):
    '''
    The string representation of the class object
    :return:
    '''
    dbms_name = dbsetts.get_dbms_name(self.dbms)
    outstr = '''DBFactoryToConnection object::
    dbms = %s ''' %dbms_name
    outstr += '\n'
    if self.dbms == PYMIRROR_DB_PARAMS.DBMS.SQLITE:
      outstr += self.sqlite_db_filepath
    elif self.dbms == PYMIRROR_DB_PARAMS.DBMS.MYSQL:
      try:
        outstr += self.dbms_params_dict['mysql_tuple_params']
      except KeyError:
        pass
    elif self.dbms == PYMIRROR_DB_PARAMS.DBMS.POSTGRESQL:
      try:
        outstr += self.dbms_params_dict['postgresql_tuple_params']
      except KeyError:
        pass
    outstr += '\n'
    return outstr

def get_db_connection(dbms_params_dict=None):
  '''
  This is a wrapper function. It used the DBFactoryToConnection class
    to get the sqlite db connection via dbms_params_dict
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


def get_args_to_dict():
  args_dict = {}
  for arg in sys.argv:
    if arg.startswith('-p='):
      device_root_abspath = arg [ len( '-p=') : ]
      args_dict['device_root_abspath'] = device_root_abspath
  return args_dict

def test1():
  db_obj = DBFactoryToConnection()
  print 'db_obj', db_obj


def main():
  test1()

if __name__ == '__main__':
  main()
