#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
batchWalkHashingFilesToRootFolderSqliteDB.py
Explanation:
  This script walks all files and folders up the directory tree.
  As it encounters files it sha1-hashes them and store sha1hex on a database.
  As for now, this database is a SQLite file staged on the device's root folder.

  In a nutshell, it keeps a database of hashes so that it can be used
    for external back-up programs that need to know
    whether or not a file exists and, if so, where it is located.

  Written on 2015-01-13 Luiz Lewis
'''
import hashlib
import os
import sqlite3
import sys
import time
import string

import Sha1FileSystemComplementerSqlite as db_accessor

class SHA1_NOT_OBTAINED(Exception):
  pass


class Sha1FileSystemComplementer(object):

  def __init__(self, DEVICE_PREFIX_ABSPATH):
    '''

    :param DEVICE_PREFIX_ABSPATH:
    :return:
    '''
    if not os.path.isdir(DEVICE_PREFIX_ABSPATH):
      raise Exception, 'DEVICE_PREFIX_ABSPATH [%s] is not valid.' %DEVICE_PREFIX_ABSPATH
    self.DEVICE_PREFIX_ABSPATH = DEVICE_PREFIX_ABSPATH

  def get_dirnames_on_db_with_same_parent_id(self, parent_dir_id):
    '''

    :param parent_dir_id:
    :return:
    '''
    return db_accessor.get_dirnames_on_db_with_same_parent_id(parent_dir_id)

  def db_insert_dirnames(self, dirnames, dirpath):
    '''

    :param dirnames:
    :param dirpath:
    :return:
    '''
    if dirnames == None or len(dirnames) == 0:
      return
    parent_dir_id = db_accessor.find_entry_id_for_dirpath(dirpath)
    dirnames_on_db = self.get_dirnames_on_db_with_same_parent_id(parent_dir_id)
    dirnames_needing_inclusion = []
    for dirname in dirnames:
      if dirname in dirnames_on_db:
        continue
      dirnames_needing_inclusion.append(dirname)
    db_accessor.insert_dirnames_on_parent_dir_id(dirnames_needing_inclusion)

  def get_sha1hex_from_file(self, current_abs_dirpath, filename):
    '''

    :param current_abs_dirpath:
    :param filename:
    :return:
    '''

    file_abspath = os.path.join(current_abs_dirpath, filename)
    sha1obj = hashlib.sha1()
    try:
      f = open(file_abspath, 'r')
      sha1obj.update(f.read())
      sha1hex = sha1obj.hexdigest()
    except Exception:
      raise SHA1_NOT_OBTAINED, 'SHA1_NOT_OBTAINED'
    # verify previous record existence and check equality
    return sha1hex

  def add_or_update_filename_on_folder(self, filename, parent_dir_id, current_abs_dirpath):
    '''

    :param current_abs_dirpath:
    :param filenames:
    :return:
    '''
    sha1hex = self.get_sha1hex_from_file(current_abs_dirpath, filename)
    entry_id, found_entryname, found_parent_dir_id = db_accessor.fetch_record_if_sha1hex_exists(sha1hex)

    # 1st hypothesis: file is already there, nothing to be done
    if filename == found_entryname and parent_dir_id == found_parent_dir_id:
      # nothing to be done
      return

    # 2nd hypothesis: file has not entered db yet, insert it there
    if entry_id == None:
      entry_id = db_accessor.insert_filename_on_parent(filename, parent_dir_id, sha1hex)
      return

    # 3rd hypothesis: file exists, but (3.1) it may have a different name or (3.2) be somewhere else having or not the same name
    # 3.1 rename it to filename
    if filename != found_entryname and parent_dir_id == found_parent_dir_id:
      db_accessor.rename_file_by_entry_id(entry_id, filename)
      return
    # 3.2 move it across folders
    if parent_dir_id != found_parent_dir_id:
      db_accessor.move_file_to_folder_by_entry_id(entry_id, parent_dir_id, filename)
      return

    raise Exception, "Inconsistency. One of the 3 hypotheses in process_filename_on_folder() was not logically caught. It may be a program error."


  def process_filenames_on_folder(self, current_abs_dirpath, filenames):
    '''

    :param current_abs_dirpath:
    :param filenames:
    :return:
    '''
    print 'process_filenames_on_folder:', current_abs_dirpath
    parent_dir_id = self.find_entry_id_for_dirpath(current_abs_dirpath)
    if parent_dir_id == None:
      raise Exception, 'Inconsistency: failed to retrieve parent_dir_id in Sha1FileSystemCompleter. Please check database is online.'
    for filename in filenames:
      self.add_or_update_filename_on_folder(filename, parent_dir_id, current_abs_dirpath)


if __name__ == '__main__':
  main()
