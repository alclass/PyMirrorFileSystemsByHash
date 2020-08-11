#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Sha1FileSystemComplementer.py
Explanation:
  This script contains class Sha1FileSystemOnFolderComplementer().
  This class models a folder containing files and subfolders.
  These files and subfolders are read from each folder on the actual target file system
    and the sha1hex for each file is, together with the former info, stored in a database.

  As for now, this database is a SQLite file staged on the device's root folder.

  The reads and writes to the db itself is done
    by class DBAccessor in module sqlite_accessor_mod,
    within the same __init__ package to this script.

  2015-01-13 Luiz Lewis: Written first sketch
  2015-01-18 Luiz Lewis: Improved to the concept in the description above mentioned.
'''
import hashlib
import os

# from db import sqlite_accessor_mod as db_accessor_mod


class SHA1_NOT_OBTAINED(Exception):
  pass

def calculate_sha1hex_from_file(file_abspath):
  '''

  :param file_abspath:
  :return:
  '''
  sha1obj = hashlib.sha1()
  try:
    f = open(file_abspath, 'r')
    sha1obj.update(f.read())
    sha1hex = sha1obj.hexdigest()
  except Exception:
    error_msg = 'Could not calculate the SHA1 hash for file: [%s]' %file_abspath
    raise SHA1_NOT_OBTAINED(error_msg)
  return sha1hex


class Sha1FileSystemOnFolderComplementer(object):

  def __init__(self, DEVICE_PREFIX_ABSPATH):
    '''

    :param DEVICE_PREFIX_ABSPATH:
    :return:
    '''
    if not os.path.isdir(DEVICE_PREFIX_ABSPATH):
      error_msg = 'DEVICE_PREFIX_ABSPATH [%s] is not valid.' %DEVICE_PREFIX_ABSPATH
      raise Exception(error_msg)
    self.DEVICE_PREFIX_ABSPATH = DEVICE_PREFIX_ABSPATH
    self.db_accessor = db_accessor_mod.DBAccessor(self.DEVICE_PREFIX_ABSPATH)

  def get_dirnames_on_db_with_same_parent_id(self):
    '''

    :return:
    '''
    return self.db_accessor.get_dirnames_on_db_with_same_parent_id(self.parent_dir_id)


  def verify_folder_exclusion_from_db(self, dirnames):
    '''

    :param dirnames:
    :return:
    '''
    dirnames_on_db_to_be_excluded = []
    for dirname_on_db in dirnames_on_db_to_be_excluded:
      if dirname_on_db not in dirnames:
        dirnames_on_db_to_be_excluded.append(dirname_on_db)
    self.db_acessor.exclude_dirnames_on_parent_dir_id(dirnames_on_db_to_be_excluded, self.parent_dir_id)

  def verify_folder_inclusion_into_db(self, dirnames):
    '''

    :param dirnames:
    :return:
    '''
    dirnames_on_db = self.get_dirnames_on_db_with_same_parent_id()
    dirnames_needing_inclusion = []
    for dirname in dirnames:
      if dirname in dirnames_on_db:
        continue
      dirnames_needing_inclusion.append(dirname)
    self.db_acessor.insert_dirnames_with_parent_dir_id(dirnames_needing_inclusion, self.parent_dir_id)

  def verify_add_or_update_folders_on_parent(self, dirnames):
    '''

    :param dirnames:
    :return:
    '''
    self.verify_folder_inclusion_into_db(dirnames)
    self.verify_folder_exclusion_from_db(dirnames)


  def calculate_sha1hex_from_file(self, current_abs_dirpath, filename):
    '''

    :param current_abs_dirpath:
    :param filename:
    :return:
    '''
    file_abspath = os.path.join(current_abs_dirpath, filename)
    sha1hex = calculate_sha1hex_from_file(file_abspath)
    return sha1hex

  def verify_add_or_update_file_on_parent(self, filename):
    '''

    :param filename:
    :return:
    '''
    sha1hex = self.calculate_sha1hex_from_file(filename)  # current_abs_dirpath is local to the class
    entry_id, entryname_found, parent_dir_id_found = self.db_accessor.fetch_record_if_sha1hex_exists(sha1hex)

    # 1st hypothesis: file is already there, nothing to be done
    if filename == entryname_found and self.parent_dir_id == parent_dir_id_found:
      # nothing to be done
      return

    # 2nd hypothesis: file has not entered db yet, insert it there
    if entry_id == None:
      entry_id = self.db_accessor.insert_filename_on_parent(filename, self.parent_dir_id, sha1hex)
      return

    # 3rd hypothesis: file exists, but (3.1) it may have a different name or (3.2) be somewhere else having or not the same name
    # 3.1 rename it to filename
    if filename != entryname_found and self.parent_dir_id == parent_dir_id_found:
      self.db_accessor.rename_file_by_entry_id(entry_id, filename)
      return
    # 3.2 move it across folders
    if self.parent_dir_id != parent_dir_id_found:
      self.db_accessor.move_file_to_folder_by_entry_id(entry_id, self.parent_dir_id, filename)
      return
    error_msg = "Inconsistency. One of the 3 hypotheses in process_filename_on_folder() was not logically caught. It may be a program error."
    raise Exception(error_msg)

  def verify_add_or_update_files_on_parent(self, filenames):
    '''

    :param filenames:
    :return:
    '''
    for filename in filenames:
      self.verify_add_or_update_file_on_parent(filename)

  def verify_add_or_update_files_and_folders_on_dirpath(self, current_abs_dirpath, dirnames, filenames):
    self.current_abs_dirpath = current_abs_dirpath
    parent_dir_id = self.find_entry_id_for_dirpath(current_abs_dirpath)
    if parent_dir_id == None:
      error_msg = 'Inconsistency: failed to retrieve parent_dir_id in Sha1FileSystemCompleter. Please check database is online.'
      raise Exception(error_msg)
    self.parent_dir_id = parent_dir_id
    self.verify_add_or_update_files_on_parent(filenames)
    self.verify_add_or_update_folders_on_parent(dirnames)

def main():
  '''

  :return:
  '''
  # tests to do
  pass

if __name__ == '__main__':
  main()
