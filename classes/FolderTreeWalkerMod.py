#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
ShaModel.py


  Written on 2016-02-15 Luiz Lewis
'''
import datetime
import os
import ShaModel
import DBSha1ModelMod

class FolderTreeWalker(object):
  '''
  This class models the following processing:
    - it starts from a relative root/top folder
    - it 'walks' down the folder tree
    - as it 'walks' down, it registers the sha1hex of files into a db
    - it also records the folders themself, but folders do not have yet a sha1 here
  Obs.:
    - this 'db' is generally a sqlite file placed at the relative root
    - the sha1hex accumulated is used later on to help check whether or not a copy
        exists elsewhere (the check/moving/copying is done by class ShaModel)
  '''

  def __init__(self, abs_device_basepath, relative_top_basepath=None):
    '''

    :param abs_device_basepath:
    :param relative_top_basepath:
    :return:
    '''
    if not os.path.isdir(abs_device_basepath):
      raise OSError('Path [%s] does not exist.' %abs_device_basepath)
    self.abs_device_basepath = abs_device_basepath
    self.update_abs_device_basepath_if_needed(relative_top_basepath)
    self.abs_current_walkpath = None  # just to give the IDE a hint it's here, its value is put during 'folder-walk'
    self.db_model = DBSha1Model(self.abs_device_basepath)

  def update_abs_device_basepath_if_needed(self, relative_top_basepath):
    '''

    :param relative_top_basepath:
    :return:
    '''
    try_this_folder = os.path.join(self.abs_device_basepath, relative_top_basepath)
    if os.path.isdir(try_this_folder):
      self.abs_device_basepath = try_this_folder

  @property
  def relative_current_walkpath(self):
    '''
    This property/attribute is derived, ie, it's taken from the TWO others below
    :return:
    '''
    return self.abs_current_walkpath[ len(self.abs_device_basepath) : ]

  def process_folder(self, dirnames, filenames):
    '''

    :param self:
    :param dirnames:
    :param filenames:
    :return:
    '''
    relative_parent_path = self.take_relative_parent_path()
    for filename in filenames:
      # calc sha1
      sha1hex = 1; modified_datetime = 1
      sha1item = ShaModel.ShaItem(
        sha1hex,  \
        filename, \
        self.relative_current_walkpath, \ # class @property/attribute that is derived from base_folder and abs_current_folder
        self.abs_device_basepath,       \ # for device_and_middle_path,
        modified_datetime, \
        filesize,
      )
      self.db_model.insert(sha1item)


  def walk_top_down(self):
    for self.abs_device_dirpath, dirnames, filenames in os.walk(self.abs_device_basepath):
      # abs_device_dirpath = os.path.join(self.abs_device_basepath, rel_dirpath)
      self.process_folder(dirnames, filenames)


def main():

  abs_device_basepath = os.path.abspath('.')
  folder_tree_walker = FolderTreeWalker(abs_device_basepath)
  folder_tree_walker.walk_top_down()


if __name__ == '__main__':
  main()
