#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
ShaModel.py


  Written on 2016-02-15 Luiz Lewis
'''
import datetime
import os
import ShaModel
import db.db_proxy_mod as dbproxy

class FolderTreeWalker(object):
  '''
  This class models the following processing:
    - it starts from a relative root/top folder
    - it 'walks' down the folder tree
    - as it 'walks' down, it registers the sha1hex of files into a db
    - it also records the folders themselves, but folders do not have yet a sha1 here
  Obs.:
    - this 'db' is generally a sqlite file placed at the relative root
    - the sha1hex accumulated is used later on to help check whether or not a copy
        exists elsewhere (the check/moving/copying is done by class ShaModel)

    Explanation on the TWO paths in this system, ie:

    1) sqlite data file path
    2) the device_n_middle_abspath that marks the starting top path for the relative path recordings.

    Thus, there are 2 path positions that this system need to use and known.
    One (the sqlite path) is the path where the sqlite data file resides. In general,
      this data file resides at the top basepath of the relative paths.

    The paths recorded in the database are not the absolute paths,
      they are paths relative to a chosen top (known as the device_n_middle_abspath,
      the 2nd kind of path here).

    This device_n_middle_abspath is the path from which the down-tree will be db-recorded.

    In general, the TWO paths are the same, ie, the path in which the sqlite is located is also
      the ROOT-TOP-PATH from where the down-relative paths are db-recorded.

    Notice that the sqlite database may well be somewhere else but in this case there must be some
      kind of marker in the ROOT-TOP-PATH (or the app must have it) telling where this sqlite is,
      otherwise the relative paths will be lost in the sqlite db useless.

    Because of that, one path tells where the sqlite is. The other is the device_n_middle_abspath
      on which the starting top path resides and marks from where the relative paths were derived.

    Let's see it via an example.

    Suppose the relative top is the following:

    /media/user1/EXT_HD_DISK/videos/maths

    Suppose further that inside 'maths' there are other folders and from them subfolders and so forth,
      such as:

    ./calculus/course1
    ./calculus/course1/videolectures
    ./geometry/great_course/
    ./geometry/great_course/general_videos
    ./algebra/history/documentaries/fromTV/EnglishDocs/
    etc.

    As said above, to the sqlite database, /media/user1/EXT_HD_DISK/videos/maths is unknown.
      The only paths known are the relative paths listed above (./calculus/course1 and so on).

    So the ./algebra/history/documentaries/fromTV/EnglishDocs/ path must be joint to the
      /media/user1/EXT_HD_DISK/videos/maths (which is, in this case, the device_n_middle_abspath).

    The resulting full os-path is:

    ---------------------------------------
    /media/user1/EXT_HD_DISK/videos/maths/algebra/history/documentaries/fromTV/EnglishDocs/
    ---------------------------------------

    When the back-up functions take place, this above joint path will be compared to a target
      path somewhere else, perhaps in a different mounted EXT_HD_DISK, for example:

    /media/user1/EXT_HD_BACKUP/videos/great_material/algebra/history/documentaries/fromTV/EnglishDocs/

    The relative path: /algebra/history/documentaries/fromTV/EnglishDocs/ is the same among
      original DISK and target DISK, but the device_n_middle_abspath is always different.

  '''

  def __init__(self, \
               device_n_middle_abspath=None, \
               p_sqlite_db_filepath=None, \
               relative_further_down_top_basepath_if_any=None):
    '''
    The __init__() constructor

    :param device_n_middle_abspath:
    :param relative_further_down_top_basepath_if_any:
    :return:
    '''
    if device_n_middle_abspath == None:
      device_n_middle_abspath = os.path.abspath('.')

    if not os.path.isdir(device_n_middle_abspath):
      raise OSError('Path [%s] does not exist.' % device_n_middle_abspath)

    self.device_n_middle_abspath = device_n_middle_abspath
    self.current_walk_abspath = None  # just to give the IDE a hint it's here, its value is put during 'folder-walk'
    dbms_params_dict = {}
    sqlite_db_filepath = None
    if p_sqlite_db_filepath <> None:
      if os.path.isdir(p_sqlite_db_filepath):
        sqlite_db_filepath = p_sqlite_db_filepath
    if sqlite_db_filepath <> None:
      dbms_params_dict['sqlite_db_filepath'] = self.sqlite_db_filepath
    else:
      dbms_params_dict['sqlite_db_filepath'] = self.device_n_middle_abspath

    self.dbproxier = dbproxy.DBProxier(dbms_params_dict)

  @property
  def top_bottom_relative_current_walk_ossep_prefixed(self):
    '''
    This property/attribute is derived, ie, it's taken from the TWO others below
    The well functioning of this property depends on correct setting of
      'passing' self.current_walk_abspath (it's generated in the os.walk())
    :return:
    '''

    relativepath_ossep_prefixed = self.current_walk_abspath[len(self.device_n_middle_abspath):]
    if not relativepath_ossep_prefixed.startswith(os.path.sep):
      relativepath_ossep_prefixed = os.path.sep + relativepath_ossep_prefixed
    return relativepath_ossep_prefixed

  def insert_folder(self, top_bottom_relative_folderpath):
    '''
    '''
    self.dbproxier.insert_folder(top_bottom_relative_folderpath)

  def insert_folders(self, dirnames):
    '''
    '''
    for foldername in dirnames:
      top_bottom_relative_folderpath = os.path.join(self.top_bottom_relative_current_walk_ossep_prefixed, foldername)
      self.insert_folder(top_bottom_relative_folderpath)

  def insert_files(self, filenames):
    '''

    self.relative_from_top_filepath         = relative_from_top_filepath
    self.device_dependent_absolute_filepath = device_dependent_absolute_filepath

               filename,
               sha1hex,
               relative_from_repotop_filepath,
               device_dependent_absolute_filepath,
               filesize,
               modified_datetime,
               mockmode = False):


    :param self:
    :param dirnames:
    :param filenames:
    :return:
    '''
    for filename in filenames:
      # calc sha1
      relative_from_repotop_filepath_ossep_prefixed = os.path.join(self.top_bottom_relative_current_walk_ossep_prefixed, filename)
      relative_from_repotop_filepath_not_ossep_prefixed = relative_from_repotop_filepath_ossep_prefixed[1:]
      device_dependent_absolute_filepath = os.path.join(self.device_n_middle_abspath, relative_from_repotop_filepath_not_ossep_prefixed)

      sha1hex = '0123456789'*4; modified_datetime = '2012-12-12 12:12:12'; filesize = 1000
      sha1file = ShaModel.ShaItem(
        filename, \
        sha1hex, \
        relative_from_repotop_filepath_ossep_prefixed, \
        device_dependent_absolute_filepath,     \
        filesize,
        modified_datetime \
      )
      conventioned_filedict = sha1file.conventioned_filedict
      self.dbproxier.insert_file(conventioned_filedict)

  def check_folders_inserting_missing_n_deleting_nonexisting(self, dirnames):
    foldernames_to_insert = dirnames[:]
    foldernames_to_delete = []
    foldernames_in_db = self.dbproxier.fetch_foldernames_in_reporelativehomedirpath(self.top_bottom_relative_current_walk_ossep_prefixed)
    for foldername_in_db in foldernames_in_db:
      if foldername_in_db in foldernames_to_insert:
        foldernames_to_insert.remove(foldername_in_db)
      else:
        foldernames_to_delete.append(foldername_in_db)
    self.insert_folders(foldernames_to_insert)
    self.delete_folders(foldernames_to_delete)

  def check_files_inserting_missing_n_deleting_nonexisting(self, filenames):
    filenames_to_insert = filenames[:]
    filenames_to_delete = []
    filenames_in_db = self.dbproxier.fetch_filenames_in_reporelativehomedirpath(self.top_bottom_relative_current_walk_ossep_prefixed)
    for filename_in_db in filenames_in_db:
      if filename_in_db in filenames_to_insert:
        filenames_to_insert.remove(filename_in_db)
      else:
        filenames_to_delete.append(filename_in_db)
    self.insert_folders(filenames_to_insert)
    self.delete_folders(filenames_to_delete)

  def check_db_against_fstopdowntree(self):
    '''
    '''
    for self.current_walk_abspath, dirnames, filenames in os.walk(self.device_n_middle_abspath):
      self.check_folders_inserting_missing_n_deleting_nonexisting(dirnames)
      self.check_folders_inserting_missing_n_deleting_nonexisting(filenames)


  def walk_top_down(self):
    for self.current_walk_abspath, dirnames, filenames in os.walk(self.device_n_middle_abspath):
      self.insert_folders(dirnames)
      self.insert_files(filenames)


def main():

  device_n_middle_abspath = os.path.abspath('.')
  folder_tree_walker = FolderTreeWalker(device_n_middle_abspath)
  folder_tree_walker.walk_top_down()


if __name__ == '__main__':
  main()
