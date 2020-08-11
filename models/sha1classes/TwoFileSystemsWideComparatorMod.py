#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
TwoFileSystemsWideComparatorMod.py
'''
import os, sys

import __init__
# import local_settings as ls
from SourceAndTargetBaseDirsKeeperMod import SourceAndTargetBaseDirsKeeper
from  Sha1ContentsDictMod import Sha1ContentsDict    
from  Sha1XMLReaderWriterMod import Sha1XMLReader

class TwoFileSystemsWideComparator(object):

  SOURCE = 1
  TARGET = 2

  def __init__(self, source_basepath, target_basepath=None):
    self.n_of_dir_walks = 0
    self.current_dirpath = None
    self.all_sha1s_dict = Sha1ContentsDict()
    self.init_filesystems_rootpaths(source_basepath, target_basepath)

  def init_filesystems_rootpaths(self, source_basepath, target_basepath=None):
    SourceAndTargetBaseDirsKeeper.set_source_basepath(source_basepath)
    if target_basepath != None:
      SourceAndTargetBaseDirsKeeper.set_target_basepath(target_basepath)
    
  def fetch_all_files_on_both_disks(self):
    for which_base_path in [self.SOURCE, self.TARGET]:
      self.fetch_all_files_from_basepath_uptree(which_base_path)
      
  def fetch_all_files_from_basepath_uptree(self, which_base_path = None):
    '''
    '''
    if which_base_path == None:
      which_base_path = self.SOURCE

    if which_base_path == self.SOURCE:
      basepath = SourceAndTargetBaseDirsKeeper.get_source_basepath()
    elif which_base_path == self.TARGET:
      basepath = SourceAndTargetBaseDirsKeeper.get_target_basepath()
    else:
      raise ValueError, 'ErrorInParameter which_base_path (=%s) in method fetch_all_files_from_basepath_uptree() [either SOURCE=1 or TARGET=2]' %str(which_base_path) 
    
    for self.current_dirpath, _, _ in os.walk(basepath): # dirnames, filenames
      self.n_of_dir_walks +=1
      # print self.n_of_dir_walks, 'chdir', self.current_dirpath
      try:
        sha1_XML_reader = Sha1XMLReader(self.current_dirpath)
        sha1_XML_reader.read_and_update_if_needed()
        self.sha1sum_and_filenames_dict = sha1_XML_reader.get_sha1sum_and_filenames_dict()
        self.update_all_sha1s_dict(which_base_path)
      except Exception:
        return

  def update_all_sha1s_dict(self, which_base_path = None):
    '''
    '''
    if which_base_path == None:
      which_base_path = self.SOURCE
    sha1hexs = self.sha1sum_and_filenames_dict.keys()
    for sha1hex in sha1hexs:
      # print sha1hex
      sha1FileComparator = self.all_sha1s_dict[sha1hex]
      filenames = self.sha1sum_and_filenames_dict[sha1hex]
      
      for filename in filenames:
      
        file_abspath = os.path.join(self.current_dirpath, filename)
        if not os.path.isfile(file_abspath):
          raise OSError, 'Cannot continue because XML sha1 repo is out of date (non-existing file: %s)' %file_abspath
        
        # print 'Sha1', sha1hex, filename, self.current_dirpath
        
        if which_base_path == self.SOURCE:
          filename_and_its_abspath = (filename, self.current_dirpath)
          sha1FileComparator.add_source_filename_and_its_abspath_tuple(filename_and_its_abspath)
        elif which_base_path == self.TARGET:
          filename_and_its_abspath = (filename, self.current_dirpath)
          sha1FileComparator.add_target_filename_and_its_abspath_tuple(filename_and_its_abspath)
            
  def equalize_all_files_on_both_disks(self):
    self.fetch_all_files_on_both_disks()
    sha1hexs = self.all_sha1s_dict.keys()
    for sha1hex in sha1hexs:
      sha1FileComparator = self.all_sha1s_dict[sha1hex]
      sha1FileComparator.equalize_if_needed()

  def pickup_equal_files_on_disk(self):
    self.all_sha1s_dict.keys()
    sha1hexs = self.all_sha1s_dict.keys()
    for sha1hex in sha1hexs:
      sha1FileComparator = self.all_sha1s_dict[sha1hex]
      if sha1FileComparator.get_n_of_repeats() > 0:
        sha1FileComparator.print_files_with_same_sha1()
        
  def total_sha1s(self):
    return len(self.all_sha1s_dict)
  
  def print_summary(self):
    print self.all_sha1s_dict
          
  def __str__(self):
    outstr = 'Total of sha1s: %d' %self.total_sha1s()
    return outstr
    
  

import unittest
class Test1(unittest.TestCase):
  
  def test_1(self):
    pass

def unittests():
  unittest.main()

def process():
  source_basepath = sys.argv[1]
  target_basepath = sys.argv[2]
  wideComparator = TwoFileSystemsWideComparator(source_basepath, target_basepath)
  wideComparator.equalize_all_files_on_both_disks()

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()
