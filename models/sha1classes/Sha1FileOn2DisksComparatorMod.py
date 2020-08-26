#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Sha1FileOn2DisksComparatorMod.py
'''
import os, sys # shutil

from .SourceAndTargetBaseDirsKeeperMod import SourceAndTargetBaseDirsKeeper
#import local_settings as ls
from fs.hashfunctions.hexfunctionsmod import generate_a_40char_random_hex
#from sha1utils.osutilsMod import verify_path_existence_then_raise_or_return_with_trailing_slash

class Sha1FilePlusBaseAndRelPaths(object):
  '''
  This class is not yet in use
  '''
  
  def __init__(self, filename, basepath, relpath):
    self.filename = filename 
    self.basepath = basepath 
    self.relpath  = relpath
    


class Sha1FileOn2DisksComparator(object):

  def __init__(self, sha1hex):
    self.source_filename_and_its_relpath_tuple_list = []
    self.target_filename_and_its_relpath_tuple_list = []
    self.sha1hex  = sha1hex

#===============================================================================
#   def set_source_basepath(self, source_basepath):
#     # THIS ATTRIBUTE belongs to the static class SourceAndTargetBaseDirsKeeper 
#     self.source_basepath = source_basepath
# 
#   def set_target_basepath(self, target_basepath):
#     # THIS ATTRIBUTE belongs to the static class SourceAndTargetBaseDirsKeeper 
#     self.target_basepath = target_basepath
#===============================================================================

  def add_source_filename_and_its_relpath_tuple(self, source_filename_and_its_relpath_tuple):
    if source_filename_and_its_relpath_tuple in self.source_filename_and_its_relpath_tuple_list:
      return
    self.source_filename_and_its_relpath_tuple_list.append(source_filename_and_its_relpath_tuple)

  def add_target_filename_and_its_relpath_tuple(self, target_filename_and_its_relpath_tuple):
    if target_filename_and_its_relpath_tuple in self.target_filename_and_its_relpath_tuple_list:
      return
    self.target_filename_and_its_relpath_tuple_list.append(target_filename_and_its_relpath_tuple)

  def get_source_filename_and_its_relpath_tuple_list(self):
    return self.source_filename_and_its_relpath_tuple_list

  def get_source_filename_and_its_abspath_tuple_list(self):
    for source_filename_and_its_relpath_tuple in self.source_filename_and_its_relpath_tuple_list:
      relpath = source_filename_and_its_relpath_tuple[1]
      abspath


  def get_source_filename_and_its_abspath_tuple_list(self):
    source_filename_and_its_abspath_tuple_list = []
    for tlist in self.get_source_filename_and_its_relpath_tuple_list():
      filename, relpath = tlist
      abspath = os.path.join(SourceAndTargetBaseDirsKeeper.get_source_basepath(), relpath)
      source_filename_and_its_abspath_tuple_list.append((filename, abspath))
    return source_filename_and_its_abspath_tuple_list

  def get_target_filename_and_its_abspath_tuple_list(self):
    target_filename_and_its_abspath_tuple_list = []
    for tlist in self.get_target_filename_and_its_relpath_tuple_list():
      filename, relpath = tlist
      abspath = os.path.join(SourceAndTargetBaseDirsKeeper.get_target_basepath(), relpath)
      target_filename_and_its_abspath_tuple_list.append((filename, abspath))
    return target_filename_and_its_abspath_tuple_list

  def get_target_filename_and_its_relpath_tuple_list(self):
    return self.target_filename_and_its_relpath_tuple_list

  def get_both_source_and_target_filename_and_its_relpath_tuple_list(self):
    return self.get_source_filename_and_its_relpath_tuple_list() + self.get_target_filename_and_its_relpath_tuple_list()

  def get_both_source_and_target_filename_and_its_abspath_tuple_list(self):
    return self.get_source_filename_and_its_abspath_tuple_list() + self.get_target_filename_and_its_abspath_tuple_list()

  def get_source_folder_abspath(self):
    if len(self.source_filename_and_its_relpath_tuple_list) != 1:
      return None
    source_filename_and_its_relpath_tuple = self.source_filename_and_its_relpath_tuple_list[0]
    _, source_relpath = source_filename_and_its_relpath_tuple
    source_folder_abspath = os.path.join(SourceAndTargetBaseDirsKeeper.get_source_basepath(), source_relpath)
    return source_folder_abspath

  def get_source_filename(self):
    if len(self.source_filename_and_its_abspath_tuple_list) != 1:
      return None
    source_filename, _ = self.source_filename_and_its_abspath_tuple_list[0]
    return source_filename

  def get_source_file_abspath(self):
    source_folder_abspath = self.get_source_folder_abspath()
    if source_folder_abspath == None:
      return None
    # if source_folder_abspath exists, so will source_filename (ie, a None or IndexError will not happen below)
    return os.path.join(source_folder_abspath, self.get_source_filename())

  def get_target_folder_abspath(self):
    if len(self.target_filename_and_its_relpath_tuple_list) != 1:
      return None
    target_filename_and_its_relpath_tuple = self.target_filename_and_its_relpath_tuple_list[0]
    _, target_relpath = target_filename_and_its_relpath_tuple
    target_folder_abspath = os.path.join(SourceAndTargetBaseDirsKeeper.get_target_basepath(), target_relpath)
    return target_folder_abspath

  def get_target_filename(self):
    if len(self.target_filename_and_its_abspath_tuple_list) != 1:
      return None
    target_filename, _ = self.target_filename_and_its_abspath_tuple_list[0]
    return target_filename

  def get_target_file_abspath(self):
    target_folder_abspath = self.get_target_folder_abspath()
    if target_folder_abspath == None:
      return None
    # if target_folder_abspath exists, so will target_filename (ie, a None or IndexError will not happen below)
    return os.path.join(target_folder_abspath, self.get_target_filename())

  def delete_file_on_target(self):
    target_file_abspath = self.get_target_file_abspath()
    if target_file_abspath == None:
      return
    print('Deleting:')
    print('From:', target_file_abspath) 
    # os.remove(target_file_abspath)

  def get_target_basepath(self):
    return SourceAndTargetBaseDirsKeeper.get_target_basepath()

  def get_source_relpath(self):
    source_abspath = self.get_source_abspath()
    if source_abspath == None:
      return None
    source_basepath = SourceAndTargetBaseDirsKeeper.get_source_basepath()
    source_relpath = source_abspath [ len(source_basepath) : ]
    return source_relpath 
  
  def get_target_abspath_that_is_source_equivalent(self):
    '''
    This method only makes sense when there is only 
      ONE target file on a different middlepath position.
    
    It doesn't make sense for multiple target files because the intended move operation
      would not know which one to move 
    '''
    source_relpath = self.get_source_relpath()
    if source_relpath == None:
      return None
    source_equivalent_target_abspath = os.path.join(self.get_target_basepath(), source_relpath)
    return source_equivalent_target_abspath

  def move_target_file_on_target_filesystem(self):
    old_file_abspath = self.get_target_file_abspath()
    if old_file_abspath == None:
      self.copy_source_file_to_target()
      return
    moving_dir_abspath = self.get_target_abspath_that_is_source_equivalent()
    if moving_dir_abspath == None:
      return
    # a rename may happen here, for the to-be name is the source filename itself, not the target filename if different
    source_filename = self.get_source_filename()
    moving_file_abspath = os.path.join(moving_dir_abspath, source_filename)
    if os.path.isfile(moving_file_abspath):
      raise OSError('Failed to move %s because there is also a same-name file there.' %moving_file_abspath)
      
    print('Moving file with sha1 =', self.sha1hex)
    print('From:', old_file_abspath)
    print('To:',   moving_file_abspath)
    # shutil.move(old_file_abspath, move_dir_abspath)
    # update attributes?

  def copy_source_file_to_target(self):
    source_file_abspath = self.get_source_file_abspath()
    if source_file_abspath == None:
      raise ValueError('source_file_abspath does not exist.')
    move_dir_abspath = self.get_target_abspath_that_is_source_equivalent()
    print('Copying:')
    print('From:', source_file_abspath)
    print('To:',   move_dir_abspath)
    # shutil.copy2(source_file_abspath, move_dir_abspath)

  def equalize_if_needed(self):
    if len(self.source_filename_and_its_abspath_tuple_list) > 1:
      # this is a process-problem, ie, either the clean-up process has not been run, or these files are small (there is no solution for this case, for the time being)
      # raise RepeatsMustBeCleanedUpBeforeRunningEqualization, 'Please, run the repeats clean-up processing before running this 2-tree equalization process.'
      return
    if self.source_filename == None:
      self.delete_file_on_target()
      return
    if self.target_filename == None:
      self.copy_source_file_to_target()
      return
    # the two are not None, ie, they exist
    move_dir_abspath = self.get_target_abspath_that_is_source_equivalent()
    # TEST THIS!
    worktargetpath = self.target_abspath
    if not worktargetpath.endswith('/'):
      worktargetpath = worktargetpath + '/'
    if worktargetpath != move_dir_abspath:
      self.move_target_file_on_target_filesystem()
      return
    print('Nothing to be done, both source and target exist and are the same.')
    print('Source:', self.get_source_file_abspath())
    print('Target:', self.get_target_file_abspath())

  def get_n_of_repeats_on_source(self):
    n_of_repeats = len(self.source_filename_and_its_abspath_tuple_list) - 1 
    return n_of_repeats

  def get_n_of_repeats_on_target(self):
    n_of_repeats = len(self.target_filename_and_its_abspath_tuple_list) - 1 
    return n_of_repeats

  def print_files_with_same_sha1_on_source(self):
    if self.get_n_of_repeats_on_source() > 0:
      for source_filename_and_its_abspath_tuple in self.source_filename_and_its_abspath_tuple_list:
        source_filename, source_abspath = source_filename_and_its_abspath_tuple
        size = len(self.source_filename_and_its_abspath_tuple_list)
        print(size, self.sha1hex, source_filename, source_abspath) 

  def copy(self):
    copyobj = Sha1FileOn2DisksComparator(self.sha1hex)
    copyobj.source_filename_and_its_relpath_tuple_list = self.source_filename_and_its_relpath_tuple_list
    copyobj.target_filename_and_its_relpath_tuple_list = self.target_filename_and_its_relpath_tuple_list
    return copyobj
    
  def __eq__(self, other):
    if type(other) != Sha1FileOn2DisksComparator:
      return False
    if self.sha1hex != other.sha1hex:
      return False
    if len(self) != len(other):
      return False
    source_and_target_tuple_list = self.get_both_source_and_target_filename_and_its_relpath_tuple_list()[:]
    for source_and_target_tuple in other.get_both_source_and_target_filename_and_its_relpath_tuple_list():
      if source_and_target_tuple in source_and_target_tuple_list:
        source_and_target_tuple_list.remove(source_and_target_tuple)
    if len(source_and_target_tuple_list) > 0:
      return False
    return True

  def __len__(self):
    return len(self.source_filename_and_its_relpath_tuple_list) + len(self.target_filename_and_its_relpath_tuple_list)

  def __str__(self):
    outstr = '''sha1hex = %s
    Contents:\n''' %self.sha1hex
    for source_filename_and_its_relpath_tuple in self.source_filename_and_its_relpath_tuple_list:
      filename, relpath = source_filename_and_its_relpath_tuple
      outstr += '%s [in] %s \n' %(filename, relpath)
    return outstr



import unittest
class TestCaseModWide(unittest.TestCase):
  
  def setUp(self):
    self.fixed_sha1hex = generate_a_40char_random_hex() 
    self.sha1FileOn2DisksComparator = None
    SourceAndTargetBaseDirsKeeper.set_source_basepath('/a/b/', allowNonExistent=True)

  def test_1_simple_set_and_set(self):
    self.sha1FileOn2DisksComparator = Sha1FileOn2DisksComparator(self.fixed_sha1hex)
    source_filename_and_its_relpath_tuple = ('file1.fil', 'c/d/e/')
    self.sha1FileOn2DisksComparator.add_source_filename_and_its_relpath_tuple(source_filename_and_its_relpath_tuple)
    source_filename_and_its_relpath_tuple_list = [source_filename_and_its_relpath_tuple]
    self.assertEqual(source_filename_and_its_relpath_tuple_list, self.sha1FileOn2DisksComparator.get_source_filename_and_its_relpath_tuple_list())
    
  def test_2_complete_abspath(self):
    self.sha1FileOn2DisksComparator = Sha1FileOn2DisksComparator(self.fixed_sha1hex)
    source_filename_and_its_relpath_tuple = ('file1.fil', 'c/d/e/')
    self.sha1FileOn2DisksComparator.add_source_filename_and_its_relpath_tuple(source_filename_and_its_relpath_tuple)
    basepath = SourceAndTargetBaseDirsKeeper.get_source_basepath()
    source_filename_and_its_abspath_tuple_list = [('file1.fil', os.path.join(basepath, 'c/d/e/'))]
    self.assertEqual(source_filename_and_its_abspath_tuple_list, self.sha1FileOn2DisksComparator.get_source_filename_and_its_abspath_tuple_list())
    SourceAndTargetBaseDirsKeeper.set_source_basepath('/x/y/', allowNonExistent=True)
    self.assertNotEqual(source_filename_and_its_abspath_tuple_list, self.sha1FileOn2DisksComparator.get_source_filename_and_its_abspath_tuple_list())
    
    
def unittests():
  unittest.main()

def process():
  '''
  '''
  pass

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()  
  process()
