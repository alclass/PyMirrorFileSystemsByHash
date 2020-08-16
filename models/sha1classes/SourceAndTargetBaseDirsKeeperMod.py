#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''

'''
import os, sys
#import local_settings as ls
from fs.os.osutilsMod import verify_path_existence_then_raise_or_return_with_trailing_slash
from Sha1XMLReaderWriterMod import Sha1XMLReader

class SourceAndTargetBaseDirsKeeper: # (object)
  '''
  This is a 'structure-like' class, ie, it's a static class that holds only 2 attributes,
  they are:
    + source_basepath = absolute directory path related to 'source'
    + target_basepath = absolute directory path related to 'target'
  '''
  source_basepath = None
  target_basepath = None
  
  SHA1HEX_INEXISTS_THRU_TARGET_DIRTREE = 0
  SHA1HEX_EXISTS_BUT_NOT_ON_THE_SAME_RELPATH_ON_TARGET = 1
  SHA1HEX_EXISTS_ON_THE_SAME_RELPATH_ON_TARGET = 2

  target_sha1hex_dict = None
  
  @classmethod
  def get_source_basepath(cls):
    if cls.source_basepath == None:
      raise OSError, 'An error has occurred: source_basepath (the absolute directory path related to "source") was not initialized.'
    return cls.source_basepath

  @classmethod
  def get_target_basepath(cls):
    if cls.target_basepath == None:
      raise OSError, 'An error has occurred: target_basepath (the absolute directory path related to "target") was not initialized.'
    return cls.target_basepath

  @classmethod
  def set_source_basepath(cls, basepath, allowNonExistent=False):
    # raise OSError, 'An error has occurred: directory for source_basepath (%s) does not exist.' %source_basepath
    # the function verify_path_existence_then_raise_or_return_with_trailing_slash() may RAISE OSError
    if allowNonExistent:
      cls.source_basepath = basepath
      return
    cls.source_basepath = verify_path_existence_then_raise_or_return_with_trailing_slash(basepath)

  @classmethod
  def set_target_basepath(cls, basepath, allowNonExistent=False):
    #  raise OSError, 'An error has occurred: directory for target_basedir (%s) does not exist.' %target_basepath
    # the function verify_path_existence_then_raise_or_return_with_trailing_slash() may RAISE OSError
    if allowNonExistent:
      cls.target_basepath = basepath
      return
    cls.target_basepath = verify_path_existence_then_raise_or_return_with_trailing_slash(basepath)
    
  @classmethod
  def clear_source_basepath(cls):
    cls.source_basepath = None

  @classmethod
  def clear_target_basepath(cls):
    cls.target_basepath = None

  @classmethod
  def init_sha1hex_dict(cls):
    cls.target_sha1hex_dict = {}
    for dirpath, _, _ in os.walk(cls.get_target_basepath()):
      sha1reader = Sha1XMLReader(dirpath)
      for sha1hex in sha1reader.get_sha1sum_and_its_filenames_dict():
        filenames = sha1reader.get_sha1sum_and_its_filenames_dict()[sha1hex]
        for filename in filenames: 
          file_abspath = os.path.join(dirpath, filename)
          if sha1hex in cls.target_sha1hex_dict:
            raise IndexError, 'sha1hex has ambiguity in target. Process must be manual in this case. Data: %s = %s' %(sha1hex, file_abspath) 
          cls.target_sha1hex_dict[sha1hex] = file_abspath
      
  @classmethod
  def get_target_sha1hex_dict(cls, reread=False):
    if cls.target_sha1hex_dict != None and not reread:
      return cls.target_sha1hex_dict
    cls.init_sha1hex_dict()
    return cls.target_sha1hex_dict

  @classmethod
  def does_target_dirtree_contain_sha1hex(cls, p_sha1hex, p_relpath, p_filename):
    target_sha1hex_dict = cls.get_target_sha1hex_dict()
    if p_sha1hex not in target_sha1hex_dict:
      return cls.SHA1HEX_INEXISTS_THRU_TARGET_DIRTREE
    as_is_target_file_abspath = target_sha1hex_dict[p_sha1hex]
    p_dir_abspath = os.path.join(cls.get_target_basepath(), p_relpath)
    p_file_abspath = os.path.join(p_dir_abspath, p_filename)
    if as_is_target_file_abspath == p_file_abspath:
      return cls.SHA1HEX_EXISTS_ON_THE_SAME_RELPATH_ON_TARGET
    return cls.SHA1HEX_EXISTS_BUT_NOT_ON_THE_SAME_RELPATH_ON_TARGET 

  @classmethod
  def move_file_on_target(cls, p_sha1hex, p_to_dir_relpath, p_filename):
    target_sha1hex_dict = cls.get_target_sha1hex_dict()
    from_file_abspath = target_sha1hex_dict[p_sha1hex]
    to_dir_abspath  = os.path.join(cls.get_target_basepath(), p_to_dir_relpath)
    if not os.path.isdir(to_dir_abspath):
      print 'create directory: %s' %to_dir_abspath
      os.makedirs(to_dir_abspath)
    to_file_abspath = os.path.join(to_dir_abspath, p_filename)
    print 'shutil.move(from_file_abspath, to_file_abspath)'
    print 'from: %s' %from_file_abspath 
    print 'to: %s' %to_file_abspath
    # shutil.move(from_file_abspath, to_file_abspath)
       
  @classmethod
  def copy_file_from_source_to_target(cls, source_dir_abspath, source_filename, to_dir_relpath):
    from_file_abspath = os.path.join(source_dir_abspath, source_filename)
    to_dir_abspath  = os.path.join(cls.get_target_basepath(), to_dir_relpath)
    if not os.path.isdir(to_dir_abspath):
      print 'create directory: %s' %to_dir_abspath
      os.makedirs(to_dir_abspath)
    to_file_abspath = os.path.join(to_dir_abspath, source_filename)
    print 'shutil.copy2(from_file_abspath, to_file_abspath)'
    print 'from: %s' %from_file_abspath 
    print 'to: %s' %to_file_abspath
    # shutil.copy2(from_file_abspath, to_file_abspath)

import unittest
class Test1(unittest.TestCase):

  def setUp(self):
    self.hardly_an_existent_folder = '/a/c/b/'
  
  def test_1_verify_clear_folders_and_raise_against_get(self):
    SourceAndTargetBaseDirsKeeper.clear_source_basepath()
    self.assertRaises(OSError, SourceAndTargetBaseDirsKeeper.get_source_basepath)
    SourceAndTargetBaseDirsKeeper.clear_target_basepath()
    self.assertRaises(OSError, SourceAndTargetBaseDirsKeeper.get_target_basepath)

  def test_2_set_nonexist_folder_in_allowNonExistentMode(self):
    SourceAndTargetBaseDirsKeeper.set_source_basepath(self.hardly_an_existent_folder, allowNonExistent=True)
    self.assertEqual(self.hardly_an_existent_folder, SourceAndTargetBaseDirsKeeper.get_source_basepath())
    SourceAndTargetBaseDirsKeeper.set_target_basepath(self.hardly_an_existent_folder, allowNonExistent=True)
    self.assertEqual(self.hardly_an_existent_folder, SourceAndTargetBaseDirsKeeper.get_target_basepath())

  def test_3_raise_OSError_against_nonexist_dir(self):
    self.assertRaises(OSError, SourceAndTargetBaseDirsKeeper.set_source_basepath, self.hardly_an_existent_folder)
    self.assertRaises(OSError, SourceAndTargetBaseDirsKeeper.set_target_basepath, self.hardly_an_existent_folder)
    

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
