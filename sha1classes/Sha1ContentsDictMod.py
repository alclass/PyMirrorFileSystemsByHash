#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Sha1ContentsDictMod.py
'''
import sys # os, shutil

from .Sha1FileOn2DisksComparatorMod import Sha1FileOn2DisksComparator
from . import __init__
from sha1utils.hexfunctionsMod import generate_a_40char_random_hex
from .SourceAndTargetBaseDirsKeeperMod import SourceAndTargetBaseDirsKeeper

class Sha1ContentsDict(dict):
  '''
  This class inherits from dict, overridding __init__() and __getitem__().
  
  This choice of "setting" via __getitem__() has two important implications, which are:
  1) the "value" of this class is an instance of Sha1FileOn2DisksComparator,
     which is created empty at a first __getitem__() to its key (a sha1hex number)  
  2) The use of this child class should consider __setitem__() to be private,
     ie, clients of this class should avoid setting values on a key directly, ie obj[k]=v 
  '''

  def __init__(self):
    super(Sha1ContentsDict, self).__init__()
  
  def __getitem__(self, sha1hex):
    '''
    This is the only method that is inherited from parent class dict
    The first time obj[k] (__getitem__() itself) is issued, 
      a Sha1FileOn2DisksComparator object is instantiated empty to its key k
    '''
    if sha1hex not in self:
      sha1FileComparator = Sha1FileOn2DisksComparator(sha1hex)
      self.__setitem__(sha1hex, sha1FileComparator)
      return sha1FileComparator
    # implicit else, if sha1FileComparator has already been instantiated, it's available in the dict 
    sha1FileComparator = super(Sha1ContentsDict, self).__getitem__(sha1hex)
    return sha1FileComparator
  
  def add_sha1hex_source_filename_and_folder_relpath(self, sha1hex, filename, folder_relpath):
    sha1FileComparator = self[sha1hex]
    source_filename_and_its_relpath_tuple = (filename, folder_relpath)
    sha1FileComparator.add_source_filename_and_its_relpath_tuple(source_filename_and_its_relpath_tuple)

  def add_sha1hex_target_filename_and_folder_relpath(self, sha1hex, filename, folder_relpath):
    sha1FileComparator = self[sha1hex]
    source_filename_and_its_relpath_tuple = (filename, folder_relpath)
    sha1FileComparator.add_target_filename_and_its_relpath_tuple(source_filename_and_its_relpath_tuple)

  def copy_the_sha1hex_value_obj(self, sha1hex):
    sha1FileOn2DisksComparator = self[sha1hex]
    return sha1FileOn2DisksComparator.copy()

  def find_equal_contents(self, sha1hex):
    '''
    File Equality is based on coincidence of their sha1sum
    '''
    sha1_repeated_files = Sha1ContentsDict()
    for sha1hex in list(self.keys()):
      sha1FileOn2DisksComparator = self[sha1hex]
      if len(sha1FileOn2DisksComparator) < 2:
        continue
      sha1FileOn2DisksComparatorWithRepeats = self.copy_the_sha1hex_value_obj(sha1hex)
      sha1_repeated_files[sha1hex] = sha1FileOn2DisksComparatorWithRepeats
    return sha1_repeated_files 

  def clear(self):
    '''
    Obs.:
    self = {}
    does not work as seen in the unittest
    However, iterating thru the keys and deleting their values do work
    
    "self = Sha1ContentsDict()", this one also does not work. Why?
      Maybe one cannot "override" the object itself, attribute an instance into itself.
    '''    
    for k in list(self.keys()):
      del self[k]


  def __eq__(self, other):
    if type(other) not in [dict, Sha1ContentsDict]:
      raise TypeError('type(other=%s) not in [dict, Sha1ContentsDict]' %(str(type(other))))
    if len(list(self.keys())) != len(list(other.keys())):
      return False
    these_keys = list(self.keys());  these_keys.sort()
    other_keys = list(other.keys()); other_keys.sort()
    if these_keys != other_keys:
      return False
    for sha1hex in these_keys:
      other_filename_and_dir_tuple_list = other[sha1hex].get_source_filename_and_its_abspath_tuple_list()[:]
      for filename_and_dir_tuple in self[sha1hex].get_source_filename_and_its_abspath_tuple_list():
        if filename_and_dir_tuple in other_filename_and_dir_tuple_list:
          other_filename_and_dir_tuple_list.remove(filename_and_dir_tuple)
      if len(other_filename_and_dir_tuple_list) > 0:
        return False
    return True

  def total_files(self):
    #lambda size(alist) : sum()
    return sum(map(len, list(self.values())))


import unittest
class TestCaseModWide(unittest.TestCase):
  
  def setUp(self):
    self.sha1ContentsObj = Sha1ContentsDict()
    self.fixed_sha1hex = generate_a_40char_random_hex()

  def test_1_cleared_upon_init(self):
    self.sha1ContentsObj = Sha1ContentsDict() # empty here
    self.assertEqual(self.sha1ContentsObj, {})
  
  def test_2_add_1_empty_item_check_key_exists_then_clear(self):
    _ = self.sha1ContentsObj[self.fixed_sha1hex] # one (key, value) pair is created, value is an "empty" instance of Sha1FileOn2DisksComparator
    self.assertNotEqual(self.sha1ContentsObj, {})
    self.assertTrue(self.fixed_sha1hex in list(self.sha1ContentsObj.keys()))
    self.sha1ContentsObj.clear()
    self.assertEqual(self.sha1ContentsObj, {})

  def test_3_total_files(self):
    self.sha1ContentsObj.clear()
    # total_files() should be 0 (zero), because it's empty, ie, no files are in the tuple list, either source or target 
    self.assertEqual(0, self.sha1ContentsObj.total_files())
    self.sha1ContentsObj.add_sha1hex_source_filename_and_folder_relpath(self.fixed_sha1hex, 'file1.fil', 'a/b/c/')
    # total_files() should be 1, because tuple ('file1.fil', '/a/b/c/') has been added 
    self.assertEqual(1, self.sha1ContentsObj.total_files())
    self.sha1ContentsObj.add_sha1hex_source_filename_and_folder_relpath(self.fixed_sha1hex, 'file1.fil', 'a/b/c/')
    # total_files() should STILL be 1, because tuple ('file1.fil', '/a/b/c/') has already been added previously 
    self.assertEqual(1, self.sha1ContentsObj.total_files())
    self.sha1ContentsObj.add_sha1hex_source_filename_and_folder_relpath(self.fixed_sha1hex, 'file1copied.fil', 'a/b/c/')
    # total_files() should now be 2, because a second (a different) tuple ('file1copied.fil', '/a/b/c/') has been added 
    self.assertEqual(2, self.sha1ContentsObj.total_files())

  def test_4_find_equal_data_objects(self):
    SourceAndTargetBaseDirsKeeper.set_source_basepath('/source/s/', allowNonExistent=True)
    SourceAndTargetBaseDirsKeeper.set_target_basepath('/target/t/', allowNonExistent=True)
    self.sha1ContentsObj.clear()
    self.sha1ContentsObj.add_sha1hex_source_filename_and_folder_relpath(self.fixed_sha1hex, 'file1.fil', 'a/b/c/')
    sha1ContentsObjOther = Sha1ContentsDict()
    sha1ContentsObjOther.add_sha1hex_source_filename_and_folder_relpath(self.fixed_sha1hex, 'file1.fil', 'a/b/c/')
    # the __eq__() implementation will compare the sha1hex keys and the contents of their Sha1FileOn2disksComparator value counterpart
    self.assertEqual(self.sha1ContentsObj, sha1ContentsObjOther)
    # Sha1FileOn2DisksComparatorMod.Sha1FileOn2DisksComparator will make their __repr__() be different, required they are different instances
    self.assertNotEqual(self.__repr__(), sha1ContentsObjOther.__repr__())
    
  def test_5_find_contents_with_the_same_sha1hex(self):
    self.sha1ContentsObj.clear()
    sha1FileOn2DisksObj = self.sha1ContentsObj[self.fixed_sha1hex] 
    sha1FileOn2DisksObj.add_source_filename_and_its_relpath_tuple(('file1.fil', 'a/b/c/'))
    sha1FileOn2DisksObj.add_source_filename_and_its_relpath_tuple(('file1_copied.fil', 'a/b/c/'))
    sha1ContentsObjWithRepeats = self.sha1ContentsObj.find_equal_contents(self.fixed_sha1hex)
    self.assertTrue(self.sha1ContentsObj == sha1ContentsObjWithRepeats)
    
  def test_6_copy_items(self):
    self.sha1ContentsObj.clear()
    sha1FileOn2DisksObj = self.sha1ContentsObj[self.fixed_sha1hex]
    sha1FileOn2DisksObj.add_source_filename_and_its_relpath_tuple(('file1.fil', 'a/b/c/'))
    sha1FileOn2DisksObjCopied = self.sha1ContentsObj.copy_the_sha1hex_value_obj(self.fixed_sha1hex)
    self.assertEqual(sha1FileOn2DisksObj, sha1FileOn2DisksObjCopied)
    
def unittests():
  unittest.main()

def process():
  '''
  d = Sha1ContentsDict()
  print 'd is type', type(d)

  sha1hex = generate_a_40char_random_hex()
  d1 = Sha1ContentsDict()
  _ = d1[sha1hex]
  d2 = Sha1ContentsDict()
  _ = d2[sha1hex]
  print 'd1.__repr__() =', d1.__repr__()
  print 'd2.__repr__() =', d2.__repr__()

  '''
  pass

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()  
  process()
