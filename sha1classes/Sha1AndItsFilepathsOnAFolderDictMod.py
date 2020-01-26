#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
For the Sha1AndItsFilenamesOnAFolderDict class documentation, 
  read it on its docstring below.      
'''
import copy, time, sys
import xml.etree.ElementTree as ET

from . import __init__
from sha1utils.hexfunctionsMod import is_it_a_sha1hex
from sha1utils.hexfunctionsMod import generate_a_40char_random_hex
from sha1utils.hexfunctionsMod import get_tag_text_and_encoding_if_necessary
# from Sha1FlatReaderWriterMod import get_tag_text_and_encoding_if_necessary

class Sha1AndItsFilepathsOnAFolderDict(dict):
  '''
  This class is a wrapper around class Sha1AndItsFilenamesOnAFolderDict
  '''
  
  def __setitem__(self, sha1hex, filepath_obj):
    '''
    This is the only dict's overridden method. It makes 'set' acts like an 'add-to-list' against its key
    :param sha1hex:
    :param filepath_obj:
    :return:
    '''
    if filepath_obj == None:
      return
    #if type(filepath_obj) != FilePathElement:
      #return
    if not is_it_a_sha1hex(sha1hex):
      raise ValueError('sha1hex is not a 40-digit hexadecimal number.')
    if sha1hex in self:
      filepath_objs = self[sha1hex]
      if filepath_obj in filepath_objs:
        # don't append it if it's already there, return without appending it
        return
      filepath_objs.append(filepath_obj)
      # filepath_objs.sort()
    else:
      filepath_objs = [ filepath_obj ] # the first element in list
      super(Sha1AndItsFilepathsOnAFolderDict, self).__setitem__(sha1hex, filepath_objs)

  def validate(self):
    '''
    The validate() method is responsible to detect a filename repeat, ie, a filename
    that is included in more than one sha1sum key. This is a logical error.
    
    Example:
      sha1dict[sha1sum_x] = ['file1.fil', 'file2.fil']
      sha1dict[sha1sum_y] = ['fileA.fil', 'file2.fil']
    The above example shows that file2.fil has 2 sha1sums (ie, sha1sum_x and sha1sum_y),
    which, as said previously, cannot happen.
    '''
    # raises IndexError if a filename has repeats
    _ = self.get_all_filenames() 

  def get_all_filepath_objs(self):
    '''
    Gets all filenames from all sha1sum keys.
    :return:
    '''
    p_out_filepath_objs = []
    for sha1hex in list(self.keys()):
      p_out_filepath_objs += self[sha1hex]
    # raises IndexError if a filename has repeats
    return p_out_filepath_objs

  def remove_filepath_obj(self, filepath_obj):
    '''
    Remove a particular filename from the dict, finding its key and removing the filename from it.
    An implementation detail is that if the sha1sum has various files to it, filename is removed from its list.
    If the sha1sum-key's list has only that filename, the key itself is deleted from dict.
    No exception is raised if filename is not found.
    '''
    for sha1hex in list(self.keys()):
      if filepath_obj in self[sha1hex]:
        if len(self[sha1hex]) == 1:
          del self[sha1hex]
          return
        self[sha1hex].remove(filepath_obj)
        return


  def clear(self):
    '''
    Obs.:
    self = {}
    does not work as seen in the unittest
    However, iterating thru the keys and deleting their values do work
    '''
    for k in list(self.keys()):
      del self[k]

  def copy(self):
    '''
    Obs.:
    The 3 following options:
      + return super(Sha1AndItsFilenamesOnAFolderDict, self).copy()
      + return copy.copy(self)
      + leave to the implement of dict (ie, no overriding)
    work, but they don't conserve this Child Class Type, ie, object return is dict or,
      though not further investigated, copy.copy() seems not to copy at all (see this later)

      Also, the direct setting cannot be done via "copied_self[k] = self[k]", 
      it should be done via the "super()" function, ie, via the parent (dict) class,
      this is because __setitem__() is overridden to act like an add(), instead of a regular set().
    
    So we'll copy each key "deeply".
    '''
    copied_from_self = Sha1AndItsFilepathsOnAFolderDict()
    for sha1hex in list(self.keys()):
      filenames_under_this_sha1 = self[sha1hex][:]
      super(Sha1AndItsFilepathsOnAFolderDict, copied_from_self).__setitem__(sha1hex, filenames_under_this_sha1)
    return copied_from_self

  def __eq__(self, other):
    if type(other) not in [dict, Sha1AndItsFilepathsOnAFolderDict]:
      return False
    if len(self) != len(other):
      return False
    if sum(map(len, list(self.values()))) != sum(map(len, list(other.values()))):
      return False
    for sha1hex in list(self.keys()):
      # order in filenames has been conserved in __setitem__()
      # notice it's a strong assumption that may be retired in the future
      #if self[sha1hex] != other[sha1hex]:
        #return False
      pass
    return True

  def add(self, another_fpe):
    for sha1hex in list(another_fpe.keys()):
      fpes_there = another_fpe[sha1hex]
      if len(fpes_there) == 0:
        # in principle, it'd be an error to have an empty key (sha1hex with nothing). However the case, this 'if' may be removed in the future.
        return
      if sha1hex in list(self.keys()):
        fpes_here = self[sha1hex]
        fpes_here += fpes_there
      else:
        super(Sha1AndItsFilepathsOnAFolderDict, self).__setitem__(sha1hex, fpes_there)


import unittest
class TestSha1Dict(unittest.TestCase):
  
  def setUp(self):
    self.sha1_filenames_dict = Sha1AndItsFilenamesOnAFolderDict()
    self.fixed_sha1hex       = self.generate_nonrepeat_sha1sum()

  def generate_nonrepeat_sha1sum(self, n_of_retries=0):
    # the protect is done here so that a unittest method could be written
    # it would be harder to device a test if protect were after the generate..() method
    if n_of_retries >= 3:
      raise ValueError('Could not randomly generate a sha1sum within 3 tries. It is serious, giving up. Look up into the matter.')
    sha1hex = generate_a_40char_random_hex()
    if sha1hex in list(self.sha1_filenames_dict.keys()):
      n_of_retries+=1
      return self.generate_nonrepeat_sha1sum(n_of_retries)
    return sha1hex

  def test_1_generate_nonrepeat_sha1sum(self):
    '''
    This test is to cover the helper 
      generate_nonrepeat_sha1sum() unittest method above,
      this is not a business rule, so to say, unittest
    '''
    sha1hex = self.generate_nonrepeat_sha1sum()
    self.sha1_filenames_dict[sha1hex] = 'self test in generate_nonrepeat_sha1sum()'
    n_of_retries = 3
    self.assertRaises(ValueError, self.generate_nonrepeat_sha1sum, n_of_retries)

  def test_2_set_and_verify_get_as_list(self):
    self.sha1_filenames_dict.clear()
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file.fil'
    self.assertEqual(['file.fil'], self.sha1_filenames_dict[self.fixed_sha1hex])
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file.fil'
    self.assertEqual(['file.fil'], self.sha1_filenames_dict[self.fixed_sha1hex])
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file2.fil'
    self.assertEqual(['file.fil', 'file2.fil'], self.sha1_filenames_dict[self.fixed_sha1hex])

  def test_3_validate_returning_without_raising(self):
    sha1hex = self.fixed_sha1hex
    self.sha1_filenames_dict[sha1hex] = 'file1.fil'
    self.sha1_filenames_dict[sha1hex] = 'file1.copy.fil'
    sha1hex = generate_a_40char_random_hex()
    self.sha1_filenames_dict[sha1hex] = 'file2.fil'
    #sha1_filenames_dict[sha1hex] = 'file1.copy.fil'
    self.assertIsNone(self.sha1_filenames_dict.validate(), 'validate() returned so it did not raise ValueError')

  def test_4_validate_raising(self):
    sha1hex = self.fixed_sha1hex
    self.sha1_filenames_dict[sha1hex] = 'file1.fil'
    self.sha1_filenames_dict[sha1hex] = 'file1.copy.fil'
    sha1hex = self.generate_nonrepeat_sha1sum()
    self.sha1_filenames_dict[sha1hex] = 'file2.fil'
    self.sha1_filenames_dict[sha1hex] = 'file1.copy.fil'
    self.assertRaises(IndexError, self.sha1_filenames_dict.validate)
    
  def test_5_from_and_to_xml(self):
    self.sha1_filenames_dict.clear()
    sha1_dict2 = Sha1AndItsFilenamesOnAFolderDict()
    xmltree = self.sha1_filenames_dict.to_xmltree()
    sha1_dict2.from_xmltree(xmltree)
    self.assertEqual(self.sha1_filenames_dict, sha1_dict2)
    # clear, fill in, and test from and to again
    self.sha1_filenames_dict.clear()
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file1.fil'
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file1.copy.fil'
    sha1hex = generate_a_40char_random_hex()
    self.sha1_filenames_dict[sha1hex] = 'file2.fil'
    sha1_dict2.clear()
    xmltree = self.sha1_filenames_dict.to_xmltree()
    sha1_dict2.from_xmltree(xmltree)
    self.assertEqual(self.sha1_filenames_dict, sha1_dict2)

  def test_6_copy_and_clear_sha1dict(self):
    self.sha1_filenames_dict.clear()
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file1.fil'
    sha1_dict_copied = self.sha1_filenames_dict.copy()
    self.assertEqual(self.sha1_filenames_dict, sha1_dict_copied)
    self.assertTrue(type(sha1_dict_copied) == Sha1AndItsFilenamesOnAFolderDict)
    sha1_dict_copied.clear()
    self.assertEqual(sha1_dict_copied, {})
    self.assertNotEqual(self.sha1_filenames_dict, sha1_dict_copied)

  def test_7_equality_when_add_has_different_order(self):
    self.sha1_filenames_dict.clear()
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file1.fil'
    self.sha1_filenames_dict[self.fixed_sha1hex] = 'file1.copy.fil'
    sha1hex = generate_a_40char_random_hex()
    self.sha1_filenames_dict[sha1hex] = 'file2.fil'
    sha1_dict2 = Sha1AndItsFilenamesOnAFolderDict()
    # adding here an inverted order 
    sha1_dict2[self.fixed_sha1hex] = 'file1.copy.fil'
    sha1_dict2[self.fixed_sha1hex] = 'file1.fil'
    sha1_dict2[sha1hex] = 'file2.fil'
    self.assertEqual(self.sha1_filenames_dict, sha1_dict2)

  def test_8_set_nonsha1_raises_ValueError(self):
    self.sha1_filenames_dict.clear()
    nonsha1 = 'blah-blah' # This is far from being a valid sha1sum! ValueError should be raised upon setting
    self.assertRaises(ValueError, self.sha1_filenames_dict.__setitem__, nonsha1, 'file1.fil')
    

def unittests():
  unittest.main()

def process():
  '''
  '''
  pass
  # test1()

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()  
  process()
