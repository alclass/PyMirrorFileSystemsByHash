#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
For the Sha1AndItsFilenamesOnAFolderDict class documentation, 
  read it on its docstring below.      
'''
import time, sys
import xml.etree.ElementTree as ET

from fs.hashfunctions.hexfunctionsmod import is_it_a_sha1hex
from fs.hashfunctions.hexfunctionsmod import generate_a_40char_random_hex
from fs.hashfunctions.hexfunctionsmod import get_tag_text_and_encoding_if_necessary
# from Sha1FlatReaderWriterMod import get_tag_text_and_encoding_if_necessary


class Sha1AndItsFilenamesOnAFolderDict(dict):
  '''
  This dict-inherited class aims to allow storing filenames belonging
  to a directory into an instance of itself,
  having file sha1sum as key.
  
  One important fact of this class is that one sha1sum may connect to 
  more than one filename, ie, file copies
  are allowed to exist on the same folder.
  
  From dict itself, only __setitem__() is overridden.
  The change was so to make it act like an add(), ie:
  
  d[sha1]=<filename>
  
  will, in fact, add <filename> to the list of filenames having the same sha1hex key.
  
  The non-parent method validate() is responsible to detect a filename repeat
  having a different sha1sum, which is a logical error. 
  More info about validate() is found on its docstring. 

  The non-parent methods to_xmltree() and from_xmltree()
  are transformation methods that generate this dict data
  equivalent in an xml tree.
  
  The actual XML read-from/write-to is done outside of this class, 
  moment when the XML file abs. path is joint to the XML object.

  More info about to_xmltree() and from_xmltree() 
  is found on their respective methods' docstring.
  '''
  
  def __setitem__(self, sha1hex, filename):
    '''
    This is the only dict's overridden method. It makes 'set' acts like an 'add-to-list' against its key
    :param sha1hex:
    :param filename:
    :return:
    '''
    if filename == None or type(filename) != str:
      return
    if not is_it_a_sha1hex(sha1hex):
      raise ValueError('sha1hex [%s] is not a 40-digit hexadecimal number.' %sha1hex)
    if sha1hex in self:
      filenames = self[sha1hex]
      if filename in filenames:
        # don't append it if it's already there, return without appending it
        return
      filenames.append(filename)
      filenames.sort()
    else:
      filenames = [ filename ]
      super(Sha1AndItsFilenamesOnAFolderDict, self).__setitem__(sha1hex, filenames)

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

  def get_all_filenames(self):
    '''
    Gets all filenames from all sha1sum keys.
    :return:
    '''
    filenames = []
    for k in list(self.keys()):
      filenames += self[k]
    # raises IndexError if a filename has repeats
    if len(filenames) != len(set(filenames)):
      raise IndexError('Consistency Error (probably a program error): One or more filenames showed up having 2 or more different sha1sums on the same directory.')
    return filenames

  def remove_filename(self, filename):
    '''
    Remove a particular filename from the dict, finding its key and removing the filename from it.
    An implementation detail is that if the sha1sum has various files to it, filename is removed from its list.
    If the sha1sum-key's list has only that filename, the key itself is deleted from dict.
    No exception is raised if filename is not found.
    '''
    for sha1hex in list(self.keys()):
      if filename in self[sha1hex]:
        if len(self[sha1hex]) == 1:
          del self[sha1hex]
          return
        self[sha1hex].remove(filename)
        return

  def to_xmltree(self):
    '''
    This method transforms this dict-inherited data structure into an XML tree object.
    
    to_xmltree() creates the xml root node, invoking ET.Element(rootnodename).
    
    '''
    xml_root = ET.Element("sha1files")
    xml_root.set("timestamp", str(time.time()))
    for sha1hex in list(self.keys()):
      sha1file_tag = ET.SubElement(xml_root, "sha1file")
      sha1file_tag.set("sha1hex", sha1hex)
      filenames = self[sha1hex]
      if len(filenames) == 0: # None or filename == '':
        error_msg = 'A sha1hex (%s) was sent to be file-written without having one or more corresponding filenames.' %sha1hex
        raise OSError(error_msg)
      filenames_tag = ET.SubElement(sha1file_tag, 'filenames')
      for filename in filenames:
        filename_tag = ET.SubElement(filenames_tag, 'filename')
        try:
          filename_tag.text = str(filename)
          # print 'filename in tag is', filename
        except UnicodeDecodeError:
          get_tag_text_and_encoding_if_necessary(filename_tag, filename)
    xml_tree = ET.ElementTree(xml_root)
    return xml_tree 

  def from_xmltree(self, xml_tree):
    '''
    This method converts the data in an XML tree object 
    to its internal dict data.
    
    The from_xmltree() receives an xml tree as parameter.
    '''
    xml_root = xml_tree.getroot()
    if len(xml_root) == 0:
      return
    self.clear()
    for sha1hex_node in xml_root:
      sha1hex = sha1hex_node.get('sha1hex')
      try:
        filenames_tag = sha1hex_node.getchildren()[0]
      except IndexError:
        continue
      for filename_tag in filenames_tag:
        filename = filename_tag.text
        try:
          filename = str(filename)
        except UnicodeDecodeError:
          # attribute 'encoding' may not exist, if it does, the unicode variable filename 
          # will have to receive method .encode(encoding), filename.encode(encoding)
          encoding = filename_tag.get('encoding')
          if encoding != None:
            filename = filename.encode(encoding)
        self[sha1hex] = filename
    self.validate()

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
    copied_from_self = Sha1AndItsFilenamesOnAFolderDict()
    for sha1hex in list(self.keys()):
      filenames_under_this_sha1 = self[sha1hex][:]
      super(Sha1AndItsFilenamesOnAFolderDict, copied_from_self).__setitem__(sha1hex, filenames_under_this_sha1)
    return copied_from_self

  def __eq__(self, other):
    if type(other) not in [dict, Sha1AndItsFilenamesOnAFolderDict]:
      return False
    if len(self) != len(other):
      return False
    if sum(map(len, list(self.values()))) != sum(map(len, list(other.values()))):
      return False
    for sha1hex in list(self.keys()):
      # order in filenames has been conserved in __setitem__()
      # notice it's a strong assumption that may be retired in the future
      if self[sha1hex] != other[sha1hex]:
        return False
    return True

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
