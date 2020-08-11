#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
      
'''
import os, sys

# import sha1classes.XmlSha1ExceptionClassesMod as sha1exceptions
from fs.hashpackage import defaults
from .Sha1AndItsFilenamesOnAFolderDictMod import Sha1AndItsFilenamesOnAFolderDict
# from Sha1XMLReaderWriterMod import Sha1XMLReader
from fs.hashpackage.sha1utilsMod import generate_sha1hexdigest_from_filepath
# from Sha1XmlFileFunctions import read_xml_sha1file_into_sha1sum_and_filename_dict


# ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1','windows-1250','windows-1252']
# file_filter = lambda f : os.path.isfile(f) # unfortunately this lambda only works on current dir processing, deactivating it as a comment


class Sha1XmlOnFolder(object):

  def __init__(self, folder_abspath=None):
    self.sha1sum_and_its_filenames_dict = None # to be an instance of Sha1AndItsFilenamesOnAFolderDict()
    self.filename_sha1sum_inverted_dict = None # a simple dict to be lazily set
    self.overwrite_sha1_xml_upon_writing_to_folder = False
    self.sha1file_candidates = [] 
    self.xml_filename        = None
    self.set_folder_abspath(folder_abspath) # if it's None, it will be set as the current folder
    self.folder_filenames_missing_on_xmlsha1file                   = []
    self.filenames_listed_on_xmlsha1file_that_dont_exist_on_folder = []

  def clear(self, folder_abspath=None):
    self.__init__(folder_abspath)

  def set_folder_abspath(self, folder_abspath=None): # if it's None, it will be set as the current folder
    if folder_abspath==None:
      current_abspath = os.path.abspath('')
      self.folder_abspath = current_abspath
      return
    if not os.path.isdir(folder_abspath):
      raise OSError('folder_abspath (=%s) does not exist.')
    self.folder_abspath = folder_abspath

  def get_folder_abspath(self):
    return self.folder_abspath

  def get_xml_filename(self):
    if self.xml_filename == None:
      self.set_xml_filename() # this will set the default xml_filename
    return self.xml_filename

  def get_xml_sha1file_abspath(self):
    if self.folder_abspath == None:
      return None
    xml_sha1file_abspath = os.path.join(self.folder_abspath, self.get_xml_filename())
    return xml_sha1file_abspath
  
  def does_xml_sha1file_abspath_exist(self):
    if os.path.isfile(self.get_xml_sha1file_abspath()):
      return True
    return False
    
  def generate_folder_list_of_filenames(self):
    filenames = []
    contents = os.listdir(self.folder_abspath)
    for content_name in contents:
      content_abspath = os.path.join(self.folder_abspath, content_name)
      if os.path.isfile(content_abspath):
        filenames.append(content_name)
    return filenames

  def set_overwrite_sha1_xml_upon_writing_to_folder(self, overwrite = False):
    self.overwrite_sha1_xml_upon_writing_to_folder = overwrite
 
  def set_xml_filename(self, xml_filename=None):
    if xml_filename == None:
      xml_filename = defaults.DEFAULT_XML_SHA1_FILENAME
    self.xml_filename = xml_filename

  def get_sha1sum_and_its_filenames_dict(self):
    if self.sha1sum_and_its_filenames_dict == None:
      return None
    return self.sha1sum_and_its_filenames_dict

  def init_inverted_sha1hex_filenames_dict(self):
    self.filename_sha1sum_inverted_dict = {}
    if self.sha1sum_and_its_filenames_dict == None:
      return
    for sha1hex in self.sha1sum_and_its_filenames_dict:
      filenames = self.sha1sum_and_its_filenames_dict[sha1hex]
      for filename in filenames:
        self.filename_sha1sum_inverted_dict[filename] = sha1hex 

  def get_filename_sha1sum_inverted_dict(self, reread=False):
    if self.filename_sha1sum_inverted_dict == None or reread:
      self.init_inverted_sha1hex_filenames_dict()
    return self.filename_sha1sum_inverted_dict

  def get_sha1file_filenames(self):
    return list(self.get_filename_sha1sum_inverted_dict().keys())

  def get_sha1hex_from_filename(self, filename):
    try:
      sha1hex = self.get_filename_sha1sum_inverted_dict()[filename]
      return sha1hex 
    except KeyError:
      pass
    return None
      
  def set_sha1sum_and_its_filenames_dict(self, sha1sum_and_its_filenames_dict):
    if sha1sum_and_its_filenames_dict == None:
      raise ValueError('sha1sum_and_its_filenames_dict is None')
    if type(sha1sum_and_its_filenames_dict) != Sha1AndItsFilenamesOnAFolderDict:
      raise TypeError('sha1sum_and_its_filenames_dict is not an instance of Sha1AndItsFilenamesOnAFolderDict')
    self.sha1sum_and_its_filenames_dict = sha1sum_and_its_filenames_dict

  def check_files_existence_on_folder_based_on_sha1sum_and_filenames_dict(self, sha1sum_and_filenames_dict):
    for sha1hex in self.sha1sum_and_its_filenames_dict:
      filenames = self.sha1sum_and_its_filenames_dict[sha1hex]
      for filename in filenames:
        file_abspath = os.path.join(self.folder_abspath, filename)
        if not os.path.isfile(file_abspath):
          return False
    return True

  def hash_file(self, filename):
    '''
    '''
    file_abspath = os.path.join(self.get_folder_abspath(), filename)
    if not os.path.isfile(file_abspath):
      raise OSError('file_abspath (%s) does not exist for sha1-hashing.' %file_abspath)
    sha1hex = generate_sha1hexdigest_from_filepath(file_abspath)
    return sha1hex

  def hash_file_and_update_both_dicts(self, filename):
    sha1hex = self.hash_file(filename)
    self.filename_sha1sum_inverted_dict[filename] = sha1hex
    self.sha1sum_and_its_filenames_dict[sha1hex] = filename

  def delete_filename_from_both_dicts(self, filename):
    del self.filename_sha1sum_inverted_dict[filename]
    self.sha1sum_and_its_filenames_dict.remove_filename(filename)

  def hash_files_and_reset_sha1sum_and_its_filenames_dict(self):
    '''
    Loop thru all files on a folder. Hash each one's SHA1 sum, return them as a sha1sum_and_filename_dict 
    '''
    
    files = os.listdir(self.folder_abspath)
    sha1sum_and_its_filenames_dict = Sha1AndItsFilenamesOnAFolderDict()
    if len(files) == 0:
      return sha1sum_and_its_filenames_dict
    for filename in files:
      if filename == self.get_xml_filename():
        continue
      file_abspath = os.path.join(self.folder_abspath, filename)
      if not os.path.isfile(file_abspath):
        continue
      print('Generating sha1hex for file', filename, '...', end=' ')
      sha1hex = generate_sha1hexdigest_from_filepath(file_abspath)
      print(sha1hex)
      sha1sum_and_its_filenames_dict[sha1hex] = filename
    self.sha1sum_and_its_filenames_dict = sha1sum_and_its_filenames_dict 

  def find_xml_sha1_filenames_candidates(self):
    files = os.listdir(self.folder_abspath)
    self.sha1file_candidates = []
    for filename in files:
      name, ext = os.path.splitext(filename)
      if ext.lower() == '.xml':
        if name.find('sha1') > -1:
          self.sha1file_candidates.append(filename)

  def __str__(self):
    if self.sha1sum_and_its_filenames_dict == None or len(self.sha1sum_and_its_filenames_dict) == 0:
      outstr = 'There are no sha1s/files on folder  %s' %self.folder_abspath
      return outstr
    outstr = '='*40 + '\n'
    outstr += 'Sha1hexes per file on folder %s\n' %self.folder_abspath
    outstr += '='*40 + '\n'
    for sha1hex in self.sha1sum_and_its_filenames_dict:
      filenames = self.sha1sum_and_its_filenames_dict[sha1hex]
      for filename in filenames:
        outstr += 'sha1 = [%s] :: [[%s]]\n' %(sha1hex, filename)
    return outstr 


import unittest
class Test1(unittest.TestCase):
  
  def setUp(self):
    current_folder_abspath = os.path.abspath('')
    print('current', current_folder_abspath)
    self.xmlsha1onfolder = Sha1XmlOnFolder(current_folder_abspath)
    
  #=============================================================================
  # def test_1(self):
  #   print 'hi'
  #   self.assertTrue(True, 'must pass')
  #=============================================================================

  def test_get_default_xml_sha1_filename(self):
    self.assertEqual(self.xmlsha1onfolder.get_xml_filename(), defaults.DEFAULT_XML_SHA1_FILENAME)
  
  def test_hash_files_to_dict_and_verify_files_existence(self):
    sha1sum_and_filenames_dict = self.xmlsha1onfolder.hash_files_and_return_sha1sum_and_its_filenames_dict()
    shouldToTrue = self.xmlsha1onfolder.check_files_existence_on_folder_based_on_sha1sum_and_filenames_dict(sha1sum_and_filenames_dict)
    self.assertTrue(shouldToTrue)

def unittests():
  unittest.main()

def process():
  '''
  '''
  folder_abspath = sys.argv[1]
  sha1folder = Sha1XmlOnFolder(folder_abspath)
  print(sha1folder) 

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()  
  process()
