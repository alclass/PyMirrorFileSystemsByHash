#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''

'''

import os, sys # codecs, sha, shutil, time
import xml.etree.ElementTree as ET

#import XmlSha1ExceptionClassesMod as sha1exceptions

from . import __init__
# import local_settings as ls
#from sha1utils import defaults
from sha1utils.hexfunctionsMod import generate_a_40char_random_hex
from sha1utils.Sha1XmlFileFunctions import generate_xml_sha1file_against_all_files_on_its_folder
# from sha1utils.sha1utilsMod import get_tag_text_and_encoding_if_necessary
from .Sha1AndItsFilenamesOnAFolderDictMod import Sha1AndItsFilenamesOnAFolderDict
from .Sha1XmlOnFolderMod import Sha1XmlOnFolder

SHA1_CHUNK_SIZE = 40
ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1','windows-1250','windows-1252']

class MismatchAmountOfFilesBetweenXMLAndFolderFiles(IndexError):
  pass

class FailedToUpdateSha1XMLSoThatItIsEqualToFolder(MismatchAmountOfFilesBetweenXMLAndFolderFiles):
  pass

class Sha1XMLReader(Sha1XmlOnFolder):
  
  def __init__(self, folder_abspath=None):
    super(Sha1XMLReader, self).__init__(folder_abspath)
    self.read_xml_sha1file_on_folder_into_sha1sum_and_its_filenames_dict()
    self.verify_filenames_are_equal()
    
  def filter_out_nonbackables(self, filename_list):
    newlist = []
    for filename in filename_list:
      if filename.endswith('~'):
        continue
      if filename == self.get_xml_filename(): # get_xml_filename() is on parent class
        continue
      newlist.append(filename)
    return newlist
    
  def verify_filenames_are_equal(self, nOfTries=0):
    '''
    This method has a tricky situation.
    This is that the object (writer) that updates the 2 dict's is not this instance "reader".
    Because of that, the xml file should be REREAD by this instance when nOfTries is greater than 0.
    '''
    folder_contents = os.listdir(self.folder_abspath)
    folder_filenames = []
    for content in folder_contents:
      content_abspath = os.path.join(self.folder_abspath, content)
      if not os.path.isfile(content_abspath):
        continue
      encoded = content.decode('latin-1')
      decoded = encoded.encode('utf-8')
      try:
        
        folder_filenames.append(''+content)
      except UnicodeDecodeError as e:
        print('UnicodeDecodeError', '\n', \
          'Filename:', content, '\n', \
          'Encoded:', encoded, type(encoded),'\n', \
          'Decoded:', decoded, type(decoded), '\n', \
          'Folder:', self.folder_abspath, '\n', \
          'Exception: ', e)
        sys.exit(-1)
    # if nOfTries > 0, then both dicts must be updated, for the update process occurred in the writer object, so this reader must be updated with the xml on disk
    if nOfTries > 0:
      self.read_xml_sha1file_on_folder_into_sha1sum_and_its_filenames_dict()
    sha1file_filenames = self.get_sha1file_filenames()[:]
    sha1file_filenames = self.filter_out_nonbackables(sha1file_filenames)
    folder_filenames   = self.filter_out_nonbackables(folder_filenames)
    folder_filenames_cp = folder_filenames[:]
    #print 'sha1file_filenames:', sha1file_filenames, 'nOfTries', nOfTries
    #print 'folder_filenames  :', folder_filenames
    for sha1file_filename in sha1file_filenames:
      if sha1file_filename in folder_filenames_cp:
        folder_filenames_cp.remove(sha1file_filename) 
    if len(folder_filenames_cp) > 0:
      if nOfTries > 2:
        raise ValueError('Could not equalize folder filenames and sha1file filenames: len(folder_filenames_cp)=%d > 0 :: %s nOfTries=%d' %(len(folder_filenames_cp), str(folder_filenames_cp), nOfTries)) 
      writer = self.get_writer()
      writer.resync_sha1xml_with_folder_files()
      return self.verify_filenames_are_equal(nOfTries+1)

    sha1file_filenames_cp = sha1file_filenames[:]
    for folder_filename in folder_filenames:
      if folder_filename in sha1file_filenames_cp:
        sha1file_filenames_cp.remove(folder_filename)
    if len(sha1file_filenames_cp) > 0:
      if nOfTries > 2:
        raise ValueError('Could not equalize folder filenames and sha1file filenames: len(sha1file_filenames_cp)=%d > 0 :: %s nOfTries=%d' %(len(sha1file_filenames_cp), str(sha1file_filenames_cp), nOfTries))
      writer = self.get_writer()
      writer.resync_sha1xml_with_folder_files()
      return self.verify_filenames_are_equal(nOfTries+1)

      
  #@override
  def get_sha1hex_from_filename(self, filename):
    '''
    Method: get_sha1hex_from_filename(filename) comes in inherited from parent class
  
    '''
    file_abspath = os.path.join(self.folder_abspath, filename)
    if not os.path.isfile(file_abspath):
      raise OSError('File %s does not exist for getting its sha1hex.' %file_abspath)
    sha1hex = super(Sha1XMLReader, self).get_sha1hex_from_filename(filename)
    if sha1hex == None:
      writer = self.get_writer()
      writer.update_sha1dict_and_immediately_update_sha1file()
      sha1hex = super(Sha1XMLReader, self).get_sha1hex_from_filename(filename)
      if sha1hex == None:
        raise ValueError('System is not retrieving a sha1hex for file [%s]' %file_abspath)
    return sha1hex
  

  def read_xml_sha1file_on_folder_into_sha1sum_and_its_filenames_dict(self):
    '''
    This function reads a given XML SHA1 File and return sha1sum_and_filename_dict
    '''
    if self.folder_abspath == None:
      # self.folder_abspath can be None for unittests purpose, so this check is necessary
      raise ValueError('Error: folder_abspath was not initialized.')
    xml_sha1file_abspath = self.get_xml_sha1file_abspath()
    xml_tree = ET.parse(xml_sha1file_abspath)
    # reinstantiate self.sha1sum_and_its_filenames_dict
    self.sha1sum_and_its_filenames_dict = Sha1AndItsFilenamesOnAFolderDict()
    self.sha1sum_and_its_filenames_dict.from_xmltree(xml_tree)
    # update 2nd dict (the inverted one)
    self.init_inverted_sha1hex_filenames_dict()
    # _ = self.get_filename_sha1sum_inverted_dict(reread=True)

  def find_folder_files_missing_in_xml_and_excess_files_in_xml_files_not_on_folder(self):
    '''
    '''
    on_folder_filenames = self.generate_folder_list_of_filenames()
    in_xml_filenames    = self.sha1sum_and_its_filenames_dict.get_all_filenames()
    self.folder_filenames_missing_on_xmlsha1file                   = []
    self.filenames_listed_on_xmlsha1file_that_dont_exist_on_folder = []
    for in_xml_filename in in_xml_filenames:
      if in_xml_filename not in on_folder_filenames:
        self.filenames_listed_on_xmlsha1file_that_dont_exist_on_folder.append(in_xml_filename)
    for on_folder_filename in on_folder_filenames:
      if on_folder_filename not in  in_xml_filenames:
        self.folder_filenames_missing_on_xmlsha1file.append(on_folder_filename)
    #return folder_filenames_missing_on_xmlsha1file, filenames_listed_on_xmlsha1file_that_dont_exist_on_folder

  def is_filenames_correspondence_good(self):
    self.find_folder_files_missing_in_xml_and_excess_files_in_xml_files_not_on_folder()
    if len(self.folder_filenames_missing_on_xmlsha1file) > 0:
      return False
    if len(self.filenames_listed_on_xmlsha1file_that_dont_exist_on_folder) > 0:
      return False
    return True

  def get_writer(self):
    '''
    writer.set_sha1sum_and_its_filenames_dict() can raise ValueError    
    '''
    return Sha1XMLWriter.get_writer(self)

  @staticmethod
  def get_reader(sha1XmlOnFolderObj):
    '''
    reader.set_sha1sum_and_its_filenames_dict() can raise ValueError    
    '''
    reader = Sha1XMLReader(sha1XmlOnFolderObj.get_folder_abspath())
    reader.set_xml_filename(sha1XmlOnFolderObj.get_xml_filename())
    reader.set_sha1sum_and_its_filenames_dict(sha1XmlOnFolderObj.get_sha1sum_and_its_filenames_dict())
    return reader



class Sha1XMLWriter(Sha1XmlOnFolder):
  '''
  
  Methods inherited:
  
  + get_xml_filename()
  + get_xml_sha1file_abspath()
  + does_xml_sha1file_abspath_exist()
  + set_overwrite_sha1_xml_upon_writing_to_folder()
  + set_xml_filename()
  + get_sha1sum_and_filenames_dict()
  + set_sha1sum_and_its_filenames_dict()
  + check_files_existence_on_folder_based_on_sha1sum_and_filenames_dict()
  + hash_files_and_return_sha1sum_and_its_filenames_dict()
  + find_xml_sha1_filenames_candidates()
  '''
  
  def write_xml_sha1file_given_sha1sum_and_filenames_dict(self, p_sha1sum_and_its_filenames_dict, rewrite=True):
    '''
    '''
    if p_sha1sum_and_its_filenames_dict == None or type(p_sha1sum_and_its_filenames_dict) != Sha1AndItsFilenamesOnAFolderDict:
      raise TypeError('sha1sum_and_filenames_dict == None or type(sha1sum_and_filenames_dict) != Sha1AndItsFilenamesOnAFolderDict')
    self.sha1sum_and_its_filenames_dict = p_sha1sum_and_its_filenames_dict.copy()
    self.write_xml_sha1file(rewrite)

  def write_xml_sha1file(self, rewrite=True):
    '''
    '''
    if not rewrite:
      if self.does_xml_sha1file_abspath_exist():
        raise OSError('xml sha1file exists and parameter rewrite was set to False.')
    xmltree = self.get_sha1sum_and_its_filenames_dict().to_xmltree()
    xml_sha1file_abspath = self.get_xml_sha1file_abspath()
    if xml_sha1file_abspath == None:
      raise OSError('xml_sha1file_abspath = None, ie, it has not been initialized.')
    xmltree.write(xml_sha1file_abspath)

  def rewrite_xml_sha1file_rehashing_folder_files(self):
    self.hash_files_and_return_sha1sum_and_its_filenames_dict()
    self.write_xml_sha1file()

  def resync_sha1xml_with_folder_files(self):
    folder_filenames = []
    folder_listing = os.listdir(self.folder_abspath)
    for filename in folder_listing:
      file_abspath = os.path.join(self.folder_abspath, filename)
      if os.path.isfile(file_abspath):
        folder_filenames.append(filename)
        if filename not in list(self.get_filename_sha1sum_inverted_dict().keys()):
          self.hash_file_and_update_both_dicts(filename)
    for filename in list(self.get_filename_sha1sum_inverted_dict().keys()):
      if filename not in folder_filenames:
        self.delete_filename_from_both_dicts(filename)
    filename_dict = self.get_filename_sha1sum_inverted_dict()
    self.write_xml_sha1file()
    sha1_dict = self.get_sha1sum_and_its_filenames_dict()
    filename_dict, sha1_dict
    
  def update_sha1dict_and_immediately_update_sha1file(self):
    '''
    '''
    # 1st step: remove excess files from dict (which will reflect on the xml tree upon write())
    for filename in self.filenames_listed_on_xmlsha1file_that_dont_exist_on_folder:
      self.sha1sum_and_its_filenames_dict.remove_filename(filename)
    # 2nd step: add missing folder filenames to dict  (which will reflect on the xml tree upon write())
    for filename in self.folder_filenames_missing_on_xmlsha1file:
      sha1hex = self.hash_file(filename)
      self.sha1sum_and_its_filenames_dict[sha1hex] = filename
    self.write_xml_sha1file()
    
  def synchronize_xmlsha1file_and_folder_by_file_count(self):
    self.update_sha1dict_and_immediately_update_sha1file()

  def get_reader(self):
    '''
    reader.set_sha1sum_and_its_filenames_dict() can raise ValueError    
    '''
    return Sha1XMLReader.get_reader(self)

  @staticmethod
  def get_writer(sha1XmlOnFolderObj):
    '''
    writer.set_sha1sum_and_its_filenames_dict() can raise ValueError    
    '''
    writer = Sha1XMLWriter(sha1XmlOnFolderObj.get_folder_abspath())
    writer.set_xml_filename(sha1XmlOnFolderObj.get_xml_filename())
    writer.set_sha1sum_and_its_filenames_dict(sha1XmlOnFolderObj.get_sha1sum_and_its_filenames_dict())
    return writer

def read_xml_sha1file_into_sha1sum_and_filename_dict(xmlsha1file_abspath, create=True, second_try=False):
  '''
  This is only function that reads a given XML SHA1 File and return sha1sum_and_filename_dict
  '''
  if not os.path.isfile(xmlsha1file_abspath):
    absfolder, xmlsha1filename = os.path.split(xmlsha1file_abspath)
    if not os.path.isdir(absfolder):
      raise OSError('Folder %s does not exist.' %absfolder)
    xmlsha1file_abspath = os.path.join(absfolder, xmlsha1filename)
    if not create:
      return None
    else: # ie, if create:
      if second_try:
        error_msg = 'xml_sha1_absfile [%s] DOES NOT STILL EXIST after attempt to create it. Interrupting execution.' %xmlsha1file_abspath
        raise OSError(error_msg) 
      print('generate_xml_sha1file(absfolder) xmlsha1file_abspath', xmlsha1file_abspath)
      generate_xml_sha1file_against_all_files_on_its_folder(xmlsha1file_abspath)
      return read_xml_sha1file_into_sha1sum_and_filename_dict(absfolder, create=True, second_try=True)
  sha1sum_and_filename_dict = Sha1AndItsFilenamesOnAFolderDict()
  xml_tree = ET.parse(xmlsha1file_abspath)
  xml_root = xml_tree.getroot()
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
        # attribute 'encoding' may not exist, if it does, the unicode variable filename will have to receive method .encode(encoding), filename.encode(encoding)
        encoding = filename_tag.get('encoding')
        if encoding != None:
          filename = filename.encode(encoding)
      sha1sum_and_filename_dict[sha1hex] = filename
      #print 'sha1hex, filename', sha1hex, filename
  #print 'sha1sum_and_filename_dict', sha1sum_and_filename_dict
  return sha1sum_and_filename_dict # an instance of Sha1AndItsFilenamesOnAFolderDict

def build_test_xmlsha1dict_1():
  sha1hex = generate_a_40char_random_hex()  
  xmlsha1dict = Sha1AndItsFilenamesOnAFolderDict()
  xmlsha1dict[sha1hex] = 'file1.fil'
  xmlsha1dict[sha1hex] = 'file1.fil' # this doesn't "enter", it's equal to the one above
  xmlsha1dict[sha1hex] = 'file1copied.fil'
  not_colliding_with = [sha1hex]
  sha1hex = generate_a_40char_random_hex(not_colliding_with)
  xmlsha1dict[sha1hex] = 'fileA.fil'
  xmlsha1dict[sha1hex] = 'fileB.fil'
  xmlsha1dict.validate()
  return xmlsha1dict 

import unittest
class Test1(unittest.TestCase):
  
  def recreate_sha1reader_and_writer(self):
    self.sha1reader = Sha1XMLReader()
    self.sha1writer = Sha1XMLWriter()

  def setUp(self):
    self.current_folder_abspath = os.path.abspath('.')
    self.xmlsha1dict_1 = build_test_xmlsha1dict_1()
    self.recreate_sha1reader_and_writer()
    
  def test_1_recreate_sha1writer_and_reader(self):
    # self.recreate_sha1reader_and_writer()
    self.assertEqual(self.sha1reader.get_folder_abspath(), self.current_folder_abspath)
    self.assertEqual(self.sha1writer.get_folder_abspath(), self.current_folder_abspath)

  def test_2_sha1readerwriter_clear_sha1dict(self):
    self.sha1reader.clear()
    self.assertIsNone(self.sha1reader.get_sha1sum_and_its_filenames_dict())
    self.sha1writer.clear()
    self.assertIsNone(self.sha1writer.get_sha1sum_and_its_filenames_dict())
    
  def test_3_write_read_xml_sha1file(self):
    self.sha1writer.clear()
    self.assertIsNone(self.sha1writer.get_sha1sum_and_its_filenames_dict())
    self.sha1writer.write_xml_sha1file_given_sha1sum_and_filenames_dict(self.xmlsha1dict_1)
    sha1reader = self.sha1writer.get_reader()
    sha1reader.read_xml_sha1file_on_folder_into_read_sha1sum_and_its_filenames_dict()
    sha1dict = sha1reader.get_read_sha1sum_and_its_filenames_dict()
    self.assertEqual(sha1dict, self.xmlsha1dict_1)

  def test_4_rehashing_folder_files(self):
    self.sha1writer.write_xml_sha1file_given_sha1sum_and_filenames_dict(self.xmlsha1dict_1)
    self.assertEqual(self.sha1writer.get_write_sha1sum_and_its_filenames_dict(), self.xmlsha1dict_1)
    self.sha1writer.hash_files_and_reset_sha1sum_and_its_filenames_dict()
    self.assertNotEqual(self.sha1writer.get_sha1sum_and_its_filenames_dict(), self.xmlsha1dict_1)
    self.assertEqual(self.sha1writer.get_write_sha1sum_and_its_filenames_dict(), self.xmlsha1dict_1)
    self.sha1writer.write_xml_sha1file()
    self.assertNotEqual(self.sha1writer.get_write_sha1sum_and_its_filenames_dict(), self.xmlsha1dict_1)


def unittests():
  unittest.main()

def process():
  '''
  '''
  # folder_abspath = sys.argv[1]
  folder_abspath = "/home/dados/TmpDados/dirtree_bak_test/testdir_tree1/"
  reader = Sha1XMLReader(folder_abspath)
  print(reader)

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()  
  process()
