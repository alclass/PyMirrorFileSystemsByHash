#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''

'''

import codecs, os, sha,  time, sys #, shutil,
import xml.etree.ElementTree as ET

import __init__
import local_settings as ls
from sha1utils import defaults
#from sha1classes.Sha1FlatReaderWriterMod import read_xml_sha1file_into_sha1sum_and_filename_dict 
#from sha1utilsRWMod import generate_xml_sha1file_against_all_files_on_its_folder
from hexfunctionsMod       import generate_sha1hexdigest_from_filepath
from hexfunctionsMod       import transpose_sha1sum_and_filename_dict_into_tuple_list
from Sha1FlatFileFunctions import find_flat_sha1sum_absfile_ok_or_None
from hexfunctionsMod       import get_tag_text_and_encoding_if_necessary
 
import sha1classes.XmlSha1ExceptionClassesMod as sha1exceptions
from sha1classes.Sha1AndItsFilenamesOnAFolderDictMod import Sha1AndItsFilenamesOnAFolderDict


#SHA1_CHUNK_SIZE = 40
# ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1','windows-1250','windows-1252']
# file_filter = lambda f : os.path.isfile(f) # unfortunately this lambda only works on current dir processing, deactivating it as a comment

def hash_files_and_get_sha1sum_and_filename_dict_from_folder(folder_abspath):
  '''
  Loop thru all files on a folder. Hash each one's SHA1 sum, return them as a sha1sum_and_filename_dict 
  '''
  files = os.listdir(folder_abspath)
  sha1sum_and_filenames_dict = Sha1AndItsFilenamesOnAFolderDict()
  for filename in files:
    file_abspath = os.path.join(folder_abspath, filename)
    if not os.path.isfile(file_abspath):
      continue
    print 'Generating sha1hex for file', filename, '(Please, wait a bit for larger files.)...'
    sha1hex = generate_sha1hexdigest_from_filepath(file_abspath)
    print '... Calculation finished, sha1hex is', sha1hex
    sha1sum_and_filenames_dict[sha1hex] = filename
  return sha1sum_and_filenames_dict

def hash_files_and_get_sha1sum_and_filename_tuple_list_from_folder(folder_abspath):
  '''
  Call the calculating hash_files_and_get_sha1sum_and_filename_dict_from_folder()
  Transpose the dict received into sha1sum_and_filename_tuple_list and return the latter.
  '''
  sha1sum_and_filename_dict = hash_files_and_get_sha1sum_and_filename_dict_from_folder(folder_abspath)
  sha1sum_and_filename_tuple_list = transpose_sha1sum_and_filename_dict_into_tuple_list(sha1sum_and_filename_dict)
  return sha1sum_and_filename_tuple_list  

def decompose_and_verify_xmlsha1file_abspath_is_on_top_of_an_existint_dir_and_is_not_itself_a_dir(xmlsha1file_abspath):
  '''
  This function does the following:
    1) verifies that the xmlsha1file folder exists, if not, it will raised PassedAXmlSha1FileThatIsNotOnTopOfADir
    2) verifies that the xmlsha1file is not a folder, if it is, it will raised PassedAXmlSha1FileThatIsADirNotAFile
    3) returns the tuple folder_abspath, xmlsha1filename 
  '''
  folder_abspath, xmlsha1filename = os.path.split(xmlsha1file_abspath) # _ = xmlsha1filename
  if not os.path.isdir(folder_abspath):
    error_msg = 'Passed Xml Sha1 File (%s) Is Not On Top of A Folder.' %xmlsha1file_abspath
    raise sha1exceptions.PassedAXmlSha1FileThatIsNotOnTopOfADir, error_msg
  if os.path.isdir(xmlsha1file_abspath):
    error_msg = 'Passed Xml Sha1 File (%s) Is Not On Top of A Folder.' %xmlsha1file_abspath  
    raise sha1exceptions.PassedAXmlSha1FileThatIsADirNotAFile, error_msg
  return folder_abspath, xmlsha1filename 

def generate_xmlsha1file_on_folder(xmlsha1file_abspath):
  '''
  This function generate the XML SHA1 file passed in as argument.
  Before attempting any hash calculation:
    + it looks up for a flat sha1 file, 
    ++ if it finds it, it converts it to its XML counterpart
    ++ if a flat sha1 file is not found, it invokes function generate_xml_sha1file_against_all_files_on_its_folder() 
  '''
  folder_abspath, xmlsha1filename = decompose_and_verify_xmlsha1file_abspath_is_on_top_of_an_existint_dir_and_is_not_itself_a_dir(xmlsha1file_abspath)
  #flatsha1file_abspath = find_flat_sha1sum_absfile_or_None(folder_abspath)
  flatsha1file_abspath = find_flat_sha1sum_absfile_ok_or_None(folder_abspath)
  if flatsha1file_abspath != None:
    convert_flat_sha1file_to_xml_sha1file(flatsha1file_abspath, xmlsha1filename)
  else:
    generate_xml_sha1file_against_all_files_on_its_folder(xmlsha1file_abspath)

def generate_xml_sha1file_against_all_files_on_its_folder(xmlsha1file_abspath):
  '''
  This function generates an XML sha1hex file containing the sha1hex of all files
    residing on the same folder of the xmlsha1file passed in.
  
  Obs.: No up directory tree walk is done, ie, subdirectories are discarded, 
        only files within the folder are considered.
  '''
  folder_abspath, _ = decompose_and_verify_xmlsha1file_abspath_is_on_top_of_an_existint_dir_and_is_not_itself_a_dir(xmlsha1file_abspath)
  sha1sum_and_filename_tuple_list = hash_files_and_get_sha1sum_and_filename_tuple_list_from_folder(folder_abspath)
  write_xml_sha1file(sha1sum_and_filename_tuple_list, xmlsha1file_abspath)

def read_xml_sha1file_into_sha1sum_and_filename_dict(xmlsha1file_abspath, create=True, second_try=False):
  '''
  This is only function that reads a given XML SHA1 File and return sha1sum_and_filename_dict
  '''
  if not os.path.isfile(xmlsha1file_abspath):
    absfolder, xmlsha1filename = os.path.split(xmlsha1file_abspath)
    if not os.path.isdir(absfolder):
      raise OSError, 'Folder %s does not exist.' %absfolder
    xmlsha1file_abspath = os.path.join(absfolder, xmlsha1filename)
    if not create:
      return None
    else: # ie, if create:
      if second_try:
        error_msg = 'xml_sha1_absfile [%s] DOES NOT STILL EXIST after attempt to create it. Interrupting execution.' %xmlsha1file_abspath
        raise OSError, error_msg 
      print 'generate_xml_sha1file(absfolder) xmlsha1file_abspath', xmlsha1file_abspath
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
        filename = unicode(filename)
      except UnicodeDecodeError:
        # attribute 'encoding' may not exist, if it does, the unicode variable filename will have to receive method .encode(encoding), filename.encode(encoding)
        encoding = filename_tag.get('encoding')
        if encoding != None:
          filename = filename.encode(encoding)
      sha1sum_and_filename_dict[sha1hex] = filename
      #print 'sha1hex, filename', sha1hex, filename
  #print 'sha1sum_and_filename_dict', sha1sum_and_filename_dict

def read_xml_sha1file_into_sha1sum_and_filename_tuple_list(xmlsha1file_abspath, create=True):
  '''
  This function is a wrapper around the above one (read_xml_sha1file_into_sha1sum_and_filename_dict())
  Once received the dict, it return its tuple list transpose.
  '''
  sha1sum_and_filename_dict = read_xml_sha1file_into_sha1sum_and_filename_dict(xmlsha1file_abspath, create)
  return transpose_sha1sum_and_filename_dict_into_tuple_list(sha1sum_and_filename_dict)

def write_xml_sha1file(sha1sum_and_filename_tuple_list, xmlsha1file_abspath, deleteXmlSha1FileIfItExists=False):
  '''
  This is (and must be anyway!) the only function that writes to a given XML SHA1 File.
  '''
  _, _ = decompose_and_verify_xmlsha1file_abspath_is_on_top_of_an_existint_dir_and_is_not_itself_a_dir(xmlsha1file_abspath) 
  if os.path.isfile(xmlsha1file_abspath):
    if deleteXmlSha1FileIfItExists:
      os.remove(xmlsha1file_abspath)
    else:
      pass # ok, it's a rewrite, ie, file exists and do not delete it
  xml_root = ET.Element("sha1files")
  xml_root.set("timestamp", str(time.time()))
  for sha1sum_and_filename_tuple in sha1sum_and_filename_tuple_list:
    sha1file_tag = ET.SubElement(xml_root, "sha1file")
    sha1hex = sha1sum_and_filename_tuple[0]
    sha1file_tag.set("sha1hex", sha1hex)
    filenames = sha1sum_and_filename_tuple[1]
    if len(filenames) == 0: # None or filename == '':
      error_msg = 'A sha1hex (%s) was sent to be file-written with a corresponding filename' %sha1hex
      raise OSError, error_msg

    filenames_tag = ET.SubElement(sha1file_tag, 'filenames')
    
    for filename in filenames:
      filename_tag = ET.SubElement(filenames_tag, 'filename')
      try:
        filename_tag.text = unicode(filename)
        # print 'filename in tag is', filename
      except UnicodeDecodeError:
        get_tag_text_and_encoding_if_necessary(filename_tag, filename)
  tree = ET.ElementTree(xml_root)
  print ':: Writing XML', xmlsha1file_abspath
  tree.write(xmlsha1file_abspath)

def write_xml_sha1file_with_sha1sum_and_filename_dict(sha1sum_and_filename_dict, xmlsha1file_abspath):
  '''
  This function is a wrapper for the above function write_xml_sha1file(abspath, sha1sum_and_filename_tuple_list)
  It firstly transform the sha1sum/filename dict into a tuple list, then it's ready to call the wrapped function
  '''
  # the validate() method guarantees there won't be a filename having 2 sha1sums
  # if it does, an exception will be raised
  sha1sum_and_filename_dict.validate()
  sha1sum_and_filename_tuple_list = transpose_sha1sum_and_filename_dict_into_tuple_list(sha1sum_and_filename_dict)
  write_xml_sha1file(sha1sum_and_filename_tuple_list, xmlsha1file_abspath)


import unittest
class Test1(unittest.TestCase):
  
  def test_convert_one_digit_hex_to_str(self):
    pass

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
