#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This Python module has a collection of sha1sum functions to deal with both 
  1) an XML sha1 file and
  2) a "flat" (plain text) sha1 file
  
These functions are planned to be used from client OO Classes.
One such class, XmlSha1HexFile (in package sha1classes),
  models an XML file (defaulted to name "z-sha1sum.xml") that stores
  sha1hex with filename(s) (yes, a unique sha1sum may corresponding to
  more than one file [ie, copies of it] on a folder
   
Attention to the "encoding problem":
  There is a tricky logic that happens when both reading and writing the Sha1sum XML,
    this is the following:
    ==>>> filenames come from the os.listdir('.') (or perhaps glob.glob())
    When they, the filenames, arrive, their name string encoding is unknown.
    At a moment first, "hope" is set to their being unicode-utf-8.
      Sometimes they are not utf-8 and no clue is given to the encoding under which
      their names were formed.
    
    Because of that, when the filename string is attributed to node.text,
      the UnicodeDecodeError is raised whenever the filename, under a different encoding,
      contains characters other than ASCII or the unicode ones.
      (In our case, we are plagued, so to say, with accented letters in latin-1.)
      
    This program, in such cases, then catches the exception and 
      invoke a function that tries
      to get through into the unicode node.text as the following piece of code shows:

  sha1file_tag.text = None
  for encoding in ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES:
    try:
      sha1file_tag.text = unicode(filename, encoding, 'strict')
      break
    except UnicodeDecodeError:
      pass
  if sha1file_tag.text == None:
    error_msg = 'File System Has Filenames That Have An Unknown Encoding, please try to rename them removing accents.'
    raise sha1exceptions.FileSystemHasFilenamesThatHaveAnUnknownEncoding, error_msg
 

  It's expected that latin-1 (ie, iso-8859-1) will happen a lot
    because external HDs have NTFS file systems with names under latin-1.
    Those HDs are shareable disks between Linux and Windows
    (unfortunately, the Linux ext3/ext4 file systems are very likely not readable by Windows).
    
  So, the above mentioned procedure solves the value attribution at the XML writing moment.
  Because of that, the latin-1 accented filename becomes "dirty" for the human eye inside the XML, 
    but that is not a bug, that is because we did not implement a full conversion, 
    mapping characters from one encoding into utf-8.
  It's not a problem when reading the XML either,
    because, only for those that raised UnicodeDecodeError,
    we keep an extra attribute called 'encoding'.
    If, at reading time, that attribute is there, the code will do:

  filename = node.text
  encoding = node.get('encoding')
  if encoding != None:
    filename = filename.encode(encoding)
    
  ie, if encoding is available, method encode() will be invoked.  This guarantees that
    the os.listdir() filename
    will equal the filename.encode(encoding) value, which is required by this system.    
      
'''
import codecs, os, sha,  time, sys #, shutil,
import xml.etree.ElementTree as ET

import __init__
import local_settings as ls
from sha1utils import defaults
#from sha1utils.Sha1XmlFileFunctions import read_xml_sha1file_into_sha1sum_and_filename_dict 
#from sha1utils.Sha1XmlFileFunctions import generate_xml_sha1file_against_all_files_on_its_folder
from sha1utils.Sha1XmlFileFunctions import transpose_sha1sum_and_filename_dict_into_tuple_list 
from sha1utils.Sha1XmlFileFunctions import generate_sha1hexdigest_from_filepath
# import sha1classes.XmlSha1ExceptionClassesMod as sha1exceptions
from Sha1AndItsFilenamesOnAFolderDictMod import Sha1AndItsFilenamesOnAFolderDict

SHA1_CHUNK_SIZE = 40
ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1','windows-1250','windows-1252']
# file_filter = lambda f : os.path.isfile(f) # unfortunately this lambda only works on current dir processing, deactivating it as a comment

def hash_files_and_get_sha1sum_and_filename_tuple_list_from_folder(folder_abspath):
  '''
  Call the calculating hash_files_and_get_sha1sum_and_filename_dict_from_folder()
  Transpose the dict received into sha1sum_and_filename_tuple_list and return the latter.
  '''
  sha1sum_and_filename_dict = hash_files_and_get_sha1sum_and_filename_dict_from_folder(folder_abspath)
  sha1sum_and_filename_tuple_list = transpose_sha1sum_and_filename_dict_into_tuple_list(sha1sum_and_filename_dict)
  return sha1sum_and_filename_tuple_list  

def hash_files_and_get_sha1sum_and_filename_dict_from_folder(folder_abspath):
  '''
  Loop thru all files on a folder. Hash each one's SHA1 sum, return them as a sha1sum_and_filename_dict 
  '''
  files = os.listdir(folder_abspath)
  # absfiles = bulk_os_path_join(abspath, files) # map(ospathjoinlambda, (abspath, files))
  # print ' 1 :: in get_sha1sum_and_filename_dict_from_folder()... files=', files
  #absfiles = filter(file_filter, absfiles)
  # print ' 2 :: in get_sha1sum_and_filename_dict_from_folder()... files=', files
  # print 'abspath', abspath
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
