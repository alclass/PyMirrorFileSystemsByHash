#!/usr/bin/env python3
'''
This Python module has a collection of sha1sum functions to deal with both 
  1) an XML sha1 file and
  2) a "flat" (plain text) sha1 file
  
These functions are planned to be used from client OO Classes.
One such class sha1classes XmlSha1HexFile (in sha1classes.XmlSha1HexFileMod).
  which models an XML file (defaulted to name "z-sha1sum.xml") that stores
  sha1hex with filename(s) (yes, a unique sha1sum may corresponding to
  more than one file [ie, copies of it] on a folder
   
Attention to the "encoding problem":
  There is a tricky logic that happens when both reading and writing the Sha1sum XML,
    this is the following:
    ==>>> filenames come from the os.listdir('.') (or perhaps glob.glob())
    When they, the filenames, arrive, their name string encoding is unknown, 
    though "hope" is set to their being unicode-utf-8, but sometimes they are not utf-8. 
    
    Because of that, when the filename string is attribute to node.text,
      the UnicodeDecodeError will be raised whenever the filename, in a different encoding,
      for example, contains accents (or, generally, characters other than ASCII).
      
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
    for the collection we have under NTFS,
    those are shareable disks by Linux and Windows
    (the ext3/ext4 are very likely not readable by Windows)
    
  So, this solves the value attribution at the XML writing moment.
  Because of that, the latin-1 accented filename becomes "dirty" for the human eye inside the XML, 
    but that is not a bug, that is because we did not implement a full conversion, 
    mapping characters from one encoding into utf-8.
  It's not a problem either when reading the XML, because, only for those that raised UnicodeDecodeError, we
    keep an extra attribute called 'encoding'. If, at reading time, that attribute is there, the code will do:

  filename = node.text
  encoding = node.get('encoding')
  if encoding != None:
    filename = filename.encode(encoding)
    
  ie, if encoding is available, method encode() will be invoked.  This guarantees that the os.listdir() filename
    will equal the filename.encode(encoding) value, which is required by this system.    
      
'''
import codecs, os, sha,  time, sys #, shutil,
import xml.etree.ElementTree as ET

import defaults
from models import sha1classes as sha1exceptions

#from sha1utilsRWMod import find_flat_sha1sum_absfile_ok_or_None
#from XmlFunctions import generate_xml_sha1file_against_all_files_on_its_folder

ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1','windows-1250','windows-1252']

def generate_sha1hexdigest_from_filepath(file_abspath):
  '''
  This functions mimics, so to say, the sha1sum "bash" executable from the command line.
  It reads a file and passes its contents to the sha.new() method,
    then, returns the object's hex-digest 40-char hexadecimal string. 
  '''
  if os.path.isfile(file_abspath):
    content = open(file_abspath, 'rb').read()
    shaObj  = sha.new(content)
    return shaObj.hexdigest()
  return None

def fetch_sha1sum_and_filename_dict_if_a_conventioned_flatsha1file_exists(abspath):
  '''
  It tries to find the a flat sha1 file on the folder given as parameter.
  If it finds it, return its corresponding sha1sum_and_filename_dict
  '''
  sha1_file_abspath = find_flat_sha1sum_absfile_or_None(abspath)
  if sha1_file_abspath == None:
    return None
  return fetch_sha1sum_and_filename_dict_from_flat_sha1file(sha1_file_abspath)

def fetch_sha1sum_and_filename_tuple_list_if_a_conventioned_flatsha1file_exists(abspath):
  '''
  This function is a wrapper around the above one (fetch_sha1sum_and_filename_dict_if_a_conventioned_flatsha1file_exists())
  If a dict is returned, it will return its "tuple list transpose".
  '''
  sha1sum_and_filename_dict = fetch_sha1sum_and_filename_dict_if_a_conventioned_flatsha1file_exists(abspath)
  if sha1sum_and_filename_dict == None:
    return None
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



def add_sha1hex_filename_line_to_flatsha1file_on_folder(sha1hex_and_filename_line, folder_abspath, flatsha1filename=None):
  '''
  [[[ This function needs testing ! ]]]
  
  [[[ Obs.: This functionality, in the case of XML SHA1 files, should be implement in a OO Class, 
            not a module function. ]]]
  
  Perhaps it's better to refactor it to instead
    1) copy the flat sha1 file,
    2) change the copy (adding the new line for instance), 
    3) delete the original flat sha1 file, and then
    4) rename the copied file back to its original name
  '''
  if flatsha1filename == None:
    sha1_file_abspath = find_flat_sha1sum_absfile_or_None(folder_abspath)
    if not os.path.isfile(sha1_file_abspath):
      flatsha1filename = defaults.DEFAULT_TEXT_SHA1_FILENAME
      sha1_file_abspath = os.path.join(folder_abspath, flatsha1filename) 
  sha1hex_and_filename_line += '\n' + sha1hex_and_filename_line
  f = codecs.open(sha1_file_abspath, 'a', 'utf8')
  f.write(sha1hex_and_filename_line)
  f.close()
  return True


def get_tag_text_and_encoding_if_necessary(sha1file_tag, filename):
  '''
  This is an important function and has been also documented at the beginning of this modolue.
  It is called when UnicodeDecodeError is raised, meaning that we need to try some encodings to "make it go" to the node.text field.
  Once it's successful, an extra XML-attribute, called 'encoding', is added to the file tag.
  This extra attribute is essential at the moment when this file-tag is read from the XML.
  '''
  sha1file_tag.text = None
  for encoding in ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES:
    try:
      sha1file_tag.text = unicode(filename, encoding, 'strict')
      #print 'filename in tag is', filename, 'encoding is', encoding 
      break
    except UnicodeDecodeError:
      pass
  if sha1file_tag.text == None:
    error_msg = 'File System Has Filenames That Have An Unknown Encoding, please try to rename them removing accents.'
    raise sha1exceptions.FileSystemHasFilenamesThatHaveAnUnknownEncoding, error_msg
  sha1file_tag.set('encoding', encoding)



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
