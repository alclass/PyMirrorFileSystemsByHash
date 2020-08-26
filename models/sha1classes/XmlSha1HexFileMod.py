#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
XmlSha1HexFileMod.py
This module contains the XmlSha1HexFile class
'''

import datetime
import os, time, sys
import xml.etree.ElementTree as ET

# import local_settings as ls
# import sha1utils.sha1utilsMod as sha1
from fs.hashfunctions.hexfunctionsmod import generate_sha1hexdigest_from_filepath
from fs.os.Sha1XmlFileFunctions import read_xml_sha1file_into_sha1sum_and_filename_dict
from fs.os.Sha1XmlFileFunctions import generate_xmlsha1file_on_folder
from fs.os.Sha1XmlFileFunctions import hash_files_and_get_sha1sum_and_filename_dict_from_folder
from fs.os.Sha1XmlFileFunctions import write_xml_sha1file

from fs.os import defaults
import XmlSha1ExceptionClassesMod as sha1exceptions


class XmlSha1HexFile(object):
  '''
  TO-DO:
    This is one design issue to improve when time allows.
    This TO-DO is mentioned in the __doc__ below the UnicodeDecodeError 'catch' (except) in method update_sha1_xml_file_if_needed() 
  
        This problem was observed a bit later.
        From the design viewpoint, we read only the sha1hex and filename into a dict and forget about the encoding when it happens
        Because of that, a UnicodeDecodeError is raised and caught here
        TO-DO: to avoid the raising and catching, we have to extend the dict's functionality to bring out the encoding information to it

  
  This class models, via its per-folder respective XML file (defaulted as 'z-sha1sum.xml'),
    files with their corresponding sha1 hash.

  The XML file conveys, at least, the following info:
  
  <file sha1hex="<the 40-char hex string>">filename</file>
  
  ie, it brings at least a tag with an attribute called sha1hex and its content being the filename
  
  Some important functionality of this class:
  
  1) when an object is instantiated, it verifies that the target folder has an XML SHA1 file
  2) If it does not, this XML SHA1 file will be created automatically
  3) It also verifies whether or not the folder's files coincide with the XML info,
     it it does not, a synchronization takes place, and files that are in the XML that do not exist
     on folder will be deleted and files on folder not on the XML will be included in the XML
  
  The synchronization mentioned above just checks for filenames equality, ie, their sha1's are not checked.
  
  However, method verify_recalculating_sha1sums() will recalculate the sha1's and check them against the XML.
    The recalculated replace the old sha1s in case they differ. 
    
  Internally, there are lots of methods in this class that should be "understood" as "private", for they
    manipulate the XML internally and should not be invoked by other models.
  
  '''
  @staticmethod
  def get_sha1hex_from_file_on_dir(filename, dir_abspath):
    sha1hexer = XmlSha1HexFile(dir_abspath)
    return sha1hexer.get_corresponding_sha1hex_having_filename_from_dict(filename)

  def __init__(self, folder_abspath, xml_sha1hex_filename=None):
    self.xmlsha1file_abspath = None
    self.xml_tree = None
    self.folder_abspath = folder_abspath
    self.set_xml_sha1hex_filename_or_default(xml_sha1hex_filename)
    self.sha1sum_and_filenames_dict = read_xml_sha1file_into_sha1sum_and_filename_dict(self.xmlsha1file_abspath)
    self.filename_sha1sum_inverted_dict = None # to be init'ed lazily upon demand
    # the xml_sha1hex_filename is the only file that is excluded from the sha1 catalog (ie, the file that informs the sha1's, itself, is out of this compendium -- see also method verify_recalculating_sha1sums() for more info.)
    self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()
    self.remove_file(self.xml_sha1hex_filename)
    self.update_sha1_xml_file_if_needed()

  def set_xml_sha1hex_filename_or_default(self, xml_sha1hex_filename):
    if xml_sha1hex_filename != None:
      self.xml_sha1hex_filename = xml_sha1hex_filename
    else:
      self.xml_sha1hex_filename = defaults.XML_SHA1_FILENAME
    self.verify_sha1_folder_and_file_existence()

  def verify_sha1_folder_and_file_existence(self):
    if not os.path.isdir(self.folder_abspath):
      error_msg = 'Xml Sha1 Folder %s Does Not Exist' %self.folder_abspath
      raise sha1exceptions.XmlSha1FolderDoesNotExist, error_msg 
    self.xmlsha1file_abspath = os.path.join(self.folder_abspath, self.xml_sha1hex_filename)
    if not os.path.isfile(self.xmlsha1file_abspath):
      generate_xmlsha1file_on_folder(self.xmlsha1file_abspath)

  def get_filenames_encoding_if_any(self, xml_filename):
    '''
    '''
    encoding = None
    if self.xml_tree == None:
      self.xml_tree = ET.parse(self.xmlsha1file_abspath)
    xml_root = self.xml_tree.getroot()
    #xml_element = xml_root.findtext(xml_filename)
    for xml_element in xml_root.iter():
      if xml_filename == xml_element.text:
        encoding = xml_element.get('encoding')
        break
    #self.get_corresponding_sha1hex_having_filename_from_dict(xml_filename)
    return encoding

  def update_sha1_xml_file_if_needed(self):
    '''
    This method is executed from the constructor, ie, at instantiation time
    '''
    print 'Executing update_sha1_xml_file_if_needed()'
    if self.sha1sum_and_filenames_dict == None:
      return
    xml_filenames = self.sha1sum_and_filenames_dict.values()
    xml_filenames_yet_to_check_for_corresponding_filename_on_folder = xml_filenames[:] 
    on_folder_filenames = os.listdir(self.folder_abspath)
    has_updated = False
    sha1hexs_to_include_into_xml = []
    sha1hex_to_delete_from_xml   = []
    for on_folder_filename in on_folder_filenames:
      if on_folder_filename == self.xml_sha1hex_filename:
        continue
      if on_folder_filename in xml_filenames:
        xml_filenames_yet_to_check_for_corresponding_filename_on_folder.remove(on_folder_filename)
        continue
      on_folder_filename_abspath = os.path.join(self.folder_abspath, on_folder_filename)
      if os.path.isfile(on_folder_filename_abspath):
        sha1hex = generate_sha1hexdigest_from_filepath(on_folder_filename_abspath)
        self.sha1sum_and_filenames_dict[sha1hex] = on_folder_filename
        sha1hexs_to_include_into_xml.append(sha1hex)
        has_updated = True
    for xml_filenames in xml_filenames_yet_to_check_for_corresponding_filename_on_folder:
      for xml_filename in xml_filenames:
        try:
          supposed_on_folder_filename_abspath = os.path.join(self.folder_abspath, xml_filename)
        except UnicodeDecodeError:
          '''
          This problem was observed a bit later.
          From the design viewpoint, we read only the sha1hex and filename into a dict and forget about the encoding when it happens
          Because of that, a UnicodeDecodeError is raised and caught here
          TO-DO: to avoid the raising and catching, we have to extend the dict's functionality to bring out the encoding information to it
          '''
          encoding = self.get_filenames_encoding_if_any(xml_filename)
          if encoding == None:
            continue
          xml_filename = xml_filename.encode(encoding)
          supposed_on_folder_filename_abspath = os.path.join(self.folder_abspath, xml_filename)
          if not os.path.isfile(supposed_on_folder_filename_abspath):
            sha1hex = self.get_corresponding_sha1hex_having_filename_from_dict(xml_filename)
            # in renaming cases, the including sha1hex will also be the excluding sha1hex, so do not remove it in this case
            if sha1hex in sha1hexs_to_include_into_xml:
              continue  
            del self.sha1sum_and_filenames_dict[sha1hex]
            sha1hex_to_delete_from_xml.append(sha1hex) 
            has_updated = True
        # except's end
    if has_updated:
      print 'Update was needed.'
      print '+ Inclusions: %d sha1hexs included: %s' %(len(sha1hexs_to_include_into_xml), str(sha1hexs_to_include_into_xml))
      print '- Removals:   %d sha1hexs removed:  %s' %(len(sha1hex_to_delete_from_xml), str(sha1hex_to_delete_from_xml))
      self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()
      self.save()
    else:
      print 'No update was needed. Filenames, both on the XML Sha1 File and on folder, are the same. (Hashes themselves were not recalculated.)'

  def transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files(self):
    '''
    This method transposes the xmldict to TWO lists: one is sha1hexs, the other is files_in_xml
    '''
    if self.sha1sum_and_filenames_dict == None:
      return
    self.sha1sum_and_filename_tuple_list = self.sha1sum_and_filenames_dict.items()
    self.sha1hexs_in_xml, self.files_in_xml = [], []
    if len(self.sha1sum_and_filename_tuple_list) > 0: # the tuplelist is empty, a ValueError exception is raised by zip(*), another impoementation is a try/except ValueError   
      self.sha1hexs_in_xml, self.files_in_xml = zip(*self.sha1sum_and_filename_tuple_list)

  def init_inverted_sha1hex_filenames_dict(self):
    self.filename_sha1sum_inverted_dict = {}
    if self.sha1sum_and_filenames_dict == None:
      return
    for sha1hex in self.sha1sum_and_filenames_dict:
      filenames = self.sha1sum_and_filenames_dict[sha1hex]
      for filename in filenames:
        self.filename_sha1sum_inverted_dict[filename] = sha1hex 

  def get_corresponding_sha1hex_having_filename_from_dict(self, filename, reread=False):
    if self.filename_sha1sum_inverted_dict == None and not reread:
      self.init_inverted_sha1hex_filenames_dict()
    try:
      sha1hex = self.filename_sha1sum_inverted_dict[filename]
      return sha1hex
    except KeyError:
      pass
    return None

  def get_files_in_xml(self):
    files = []
    if self.sha1sum_and_filenames_dict == None:
      return []
    for sha1hex in self.sha1sum_and_filenames_dict:
      files = self.sha1sum_and_filenames_dict[sha1hex]
      for filename in files:
        files.append(filename)
    return files

  def find_corresponding_sha1hex_in_xml(self, filename):
    if filename not in self.get_files_in_xml():
      if filename == self.xml_sha1hex_filename:
        return None
      error_msg = 'find_corresponding_sha1hex_in_xml(self, filename=[%s]' %(filename)
      raise sha1exceptions.CorrespondingFileInSha1XmlNotFound, error_msg
    index = self.files_in_xml.index(filename)
    sha1hex_in_xml = self.sha1hexs_in_xml[index]
    shouldbe_filename = self.sha1sum_and_filenames_dict[sha1hex_in_xml]
    if filename != shouldbe_filename:
      error_msg = 'A logical error happened.  sha1hex was not retrieved correctly :: sha1hex_in_xml=%s file (is=%s, shouldbe=%s)' %(sha1hex_in_xml, filename, shouldbe_filename)
      raise sha1exceptions.LogicalProgramErrorCorrespondingFileAndSha1hexMismatch, error_msg
    return sha1hex_in_xml   
  
  def generate_sha1hexdigest_for_filename(self, filename):
    file_abspath = os.path.join(self.abspath, filename)
    # print 'Generating sha1hex for', filename, '...'
    generated_sha1hex = generate_sha1hexdigest_from_filepath(file_abspath)
    # print 'ready...', generated_sha1hex 
    return generated_sha1hex

  def correct_sha1hex_for_filename(self, incorrect_sha1hex, correct_sha1hex, filename):
    '''
    This method deletes the dict key incorrect_sha1hex and sets filename to the key correct_sha1hex 
    '''
    del self.sha1sum_and_filenames_dict[incorrect_sha1hex] #self.remove_sha1hex(incorrect_sha1hex)
    self.sha1sum_and_filenames_dict[correct_sha1hex] = filename
    self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()

  def insert_sha1hex_filename_into_dict(self, sha1hex_to_insert, filename_to_insert):
    if sha1hex_to_insert == None:
      error_msg = 'sha1hex_to_insert is None, filename = [%s]' %filename_to_insert
      raise sha1exceptions.Sha1hexPassedAsNone, error_msg
    print '>>> XML-sha1file inserting', sha1hex_to_insert, filename_to_insert
    self.sha1sum_and_filenames_dict[sha1hex_to_insert] = filename_to_insert
    self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()

  def insert_file(self, filename_to_insert, sha1hex_to_insert=None):
    '''
    Possible scenarios:
    1) neither in-file nor its corresponding sha1hex exists in xml, so insert "tuple"
    2) in-file does not exist, but sha1hex does,
     so rename sha1hex's current filename:
        2.1) if sha1hex coincides,
        2.2) if not, raise exception
    3) both in-file and sha1hex exist, so raise an exception
    '''
    if filename_to_insert in self.files_in_xml:
      return self.treat_insert_file_when_file_is_already_in_xml(filename_to_insert, sha1hex_to_insert)
    generated_sha1hex = self.generate_sha1hexdigest_for_filename(filename_to_insert)
    if sha1hex_to_insert != None:
      return self.treat_insert_file_when_sha1hex_is_given(filename_to_insert, sha1hex_to_insert, generated_sha1hex)
    return self.insert_sha1hex_filename_into_dict(generated_sha1hex, filename_to_insert)

  def treat_insert_file_when_sha1hex_is_given(self, filename_to_insert, sha1hex_to_insert, generated_sha1hex):
    if generated_sha1hex != sha1hex_to_insert:
      error_msg = 'Tried to xml-insert a filename with incorrect sha1hex %s sha1hex-sent=%s shouldbe=%s' %(filename_to_insert, sha1hex_to_insert, generated_sha1hex)
      raise sha1exceptions.IncorrectFilenameXmlInsertionWithErroneousSha1hex, error_msg
    # else: # sha1hex_to_insert == None
    # this is a rename/replace!
    return self.insert_sha1hex_filename_into_dict(generated_sha1hex, filename_to_insert)

  def treat_insert_file_when_file_is_already_in_xml(self, filename_to_insert, sha1hex_to_insert=None):
    sha1hex_in_xml = self.find_corresponding_sha1hex_in_xml(filename_to_insert)
    if sha1hex_to_insert != None:
      if sha1hex_in_xml == sha1hex_to_insert:
        error_msg = 'Tried to insert a filename that is already inside xml.'
        raise sha1exceptions.IssuedAFilenameXmlInsertionForAFileAlreadyInserted, error_msg
      else: # ie, sha1hex is different from the one in xml
        generated_sha1hex = self.generate_sha1hexdigest_for_filename(filename_to_insert)
        if generated_sha1hex == sha1hex_in_xml:
          error_msg = 'Tried to insert a filename with an incorrect sha1hex file=%s tried_sha1=%s shouldbe_sha1=%s' %(filename_to_insert, generated_sha1hex, sha1hex_in_xml)
          raise sha1exceptions.IncorrectFilenameXmlInsertionWithErroneousSha1hex, error_msg
        else:
          return self.correct_sha1hex_for_filename(sha1hex_in_xml, generated_sha1hex, filename_to_insert)  
    else: # ie, sha1hex_to_insert is None
      generated_sha1hex = self.generate_sha1hexdigest_for_filename(filename_to_insert)
      return self.insert_sha1hex_filename_into_dict(filename_to_insert, generated_sha1hex)
    # program flow should not enter here
    error_msg = 'Could not insert a filename / sha1hex pair (possible logical flow problem) file=%s sha1_to_insert=%s' %(filename_to_insert, sha1hex_to_insert)
    raise sha1exceptions.LogicalErrorFilenameAndSha1hexPairWereNotXmlInserted, error_msg
  
  def remove_file(self, filename):
    sha1hex_in_xml = self.find_corresponding_sha1hex_in_xml(filename)
    if sha1hex_in_xml == None:
      return
    print '>>> XML-sha1file removing', sha1hex_in_xml, filename
    self.remove_sha1hex(sha1hex_in_xml)
    
  def remove_sha1hex(self, sha1hex):
    del self.sha1sum_and_filenames_dict[sha1hex]
    self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()
    
  def rename_file(self, old_filename, new_filename):
    sha1hex_in_xml = self.find_corresponding_sha1hex_in_xml(old_filename)
    if sha1hex_in_xml == None:
      error_msg = 'Tried to rename a filename with no corresponding sha1hex file=%s' %(old_filename)
      raise sha1exceptions.RenameFileWithNoCorrespondingSha1hex, error_msg
    self.insert_sha1hex_filename_into_dict(sha1hex_in_xml, new_filename)
    
  def is_xmlsha1files_timestamp_less_than_3_hours(self):
    '''
    The purpose of this method is to read the xml sha1 file's internal timestamp and 
    return True if it's less than 3 hours old, False otherwise
    '''
    xml_tree = ET.parse(self.xmlsha1file_abspath)
    xml_root = xml_tree.getroot()  
    timestamp = xml_root.get('timestamp')
    datetime_then = datetime.datetime.fromtimestamp(float(timestamp))
    datetime_now = datetime.datetime.now()
    delta_t = datetime_now - datetime_then
    if delta_t.days > 0:
      return False
    decimal_hours = delta_t.seconds / 3600.0
    if decimal_hours >= 3:
      return False
    return True
        
  def remove_default_xml_sha1_file_from_the_recalculated_dict(self, recalculated_sha1sum_and_filename_dict):
    recalculated_sha1hex_and_filename_tuple_list = recalculated_sha1sum_and_filename_dict.items()
    recalculated_sha1s, recalculated_filenames = zip(*recalculated_sha1hex_and_filename_tuple_list)
    try:
      i = recalculated_filenames.index(self.xml_sha1hex_filename)
      sha1_to_remove_from_recalculated_dict = recalculated_sha1s[i]
      del recalculated_sha1sum_and_filename_dict[sha1_to_remove_from_recalculated_dict]
    except ValueError:
      pass
  
  def verify_recalculating_sha1sums(self):
    '''
    This method calls function sha1.get_sha1sum_and_filename_dict_from_folder(folder)
      that recalculates all sha1sums on folder.
    It then compares the recalculated pairs (sha1hex and filenames) with the current ones held.
    This comparison may generate 3 kinds of action:
      1) remove sha1hexs that do not exist anymore
      2) include sha1hexs that appeared on the recalculated process
      3) rename a filename according to its opposing sha1hex that has a different/new filename
      
    Exception to running this method:
    1) if the sha1hexs have "just" (*) been calculated, 
       this method will not run (*) this "just" is within 3 hours)
    2) if more than 3 hours have elapsed, okay, the method will
    This restriction is just to avoid a recalculation when at instantiation time 
      a calculation will issued due to the fact that no xml sha1 file existed previously
    '''
    if self.is_xmlsha1files_timestamp_less_than_3_hours():
      return False # meaning "method has not run"
    print 'verify_recalculating_sha1sums:', time.ctime()
    recalculated_sha1sum_and_filename_dict = hash_files_and_get_sha1sum_and_filename_dict_from_folder(self.folder_abspath)
    # the xml sha1 file does not enter into play, ie, it will always need to update itself (a recursing cycling), so it's put out
    self.remove_default_xml_sha1_file_from_the_recalculated_dict(recalculated_sha1sum_and_filename_dict)
    sha1sums_to_include_into_xml     = []
    sha1sums_to_erase_from_xml       = []
    sha1hexs_with_their_newfilenames = []
    for sha1sum in self.sha1sum_and_filenames_dict.keys():
      if sha1sum not in recalculated_sha1sum_and_filename_dict.keys():
        sha1sums_to_erase_from_xml.append(sha1sum)
      else:
        origin_filename = self.sha1sum_and_filenames_dict[sha1sum]
        target_filename = recalculated_sha1sum_and_filename_dict[sha1sum]
        if origin_filename != target_filename:
          sha1hex_with_its_newfilename = (sha1sum, target_filename)
          sha1hexs_with_their_newfilenames.append(sha1hex_with_its_newfilename)
    for target_sha1sum in recalculated_sha1sum_and_filename_dict.keys():
      if target_sha1sum not in self.sha1sum_and_filenames_dict.keys():
        target_filename = recalculated_sha1sum_and_filename_dict[target_sha1sum]
        sha1hex_and_filename_to_include = (target_sha1sum, target_filename)
        sha1sums_to_include_into_xml.append(sha1hex_and_filename_to_include)
    self.do_removals(sha1sums_to_erase_from_xml)
    self.do_includes(sha1sums_to_include_into_xml)
    self.do_renames(sha1hexs_with_their_newfilenames)
    print 'Verify-Stats:'
    print 'Total sha1sums_to_erase_from_xml', len(sha1sums_to_erase_from_xml)
    print 'Total sha1sums_to_include_into_xml', len(sha1sums_to_include_into_xml)
    print 'Total sha1hexs_with_their_newfilenames', len(sha1hexs_with_their_newfilenames)
    return True # meaning "method has run"

  def do_removals(self, sha1sums_to_erase_from_xml):
    for sha1hex in sha1sums_to_erase_from_xml:
      del self.sha1sum_and_filenames_dict[sha1hex]
    self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()
      
  def do_includes(self, sha1sums_to_include_into_xml):
    for sha1hex_and_filename_to_include in sha1sums_to_include_into_xml:
      sha1hex_to_insert, filename_to_insert = sha1hex_and_filename_to_include
      self.sha1sum_and_filenames_dict[sha1hex_to_insert] = filename_to_insert
    self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()
      
  def do_renames(self, sha1hexs_with_their_newfilenames):
    for sha1hex_with_its_newfilename in sha1hexs_with_their_newfilenames:
      sha1hex, newfilename = sha1hex_with_its_newfilename 
      self.sha1sum_and_filenames_dict[sha1hex] = newfilename
    self.transpose_xmldict_to_the_2_list_attribs_sha1hexs_and_files()
    
  def save(self):
    try:
      self.sha1sum_and_filename_tuple_list.sort(key = lambda list_obj: list_obj[1])
    except UnicodeDecodeError:
      pass # unfortunelaty, as we have it now, when accented letters from different encodings are found, sorting will not take place (to think about a solution) 
    # print 'Save', self.sha1sum_and_filename_tuple_list 
    write_xml_sha1file(self.sha1sum_and_filename_tuple_list, self.xmlsha1file_abspath)


def update_xmlsha1file_if_needed():
  folder_abspath = '/home/dados/Sw3/SwDv/CompLang SwDv/Python SwDv/pyosutils_etc/dir_trees_comparator/'
  try:
    folder_abspath = sys.argv[1] # '/media/SAMSUNG/youtube-various/youtube.com-user-SecurityTubeCons (BlackHat 2010 et al)/'
  except IndexError:
    pass
  if not os.path.isdir(folder_abspath):
    raise OSError, 'folder_abspath %s does not exist. Either enter a existing path or redefine the default one.'
  _ = XmlSha1HexFile(folder_abspath) # _ = sha1obj
  #sha1obj.verify_recalculating_sha1sums()

def do_generate_xmlsha1file_for_folder_cli_option():
  folder_abspath = None
  try:
    folder_abspath = sys.argv[2]
  except IndexError:
    pass
  if folder_abspath == None:
    folder_abspath = os.path.abspath('')
  if not os.path.isdir(folder_abspath):
    print folder_abspath, 'does not exist. Stopping.'
    sys.exit(1)
  xmlsha1obj = XmlSha1HexFile(folder_abspath)
  xmlsha1obj.update_sha1_xml_file_if_needed()
  print 'Opening folder %s' %folder_abspath
  os.system('caja "%s"' %folder_abspath) 


def process():
  if 'genxmlsha1' in sys.argv:
    do_generate_xmlsha1file_for_folder_cli_option()
    return
  update_xmlsha1file_if_needed()
  pass
  
if __name__ == '__main__':
  process()
