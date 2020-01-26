#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
      
'''
import codecs, os, sha,  time, sys #, shutil,
import xml.etree.ElementTree as ET

import __init__
import local_settings as ls
import defaults

ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1','windows-1250','windows-1252']

def is_flat_sha1file_ok(flat_sha1_absfile):
  '''
  This function contains 3 logical tests to help verify that the contents of a (Unix) flat sha1file are consistent.
  For this, it does:
    1st) it takes the sha1 40-hex-char string and verify that all 40 are within '0123456789abcdef'
    2nd) verify there is a double-space separating the sha1hex from the corresponding file name
    3rd) verify that the corresponding file name exists as a file on the same folder as the sha1file resides
  
  If all 3 verifications pass, an OK, as True, is returned
  If entering parameter flat_sha1_absfile is None, False is returned
  '''
  if flat_sha1_absfile == None:
    return False
  try:
    lines = codecs.open(flat_sha1_absfile, 'r', 'utf-8').readlines()
  except OSError:
    return False
  for line in lines:
    # print 'line', line
    if line == '\n':
      continue
    sha1_chunk = line[ : 40]
    should_be_empty = filter(nonhex_filter, sha1_chunk)
    if len(should_be_empty) > 0:
      print 'false should_be_empty [%s]' %should_be_empty, 'line', line
      return False 
    double_space = line[ 40 : 42 ]
    if double_space != '  ':
      # print 'False double_space = [%s]' %double_space, 'line', line
      return False
    filename = line [42 : ]
    if filename.endswith('\n'):
      filename = filename.rstrip('\n')
    # in fact, file existence check is not needed, because files might have been renamed
    ''' 
    dirpath, xml_sha1file = os.path.split(flat_sha1_absfile)
    should_be_absfile = os.path.join(dirpath, filename)
    if not os.path.isfile(should_be_absfile):
      print 'false NOT EXISTS [%s]' %should_be_absfile, 'line', line
      return False
    '''
  return True

def find_flat_sha1sum_absfile_or_None(folder_abspath):
  '''
  This function searches for a file with name starting with 'sha1sum-' and having extension .txt
  It raises an exception if more than one file meet that condition
  On the other hand, if no files meet that condition, None is returned
  '''
  flat_sha1file_abspath = None
  contents = os.listdir(folder_abspath)
  for content in contents:
    if content.endswith('.txt'):
      if content.find('sha1sum') > -1:
        flat_sha1file_abspath = os.path.join(folder_abspath, content)
        if os.path.isfile(flat_sha1file_abspath):
          return flat_sha1file_abspath
  return None

def find_flat_sha1sum_absfile_ok_or_None(abspath):
  '''
  It first tries to find a file with a consistent name for a flat sha1 file (eg. z-sha1sum.txt)
  Then, it verifies the consistency of its content lines. If ok, the file's abspath will be returned.
  Otherwise, None is returned. An exception may also be raised.
  '''
  try:
    flat_sha1file_abspath = find_flat_sha1sum_absfile_or_None(abspath)
    if is_flat_sha1file_ok(flat_sha1file_abspath):
      return flat_sha1file_abspath
  except OSError:
    pass
  return None

def generate_flat_sha1file_text_for_folder(folder_abspath):
  '''
  This function generates a string line in the same manner as the command line "sha1sum" (Linux/Unices),
    ie, it puts the 40-char hexadecimal string first, then a double-space, then the corresponding file name
    (...)
  '''
  sha1sum_and_filename_tuple_list = hash_files_and_get_sha1sum_and_filename_tuple_list_from_folder(folder_abspath)
  str_lines = u''
  for sha1sum_and_filename_tuple in sha1sum_and_filename_tuple_list:
    sha1hex, filename = sha1sum_and_filename_tuple
    line = u'%s  %s'  %(sha1hex, filename)
    str_lines += u'%s\n' %line
  str_lines = str_lines.rstrip('\n')    
  return str_lines

def fetch_sha1sum_and_filename_dict_from_flat_sha1file(flatsha1file_abspath):
  '''
  This function loops all lines within a sha1sum file
    (the one describe above containing the 40-char hexadecimal string, a double-space,
     and the corresponding file name)
    and puts all pairs into a dict, sha1hex as key, filename as value 
  '''
  lines = codecs.open(flatsha1file_abspath, 'r', 'utf8').readlines()
  sha1sum_and_filename_dict = {}
  for line in lines:
    line = line.rstrip(' \r\n')
    if len(line) > SHA1_CHUNK_SIZE + 3:
      sha1sum  = line[ : SHA1_CHUNK_SIZE]
      filename = line[SHA1_CHUNK_SIZE + 2 : ]
      sha1sum_and_filename_dict[sha1sum] = filename
  return sha1sum_and_filename_dict

def fetch_sha1sum_and_filename_tuple_list_from_flat_sha1file(flatsha1file_abspath):
  '''
  This function uses the fetch_sha1sum_and_filename_dict_from_flat_sha1file() function above
  That function transforms the sha1hex/filename pairs into a dict.
  From the dict, the transform_sha1sum_and_filename_dict_into_tuple_list() function
    produces the pairs as tuples in a list
  '''
  sha1sum_and_filename_dict = fetch_sha1sum_and_filename_dict_from_flat_sha1file(flatsha1file_abspath)
  return transpose_sha1sum_and_filename_dict_into_tuple_list(sha1sum_and_filename_dict)

def regenerate_flat_sha1file_if_needed_and_return_it(absfolder, retry=False):
  '''
  This function needs some rework.
  '''
  try:
    flat_sha1sum_absfile = find_flat_sha1sum_absfile_or_None(absfolder)
    if flat_sha1sum_absfile != None:
      if is_flat_sha1file_ok(flat_sha1sum_absfile):
        return flat_sha1sum_absfile
      else:
        # ie, it's not ok
        print 'Deleting', flat_sha1sum_absfile
        os.remove(flat_sha1sum_absfile)
    flat_sha1sum_filename = 'sha1sum-%s.txt' %(str(time.time()))
    flat_sha1sum_absfile = os.path.join(absfolder, flat_sha1sum_filename)
    print 'Regenerating', flat_sha1sum_absfile
    text = generate_flat_sha1file_text_for_folder(absfolder)
    f = codecs.open(flat_sha1sum_absfile, 'w', 'utf-8')
    f.write(text)
    f.close()
    if retry:
      error_msg = ' :: regenerate_flat_sha1file_if_needed() :: flat_sha1sum_absfile [%s] NOT OK' %flat_sha1sum_absfile
      raise OSError, error_msg 
    return regenerate_flat_sha1file_if_needed_and_return_it(absfolder, retry=True)
  except OSError:
    if retry:
      error_msg = ' :: removal of sha1sum flat files did not work in [ regenerate_flat_sha1file_if_needed() ] :: Stopping with exception.'
      raise OSError, error_msg 
    files = os.listdir(absfolder)
    for filename in files:
      if filename.startswith('sha1sum-') and filename.endswith('.txt'):
        absfile = os.path.join(absfolder, filename)
        os.remove(absfile)
    return regenerate_flat_sha1file_if_needed_and_return_it(absfolder, retry=True)

def fetch_sha1sum_and_filename_dict_if_a_conventioned_flatsha1file_exists(abspath):
  '''
  It tries to find the a flat sha1 file on the folder given as parameter.
  If it finds it, return its corresponding sha1sum_and_filename_dict
  '''
  sha1_file_abspath = find_flat_sha1sum_absfile_or_None(abspath)
  if sha1_file_abspath == None:
    return None
  return fetch_sha1sum_and_filename_dict_from_flat_sha1file(sha1_file_abspath)


def convert_flat_sha1file_to_xml_sha1file(flat_sha1_absfile, xmlsha1filename=None):
  '''
  This function converts a flat sha1sum file to an equivalent XML sha1file on the same folder.
  To do so, it calls function fetch_sha1sum_and_filename_tuple_list_from_flat_sha1file(flat_sha1_absfile)
    and expects a tuple list from it. If this tuple list appears as None, False is returned.
  If this tuple list is not None, it will be passed on to the function that writes the XML SHA1 file.
  If no exceptions are raised along the way, True is returned at the end.
  '''
  sha1sum_and_filename_tuple_list = fetch_sha1sum_and_filename_tuple_list_from_flat_sha1file(flat_sha1_absfile)
  if sha1sum_and_filename_tuple_list == None:
    return False
  folder_abspath, _ = os.path.split(flat_sha1_absfile)
  if xmlsha1filename == None:
    xmlsha1filename = defaults.DEFAULT_XML_SHA1_FILENAME
  xmlsha1_abspath = os.path.join(folder_abspath, xmlsha1filename)
  write_xml_sha1file(sha1sum_and_filename_tuple_list, xmlsha1_abspath)
  return True

def generate_xmlsha1file_on_folder(xmlsha1file_abspath):
  '''
  This function generate the XML SHA1 file passed in as argument.
  Before attempting any hash calculation:
    + it looks up for a flat sha1 file, 
    ++ if it finds it, it converts it to its XML counterpart
    ++ if a flat sha1 file is not found, it invokes function generate_xml_sha1file_against_all_files_on_its_folder() 
  '''
  folder_abspath, xmlsha1filename = decompose_and_verify_xmlsha1file_abspath_is_on_top_of_an_existint_dir_and_is_not_itself_a_dir(xmlsha1file_abspath)
  flatsha1file_abspath = find_flat_sha1sum_absfile_or_None(folder_abspath)
  if flatsha1file_abspath != None:
    convert_flat_sha1file_to_xml_sha1file(flatsha1file_abspath, xmlsha1filename)
  else:
    generate_xml_sha1file_against_all_files_on_its_folder(xmlsha1file_abspath)

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

def add_sha1sum_and_filename_to_flatsha1file_for_file(file_abspath, flatsha1filename=None):
  '''
  [[[ This function needs testing ! ]]]
  
  [[[ Obs.: This functionality, in the case of XML SHA1 files, should be implement in a OO Class, 
            not a module function. ]]]
  
  This function receives a file path, generates its sha1hex
    and forms the sha1hex-doublespace-filename line
    Then it calls add_line_to_sha1sum_file_on_folder(line, dirpath), above.
    The process involved in the above indicated function may not be good and may need a new process workflow
    (Please, read that function's docstring for further explanation)
  '''
  
  if not os.path.isfile(file_abspath):
    error_msg = 'File to be sha1-hashed [%s] does not exist' %file_abspath
    raise sha1exceptions.FilePassedInToBeHashedAndAddedToSha1ListingsDoesNotExist, error_msg 
  sha1hex = generate_sha1hexdigest_from_filepath(file_abspath)
  folder_abspath, name_of_file_just_hashed = os.path.split(file_abspath)
  sha1hex_and_filename_line = '%s  %s' (sha1hex, name_of_file_just_hashed)
  return add_sha1hex_filename_line_to_flatsha1file_on_folder(sha1hex_and_filename_line, folder_abspath, flatsha1filename)

def rename_file_inside_sha1file(sha1_file_abspath, sha1sum, new_filename):
  '''
  
  [[[ Function to be revised ! ]]] 
  [[[ Obs.: rename process for XML SHA1 files should be done from a O.O.-Class,
            not from a function, for flat sha1 files, this function may remain here ]]]
  
  In the same "spirit" as explained above, for function:
    add_line_to_sha1sum_file_on_folder(line, dirpath), above,
    This function needs testing and probably an overhaul of its workflow
    The idea for a probable change is that the flat sha1file should be copied, changed, and then renamed back 
  '''
  print ' [ rename_file_inside_sha1file() ] :: new line: [%s  %s]' %(sha1sum, new_filename) 
  print ' path', sha1_file_abspath 
  lines = codecs.open(sha1_file_abspath, 'r', 'utf8').readlines()
  sha1AtLine = -1; newLine = None
  for i in range(len(lines)):
    line = lines[i]
    if line.startswith(sha1sum):
      newLine = '%s  %s\n' %(sha1sum, new_filename)
      sha1AtLine = i
      break
  if sha1AtLine > -1:
    lines[sha1AtLine] = newLine
    file_content = ''.join(lines)
    f = codecs.open(sha1_file_abspath, 'w', 'utf8')
    f.write(file_content)
    f.close()
    return True
  return False



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
