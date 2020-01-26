#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''
import os, sys, time

import __init__
import local_settings as ls
from sha1classes.Sha1XMLReaderWriterMod import Sha1XMLReader

class StartFolderPathDoesNotExist(OSError):
  pass

class XMLSHA1FILENAMEIsUndeterminedProgramCannotContinue(ValueError):
  pass

class Sha1UpDirTreeRepeatVerifier(object):
  
  def __init__(self, starting_abspath=None, go_up_dirtree=True):
    self.comparer = None
    self.xmlsha1filename = None
    self.walked_folder_counter = 0
    self.sha1sum_and_filename_dict = {}
    self.go_up_dirtree = go_up_dirtree
    self.init_starting_abspath(starting_abspath)

  def init_starting_abspath(self, starting_abspath=None):
    self.starting_abspath = starting_abspath
    if self.starting_abspath==None or not os.path.isdir(self.starting_abspath):
      error_msg = 'StartFolderPathDoesNotExist'
      raise StartFolderPathDoesNotExist, error_msg 

  def set_comparer_abspath_and_extract_its_sha1hex_up_dir_tree(self, compare_abspath, go_up_dirtree=True):
    self.comparer = Sha1UpDirTreeRepeatVerifier(compare_abspath, go_up_dirtree)
    self.comparer.extract_sha1hexes_up_dir_tree()

  def set_xmlsha1filename(self, xmlsha1filename=None):
    if xmlsha1filename == None:
      self.xmlsha1filename = ls.XML_SHA1_FILENAME
      return
    self.xmlsha1filename = xmlsha1filename
    
  def get_xmlsha1filename(self):
    if self.xmlsha1filename == None:
      self.set_xmlsha1filename()
    if self.xmlsha1filename == None:
      error_msg = 'XMLSHA1FILENAMEIsUndeterminedProgramCannotContinue'
      raise XMLSHA1FILENAMEIsUndeterminedProgramCannotContinue, error_msg
    return self.xmlsha1filename

  def add_sha1hexs_on_folder(self, sha1file_abspath):
    '''
    '''
    local_sha1sum_and_filename_dict = read_xml_sha1file_into_sha1sum_and_filename_dict(sha1file_abspath)
    if local_sha1sum_and_filename_dict == None or local_sha1sum_and_filename_dict == {}:
      return
    self.absorb_local_sha1sum_and_filename_dict(local_sha1sum_and_filename_dict)
      
  def extract_sha1hexes_up_dir_tree(self):
    self.walked_folder_counter = 0
    if not self.go_up_dirtree:
      folder_abspath = self.starting_abspath
      self.add_sha1hexs_on_folder(folder_abspath)
      return
    for self.dirpath, _, _ in os.walk(self.starting_abspath): # _ = dirnames, _ = filenames
      self.walked_folder_counter += 1
      #print walk_count, dirpath
      current_abspath = os.path.join(self.starting_abspath, self.dirpath)
      self.add_sha1hexs_on_folder(current_abspath)

  def absorb_local_sha1sum_and_filename_dict(self, local_sha1sum_and_filename_dict):
    for sha1hex in local_sha1sum_and_filename_dict.keys():
      if sha1hex not in self.sha1sum_and_filename_dict.keys():
        self.sha1sum_and_filename_dict[sha1hex] = [(local_sha1sum_and_filename_dict[sha1hex], self.dirpath)]
      else:
        sha1hex_filename_tuple_list = self.sha1sum_and_filename_dict[sha1hex]
        sha1hex_filename_tuple_list.append((local_sha1sum_and_filename_dict[sha1hex], self.dirpath))
    
  def list_equal_sha1s_if_any_thru_the_same_dirtree(self):
    local_sha1hex_repeats_found = 0
    print 'At %s th File Coincidence on %s' %(self.classwide_found_counter, self.dirpath)
    for sha1hex in self.sha1sum_and_filename_dict.keys():
      sha1hex_filename_tuple_list = self.sha1sum_and_filename_dict[sha1hex]
      n_of_files_with_same_sha1hex = len(sha1hex_filename_tuple_list) 
      if n_of_files_with_same_sha1hex < 2:
        continue
      print sha1hex, ':: there are %d repeats' %n_of_files_with_same_sha1hex
      for sha1hex_filename_tuple in sha1hex_filename_tuple_list:
        local_sha1hex_repeats_found += 1
        print local_sha1hex_repeats_found, sha1hex_filename_tuple
    print 'local_sha1hex_repeats_found', local_sha1hex_repeats_found

  def list_equal_sha1s_up_dirtree_if_any_with_comparer(self):
    comparer_sha1hex_repeats_found = 0
    for sha1hex in self.sha1sum_and_filename_dict.keys():
      if sha1hex in self.comparer.sha1sum_and_filename_dict.keys():
        print 'Equality found for', sha1hex
        print 'Source:'
        sha1hex_filename_tuple_list = self.sha1sum_and_filename_dict[sha1hex]
        n_of_files_with_same_sha1hex = len(sha1hex_filename_tuple_list) 
        print sha1hex, ':: there are %d repeats' %n_of_files_with_same_sha1hex
        for sha1hex_filename_tuple in sha1hex_filename_tuple_list:
          comparer_sha1hex_repeats_found += 1
          print comparer_sha1hex_repeats_found, sha1hex_filename_tuple
        print 'Target:'
        sha1hex_filename_tuple_list = self.comparer.sha1sum_and_filename_dict[sha1hex]
        n_of_files_with_same_sha1hex = len(sha1hex_filename_tuple_list) 
        print sha1hex, ':: there are %d repeats' %n_of_files_with_same_sha1hex
        for sha1hex_filename_tuple in sha1hex_filename_tuple_list:
          comparer_sha1hex_repeats_found += 1
          print comparer_sha1hex_repeats_found, sha1hex_filename_tuple
    print 'comparer_sha1hex_repeats_found', comparer_sha1hex_repeats_found

def interactive_ask_for_source_abspath_and_return_verifier():
  print 'Please, type in the path to the source folder to be up-tree compared:'
  abspath = raw_input('[or Type an empty [ENTER] to avoid process ==>>> ')
  if abspath == '':
    return None
  if not os.path.isdir(abspath):
    return interactive_ask_for_source_abspath_and_return_verifier()
  verifier = Sha1UpDirTreeRepeatVerifier(abspath)
  return verifier
  
def interactive_ask_for_target_abspath_and_set_comparer_into_verifier(verifier):
  if verifier == None:
    return
  print "Please, type in the comparer's path to the source folder to be up-tree compared, if comparison is 'same-tree', leave it blank."
  comparer_abspath = raw_input(' [Typing [ENTER] means no comparer is wanted for the moment ==>>> ')
  if comparer_abspath == '':
    return
  if os.path.isdir(comparer_abspath):
    verifier.set_comparer_abspath_and_extract_its_sha1hex_up_dir_tree()
    return
  return interactive_ask_for_target_abspath_and_set_comparer_into_verifier(verifier)
  
def interactive_collect_abspath_args_and_return_verifier():
  verifier = interactive_ask_for_source_abspath_and_return_verifier()
  if verifier == None:
    return None
  interactive_ask_for_target_abspath_and_set_comparer_into_verifier(verifier)
  return verifier

def pick_up_args_and_return_verifier():
  source_abspath = None; target_abspath = None
  for arg in sys.argv[1:]:
    if arg == 'source':
      index = sys.argv.index('source')
      source_abspath = sys.argv[index+1]
      continue
    if arg == 'target':
      index = sys.argv.index('target')
      target_abspath = sys.argv[index+1]
  verifier = None
  if source_abspath != None and os.path.isdir(source_abspath):
    verifier = Sha1UpDirTreeRepeatVerifier(source_abspath)
  elif source_abspath == None or not os.path.isdir(source_abspath):
    verifier =  interactive_ask_for_source_abspath_and_return_verifier()
  if verifier == None:
    return None
  if target_abspath == None or not os.path.isdir(target_abspath):
    interactive_ask_for_target_abspath_and_set_comparer_into_verifier(verifier)
  return verifier
        
def process():
  verifier = pick_up_args_and_return_verifier()
  if verifier == None:
    print 'Verifier object has not been established. '
    sys.exit(1)
  if verifier.comparer != None:
    verifier.list_equal_sha1s_up_dirtree_if_any_with_comparer()
  else:
    verifier.list_equal_sha1s_if_any_thru_the_same_dirtree()

if __name__ == '__main__':
  process()
  #unittest.main()
