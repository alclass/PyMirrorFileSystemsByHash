#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''

import os, sys, time

OS_SYSTEM_COMM = 'sha1sum * > z-sha1sum.txt'

import __init__
# from sha1utils import defaults
import local_settings as ls

# PYTHON_SHA1_SYSTEM_DIR = '/home/dados/Sw3/SwDv/CompLang SwDv/PythonSwDv/python_osutils/dir_trees_comparator/'
sys.path.insert(0, ls.PYTHON_SHA1_SYSTEM_DIR)

from sha1classes.XmlSha1ExceptionClassesMod              import FolderPassedToXmlSha1GenerationDoesNotExist
from sha1classes.Sha1FilesPerFolderUpDirTreeGeneratorMod import Sha1FilesPerFolderUpDirTreeGenerator 
from sha1classes.Sha1UpDirTreeRepeatVerifierMod          import Sha1UpDirTreeRepeatVerifier
from sha1classes.XmlSha1HexFileMod                       import XmlSha1HexFile

class CLICommandDispatcher(object):
  '''
  This script will be placed somewhere in a folder under the user's PATH so that it can be executed
  However, the placing of the sha1dirtree system into PYTHON_PATH will be done dynamically
    with the help of local_settings.py's PYTHON_SHA1_SYSTEM_DIR constant
  Reminder: local_settings.py is kept per computer, it's never in the git repo, except sometimes local_settings.py.COPY
  
  The idea is to have a set of subcommands that will execute an appropriate method at the sha1dirtree system  
  '''
  
  def __init__(self):
    self.source_folder = None
    self.target_folder = None
    self.introspect_sysargv()
  
  def introspect_sysargv(self):
    self.command = sys.argv[1]
    if self.command == 'genxmlsha1':
      self.genxmlsha1(go_up_tree=False, regenerate=False)
    elif self.command == 'regenxmlsha1':
      self.genxmlsha1(go_up_tree=False, regenerate=True)
    elif self.command == 'genxmlsha1uptree':
      self.genxmlsha1(go_up_tree=True, regenerate=False)
    elif self.command == 'regenxmlsha1uptree':
      self.genxmlsha1(go_up_tree=True, regenerate=True)
    elif self.command == 'compare2folders':
      self.self.compare_folders(go_up_tree=False)
    elif self.command == 'compare2dirtrees':
      self.compare_folders(go_up_tree=True)
    elif self.command == 'mirror2folders':
      self.mirror_2_folders(go_up_tree=False)
    elif self.command == 'mirror2dirtrees':
      self.mirror_2_folders(go_up_tree=True)
    else:
      print_usage_and_exit()

  def compare_folders(self, go_up_tree=True):
    self.source_folder = sys.argv[2]
    self.target_folder = sys.argv[3]
    verifier = Sha1UpDirTreeRepeatVerifier(self.source_folder, go_up_tree)
    verifier.set_comparer_abspath_and_extract_its_sha1hex_up_dir_tree(self.target_folder, go_up_tree)
    verifier.list_equal_sha1s_up_dirtree_if_any_with_comparer()

  def genxmlsha1(self, go_up_tree=False, regenerate=False):
    '''
    '''
    try:
      self.source_folder = sys.argv[2] # starting_abspath = sys.argv[2]
      if not os.path.isdir(self.source_folder):
        error_msg = 'FolderPassedToXmlSha1GenerationDoesNotExist' 
        raise FolderPassedToXmlSha1GenerationDoesNotExist, error_msg
    except IndexError:
      self.source_folder = os.path.abspath('.')
    if go_up_tree:
      upTreeGenerator = Sha1FilesPerFolderUpDirTreeGenerator(self.source_folder)
      upTreeGenerator.set_regenerate(regenerate)
      upTreeGenerator.generate_sha1sum_up_dir_tree()
    else:
      folderGenerator = XmlSha1HexFile(self.source_folder)
      if regenerate:
        folderGenerator.verify_recalculating_sha1sums()

  def mirror_2_folders(self, go_up_tree=False, regenerate=False):
    '''
    source_folder = sys.argv[2]
    target_folder = sys.argv[3]
    '''
    pass


  def print_message_if_any(self):
    print '''
    -------------------------------
    Summary of executed subcommand:
    -------------------------------
    Subcommand : %(command)s
    Source Folder used = %(source_folder)s
    Target Folder used (if any) = %(target_folder)s
    -------------------------------
    ''' %{'command':self.command,'source_folder':self.source_folder,'target_folder':self.target_folder}
  
  def go(self):
    pass
    #self.executer.process()

def print_usage_and_exit():
  '''
  Commands available:
  
    genxmlsha1        : generates the xml sha1 file for a specific folder (given as parameter)
    regenxmlsha1      : regenerates (if more than 3h since last generation) the xml sha1 file mentioned above
    genxmlsha1uptree  : generates xml sha1 files for all folders up folder tree
    regenxmlsha1uptree: regenerates xml sha1 files for all folders up folder tree, if more than 3h elapsed since last run
    compare2folders   : compares differences between 2 folders (ie, files extra, files missing and equal files with different names)
    compare2dirtrees  : same as the one above for every folder up folder tree
    mirror2folders    : prepares (but doesn't execute) a batch file to equalize 2 folders (source and target),  
                        the user will be asked to confirm or not the batch file's execution
    mirror2dirtrees   : same as the one above for every folder up folder tree
  '''
  print print_usage_and_exit.__doc__
  sys.exit(0)
  
def process():
  if 'help' in sys.argv:
    print_usage_and_exit()
  dispatcher = CLICommandDispatcher()
  dispatcher.print_message_if_any()
  dispatcher.go()

if __name__ == '__main__':
  process()
  #unittest.main()
