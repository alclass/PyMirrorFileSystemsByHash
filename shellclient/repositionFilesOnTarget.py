#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''

import os, sys, time

OS_SYSTEM_COMM = 'sha1sum * > z-sha1sum.txt'

import __init__
# from sha1utils import defaults
from sha1classes.FileOnFolderMod import FileOnFolder
import local_settings as ls

# PYTHON_SHA1_SYSTEM_DIR = '/home/dados/Sw3/SwDv/CompLang SwDv/PythonSwDv/python_osutils/dir_trees_comparator/'
sys.path.insert(0, ls.PYTHON_SHA1_SYSTEM_DIR)

#from sha1classes.XmlSha1ExceptionClassesMod              import FolderPassedToXmlSha1GenerationDoesNotExist
#from sha1classes.Sha1FilesPerFolderUpDirTreeGeneratorMod import Sha1FilesPerFolderUpDirTreeGenerator 
#from sha1classes.XmlSha1HexFileMod                       import XmlSha1HexFile
from sha1classes.SourceAndTargetBaseDirsKeeperMod        import SourceAndTargetBaseDirsKeeper
#from sha1classes.Sha1UpDirTreeRepeatVerifierMod          import Sha1UpDirTreeRepeatVerifier

class Repositioner(object):    

  def __init__(self, startdir_relpath=None):
    '''
    Obs.: startdir_relpath MUST be the same for both source and target (what can differ is base_abspath for them)
    '''
    self.source_startdir_abspath = None
    self.calculate_source_startdir_abspath(startdir_relpath)
    
    #self.target_startdir_abspath = None
    #self.calculate_target_startdir_abspath(startdir_relpath)

  def calculate_source_startdir_abspath(self, startdir_relpath=None):
    if startdir_relpath == None:
      self.source_startdir_abspath = SourceAndTargetBaseDirsKeeper.get_source_basepath()
      return
    self.source_startdir_abspath = os.path.join(SourceAndTargetBaseDirsKeeper.get_source_basepath(), startdir_relpath)
    if not os.path.isdir(self.source_startdir_abspath):
      raise OSError, 'A failure happened. Source Directory %s does not exist.' %self.source_startdir_abspath
    
  def walk_and_ajust(self):
    for source_currdir_abspath, _, filenames in os.walk(self.source_startdir_abspath): # _ = dirnames
      self.process_folder(source_currdir_abspath, filenames)

  def process_folder(self, source_dir_abspath, source_filenames):
    to_dir_relpath = self.get_dir_relpath(source_dir_abspath)
    for source_filename in source_filenames:
      file_on_dir_obj = FileOnFolder(source_filename, source_dir_abspath)
      if file_on_dir_obj.is_file_not_backupable():
        continue
      '''
      source_sha1hex = XmlSha1HexFile.get_sha1hex_from_file_on_dir(source_filename, source_dir_abspath)
      '''  
      source_sha1hex = file_on_dir_obj.get_sha1hex()
      if source_sha1hex == None:
        raise ValueError, 'sha1sum (%s) is missing for %s in %s' %(source_sha1hex, source_filename, source_dir_abspath)
      answer = SourceAndTargetBaseDirsKeeper.does_target_dirtree_contain_sha1hex(source_sha1hex, self.get_dir_relpath(source_dir_abspath), source_filename)
      if answer == SourceAndTargetBaseDirsKeeper.SHA1HEX_EXISTS_ON_THE_SAME_RELPATH_ON_TARGET:
        continue
      elif answer == SourceAndTargetBaseDirsKeeper.SHA1HEX_EXISTS_BUT_NOT_ON_THE_SAME_RELPATH_ON_TARGET:
        self.move_file_on_target(source_sha1hex, to_dir_relpath, source_filename)
      elif answer == SourceAndTargetBaseDirsKeeper.SHA1HEX_INEXISTS_THRU_TARGET_DIRTREE:
        self.copy_file_from_source_to_target(source_dir_abspath, source_filename, to_dir_relpath)
      else:
        # halt and look into it manually
        print 'Please, look into the matter of equal filenames with different sha1sums.'
        sys.exit()

  def get_dir_relpath(self, source_dir_abspath):
    base_abspath = SourceAndTargetBaseDirsKeeper.get_source_basepath()
    return source_dir_abspath[ len(base_abspath) : ]

  def move_file_on_target(self, source_sha1hex, to_dir_relpath, source_filename):
    SourceAndTargetBaseDirsKeeper.move_file_on_target(source_sha1hex, to_dir_relpath, source_filename)
      
  def copy_file_from_source_to_target(self, source_dir_abspath, source_filename, to_dir_relpath):
    SourceAndTargetBaseDirsKeeper.copy_file_from_source_to_target(source_dir_abspath, source_filename, to_dir_relpath)
  
def process():
  source_basedir_abspath = sys.argv[1] 
  target_basedir_abspath = sys.argv[2]
  SourceAndTargetBaseDirsKeeper.set_source_basepath(source_basedir_abspath)
  SourceAndTargetBaseDirsKeeper.set_target_basepath(target_basedir_abspath)
  repositioner = Repositioner()
  repositioner.walk_and_ajust()

if __name__ == '__main__':
  process()
  #unittest.main()
