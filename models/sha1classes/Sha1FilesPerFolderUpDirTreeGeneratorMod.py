#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''

'''

import os, sys, time

OS_SYSTEM_COMM = 'sha1sum * > z-sha1sum.txt'

from XmlSha1HexFileMod import XmlSha1HexFile 
#import __init__


class Sha1FilesPerFolderUpDirTreeGenerator(object):
  '''
  This class is a wrapper around class XmlSha1HexFile (on module XmlSha1HexFileMod)
  The purpose of this class is to run XmlSha1HexFile() on each folder as it walks
    the folder tree upwards (top down, to children folders)
  
  The client program should use this class as follows:
  1) it instantiates it passing the starting_abspath from which 
     the up tree walk will move upwards (ie, top down, to children folders)
  2) it may optionally pass generation_kind (either flat text sha1 or xml sha1, the default)
  3) it may optionally set attribute self.regenerate, which is False by default
     (boolean self.regenerate will force recalcation of the sha1 hashes for every file)
  4) it then invokes method self.generate_sha1sum_up_dir_tree()
  
  Previously the above method self.generate_sha1sum_up_dir_tree() was already
    run from the constructor,
    but this has been changed to allow an in-between setting of self.regenerate 
    (otherwise the constructor parameter signature would be 
    a little bit more "cluttered" so to say
  '''
  
  FLAT_UNIX_SHA1SUM_FILE = 1
  XML_SHA1_SHA1SUM_FILE  = 2 
  GENERATION_KINDS = [FLAT_UNIX_SHA1SUM_FILE, XML_SHA1_SHA1SUM_FILE]
  
  def __init__(self, starting_abspath=None, generation_kind=None):
    self.regenerate = False
    self.set_generation_kind(generation_kind)
    self.starting_abspath = starting_abspath
    #self.generate_sha1sum_up_dir_tree() # cannot execute from here because set_regenerate(boolean) must have a chance to set self.regenerate

  def set_generation_kind(self, generation_kind=None):
    if generation_kind == None or generation_kind not in Sha1FilesPerFolderUpDirTreeGenerator.GENERATION_KINDS:
      self.generation_kind = Sha1FilesPerFolderUpDirTreeGenerator.XML_SHA1_SHA1SUM_FILE
      return
    self.generation_kind = generation_kind

  def generate_sha1sum_up_dir_tree(self):
    self.walk_count = 0
    for self.dirpath, _, _ in os.walk(self.starting_abspath): # dirnames, filenames
      self.walk_count += 1
      if self.generation_kind == Sha1FilesPerFolderUpDirTreeGenerator.FLAT_UNIX_SHA1SUM_FILE:
        self.generate_flatsha1file_on_folder()
      elif self.generation_kind == Sha1FilesPerFolderUpDirTreeGenerator.XML_SHA1_SHA1SUM_FILE:
        self.generate_xmlsha1file_on_folder()
      
  def generate_flatsha1file_on_folder(self):
    current_abspath = os.path.join(self.starting_abspath, self.dirpath)
    os.chdir(current_abspath)
    print self.walk_count, time.ctime(), 'Generating sha1sum for files in', current_abspath
    print OS_SYSTEM_COMM
    os.system(OS_SYSTEM_COMM)

  def set_regenerate(self, regenerate=False):
    self.regenerate = regenerate

  def generate_xmlsha1file_on_folder(self):
    folder_abspath = os.path.join(self.starting_abspath, self.dirpath)
    print self.walk_count, 'Processing/Verifying XmlSha1File on %s' %folder_abspath 
    generator = XmlSha1HexFile(folder_abspath)
    if self.regenerate:
      generator.verify_recalculating_sha1sums()

def get_starting_folder_arg_or_exit():
  try:
    starting_path_str = sys.argv[1] #'/media/SAMSUNG/'
    starting_abspath = os.path.abspath(starting_path_str)
    if not os.path.isdir(starting_abspath):
      print 'Given starting folder (%s) does not exist. Please, retry.' %starting_path_str
      sys.exit(1)
    return starting_abspath
  except IndexError:
    pass
  print 'Please, enter the starting folder for the generation of XML Sha1 files on all folders up directory tree.'
  sys.exit(0)

def process():
  starting_abspath = get_starting_folder_arg_or_exit()
  sha1treewalker = Sha1FilesPerFolderUpDirTreeGenerator(starting_abspath)
  sha1treewalker.generate_sha1sum_up_dir_tree()


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
