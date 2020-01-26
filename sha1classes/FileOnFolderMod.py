#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
XmlSha1HexFileMod.py
This module contains the XmlSha1HexFile class
'''

import os, sys
import __init__
#from sha1utils import defaults
#import XmlSha1ExceptionClassesMod as sha1exceptions
from Sha1XMLReaderWriterMod import Sha1XMLReader

class FileOnFolder(object):
  '''
  
  '''

  def __init__(self, filename, folder_abspath):
    if not os.path.isdir(folder_abspath):
      raise OSError, 'Folder %s does not exist.' %folder_abspath
    self.folder_abspath = folder_abspath
    self.filename = filename
    self.file_abspath = os.path.join(self.folder_abspath, self.filename)
    if not os.path.isfile(self.file_abspath):
      raise OSError, 'File %s does not exist.' %self.file_abspath
    self.sha1reader = Sha1XMLReader(self.folder_abspath) # sha1hexer = XmlSha1HexFile(self.folder_abspath)
    self.sha1hex = None
    if not self.is_file_not_backupable():
      self.find_n_set_sha1hex_from_sha1file()

  def is_file_not_backupable(self):
    if self.filename == 'z-sha1sum.xml':
      return True
    if self.filename.endswith('~'):
      return True
    return False
      
  def find_n_set_sha1hex_from_sha1file(self, nOfTries=0):
    if self.is_file_not_backupable():
      raise ValueError, 'File %s is not backupable.' %self.file_abspath
    self.sha1hex = self.sha1reader.get_sha1hex_from_filename(self.filename)
    if self.sha1hex == None:
      raise OSError, 'Could not find sha1hex for file %s.' %self.file_abspath

  def get_sha1hex(self):
    self.find_n_set_sha1hex_from_sha1file()
    return self.sha1hex

def process():
  pass
  
if __name__ == '__main__':
  process()
