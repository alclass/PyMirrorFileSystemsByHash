#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''
import os, sys
  
class LsUpTreeWalker(object):
  
  def __init__(self, start_folder_abspath):
    self.start_folder_abspath = start_folder_abspath
    self.ls_walk_up_tree()
      
  def ls_walk_up_tree(self):
    for dirpath, _, _ in os.walk(self.start_folder_abspath): # dirpath, dirnames, filenames
      print 'chdir "%s"' %dirpath
      os.chdir(dirpath)
      _, dirname = os.path.split(dirpath)
      comm = 'ls > "z ls contents on %s.txt"' %dirname
      print comm
      os.system(comm)

def get_start_folder_abspath():
  try:
    folder_abspath = os.path.abspath(sys.argv[1])
  except IndexError:
    folder_abspath = os.path.abspath('.')
  return folder_abspath

def process():
  start_folder_abspath = get_start_folder_abspath()
  LsUpTreeWalker(start_folder_abspath)

if __name__ == '__main__':
  process()
  #unittest.main()
