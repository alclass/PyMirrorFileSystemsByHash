#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''
import os, sys
  
import __init__
from sha1classes.XmlSha1HexFileMod import XmlSha1HexFile

def walk_up_tree(folder_abspath):
  
  for dirpath, dirnames, filenames in os.walk(folder_abspath):
    print '-'*40
    print 'Walking', dirpath
    _ = XmlSha1HexFile(dirpath)
    # xmlsha1obj.update_sha1_xml_file_if_needed()
  
def process():
  try:
    folder_abspath = os.path.abspath(sys.argv[1])
  except IndexError:
    folder_abspath = os.path.abspath('.')
    
  walk_up_tree(folder_abspath)
    

if __name__ == '__main__':
  process()
  #unittest.main()
