#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''
import os #, sys, time

from models.sha1classes import XmlSha1HexFile
from fs.hashpackage.sha1utilsMod import read_xml_sha1file_into_sha1sum_and_filename_tuple_list
from fs.hashpackage import defaults


def folder_walk_to_remove_default_xml_sha1_filename_entry_from_itself(start_abspath):
  '''
  This script is a run-once one, ie, it will only correct a left-over that happened, not foreseen before
  This refers to the z-sha1sum.xml registration into itself,
    ie, it has a sha1 that will change everytime it checks itself
  Because of this infinite recursive update problem, z-sha1sum.xml should not be registered into itself
  '''  
  for dirpath, _, _ in os.walk(start_abspath): # dirnames, filenames
    folder_abspath = os.path.join(start_abspath, dirpath)
    xmlsha1file_abspath = os.path.join(folder_abspath, defaults.DEFAULT_XML_SHA1_FILENAME)
    print xmlsha1file_abspath
    xml_sha1file_into_sha1sum_and_filename_tuple_list = read_xml_sha1file_into_sha1sum_and_filename_tuple_list(xmlsha1file_abspath)
    _, filenames = zip(*xml_sha1file_into_sha1sum_and_filename_tuple_list)
    found_DEFAULT_XML_SHA1_FILENAME = False
    try:
      _ = filenames.index(defaults.DEFAULT_XML_SHA1_FILENAME)
      print 'Found', defaults.DEFAULT_XML_SHA1_FILENAME
      found_DEFAULT_XML_SHA1_FILENAME = True
    except ValueError:
      print 'Not Found', defaults.DEFAULT_XML_SHA1_FILENAME
      continue
    if found_DEFAULT_XML_SHA1_FILENAME:
      xml_sha1_filer = XmlSha1HexFile(folder_abspath)
      print 'Saving/Updating', defaults.DEFAULT_XML_SHA1_FILENAME
      xml_sha1_filer.save()

def process():
  # start_abspath = '/media/SAMSUNG_/YouTube (mostly Law-related Videos)/youtube.com-user-telelivraria/' # 
  start_abspath = '/media/SAMSUNG_/'
  folder_walk_to_remove_default_xml_sha1_filename_entry_from_itself(start_abspath)

if __name__ == '__main__':
  process()
