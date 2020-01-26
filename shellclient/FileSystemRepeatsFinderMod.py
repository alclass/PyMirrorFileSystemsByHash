#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
TwoFileSystemsWideComparatorModule
'''
import os, shutil, sys

import __init__
import local_settings as ls
from sha1classes.TwoFileSystemsWideComparatorMod import TwoFileSystemsWideComparator 


def pickup_arg_and_process():
  source_basepath = sys.argv[1]
  wideComparator = TwoFileSystemsWideComparator(source_basepath)
  wideComparator.fetch_all_files_from_basepath_uptree()
  wideComparator.pickup_equal_files_on_disk()
  wideComparator.print_summary()
  print wideComparator

def process():
  pickup_arg_and_process()
  
if __name__ == '__main__':
  process()
  #unittest.main()
