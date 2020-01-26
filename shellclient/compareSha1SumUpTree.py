#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''
import os, sys, time


OS_SYSTEM_COMM = 'sha1sum * > z-sha1sum.txt'
DEFAULT_SHA1SUM_TEXT_FILENAME = 'z-sha1sum.txt'

def get_all_sha1s_from_sha1file(sha1file_abspath):
  lines = open(sha1file_abspath).readlines()
  sha1_list = []
  for line in lines:
    try:
      sha1hex = line[:40]
      # filename = line[42:]
      sha1_list.append(sha1hex)
    except IndexError:
      continue
  return sha1_list
      

class Sha1SumUpTreeWalkRepeatVerifier(object):
  
  def __init__(self, starting_abspath=None):
    self.dirpath = None
    self.classwide_found_counter = 0
    self.n_repeats = 0
    self.all_sha1hexs_up_dir_tree = {}
    self.triple_list_of_repeats = []
  
  def get_rel_path(self):
    return self.dirpath[ len(self.up_tree_start_dir_abspath) : ]

  def absorb_folder_sha1hexs(self):
    try:
      if not os.path.isfile('z-sha1sum.txt'):
        return
      lines = open('z-sha1sum.txt').readlines()
    except IOError:
      return
    for line in lines:
      try:
        filename = line[42:-1] # -1 for the ending \n character
        _, ext = os.path.splitext(filename)
        if ext in ['.html', '.js']:
          continue
        file_abspath = os.path.join(self.dirpath, filename)
        if not os.path.isfile(file_abspath):
          continue
        if os.stat(file_abspath)[6] == 0:
          continue
        sha1hex = line[0:40]
        rel_path = self.get_rel_path()
        filename_and_relpath_tuple = (filename, rel_path)
        #print filename_and_relpath_tuple
        if self.all_sha1hexs_up_dir_tree.has_key(sha1hex):
          filenames_and_relpaths_with_that_sha1hex = self.all_sha1hexs_up_dir_tree[sha1hex]
          filenames_and_relpaths_with_that_sha1hex.append(filename_and_relpath_tuple)
          self.n_repeats += 1
        else:
          self.all_sha1hexs_up_dir_tree[sha1hex] = [filename_and_relpath_tuple]
      except IndexError:
        continue
  
  def load_all_sha1hexs_up_dir_tree(self, up_tree_start_dir_abspath):
    self.up_tree_start_dir_abspath = up_tree_start_dir_abspath
    walk_count = 0
    for self.dirpath, _, _ in os.walk(self.up_tree_start_dir_abspath): # dirpath, dirnames, filenames
      walk_count += 1
      #print walk_count, 'Verifying', self.dirpath
      current_abspath = os.path.join(self.up_tree_start_dir_abspath, self.dirpath)
      os.chdir(current_abspath)
      self.absorb_folder_sha1hexs()

  def report_sha1hex_situation(self):
    print 'Amount of different sha1hexs =', len(self.all_sha1hexs_up_dir_tree)
    print 'Amount of repeats =', self.n_repeats
    print 'The repeats are:'; n_repeat = 0
    print '='*40
    for sha1hex in self.all_sha1hexs_up_dir_tree.keys():
      filenames_and_relpaths_with_that_sha1hex = self.all_sha1hexs_up_dir_tree[sha1hex]
      if len(filenames_and_relpaths_with_that_sha1hex) == 1:
        continue
      n_repeat += 1
      print 'n_repeat', n_repeat 
      for i, filename_and_relpath_tuple in enumerate(filenames_and_relpaths_with_that_sha1hex):
        filename, rel_path = filename_and_relpath_tuple
        print i,'::', filename
        print ' ==>>>', rel_path
      index_number_str = raw_input('Enter the index number of the file to keep (all other will be erased). To say No, press [ENTER] directly.')
      try:
        index_number = int(index_number_str)
        if 0 <= index_number < len(filenames_and_relpaths_with_that_sha1hex):
          for i, filename_and_relpath_tuple in enumerate(filenames_and_relpaths_with_that_sha1hex):
            if i == index_number:
              continue
            filename, rel_path = filename_and_relpath_tuple
            dir_abspath = os.path.join(self.up_tree_start_dir_abspath, rel_path)
            file_abspath = os.path.join(dir_abspath, filename)
            print 'About to delete:', file_abspath 
            _ = raw_input('Press [ENTER] to delete the above file.')
            os.remove(file_abspath) 
      except ValueError:
        continue
    
def process():
  verifier = Sha1SumUpTreeWalkRepeatVerifier()
  #compare_abspath = '/media/SAMSUNG/z coursera triage/'
  #comparator.load_comparing_sha1_list(compare_abspath)
  up_tree_start_dir_abspath = '/media/SAMSUNG/coursera.org/'
  verifier.load_all_sha1hexs_up_dir_tree(up_tree_start_dir_abspath)
  verifier.report_sha1hex_situation()
  

if __name__ == '__main__':
  process()
  #unittest.main()
