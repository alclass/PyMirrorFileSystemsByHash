#!/usr/bin/env python
import os
import shutil

'''
import sys
sys.path.insert(0, '..')
'''
from db.sqlite_accessor_mod import DBAccessor
from classes.Sha1FileSystemComplementer import calculate_sha1hex_from_file

def test1():
  from_here_abspath = os.path.abspath('.')
  db_accessor = DBAccessor(from_here_abspath)
  level2dir_relpath = 'testdir/2nd_level_dir/'
  if not os.path.isdir(level2dir_relpath):
    os.makedirs(level2dir_relpath)
  dir2ndlevel_relpath = 'testdir/2nd_level_dir/'
  dir2ndlevel_abspath = os.path.join(from_here_abspath, dir2ndlevel_relpath)
  blah_filename = 'blah.txt'
  blah_abspath = os.path.join(dir2ndlevel_abspath, blah_filename)
  f = open(blah_abspath, 'w')
  f.write(''+blah_abspath)
  f.close()
  sha1hex = calculate_sha1hex_from_file(blah_abspath)
  db_accessor.db_insert_filename_and_its_sha1hex_with_its_folder_abspath(blah_filename, dir2ndlevel_abspath, sha1hex)
  copied_blah_filename = 'blah_copied.txt'
  copied_blah_file_abspath = os.path.join(dir2ndlevel_abspath, copied_blah_filename)
  shutil.copy2(blah_abspath, copied_blah_file_abspath)
  db_accessor.db_insert_filename_and_its_sha1hex_with_its_folder_abspath(copied_blah_filename, dir2ndlevel_abspath, sha1hex)
  print db_accessor.list_up_tree_contents_as_text()

def main():
  test1()

if __name__ == '__main__':
  main()
