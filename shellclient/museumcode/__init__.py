'''
  This is the __init__.py script for the sha1cli.museumcode package
  
  museum code is generally either code that has been left as reference or run-only-once scripts
  "cli", here, stands for Command Line Interface
  
  The purpose of this module is, beyond marking a Python package,
    to insert (dynamically) the parent directory into the PYTHON PATH
'''

import os, sys

def _insert_parent_dir_to_path_if_needed():
  this_file_path = os.path.abspath(__file__)
  THIS_DIR_PATH, _ = os.path.split(this_file_path)
  try:
    PARENT_DIR_PATH = '/'.join(THIS_DIR_PATH.split('/')[:-1])
  except IndexError:
    PARENT_DIR_PATH = THIS_DIR_PATH
  if PARENT_DIR_PATH not in sys.path: 
    sys.path.insert(0, PARENT_DIR_PATH)

_insert_parent_dir_to_path_if_needed()
