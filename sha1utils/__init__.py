'''
  This is the __init__.py script "forming" package DWLGNPPython 

  The folder name DWLGNPPython (this Python Package element)
    stands for "Deutsche Welle Langsam geprochene Nachrichten Podcast Python"
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
