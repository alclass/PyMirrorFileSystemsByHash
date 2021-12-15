#!/usr/bin/env python3
"""
strfunctions_mod.py
"""


def any_dir_in_path_startswith(fpath, str_startswith):
  if fpath is None:
    return False
  pp = fpath.split('/')
  for foldername in pp:
    if foldername.startwith(str_startswith):
      return True
  return False
