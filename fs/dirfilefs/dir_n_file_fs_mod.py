#!/usr/bin/env python3
"""
dir_n_file_fs_mod.py
"""
import os


def prune_dirtree_deleting_empty_folders(base_dirpath):
  entries = os.listdir(base_dirpath)
  for e in entries:
    abspath = os.path.join(base_dirpath, e)
    if os.path.isfile(abspath):
      continue
    if os.path.isdir(abspath):
      prune_dirtree_deleting_empty_folders(abspath)
  entries = os.listdir(base_dirpath)
  if len(entries) == 0:
    print('#'*50)
    print('Folder-deleting:', base_dirpath)
    print('#'*50)
    os.rmdir(base_dirpath)
  return


def count_total_files_n_folders(mountpath):
  src_total_files = 0
  src_total_dirs = 0
  for current_path, folders, files in os.walk(mountpath):
    src_total_dirs += len(folders)
    if current_path == mountpath:
      # do not count files in root dir only count folders
      continue
    src_total_files += len(files)
  return src_total_files, src_total_dirs
