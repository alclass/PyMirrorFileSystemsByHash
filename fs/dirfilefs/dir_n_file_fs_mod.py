#!/usr/bin/env python3
"""
dir_n_file_fs_mod.py
"""
import os
import default_settings as defaults


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


def count_total_files_n_folders_with_norestriction(mountpath, restricted_dirnames, forbidden_first_level_dirs):
  src_total_files = 0
  src_total_dirs = 0
  for current_path, folders, files in os.walk(mountpath):
    if current_path == mountpath:
      # do not count files in root dir only count folders
      continue
    if is_forbidden_dirpass(current_path, restricted_dirnames, forbidden_first_level_dirs):
      # do not count files or folder inside forbidden_first_level_dirs
      continue
    if is_any_dirname_in_path_startingwith_any_in_list(current_path, restricted_dirnames):
      # do not count files or folder with paths having any restricted_dirnames (eg z-del or z-tri [for z-Triage])
      continue
    src_total_dirs += len(folders)
    src_total_files += len(files)
  return src_total_files, src_total_dirs


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


def rename_filename_if_its_already_taken_in_folder(filepath):
  """
  This function returns a filepath if it doesn't exist in its folder.
  If it exists, the function adds an integer to the end of its name and repeats checking if the file doesn't exist.
  If it doesn't, the function returns filepath with its new name.
  After 1000 tries, all names already existing in folder, it returns None.

  The function is useful for copying a file (that needs to be copied)
    when another file with the same name exists in the same folder.
  It must be noted that file equality is based on sha1-hash, not filename itself.
  """
  if not os.path.isfile(filepath):
    return filepath
  dirpath, filename = os.path.split(filepath)
  name, ext = os.path.splitext(filename)
  n = 2
  while 1:
    newfilename = name + ' ' + str(n) + ext
    newfilepath = os.path.join(dirpath, newfilename)
    if not os.path.isfile(newfilepath):
      return newfilepath
    n += 1
    if n > 1000:  # limit this rename-try to 1000
      break
  return None


def is_lowerstr_startingwith_any_in_list(name, starting_strs_list):
  if name is None:
    return False
  try:
    name = name.lower()
  except AttributeError:
    return False
  for starting_str in starting_strs_list:
    if len(name) < len(starting_str):
      continue
    cmpname = name[:len(starting_str)]
    if cmpname == starting_str:
      return True
  return False


def is_any_dirname_in_path_startingwith_any_in_list(fpath, starting_strs_list):
  if fpath is None:
    return False
  dirnames = fpath.split(os.path.sep)
  for dirname in dirnames:
    if is_lowerstr_startingwith_any_in_list(dirname, starting_strs_list):
      return True
  return False


def is_forbidden_dirpass(dirpath, restricted_dirnames=None, forbidden_first_level_dirs=None):
  """
  if dirpath starts with /, the split() result will have an '' (empty string) as first element
  if dirpath does not start with /, the split() result will have the top level dirname as first element
  path.lstrip('/') will strip out any beginning slashes if any, assuring split()[0] gives the top level dirname in path
  """
  if restricted_dirnames is None:
    restricted_dirnames = defaults.RESTRICTED_DIRNAMES_FOR_WALK
  if forbidden_first_level_dirs is None:
    forbidden_first_level_dirs = defaults.FORBIBBEN_FIRST_LEVEL_DIRS
  try:
    ongoingfolder_abspath = dirpath.lstrip('/')
    first_level_dir = ongoingfolder_abspath.split('/')[0]
    if first_level_dir.startswith('.'):
      return True
    boolres = is_any_dirname_in_path_startingwith_any_in_list(first_level_dir, restricted_dirnames)
    if boolres:
      return True
    if first_level_dir in forbidden_first_level_dirs:
      return True
  except AttributeError:
    pass
  except IndexError:
    pass
  return False


def adhoc_test():
  starting_strs_list = ['z-del', 'z-tri']
  names = ['bla', 'z-Del', 'z-Triage', 'z-tri legal', "what's up", 'z Triage', 'tri legal']
  for i, name in enumerate(names):
    boolres = is_lowerstr_startingwith_any_in_list(name, starting_strs_list)
    print(i+1, '[', name, '] starts with any', starting_strs_list, '=>', boolres)


def adhoc_test2():
  starting_strs_list = ['z-del', 'z-tri']
  paths = ['/bla/blah/balalah', '/z-Del', '/z-t/z-Triage', 'z-tri/z-tri legal', "what's up",
           'z Triage/z-Triage', 'tri legal']
  for i, fpath in enumerate(paths):
    boolres = is_any_dirname_in_path_startingwith_any_in_list(fpath, starting_strs_list)
    print(i+1, '[', fpath, '] starts with any', starting_strs_list, '=>', boolres)


def process():
  adhoc_test2()


if __name__ == '__main__':
  process()
