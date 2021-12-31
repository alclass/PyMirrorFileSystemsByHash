#!/usr/bin/env python3
"""
dir_n_file_fs_mod.py
"""
import os
import default_settings as defaults


def prune_dirtree_deleting_empty_folders(current_dirpath, n_visited=0, n_removed=0, n_failed=0):
  """
  This function deletes empty folders, starting from a dirpath and recursively visiting its subdirectory
  """
  entries = os.listdir(current_dirpath)
  for e in entries:
    abspath = os.path.join(current_dirpath, e)
    if os.path.isfile(abspath):
      continue
    if os.path.isdir(abspath):
      n_visited += 1
      # recurse here to a subdirectory
      n_visited, n_removed, n_failed = prune_dirtree_deleting_empty_folders(abspath, n_visited, n_removed, n_failed)
  entries = os.listdir(current_dirpath)
  if len(entries) == 0:
    # the directory/folder is empty, it can be removed
    print('#'*50)
    print('Total empty dirs removed', n_removed, 'removing:', current_dirpath)
    print('#'*50)
    try:
      os.rmdir(current_dirpath)
      n_removed += 1
    except (IOError, OSError):
      n_failed += 1
  return n_visited, n_removed, n_failed


def count_total_files_n_folders_with_norestriction(
    mountpath,
    restricted_dirnames=None,
    forbidden_first_level_dirs=None
  ):
  if restricted_dirnames is None:
    restricted_dirnames = defaults.RESTRICTED_DIRNAMES_FOR_WALK
  if forbidden_first_level_dirs is None:
    forbidden_first_level_dirs = defaults.FORBIBBEN_FIRST_LEVEL_DIRS
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


def count_total_files_n_folders_inc_root(mountpath):
  src_total_files = 0
  src_total_dirs = 0
  for current_path, folders, files in os.walk(mountpath):
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


def is_any_dirname_in_path_startingwith_any_in_list(fpath, starting_strs_list=None):
  if fpath is None:
    return False
  if starting_strs_list is None:
    starting_strs_list = defaults.RESTRICTED_DIRNAMES_FOR_WALK
  dirnames = fpath.split(os.path.sep)
  for dirname in dirnames:
    if is_lowerstr_startingwith_any_in_list(dirname, starting_strs_list):
      return True
  return False


def does_path_have_forbidden_dir(fpath, restricted_dirnames=None):
  """
  This function is the same as is_any_dirname_in_path_startingwith_any_in_list() with an alternative name
  """
  return is_any_dirname_in_path_startingwith_any_in_list(fpath, restricted_dirnames)


def is_forbidden_dirpass(dirpath, restricted_dirnames=None, forbidden_first_level_dirs=None):
  """
  if dirpath starts with /, the split() result will have an '' (empty string) as first element
  if dirpath does not start with /, the split() result will have the top level dirname as first element
  path.lstrip('/') will strip out any beginning slashes if any, assuring split()[0] gives the top level dirname in path
  if first_level_dir.startswith('.'):
    return True
  """
  if dirpath is None:
    return None
  try:
    ongoingfolder_abspath = dirpath.lstrip('/') # this assures topleveldir is the first element after split('/')
    if restricted_dirnames is None:
      restricted_dirnames = defaults.RESTRICTED_DIRNAMES_FOR_WALK
    boolres = is_any_dirname_in_path_startingwith_any_in_list(ongoingfolder_abspath, restricted_dirnames)
    if boolres:
      return True
    if forbidden_first_level_dirs is None:
      forbidden_first_level_dirs = defaults.FORBIBBEN_FIRST_LEVEL_DIRS
    first_level_dir = ongoingfolder_abspath.split('/')[0]
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
