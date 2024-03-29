#!/usr/bin/env python3
"""
dir_n_file_fs_mod.py
"""
import copy
import os
import default_settings as defaults
import itertools
DEFAULT_SQLITE_FILENAME = '.updirfileentries.sqlite'


def find_recurs_rootfolder_sqlfile_abspath(ongoing_path=None):
  """
  Recursive directory path descent looking up the sqlite repo default filename

  DEFAULT_SQLITE_FILENAME = '.updirfileentries.sqlite'

  :param ongoing_path: string|path
  :return: ongoing_path: string|path|None
  """
  if ongoing_path is None or not os.path.isdir(ongoing_path):
    ongoing_path = os.path.abspath('.')
  entries = os.listdir(ongoing_path)
  for e in entries:
    if e == DEFAULT_SQLITE_FILENAME:
      repo_sqlfile_abspath = os.path.join(ongoing_path, DEFAULT_SQLITE_FILENAME)
      return repo_sqlfile_abspath
  if ongoing_path == '/':
    # the sqlite file was not found after a possible recursive descent
    return None
  ongoing_path, _ = os.path.split(ongoing_path)
  return find_recurs_rootfolder_sqlfile_abspath(ongoing_path)


def prune_dirtree_deleting_empty_folders(current_dirpath, n_visited=0, n_removed=0, n_failed=0):
  """
  This function deletes empty folders, starting from a dirpath and recursively visiting its subdirectory
  IMPORTANT NOTICE for unit test:
    this folder, as most in this module, is os-dependant, care is to be taken when running this with unit tests
    TO-DO: introduce a mock-kind modality (how?)
  """
  try:
    entries = os.listdir(current_dirpath)
    for e in entries:
      abspath = os.path.join(current_dirpath, e)
      if not os.path.isdir(abspath):
        continue
      n_visited += 1
      # recurse here to a subdirectory
      n_visited, n_removed, n_failed = prune_dirtree_deleting_empty_folders(abspath, n_visited, n_removed, n_failed)
    inner_entries = os.listdir(current_dirpath)
    # print('n entries', len(inner_entries), 'dir', current_dirpath)
    if len(inner_entries) == 0:
      # the directory/folder is empty, it can be removed
      print(' ###### empty dirs removal count =', n_removed, 'removing:', current_dirpath)
      os.rmdir(current_dirpath)
      n_removed += 1
  except (IOError, OSError):
    n_failed += 1
  return n_visited, n_removed, n_failed


def count_total_files_n_folders_with_restriction(
    mountpath,
    restricted_dirnames=None,
    forbidden_first_level_dirs=None
  ):
  if restricted_dirnames is None:
    restricted_dirnames = defaults.RESTRICTED_DIRNAMES_FOR_WALK
  if forbidden_first_level_dirs is None:
    forbidden_first_level_dirs = defaults.FORBIBBEN_FIRST_LEVEL_DIRS
  total_files = 0
  total_dirs = 0
  for current_path, folders, files in os.walk(mountpath):
    if current_path == mountpath:
      # do not count files in root dir/folder nor count itself as a dir/folder
      continue
    if is_forbidden_dirpass(current_path, restricted_dirnames, forbidden_first_level_dirs):
      # do not count files or folder inside forbidden_first_level_dirs
      continue
    if is_any_name_in_path_startingwith_any_prefix_in_list(current_path, restricted_dirnames):
      # do not count files or folder with paths having any restricted_dirnames (eg z-del or z-tri [for z-Triage])
      continue
    total_dirs += 1
    total_files += len(files)
  return total_files, total_dirs


def count_total_files_n_folders_inc_root(mountpath):
  src_total_files = 0
  src_total_dirs = 0
  for current_path, folders, files in os.walk(mountpath):
    src_total_dirs += len(folders)
    src_total_files += len(files)
  return src_total_files, src_total_dirs


def count_total_files_n_folders_excl_root(mountpath):
  src_total_files = 0
  src_total_dirs = 0
  for current_path, folders, files in os.walk(mountpath):
    src_total_dirs += len(folders)
    if current_path == mountpath:
      # do not count files in root dir only count folders
      continue
    src_total_files += len(files)
  return src_total_files, src_total_dirs


def put_sufix_to_bytesize(p_bytesize):
  """
  This function takes a number of bytes and returns it with a sufix (b, K, M or G),
    these standing for bytes, Kilobytes, Megabytes and Gigabytes respectively.
  """
  try:
    if p_bytesize < 1024:
      return str(p_bytesize) + 'b'
    elif p_bytesize < (1024**2):
      bytesize = round(p_bytesize/1024, 2)
      return str(bytesize) + 'K'
    elif p_bytesize < (1024**3):
      bytesize = round(p_bytesize/(1024**2), 2)
      return str(bytesize) + 'M'
    elif p_bytesize < (1024**4):
      bytesize = round(p_bytesize/(1024**3), 2)
      return str(bytesize) + 'G'
  except TypeError:
    pass
  return 'unknown-size'


def rename_filename_if_its_already_taken_in_folder(filepath):
  """
  This function returns a filepath if it doesn't exist in its folder.
  If it exists, the function adds an integer to the end of the coinciding existing name
    to liberate the original wanted filename itself in filepath.

  Example: suppose filepath "/a/b/c/text.txt"

  If it exists, to liberate filename, that one will renamed to "/a/b/c/text 1.txt"
  If the sufixed one also exists, another rename-try will be to "/a/b/c/text 2.txt"
  And so on.

  Obs:
    o1) if the rename with a sufix is not available, the operation recurses and checks again
        (up to a limit, see example above "text 1", "text 2", "text n"...).
    o2) after 1000 tries, all names already existing in folder, it returns None
        (meaning it gave up and could not liberate filename).

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
  """
  This function is a helper to is_any_name_in_path_startingwith_any_prefix_in_list() below,
    ie the restricted dirnames are looked up comparing their start-strings (example ["z-del", "z-extra", z-triage"]).
  """
  if name is None:
    return False
  try:
    lowername = name.lower()
  except AttributeError:
    return False
  for starting_str in starting_strs_list:
    if len(lowername) < len(starting_str):
      continue
    cmpname = lowername[:len(starting_str)]
    if cmpname == starting_str:
      return True
  return False


def is_any_name_in_path_startingwith_any_prefix_in_list(fpath, starting_strs_list=None, use_lowercase=True):
  """
  This function compares if names in a path starts with some restricted prefix in a list.
    Example: ["z-del", "z-extra", z-triage"]).
  It uses helper-function is_lowerstr_startingwith_any_in_list(name, starting_strs_list) to check each name in path.
  """
  if fpath is None:
    return False
  if starting_strs_list is None:
    starting_strs_list = defaults.RESTRICTED_DIRNAMES_FOR_WALK
  dirnames = fpath.split(os.path.sep)
  for dirname in dirnames:
    if use_lowercase:
      if is_lowerstr_startingwith_any_in_list(dirname, starting_strs_list):
        return True
    else:
      for prefix in starting_strs_list:
        if dirname.startswith(prefix):
          return True
  return False


def does_path_have_forbidden_dir(fpath, restricted_dirnames=None):
  """
  This function is the same as is_any_name_in_path_startingwith_any_prefix_in_list() with an alternative name
  """
  return is_any_name_in_path_startingwith_any_prefix_in_list(fpath, restricted_dirnames)


def is_forbidden_dirpass(dirpath, restricted_dirnames=None, forbidden_first_level_dirs=None):
  """
  This function returns True if a dirpath is "forbidden" (ie should not be processed) and False otherwise.

  Note explanation for the split() method against paths and other miscellaneous with lstrip():
  =====================================================
  if dirpath starts with /, the split() result will have an '' (empty string) as first element
  if dirpath does not start with /, the split() result will have the top level dirname as first element
  path.lstrip('/') will strip out any beginning slashes if any, assuring split()[0] gives the top level dirname in path
  """
  if dirpath is None:
    return None
  try:
    ongoingfolder_abspath = dirpath.lstrip('/')  # this assures topleveldir is the first element after split('/')
    if restricted_dirnames is None:
      restricted_dirnames = defaults.RESTRICTED_DIRNAMES_FOR_WALK
    boolres = is_any_name_in_path_startingwith_any_prefix_in_list(ongoingfolder_abspath, restricted_dirnames)
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


def filter_in_files_with_exts(files, extensionlist):
  files_n_exts_product = itertools.product(files, extensionlist)
  filtered_files = list(filter(lambda t: t[0].endswith(t[1]), files_n_exts_product))
  filtered_files = [filename for filename, ext in filtered_files]
  return filtered_files


def form_abspath_with_mount_middle_n_name(mount, middle, name):
  if mount is None or name is None:
    return None
  try:
    mount = name.lstrip('/')
    mount = '/' + mount
    name = name.lstrip('/')
    if middle is None:
      return os.path.join(mount, name)
    middle = middle.lstrip('/')
    folderpath = os.path.join(mount, middle)
    return os.path.join(folderpath, name)
  except (AttributeError, TypeError):
    pass
  return None


def get_names_end_int_sufix_or_0(name):
  """
  The approach here is to reverse traverse the name in order to find a number
  """
  if name is None:
    return 0
  try:
    lastchar = name[-1]
    _ = int(lastchar)
    reversed_name = name[::-1]
  except (TypeError, ValueError):
    return 0
  char_number = ''
  for char in reversed_name:
    try:
      _ = int(char)
      char_number = char + char_number
    except ValueError:
      break
  if len(char_number) > 0:
    return int(char_number)
  return 0


def get_newfilename_based_on_filenames_end_int_sufix_or_none(filename):
  try:
    name, ext = os.path.splitext(filename)
    sufix = get_names_end_int_sufix_or_0(name)
    if sufix == 0:
      new_filename = name + ' 1' + ext
      return new_filename
    str_sufix = str(sufix)
    new_sufix = sufix + 1
    size_sufix = len(str_sufix)
    str_newsufix = str(new_sufix)
    newname = name[:-size_sufix] + str_newsufix
    new_filename = newname + ext
    return new_filename
  except (TypeError, ValueError):
    # TypeError: expected str, bytes or os.PathLike object, not int
    pass
  return None


def liberate_filename_by_renaming_with_incremental_int_sufixes(filepath):
  """
  This function renames a file in a folder to liberate its filename.
  The former file is renamed with an incremental integer sufix.
  If many tries cannot liberate it (cf. LIMIT_NUMBER_IN_WHILE_LOOP), (False, None) is returned.
  If a rename succeeds, it returns (True, newfilename)
  The logics for the new filename is delegated to function:
    get_newfilename_based_on_filenames_end_int_sufix_or_none()

  IMPORTANT for unit test: this folder is os-dependant, care is to be taken
  TO-DO: a mock version may be develop for unit test purposes.
  """
  if filepath is None:
    return True, None
  p, old_filename = os.path.split(filepath)
  if not os.path.isfile(filepath):
    return True, old_filename
  if not os.path.isdir(p):
    return True, None
  local_loop_counter = 0
  while 1:
    new_filename = get_newfilename_based_on_filenames_end_int_sufix_or_none(old_filename)
    if new_filename is None:
      return False, None
    new_trg_filepath = os.path.join(p, new_filename)
    if not os.path.isfile(new_trg_filepath):
      os.rename(filepath, new_trg_filepath)
      return True, new_filename
    old_filename = new_filename
    if len(old_filename) > 240:  # for Linux system, it's a resonable size, for Windows, this may be a problem...
      break
    local_loop_counter += 1
    if local_loop_counter > defaults.LIMIT_NUMBER_IN_WHILE_LOOP:
      break
  return False, None


def db_update_dirnode_with_newname_n_dbtree(dirnode, newname, dbtree):
  projected_dirnode = copy.copy(dirnode)
  oldfilepath = dirnode.get_abspath_with_mountpath(dbtree.mountpath)
  projected_dirnode.name = newname
  newfilepath = projected_dirnode.get_abspath_with_mountpath(dbtree.mountpath)
  try:
    os.rename(oldfilepath, newfilepath)
    # change it also in db
    sql = 'update %(tablename) set name=? where id=?;'
    tuplevalues = (newname, dirnode.get_db_id())
    dirnode.name = newname
    return dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)
  except (AttributeError, IOError, OSError):
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
    boolres = is_any_name_in_path_startingwith_any_prefix_in_list(fpath, starting_strs_list)
    print(i+1, '[', fpath, '] starts with any', starting_strs_list, '=>', boolres)


def adhoc_test3():
  files = ['f1.txt', 'f2.doc', 'u1.url', 'f1.mp4', 'f2.txt']
  exts = ['txt', 'url']
  filtered_files = filter_in_files_with_exts(files, exts)
  print(files, exts)
  print(' => filter')
  print(filtered_files)


def adhoc_test4():
  """
  test liberate_filename_by_renaming_with_incremental_int_sufixes()
  """
  filepath = '/home/dados/Sw3/SwDv/OSFileSystemSwDv/PyMirrorFileSystemsByHashSwDv/dados/trg/d1/d1f2 1002.txt'
  ret_tupl = liberate_filename_by_renaming_with_incremental_int_sufixes(filepath)
  print(ret_tupl)
  print('-'*50)
  fn = 'fasdafsd-1.bla'
  number = get_newfilename_based_on_filenames_end_int_sufix_or_none(fn)
  print('for', fn)
  print('int sufix is [', number, ']')


def process():
  adhoc_test4()


if __name__ == '__main__':
  process()
