#!/usr/bin/env python3
"""
bulk_rename_forbidden_chars.py
"""
import datetime
import os.path
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import fs.hashfunctions.hash_mod as hm
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import default_settings as defaults


class BulkRenamer:

  def __init__(self, mountpath):
    """
    """
    self.total_dirs_in_os = 0
    self.total_files_in_os = 0
    self.total_files_in_db = 0
    self.total_unique_files_in_db = 0
    self.n_processed_files = 0
    self.n_to_rename = 0
    self.n_renamed = 0
    self.rename_tuplelist = []
    self.n_restricted_dirs = 0
    self.n_updated_dbentries = 0
    self.n_files_empty_sha1 = 0
    self.n_failed_filestat = 0
    self.n_failed_sha1s = 0
    self.n_dbentries_ins_upd = 0
    self.n_dbentries_failed_ins_upd = 0
    self.all_nodes_with_osread_problem = []
    self.mountpath = mountpath
    self.dirtree = dt.DirTree('treename', self.mountpath)
    self.dbtree = self.dirtree.dbtree  # dbu.DBDirTree(mount_abspath)
    self.ongoingfolder_abspath = None

  def calc_totals(self):
    """
    count_total_files_n_folders_with_norestriction(mountpath, restricted_dirnames, forbidden_first_level_dirs)
    """
    print('Counting files and dirs in db and os. Please wait.')
    self.total_unique_files_in_db = self.dbtree.count_unique_sha1s_as_int()
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_with_norestriction(
      self.dirtree.mountpath, defaults.RESTRICTED_DIRNAMES_FOR_WALK, defaults.FORBIBBEN_FIRST_LEVEL_DIRS
    )
    self.total_files_in_os = total_files
    self.total_dirs_in_os = total_dirs

  def insert_os_entry_into_db_n_get_id(self, filename):
    # if file was moved, sha1 at this point will be recalculated anyway (script dbentry_update may avoid this)
    parentpath = self.ongoingfolder_abspath[len(self.mountpath):]
    middlepath = parentpath.lstrip('./')
    folderpath = os.path.join(self.mountpath, middlepath)
    filepath = os.path.join(folderpath, filename)
    try:
      sha1 = hm.calc_sha1_from_file(filepath)
    except (OSError, IOError):
      self.n_failed_sha1s += 1
      return None
    try:
      filestat = os.stat(filepath)
      bytesize = filestat.st_size
      mdatetime = filestat.st_mtime
    except (OSError, IOError):
      self.n_failed_filestat += 1
      return None
    dirnode = dn.DirNode(filename, parentpath, sha1, bytesize, mdatetime)
    _ = dirnode.insert_into_db(self.dbtree)
    # after insert (TO-DO: make an insert returns its row-id;)
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) == 1:
      row = fetched_list[0]
      _id = row[0]
      print('db found -> id', _id)
      return _id
    return None

  def do_rename(self, filename, newfilename, _id):
    self.n_to_rename += 1
    print(self.n_to_rename, '/', self.total_files_in_os, _id, 'name was renamed', newfilename)
    rename_tuple = (_id, newfilename)
    self.rename_tuplelist.append(rename_tuple)

  def get_files_id_in_db(self, filename):
    """
    middlepath = parentpath.lstrip('./')
    folderpath = os.path.join(self.mountpath, middlepath)
    filepath = os.path.join(folderpath, filename)
    """
    parentpath = self.ongoingfolder_abspath[len(self.mountpath):]
    if not parentpath.startswith('/'):
      parentpath = '/' + parentpath
    sql = 'SELECT * FROM %(tablename)s WHERE name=? AND parentpath=?;'
    tuplevalues = (filename, parentpath)
    fetched_list = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list is None or len(fetched_list) == 0:
      return None
    row = fetched_list[0]
    _id = row[0]
    print('db found -> id', _id)
    return _id

  def process_filename(self, filename):
    """
    A requirement is that the os-entry must have its correspondent db-entry

    The rename attempt is the following:
      1) lstrip(' \t')
      2) rstrip(' \t\r\n')
      3) replace(':', ';')
    """
    _id = self.get_files_id_in_db(filename)
    if not _id:
      _id = self.insert_os_entry_into_db_n_get_id(filename)
      if not _id:
        print('Cannot rename, id is None', _id)
        return None
    name, ext = os.path.splitext(filename)
    filtered_name = name.lstrip(' \t').rstrip(' \t\r\n').replace(':', ';')
    if name == filtered_name:
      return
    newfilename = filtered_name + ext
    self.do_rename(filename, newfilename, _id)

  def verify_filenames_for_rename(self, files):
    for filename in files:
      self.n_processed_files += 1
      print(self.n_processed_files, 'verifying', filename)
      self.process_filename(filename)

  def get_dbentry_dirnode_by_id(self, _id):
    fetched_list = self.dbtree.fetch_node_by_id(_id)
    if fetched_list and len(fetched_list) == 1:
      row =  fetched_list[0]
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      return dirnode
    return None

  def confirm_renames(self):
    total_ren = len(self.rename_tuplelist)
    if total_ren == 0:
      print('*** NO renames ***')
      return False
    print('************ confirm_renames ************')
    for i, rename_tuple in enumerate(self.rename_tuplelist):
      _id, newfilename = rename_tuple
      dirnode = self.get_dbentry_dirnode_by_id(_id)
      print(i+1, '/', total_ren, 'id', _id, '[', dirnode.name, '] to [',  newfilename, ']')
    screen_msg = 'Confirm the %d rename(s) above? (*Y/n) ' % total_ren
    ans = input(screen_msg)
    if ans in ['y', 'Y', '']:
      return True
    return False

  def do_renames(self):
    total_ren = len(self.rename_tuplelist)
    for i, rename_tuple in enumerate(self.rename_tuplelist):
      seq = i + 1
      _id, newfilename = rename_tuple
      dirnode = self.get_dbentry_dirnode_by_id(_id)
      if dirnode is None:
        print(seq, '/', total_ren, '/', self.total_files_in_os, 'id', _id, 'dirnode is None')
        continue
      print(seq, '/', total_ren, '/', self.total_files_in_os, 'id', _id, '[', dirnode.name, '] to [',  newfilename, ']')
      folderpath = dirnode.get_folderabspath_with_mountpath(self.mountpath)
      newfilepath = os.path.join(folderpath, newfilename)
      filepath = dirnode.get_abspath_with_mountpath(self.mountpath)
      if not os.path.isfile(filepath):
        continue
      if os.path.isfile(newfilepath):
        continue
      os.rename(filepath, newfilepath)
      self.n_renamed += 1
      print(id, '/', self.total_files_in_os, 'renamed', newfilepath)
      sql = 'UPDATE %(tablename)s SET name=? WHERE id=?;'
      tuplevalues = (newfilename, _id)
      _ = self.dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)
      self.n_updated_dbentries += 1
      print('dbupdated', self.n_updated_dbentries, 'id', _id, newfilename)

  def walkup_dirtree_for_renames(self):
    """

    """
    for self.ongoingfolder_abspath, dirs, files in os.walk(self.mountpath):
      if self.ongoingfolder_abspath == self.mountpath:  # this means not to process the mount_abspath folder itself
        continue
      if dirf.is_forbidden_dirpass(self.ongoingfolder_abspath):
        continue
      self.verify_filenames_for_rename(files)

  def report(self):
    self.calc_totals()
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os)
    print('n_renamed', self.n_renamed)
    print('n_processed_files', self.n_processed_files)
    print('n_updated_dbentries', self.n_updated_dbentries)
    print(
      'n_restricted_dirs', defaults.RESTRICTED_DIRNAMES_FOR_WALK,
      ':: those in ', defaults.RESTRICTED_DIRNAMES_FOR_WALK
    )
    print('n_files_empty_sha1', self.n_files_empty_sha1)
    print('n_failed_filestat', self.n_failed_filestat)
    print('n_failed_sha1s', self.n_failed_sha1s)
    print('n_dbentries_ins_upd', self.n_dbentries_ins_upd)
    print('n_dbentries_failed_ins_upd', self.n_dbentries_failed_ins_upd)

  def process(self):
    self.walkup_dirtree_for_renames()
    if self.confirm_renames():
      self.do_renames()
    self.report()


def process():
  start_time = datetime.datetime.now()
  print('Start Time', start_time)
  # ------------------
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  renamer = BulkRenamer(src_mountpath)
  renamer.process()
  finish_time = datetime.datetime.now()
  elapsed_time = finish_time - start_time
  # ------------------
  print('-'*50)
  print('Finish Time:', finish_time)
  print('Run Time:', elapsed_time)


if __name__ == '__main__':
  process()
