#!/usr/bin/env python3
"""
bulk_rename_forbidden_chars.py
"""
import datetime
import os.path
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
# import commands.dbentry_deleter_those_without_corresponding_osentry_cm as dbentry_del
import lib.hashfunctions.hash_mod as hm
import lib.dirfilefs.dir_n_file_fs_mod as dirf
import lib.strnlistfs.strfunctions_mod as strf
import default_settings as defaults


class BulkRenamer:

  def __init__(self, mountpath):
    """
    """
    self._outdict = None
    self.begin_time = datetime.datetime.now()
    self.end_time = None
    self.total_dirs_in_os = 0
    self.total_files_in_os = 0
    self.total_files_in_db = 0
    self.total_unique_files_in_db = 0
    self.n_processed_files = 0
    self.n_to_rename = 0
    self.n_renamed = 0
    self.rename_ids = []
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
    self.calc_totals()


  @property
  def runduration(self):
    if self.end_time is None:
      return datetime.datetime.now() - self.begin_time
    return self.end_time - self.begin_time

  def calc_totals(self):
    """
    count_total_files_n_folders_with_restriction(mountpath, restricted_dirnames, forbidden_first_level_dirs)
    """
    print('Counting files and dirs in db and os. Please wait.')
    self.total_unique_files_in_db = self.dbtree.count_unique_sha1s_as_int()
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_with_restriction(
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
    print(self.n_to_rename, '/', self.total_files_in_os, 'id', _id, filename, 'name was renamed', newfilename)
    self.rename_ids.append(_id)

  def get_files_id_in_db(self, filename):
    """
    middlepath = parentpath.lstrip(('./')
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
      3) replace ':', ';'
      # if parentheses are unbalanced above, it's because the IDE strangely complained about it inside a __doc__ string
    """
    newfilename = strf.clean_rename_filename_to(filename)
    if newfilename == filename:
      # if filename does not need to be renamed, return rightaway (no need to db-fetch id)
      return
    _id = self.get_files_id_in_db(filename)
    if not _id:
      _id = self.insert_os_entry_into_db_n_get_id(filename)
      if not _id:
        print('Cannot rename, id is None', _id, self.n_processed_files, '/', self.total_files_in_os)
        return None
    self.do_rename(filename, newfilename, _id)

  def verify_filenames_for_rename(self, files):
    for filename in files:
      self.n_processed_files += 1
      screen_ongoing_path = strf.put_ellipsis_in_str_middle(self.ongoingfolder_abspath, 50)
      print(self.n_processed_files, '/', self.total_files_in_os, 'verifying', filename, '@', screen_ongoing_path)
      self.process_filename(filename)

  def get_dbentry_dirnode_by_id(self, _id):
    fetched_list = self.dbtree.fetch_rowlist_by_id(_id)
    if fetched_list and len(fetched_list) == 1:
      row = fetched_list[0]
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      return dirnode
    return None

  def confirm_renames(self):
    total_ren = len(self.rename_ids)
    if total_ren == 0:
      print('*** NO renames ***')
      return False
    print('************ confirm_renames ************')
    for i, _id in enumerate(self.rename_ids):
      dirnode = self.get_dbentry_dirnode_by_id(_id)
      newfilename = strf.clean_rename_filename_to(dirnode.name)
      print(i+1, '/', total_ren, 'id', _id, '[', dirnode.name, '] to [',  newfilename, ']')
    screen_msg = 'Confirm the %d rename(s) above? (*Y/n) ' % total_ren
    ans = input(screen_msg)
    if ans in ['y', 'Y', '']:
      return True
    return False

  def do_renames(self):
    total_ren = len(self.rename_ids)
    for i, _id in enumerate(self.rename_ids):
      seq = i + 1
      dirnode = self.get_dbentry_dirnode_by_id(_id)
      newfilename = strf.clean_rename_filename_to(dirnode.name)
      if dirnode is None:
        print(seq, '/', total_ren, '/', self.total_files_in_os, 'id', _id, 'dirnode is None')
        continue
      print(seq, '/', total_ren, '/', self.total_files_in_os, 'id', _id, '[', dirnode.name, '] to [',  newfilename, ']')
      folderpath = dirnode.get_folderabspath_with_mountpath(self.mountpath)
      newfilepath = os.path.join(folderpath, newfilename)
      filepath = dirnode.get_abspath_with_mountpath(self.mountpath)
      if filepath == newfilepath:
        continue
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


  def get_attrs(self):
    outdict = {
      name: attr for name, attr in self.__dict__.items()
      if not name.startswith('__')
      and not name.startswith('_')
      and not callable(attr)
      and not type(attr) is staticmethod
    }
    return outdict

  @property
  def outdict(self):
    if self._outdict is not None:
      return self._outdict
    attrs = self.get_attrs()
    for attr in attrs:
      pyline = 'self._outdict[' + fieldname + '] = self.' + fieldname
      exec(pyline)
    return self._outdicct

  def mount_report_str(self):
    """
    defaults.RESTRICTED_DIRNAMES_FOR_WALK,
    """
    outstr = """
    total_files_in_db'   = {total_files_in_db}
    total_files_in_os'   = {total_files_in_os}
    total_dirs_in_os'    = {total_dirs_in_os}
    n_renamed', self.n_renamed)
    n_processed_files_in_trg = {n_processed_files)
    n_updated_dbentries = {n_updated_dbentries)
    n_restricted_dirs , 
    'n_files_empty_sha1', self.n_files_empty_sha1)
    'n_failed_filestat', self.n_failed_filestat)
    'n_failed_sha1s', self.n_failed_sha1s)
    'n_dbentries_ins_upd', self.n_dbentries_ins_upd)
    'n_dbentries_failed_ins_upd', self.n_dbentries_failed_ins_upd)

    """.format(**self.outdict)
    return outstr

  def report(self):
    self.calc_totals()
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os)
    print('n_renamed', self.n_renamed)
    print('n_processed_files_in_trg', self.n_processed_files)
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
  # ------------------
    print('-'*50)
    print('Finish Time:', self.end_time)
    print('Run Duration:', self.runduration)

  def process(self):
    self.walkup_dirtree_for_renames()
    if self.confirm_renames():
      self.do_renames()
    self.end_time = datetime.datetime.now()
    self.report()


def sweep_dirtree_for_cleanning_forbidden_chars():
  """
  Forbidden characters are such as: "?", ":", "\", "/" etc.

  # ------------------
  print('Running DBEntryWithoutCorrespondingOsEntryDeleter. Please wait.')
  dbentry_eraser = dbentry_del.DBEntryWithoutCorrespondingOsEntryDeleter(src_mountpath)
  dbentry_eraser.process()
  """
  start_time = datetime.datetime.now()
  print('Start Time', start_time)
  # ------------------
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  renamer = BulkRenamer(src_mountpath)
  renamer.process()



def process():
  sweep_dirtree_for_cleanning_forbidden_chars()


if __name__ == '__main__':
  process()
