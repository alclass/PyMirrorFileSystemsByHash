#!/usr/bin/env python3
"""
This script intends to "walk up" a dirtree and db-inserts file dirnodes.
After this db-inserting, one application arises which is back-up between two disks
(or in fact any two dirtrees - a dirtree is a directory with its descendants: subfolders and files).

A bit of explanation of the capabilities of mirroring one dirtree to another with file-hashes
=============================================================================================

The back-up (or mirroring) operation is possible by comparisons file to file.
Excepting where hash collisions are concerned (this subject is not treated here),
  it's postulated that equal sha1's means equal files if even they have different names and locations.
  (Its size attribute must be the same for the two files in case are equal.)

Let's visualize an instance of the above mentioned postulate:
  - suppose a file "abc" has its hash as "123" and is located in the source dirtree t1;
  - if the compared target dirtree t2 does not have it, this means that file "abc"
    is missing in the target dirtree t2.
  - Based on this sqlite data, the system should copy over file "123" from the source dirtree t1
    to the target dirtree t2 and "mirror", so to say, this file there.

Various operations for realizing mirroring (or back-up)
=======================================================

Operations like copy (in-between dirtrees), move/rename (within the same dirtree)
   and delete (for cleaning up excess or files in target non-existent in source)
   are the main back-up operations.

The data structure of dirnode:
-----------------------------
DirNode (at the time of writing) is an 8-tuple ie:
  0 _id | 1 name | 2 parentpath | 3 sha1 | 4 bytesize | 5 mdatetime
    OBS: dbtree has a property called fieldnames which must be used to avoid hardcoding the indices
    (though it's still necessary to hardcode the field names themselves).

Transient operations:
====================
At the time of this writing, one transient operation is to verify
  the presence of files recorded in db in the actual dirtree.
It's called transient, because this system is not integrated with the OS's file system.
  Because of that, the user must use this system in a non-multiuser environment
  and also abstain from changing the dirtrees during operations
  though the scripts here will always check for existence or non-existence
  of files in disk before any copying, moving/renaming or deleting operations.
"""
import datetime
import os.path
import sys
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import fs.hashfunctions.hash_mod as hm
import commands.dbentry_updater_by_filemove_based_on_size_n_mdt_cm as dbentry_upd
import commands.dbentry_deleter_those_without_corresponding_osentry_cm as dbentry_del
import fs.db.dbfailed_fileread_mod as freadfail
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import fs.strnlistfs.strfunctions_mod as strf
import default_settings as defaults


class FilesUpDirTreeWalker:

  def __init__(self, mountpath, treename='ori', restart_at_walkloopseq=None):
    """
    treename is generally 'ori' (source) or 'bak' (back-up)
    source and target are generally 'src' (source) or 'trg' (back-up)
    some operations may occur in the same dirtree,
      in such cases 'bak' may refer to a subdirectory in the same dirtree as 'ori'
    """
    self.total_dirs_in_os = 0
    self.total_files_in_os = 0
    self.total_files_in_db = 0
    self.total_unique_files_in_db = 0
    self.n_found_files_name_n_parent_in_db = 0
    self.n_found_files_size_n_date_in_db = 0
    self.n_processed_files = 0
    self.n_restricted_dirs = 0
    self.n_updated_dbentries = 0
    self.n_inserted = 0
    self.n_files_empty_sha1 = 0
    self.n_failed_filestat = 0
    self.n_failed_sha1s = 0
    self.n_empty_dirs_removed = 0
    self.n_empty_dirs_fail_rm = 0
    self.n_dbentries_ins_upd = 0
    self.n_dbentries_failed_ins_upd = 0
    self.all_nodes_with_osread_problem = []
    self.ongoingfolder_abspath = None
    self.mountpath = mountpath
    if not os.path.isdir(self.mountpath):
      error_msg = 'Missing file errror mount_abspath (%s) does not exist.'
      raise OSError(error_msg)
    self.dirtree = dt.DirTree(treename, self.mountpath)
    self.dbtree = self.dirtree.dbtree  # dbu.DBDirTree(mount_abspath)
    self.calc_totals()
    self.restart_at_walkloopseq = restart_at_walkloopseq
    self.treat_restart_at_walkloopseq()
    # freadfailer will record the sha1 fileread failed attempts
    self.freadfailer = freadfail.DBFailFileReadReporter(self.dbtree.mountpath)

  def calc_totals(self):
    """
    count_total_files_n_folders_with_restriction(mountpath, restricted_dirnames, forbidden_first_level_dirs)
    """
    print('Counting files and dirs in db and os. Please wait.')
    self.total_unique_files_in_db = self.dbtree.count_unique_sha1s_as_int()
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_with_restriction(self.dirtree.mountpath)
    self.total_files_in_os = total_files
    self.total_dirs_in_os = total_dirs

  def treat_restart_at_walkloopseq(self):
    """
    attribute restart_at_walkloopseq is an input parameter that tells the position in the walk loop
      to restart in case the processing was previously interrupted.
    """
    if self.restart_at_walkloopseq is None:
      self.restart_at_walkloopseq = 0

  def get_id_or_none_from_name_n_parentpath(self, name, parentpath):
    sql = 'SELECT * from %(tablename)s WHERE name=? and parentpath=?;'
    tuplevalues = (name, parentpath)
    reslist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    r_id = None
    if reslist or len(reslist) == 1:  # name and parent are UNIQUE in rows
      row = reslist[0]
      r_id = row[0]  # id is always index 0
    elif len(reslist) > 1:
      # this is considered a logical error in the database,
      # if it happens, table schema should be inspected, there should be an "index(name, parent) UNIQUE" in the schema
      error_msg = 'Unicity of name and parent in db is broken. There were %d found rows with same name and parent.' \
          % len(reslist)
      raise ValueError(error_msg)
    return r_id

  def get_dirnode_if_name_n_parent_exists_in_db_or_none(self, name, parentpath):
    _id = self.get_id_or_none_from_name_n_parentpath(name, parentpath)
    if _id is None:
      return None
    self.n_found_files_name_n_parent_in_db += 1
    dirnode = self.dbtree.fetch_dirnode_by_id(_id)
    return dirnode

  def update_db_entry_with_updated_file(self, _id, name, parentpath, sha1, bytesize, mdatetime):
    sql = '''UPDATE %(tablename)s SET
      name=?,
      parentpath=?,
      sha1=?,
      bytesize=?,
      mdatetime=?
    WHERE
      id=?;
    '''
    tuplevalues = (name, parentpath, sha1, bytesize, mdatetime, _id)
    retval = self.dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)
    if retval:
      self.n_updated_dbentries += 1
      print(self.n_updated_dbentries, name, parentpath)
      return True
    return False

  def update_db_entry_with_dirnode(self, dirnode):
    return self.update_db_entry_with_updated_file(
        dirnode.get_db_id(),
        dirnode.name,
        dirnode.parentpath,
        dirnode.sha1,
        dirnode.bytesize,
        dirnode.mdatetime
    )

  def insert_db_entry_with_updated_file(self, name, parentpath, sha1, bytesize, mdatetime):
    sql = 'INSERT into %(tablename)s (name, parentpath, sha1, bytesize, mdatetime) VALUES (?,?,?,?,?);'
    tuplevalues = (name, parentpath, sha1, bytesize, mdatetime)
    insert_result = self.dbtree.do_insert_with_sql_n_tuplevalues(sql, tuplevalues)
    return insert_result

  def insert_db_entry_with_dirnode(self, dirnode):
    return self.insert_db_entry_with_updated_file(
        dirnode.name,
        dirnode.parentpath,
        dirnode.sha1,
        dirnode.bytesize,
        dirnode.mdatetime
    )

  def dbinsert_or_update_file_entry(self, name, parentpath, bytesize, mdatetime, filepath):
    """
    This method is not being called (TO-DO see if it can go elsewhere)
    """
    sha1 = hm.calc_sha1_from_file(filepath)
    if sha1 == hm.EMPTY_SHA1_AS_BIN:
      self.n_files_empty_sha1 += 1
      print(
        self.n_files_empty_sha1, 'of', self.total_files_in_os,
        'DirNodoe has the empty (zero) sha1. Continuing', name, parentpath
      )
      return
    dirnode = dn.DirNode(name, parentpath, sha1, bytesize, mdatetime)
    if sha1 is None:
      self.n_failed_sha1s += 1
      print(
        self.n_failed_sha1s, 'of', self.total_files_in_os, name,
        ' >>>>>>>>>>>>>>>> failed sha1 calc (file probably unreadable). Continuing.'
      )
      self.report_unreadable_file(dirnode)
      return
    _id = self.get_id_or_none_from_name_n_parentpath(name, parentpath)
    if _id is None:
      # boolres = self.insert_db_entry_with_updated_file(name, parentpath, sha1, bytesize, mdatetime)
      insert_update_result = self.dirtree.dbinsert_dirnode(dirnode)
      if insert_update_result:
        self.n_dbentries_ins_upd += 1
        print(
          'n_dbentries_ins_upd', self.n_dbentries_ins_upd, 'of', self.total_files_in_os,
          'INSERTED dirnode', dirnode
        )
      return
    else:
      boolres = self.update_db_entry_with_updated_file(_id, name, parentpath, sha1, bytesize, mdatetime)
      if boolres:
        self.n_dbentries_ins_upd += 1
        print(
          'n_dbentries_ins_upd', self.n_dbentries_ins_upd, 'of', self.total_files_in_os, boolres,
          'UPDATED id', _id
        )
      return

  def print_screen_msg_for_file_processing(self, dirnode, screen_msg_update_insert_or_none):
    sha1 = dirnode.sha1 or '[no-sha1]'
    print(
      'ins', self.n_inserted, 'proc', self.n_processed_files, '/',
      'tot', self.total_files_in_os,
      'file', screen_msg_update_insert_or_none,
      dirnode.name, sha1, '@', strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50)
    )

  def dbinsert_file_if_needed(self, filename, parentpath):
    self.n_processed_files += 1
    if self.n_processed_files < self.restart_at_walkloopseq:
      print('Jumping', self.n_processed_files, 'until', self.restart_at_walkloopseq)
      return
    filepath = os.path.join(self.ongoingfolder_abspath, filename)
    name = filename
    try:
      filestat = os.stat(filepath)
    except OSError:
      self.n_failed_filestat += 1
      print(
        self.n_failed_filestat, 'of', self.total_files_in_os,
        'Could not filestat', filename
      )
      return
    bytesize = filestat.st_size
    mdatetime = filestat.st_mtime
    pydt = datetime.datetime.fromtimestamp(mdatetime)
    print(self.n_processed_files, 'of', self.total_files_in_os, name, parentpath)
    print('bytesize =', bytesize, hm.convert_to_size_w_unit(bytesize), ':: mdatetime =', mdatetime, pydt)
    dirnode = self.get_dirnode_if_name_n_parent_exists_in_db_or_none(name, parentpath)
    if dirnode:
      if dirnode.has_same_size_n_date(bytesize, mdatetime):
        screen_msg_update_insert_or_none = 'DB-EXISTS size & date'
        self.print_screen_msg_for_file_processing(dirnode, screen_msg_update_insert_or_none)
        return False
      if dirnode.bytesize == bytesize:  # and bytesize > 1024*1024:
        screen_msg_update_insert_or_none = 'DB-EXISTS size only (date unchecked)'
        self.print_screen_msg_for_file_processing(dirnode, screen_msg_update_insert_or_none)
        return False
      screen_msg_update_insert_or_none = 'DB-UPDATED'
      _ = self.update_db_entry_with_dirnode(dirnode)  # name and parent exists but size and/or date are different
      self.print_screen_msg_for_file_processing(dirnode, screen_msg_update_insert_or_none)
      return True
    return self.calc_sha1_n_insert_db_entry_with_fields(
      filename, parentpath, bytesize, mdatetime, filepath
    )

  def calc_sha1_n_insert_db_entry_with_fields(
      self, name, parentpath, bytesize, mdatetime, filepath
    ):
    """
    here name is filename, the same convention in db
    """
    try:
      sha1 = hm.calc_sha1_from_file(filepath)
    except IOError:
      self.n_failed_sha1s += 1
      print(
        self.n_failed_sha1s, 'of', self.total_files_in_os,
        'Could not sha1', name
      )
      return False
    newdirnode = dn.DirNode(name, parentpath, sha1, bytesize, mdatetime)
    _ = self.insert_db_entry_with_dirnode(newdirnode)  # row does not exist, insert it
    self.n_inserted += 1
    screen_msg_update_insert_or_none = 'DB-INSERTED'
    self.print_screen_msg_for_file_processing(newdirnode, screen_msg_update_insert_or_none)
    return True

  def dbinsert_files_if_needed(self, files):
    middlepath = self.ongoingfolder_abspath[len(self.mountpath):]
    middlepath = middlepath.lstrip('./')
    parentpath = '/' + middlepath
    for filename in files:
      _ = self.dbinsert_file_if_needed(filename, parentpath)  # returns a boolean

  def walkup_dirtree_files(self):
    """

    """
    for self.ongoingfolder_abspath, dirs, files in os.walk(self.mountpath):
      if self.ongoingfolder_abspath == self.mountpath:  # this means not to process the mount_abspath folder itself
        continue
      if dirf.is_forbidden_dirpass(self.ongoingfolder_abspath):
        continue
      self.dbinsert_files_if_needed(files)

  def prune_empty_folders(self):
    n_visited, n_removed, n_failed = dirf.prune_dirtree_deleting_empty_folders(self.mountpath)
    self.n_empty_dirs_removed = n_removed
    self.n_empty_dirs_fail_rm = n_failed

  def process(self):
    """
    Before walking up the dirtree, it must check whether or not file-entries have already been sha1'ed
    One strategy for that is to check file's existence in db with size and mdatetime,
      excepting smaller text-files in that case (that kind of sha1 is inexpensive and probability of
        a small text file being change with same size and mdatetime [to study/test more)
        having the same sha1 is unknown.
    """
    self.calc_totals()
    self.walkup_dirtree_files()
    dbdeleter = dbentry_del.DBEntryWithoutCorrespondingOsEntryDeleter(self.mountpath)
    dbdeleter.process()
    self.prune_empty_folders()

    print('Removing empty folders left if any.')
    dirf.prune_dirtree_deleting_empty_folders(self.dirtree.mountpath)

    self.report_all_nodes_with_osread_problem()
    self.report()

  def report_unreadable_file(self, dirnode):
    self.all_nodes_with_osread_problem.append(dirnode)
    self.n_dbentries_failed_ins_upd += 1
    print('n_dbentries_failed_ins_upd', self.n_dbentries_failed_ins_upd, dirnode, '[going to register event]')
    pdict = {
      'name': dirnode.name,
      'parentpath': dirnode.parentpath,
      'bytesize': dirnode.bytesize,
      'mdatetime': dirnode.mdatetime,
      'event_dt': datetime.datetime.now(),
    }
    self.freadfailer.do_insert_or_update_with_dict_to_prep_tuplevalues(pdict)

  def report_all_nodes_with_osread_problem(self):
    print('-='*20)
    print('report_all_nodes_with_osread_problem:')
    print('-='*20)
    for i, dirnode in enumerate(self.all_nodes_with_osread_problem):
      print(i+1, dirnode)
    n_unreadable = len(self.all_nodes_with_osread_problem)
    print('Total', n_unreadable, ':: report_all_nodes_with_osread_problem')

  def report(self):
    self.calc_totals()
    print('total_files_in_db', self.total_files_in_db)
    print('total_unique_files_in_db', self.total_unique_files_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os)
    print('n_processed_files', self.n_processed_files)
    print('n_found_files_name_n_parent_in_db', self.n_found_files_name_n_parent_in_db)
    print('n_found_files_size_n_date_in_db', self.n_found_files_size_n_date_in_db)
    print('n_updated_dbentries', self.n_updated_dbentries)
    print('n_inserted', self.n_inserted)
    print('n_restricted_dirs', self.n_restricted_dirs, ':: those in ', defaults.RESTRICTED_DIRNAMES_FOR_WALK)
    print('n_files_empty_sha1', self.n_files_empty_sha1)
    print('n_failed_filestat', self.n_failed_filestat)
    print('n_failed_sha1s', self.n_failed_sha1s)
    print('n_dbentries_ins_upd', self.n_dbentries_ins_upd)
    print('n_dbentries_failed_ins_upd', self.n_dbentries_failed_ins_upd)


def get_arg_restart_at_position_or_zero():
  """
  This cli arg is used when the user wants to continue processing after interrupting it at some position
  """
  for arg in sys.argv:
    if arg.startswith('-r='):
      try:
        arg = int(arg[len('-r='):])
        return arg
      except ValueError:
        pass
  return 0  # the default


def process():
  start_time = datetime.datetime.now()
  print('Start Time', start_time)
  # ------------------
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  restart_at_position = get_arg_restart_at_position_or_zero()
  treename = 'ori'  # ori stands for origin instead of target
  moved_updater = dbentry_upd.DBEntryUpdater(src_mountpath)
  moved_updater.process()
  walker = FilesUpDirTreeWalker(src_mountpath, treename, restart_at_position)
  walker.process()
  finish_time = datetime.datetime.now()
  elapsed_time = finish_time - start_time
  # ------------------
  print('-'*50)
  print('Finish Time:', finish_time)
  print('Run Time:', elapsed_time)


if __name__ == '__main__':
  process()
