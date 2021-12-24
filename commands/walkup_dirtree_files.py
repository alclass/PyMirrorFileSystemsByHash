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
import commands.dbentry_updater_by_filemove_based_on_size_n_mdt_mod as dbentry_upd
import commands.dbentry_deleter_those_without_corresponding_osentry_mod as dbentry_del
import fs.db.dbfailed_fileread_mod as freadfail
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import default_settings as defaults
ori_mount_abspath = '/media/friend/CompSci 2T Orig'


class FileSweeper:

  RESTRICTED_DIRNAMES_FOR_WALK = ['z-del', 'z-tri']

  def __init__(self, mountpath=None, treename='ori', restart_at_walkloopseq=None):
    """
    treename is generally 'ori' (source) or 'bak' (back-up)
    source and target are generally 'src' (source) or 'trg' (back-up)
    some operations may occur in the same dirtree,
      in such cases 'bak' may refer to a subdirectory in the same dirtree as 'ori'
    """
    self.total_dirs_in_dirtree = 0
    self.total_files_in_dirtree = 0
    self.n_processing_files = 0
    self.n_restricted_dirs = 0
    self.n_files_empty_sha1 = 0
    self.n_failed_filestat = 0
    self.n_dbentries_ins_upd = 0
    self.n_dbentries_failed_ins_upd = 0
    self.all_nodes_with_osread_problem = []
    self.mountpath = mountpath
    if self.mountpath is None:
      self.mountpath = ori_mount_abspath
    if not os.path.isdir(self.mountpath):
      error_msg = 'Missing file errror mount_abspath (%s) does not exist.'
      raise OSError(error_msg)
    self.dirtree = dt.DirTree(treename, self.mountpath)
    self.dbtree = self.dirtree.dbtree  # dbu.DBDirTree(mount_abspath)
    self.restart_at_walkloopseq = restart_at_walkloopseq
    self.treat_restart_at_walkloopseq()
    # freadfailer will record the sha1 fileread failed attempts
    self.freadfailer = freadfail.DBFailFileReadReporter(self.dbtree.mountpath)

  def treat_restart_at_walkloopseq(self):
    """
    attribute restart_at_walkloopseq is an input parameter that tells the position in the walk loop
      to restart in case the processing was previously interrupted.
    """
    if self.restart_at_walkloopseq is None:
      self.restart_at_walkloopseq = 0

  def exists_in_db_name_parent_n_size(self, name, parentpath, bytesize):
    sql = 'SELECT * from %(tablename)s WHERE name=? and parentpath=? and bytesize=?;'
    tuplevalues = (name, parentpath, bytesize)
    reslist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(reslist) > 0:
      print(self.n_processing_files, ' => file', name, 'EXISTS. Continuing.')
      return True
    return False

  def dbinsert(self, current_abspath, files, middlepath):
    for filename in files:
      self.n_processing_files += 1
      if self.n_processing_files < self.restart_at_walkloopseq:
        print('Jumping', self.n_processing_files, 'until', self.restart_at_walkloopseq)
        continue
      filepath = os.path.join(current_abspath, filename)
      name = filename
      parentpath = middlepath
      if not parentpath.startswith('/'):
        parentpath = '/' + parentpath
      try:
        filestat = os.stat(filepath)
      except OSError:
        self.n_failed_filestat += 1
        print(self.n_failed_filestat, 'Could not filestat', filename)
        continue
      bytesize = filestat.st_size
      mdatetime = filestat.st_mtime  # file_attr_tuple[9]
      pydt = datetime.datetime.fromtimestamp(mdatetime)
      print('bytesize =', bytesize, hm.convert_to_size_w_unit(bytesize), ':: mdatetime =', mdatetime, pydt)
      if self.exists_in_db_name_parent_n_size(name, parentpath, bytesize):
        print(self.n_processing_files, 'of', self.total_files_in_dirtree,
              'DirNode already exists in db. Continuing.')
        continue
      print(self.n_processing_files, 'of', self.total_files_in_dirtree,
            ' => calculating sha1 for', name, parentpath)
      sha1 = hm.calc_sha1_from_file(filepath)
      print('sha1 =', sha1)
      dirnode = dn.DirNode(name, parentpath, sha1, bytesize, mdatetime)
      if sha1 == hm.EMPTY_SHA1_AS_BIN:
        self.n_files_empty_sha1 += 1
        print('DirNodoe has the empty (zero) sha1. Continuing', dirnode.name, dirnode.parentpath)
      if sha1 is not None:
        insert_update_result = self.dirtree.dbinsert_dirnode(dirnode)
        if insert_update_result:
          self.n_dbentries_ins_upd += 1
          print('n_dbentries_ins_upd', self.n_dbentries_ins_upd, dirnode)
        else:
          print(' >>>>>>>>>>>>>>>> sha1 is valid but db-insert returned false')
      else:
        # sha1 is None here (file is probably unreadable from disk)
        self.report_unreadable_file(dirnode)

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

  def count_total_dirs_n_files_in_dirtree(self):
    self.total_files_in_dirtree = 0
    self.total_dirs_in_dirtree = 0
    print('Counting all files in dirtree ' + self.mountpath)
    for ongoingfolder_abspath, folders, files in os.walk(self.mountpath):
      self.total_dirs_in_dirtree += len(folders)
      if ongoingfolder_abspath == self.mountpath:  # this means not to count files in the tree's root folder
        continue
      self.total_files_in_dirtree += len(files)
    print('-' * 50)
    print('Number of files in dirtree %d' % self.total_files_in_dirtree)
    print('-'*50)

  def walkup_dirtree_files(self):
    """

    """
    for ongoingfolder_abspath, dirs, files in os.walk(self.mountpath):
      middlepath = ongoingfolder_abspath[len(self.mountpath):]
      middlepath = middlepath.lstrip('./')  # parentpath is '/' + middlepath (in some cases they are the same)
      if ongoingfolder_abspath == self.mountpath:  # this means not to process the mount_abspath folder itself
        continue
      if dirf.is_any_dirname_in_path_startingwith_any_in_list(ongoingfolder_abspath, self.RESTRICTED_DIRNAMES_FOR_WALK):
        self.n_restricted_dirs += 1
        continue
      self.dbinsert(ongoingfolder_abspath, files, middlepath)

  def report_all_nodes_with_osread_problem(self):
    print('-='*20)
    print('report_all_nodes_with_osread_problem:')
    print('-='*20)
    for i, dirnode in enumerate(self.all_nodes_with_osread_problem):
      print(i+1, dirnode)
    n_unreadable = len(self.all_nodes_with_osread_problem)
    print('Total', n_unreadable, ':: report_all_nodes_with_osread_problem')

  def report(self):
    print('n_all_files_in_dirtree', self.total_files_in_dirtree)
    print('total_files_in_dirtree', self.total_files_in_dirtree, ':: files in root-dirtree are not counted')
    print('total_dirs_in_dirtree', self.total_dirs_in_dirtree)
    print('n_processed_files', self.n_processing_files)
    print('n_restricted_dirs', self.n_restricted_dirs, ':: those in ', self.RESTRICTED_DIRNAMES_FOR_WALK)
    print('n_files_empty_sha1', self.n_files_empty_sha1)
    print('n_failed_filestat', self.n_failed_filestat)
    print('n_dbentries_ins_upd', self.n_dbentries_ins_upd)
    print('n_dbentries_failed_ins_upd', self.n_dbentries_failed_ins_upd)

  def process(self):
    self.count_total_dirs_n_files_in_dirtree()
    dbupdater = dbentry_upd.DBEntryUpdater(self.mountpath)
    dbupdater.process(self.total_files_in_dirtree)
    self.walkup_dirtree_files()
    dbdeleter = dbentry_del.DBEntryWithoutCorrespondingOsEntryDeleter(self.mountpath)
    dbdeleter.process()
    self.report_all_nodes_with_osread_problem()
    self.report()


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
  sweeper = FileSweeper(src_mountpath, treename, restart_at_position)
  sweeper.process()
  finish_time = datetime.datetime.now()
  elapsed_time = finish_time - start_time
  # ------------------
  print('-'*50)
  print('Finish Time:', finish_time)
  print('Run Time:', elapsed_time)


if __name__ == '__main__':
  process()
