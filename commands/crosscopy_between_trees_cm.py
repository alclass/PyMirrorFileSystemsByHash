#!/usr/bin/env python3
"""
mirror2trees_cm.py

This script does basically three things:
  1) it moves target-tree files to the relative position, in the target-tree itself, that exists in the source-tree;
  2) it copies to the target-tree missing files that exists in the source-tree;
  3) it deletes under confirmation excess files in target, ie files that exist in target but do not in source;

Things this script doesn't do:
  1) this script DOESN'T do removals in source (note above that it removes under confirmation excess files in target);
  2) this script DOESN'T do the inverse of the three operations above;
     (the user can run it inversely swapping the order of the two parameters: source_mountpath and target_mountpath).

Other scripts in this system/app complete the mirroring effect, for example:
  - cleaning up entries that end with whitespaces;
  - treating duplicates in a directory tree (in fact this should be run before this mirror-back-up script);
  - syncronize os-entries with db-entries
    (a GUI window manager is planned to the future to integrate db-sync with os-operations).
"""
import datetime
import os.path
import shutil
import sys
import models.entries.dirnode_mod as dn
import fs.db.dbdirtree_mod as dbdt
import fs.db.dbfailed_filecopy_mod as dbfailedcopy
import fs.hashfunctions.hash_mod as hm
import fs.strnlistfs.strfunctions_mod as strf
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import default_settings as defaults
import commands.move_rename_target_based_on_source_mod as moverename


class DoubleDirectionCopier:

  def __init__(self, ori_mountpath, bak_mountpath, restart_at=None):
    self.start_time = datetime.datetime.now()
    self.ori_dt = dbdt.DBDirTree(ori_mountpath)
    self.bak_dt = dbdt.DBDirTree(bak_mountpath)
    self.restart_at = restart_at
    self.n_files_processed = 0
    self.n_copied_files = 0
    self.n_failed_copies = 0
    self.n_looped_rows = 0
    self.n_moved_files = 0
    self.n_deleted_files = 0
    self.n_file_not_backable = 0
    self.n_rows_deleted = 0
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    self.fetch_total_files_in_src_n_trg()  # this call will be repeated for method report() at the end of processing
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    self.fetch_total_unique_files_in_src_n_trg()  # idem
    self.dbfailedcopyreporter = dbfailedcopy.DBFailFileCopyReporter(self.ori_dt.mountpath)

  @property
  def total_of_repeat_srcfiles(self):
    return self.total_srcfiles_in_db - self.total_unique_srcfiles

  @property
  def total_of_repeat_trgfiles(self):
    return self.total_trgfiles_in_db - self.total_unique_trgfiles

  def fetch_total_files_in_src_n_trg(self):
    self.total_srcfiles_in_db = self.ori_dt.count_rows_as_int()
    self.total_trgfiles_in_db = self.bak_dt.count_rows_as_int()

  def fetch_total_unique_files_in_src_n_trg(self):
    sql = 'SELECT count(distinct sha1) FROM %(tablename)s ORDER BY sha1;'
    fetched_list = self.ori_dt.do_select_with_sql_without_tuplevalues(sql)
    if fetched_list:
      self.total_unique_srcfiles = int(fetched_list[0][0])
    fetched_list = self.bak_dt.do_select_with_sql_without_tuplevalues(sql)
    if fetched_list:
      self.total_unique_trgfiles = int(fetched_list[0][0])

  def fetch_row_if_sha1_exists_in_target(self, sha1):
    """
    Duplicates may exist in which case functionality elsewhere will treat removing excess

    if len(fetched_list) == 0:
      return None
    if len(fetched_list) == 1:
      return fetched_list[0]
    error_msg = "Inconsistency Error: db has more than 1 sha1 (%s)" \
                " when it's a UNIQUE fields ie it can only contains one" % sha1
    raise ValueError(error_msg)
    """
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = self.bak_dt.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    return fetched_list

  def move_file_within_its_dirtree(self, trg_dirnode_to_move, src_ref_dirnode):
    print(self.n_moved_files, 'PATH SHOULD be moved')
    file_now_path = os.path.join(self.bak_dt.mountpath, trg_dirnode_to_move.path)
    file_new_path = os.path.join(self.ori_dt.mountpath, src_ref_dirnode.path)
    print('FROM: ', file_now_path)
    print('TO: ', file_new_path)
    print('-'*40)

  def move_file_within_target_using_src_position(self, src_dirnode, trg_dirnode):
    if src_dirnode.path == trg_dirnode.path:
      return False
    old_trg_filepath = trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    if not os.path.isfile(old_trg_filepath):
      # give up, origin file in target does not exist
      return False
    new_trg_complement_filepath = src_dirnode.path.lstrip('/')
    new_trg_filepath = os.path.join(self.bak_dt.mountpath, new_trg_complement_filepath)
    if os.path.isfile(new_trg_filepath):
      # give up, targetfile already exists
      return False
    try:
      shutil.move(old_trg_filepath, new_trg_filepath)
    except OSError:
      return False
    # update db with new_trg_path
    return trg_dirnode.dbupdate_new_path_to(src_dirnode, self.bak_dt)

  def verify_moving_files_thru_target(self):
    for rows in self.bak_dt.do_select_all_w_limit_n_offset():
      for row in rows:
        trg_dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dt.fieldnames)
        inner_sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
        tuplevalues = (trg_dirnode.sha1, )
        fetched_rows = self.ori_dt.do_select_with_sql_n_tuplevalues(inner_sql, tuplevalues)
        if len(fetched_rows) > 0:
          src_row = fetched_rows[0]
          src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.bak_dt.fieldnames)
          if src_dirnode.path != trg_dirnode.path:
            self.move_file_within_target_using_src_position(src_dirnode, trg_dirnode)

  def copy_filepath(self, srcfilepath, trgfilepath):
    """
    Up til this point, the following 3 conditions are met:
      1) sha1 is missing in target;
      2) srcfilepath exists and
      3) trgfilepath does not exist.
    """
    bool_raise_oserror = False
    try:
      bak_dirpath, _ = os.path.split(trgfilepath)
      if not os.path.isdir(bak_dirpath):
        print('Creating dir', bak_dirpath)
        os.makedirs(bak_dirpath)
      if not os.path.isfile(trgfilepath):
        self.n_copied_files += 1
        print(self.n_copied_files, 'Copying', trgfilepath)
        shutil.copy2(srcfilepath, trgfilepath)
      else:
        return False
      # if files copied does not exist, set boolean for raising an exception later on
      if not os.path.isfile(trgfilepath):
        bool_raise_oserror = True
    except OSError:
      bool_raise_oserror = True
    if bool_raise_oserror:
      # TO-DO: occurrences of fail copies should be registered somewhere (in a report db or file)
      error_msg = '****************** Runtime Error: Copy of %s failed.' % trgfilepath
      print(error_msg)
      self.n_failed_copies += 1
      # raise ValueError(error_msg)
    return True

  def insert_node_after_copy(self, trg_dirnode, trgfilepath):
    if os.path.isdir(trgfilepath):
      trg_dirnode.insert_into_db(self.bak_dt)
    else:
      error_msg = 'Runtime Error: Copy of %(trg_dirnode) failed.' % trg_dirnode
      raise ValueError(error_msg)

  def sha1_exists_in_trg_try_move_within_target(self, src_dirnode, trg_row):
    """
    If program flow gets here, target-file does not exist in its correspondent position, but exists elsewhere.
    Obs the hashkey for name and parentpath was discontinued in the app/system.
    """
    if type(trg_row) != tuple or len(trg_row) < len(self.ori_dt.fieldnames) - 1:
      return False
    idx = self.ori_dt.fieldnames.index('parentpath')
    oldparentpath = trg_row[idx]
    idx = self.ori_dt.fieldnames.index('name')
    oldname = trg_row[idx]
    old_trg_folderpath = os.path.join(self.bak_dt.mountpath, oldparentpath.lstrip('/'))
    old_trg_filepath = os.path.join(old_trg_folderpath, oldname)
    if not os.path.isfile(old_trg_filepath):
      # give up for moveable file does not exist (dirtree got unsync'ed somehow in the meanwhile)
      return False
    # building the new_trg_middlepath for the move operation
    newparentpath = src_dirnode.parentpath
    newname = src_dirnode.name
    new_trg_folderpath = os.path.join(self.bak_dt.mountpath, newparentpath.lstrip('/'))
    new_trg_filepath = os.path.join(new_trg_folderpath, newname)
    if os.path.isfile(new_trg_filepath):
      # give up for target move file exists
      return False
    try:
      # make dirs if folderpath does not exist
      if not os.path.isdir(new_trg_folderpath):
        os.makedirs(new_trg_folderpath)
      # move file to its new position
      shutil.move(old_trg_filepath, new_trg_filepath)
      self.n_moved_files += 1
      print(' -> moved file to its new position', new_trg_folderpath)
    except OSError:
      return False
    # if program flow got here, a db-UPDATE can be issued to sync file position in db
    sql = '''UPDATE %(tablename)s 
    SET
      name=?,
      parentpath=?
    WHERE
      name=? and
      parentpath=? and
      sha1=?
    '''
    tuplevalues = (newname, newparentpath, oldname, oldparentpath,  src_dirnode.sha1)
    return self.bak_dt.do_update_with_sql_n_tuplevalues(sql, tuplevalues)

  def copy_source_files_to_target_if_needed(self, src_rowlist):
    """
    TO-DO: create a constant-list that contains the index-position of fields in a centralized way
      0 id | 1 hkey | 2 name | 3 parentpath | 4 is_present | 5 sha1 | 6 bytesize | 7 mdatetime
    """
    for src_row in src_rowlist:
      self.n_files_processed += 1
      src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.fieldnames)
      print(self.n_files_processed, 'verifying copy/move for', src_row)
      # if src has repeats, it should not copy or move files, because repeats are ambiguity
      # (in thesis, they must be solved before this point and none left here)
      sql = 'SELECT count(id) FROM %(tablename)s WHERE sha1=?;'
      tuplevalues = (src_dirnode.sha1, )
      fetched_list = self.ori_dt.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      if fetched_list:
        n_of_filerepeats = int(fetched_list[0][0])
        if n_of_filerepeats > 1:
          print(
            self.n_files_processed, '/', self.total_srcfiles_in_db,
            'Ambiguity: script cannot copy or move with repeats =', n_of_filerepeats,
            ' for', src_dirnode.name, 'in dir:', src_dirnode.parentpath, 'Continuing.'
          )
          continue
      if src_dirnode.sha1 == hm.EMPTY_SHA1_AS_BIN:
        self.n_file_not_backable += 1
        print('Continuing for next. File not copiable (the zero sha1):', src_dirnode.name)
        continue
      if src_dirnode.name.endswith('.part'):
        self.n_file_not_backable += 1
        print('Continuing for next. File not copiable (.part extension):', src_dirnode.name)
        continue
      if strf.any_dir_in_path_startswith(src_dirnode.parentpath, 'mp3s '):
        self.n_file_not_backable += 1
        continue
      lowercharspath = src_dirnode.parentpath.lower()
      if lowercharspath.find('z-del') > -1:
        self.n_file_not_backable += 1
        print('Continuing for next. [z-del] foldername detected:', src_dirnode.parentpath)
        continue
      src_filepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mount_abspath)
      if not os.path.isfile(src_filepath):
        print(self.n_files_processed, '/', self.total_srcfiles_in_db,
              'Continuing for next. Source file does not exist (%s) ' % src_filepath)
        continue
      trg_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.fieldnames)
      trg_filepath = trg_dirnode.get_abspath_with_mountpath(self.bak_dt.mount_abspath)
      if os.path.isfile(trg_filepath):
        print(self.n_files_processed, '/', self.total_srcfiles_in_db,
              'Continuing for next. Target file exists (%s) ' % src_filepath)
        continue
      trg_rows = self.fetch_row_if_sha1_exists_in_target(src_dirnode.sha1)
      if trg_rows is not None and len(trg_rows) > 0:
        print('sha1 of target file exists. Check if a move is appropriate/possible.')
        trg_row = trg_rows[0]
        _ = self.sha1_exists_in_trg_try_move_within_target(src_dirnode, trg_row)
        continue
      bool_copied = self.copy_filepath(src_filepath, trg_filepath)
      if bool_copied:
        return trg_dirnode.insert_into_db(self.bak_dt)
      else:
        self.report_failed_copy(src_dirnode)

  def report_failed_copy(self, src_dirnode):
    pdict = {
      'file_id': src_dirnode.get_db_id(),
      'trg_mountpath': self.bak_dt.mountpath,
      'event_id': datetime.datetime.now()
    }
    _ = self.dbfailedcopyreporter.do_insert_or_update_with_dict_to_prep_tuplevalues(pdict)

  def verify_move_rename_thru_target_based_on_source(self):
    moverenamer = moverename.MoveRename(self.ori_dt.mountpath, self.bak_dt.mountpath)
    moverenamer.process()

  def do_copy_over(self, srcpath, src_dirnode, trgpath, trg_dirtree):
    """
    This method should only be called from delete_file() for its preparation happens there
    """
    folderpath, _ = os.path.split(trgpath)
    if not os.path.isdir(folderpath):
      try:
        os.makedirs(folderpath)
      except OSError:
        self.n_failed_copies += 1
        return False
    self.n_copied_files += 1
    print('N-copies', self.n_copied_files, 'rows', self.n_looped_rows, 'total', self.total_srcfiles_in_db)
    print('Copying file:', src_dirnode.name)
    print(' => ppath:', strf.put_ellipsis_in_str_middle(src_dirnode.parentpath, 120))
    print(' => direction:', self.ori_dt.mountpath, '=>', self.bak_dt.mountpath)
    shutil.copy2(srcpath, trgpath)
    sql = '''
      INSERT INTO %(tablename)s
        (name, parentpath, sha1, bytesize, mdatetime)
      VALUES 
        (?,?,?,?,?);'''
    tuplevalues = (
      src_dirnode.name,
      src_dirnode.parentpath,
      src_dirnode.sha1,
      src_dirnode.bytesize,
      src_dirnode.mdatetime
    )
    _ = trg_dirtree.do_insert_with_sql_n_tuplevalues(sql, tuplevalues)
    return True

  def copy_over(self, src_dirnode, src_dirtree, trg_dirtree):
    srcpath = src_dirnode.get_abspath_with_mountpath(src_dirtree.mountpath)
    if not os.path.isfile(srcpath):
      print(
        self.n_looped_rows, '/', self.total_srcfiles_in_db, 'file does not exist in os',
        strf.put_ellipsis_in_str_middle(srcpath, 50)
      )
      return False
    trgpath = src_dirnode.get_abspath_with_mountpath(trg_dirtree.mountpath)
    # if trgpath is None:
    #   return False
    if not os.path.isfile(trgpath):
      return self.do_copy_over(srcpath, src_dirnode, trgpath, trg_dirtree)
    # at this point, trgfile exists, so its sha1 must be checked before renaming trgfile
    trg_sha1 = hm.calc_sha1_from_file(trgpath)
    if trg_sha1 is None:
      # there is a problem read (TO-DO: one solution might be to treat it with another script)
      return False
    if trg_sha1 == src_dirnode.sha1:
      # target file is there though it's not in db
      return False
    # rename it and copy over, ie name is taken but sha1 is different
    trgpath = dirf.rename_filename_if_its_already_taken_in_folder(trgpath)
    return self.do_copy_over(srcpath, src_dirnode, trgpath, trg_dirtree)

  def copy_missing_files_to_trg(self, src_rows, src_dirtree, trg_dirtree):
    for src_row in src_rows:
      self.n_looped_rows += 1
      if self.restart_at and self.n_looped_rows < self.restart_at:
        print('processing', self.n_looped_rows, 'restart at', self.restart_at)
        continue
      src_dirnode = dn.DirNode.create_with_tuplerow(src_row, src_dirtree.fieldnames)
      srcfilepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mountpath)
      if dirf.is_any_dirname_in_path_startingwith_any_in_list(srcfilepath):
        print(
          self.n_looped_rows, '/', self.total_srcfiles_in_db,
          'file in FORBIDDEN path', srcfilepath
        )
        continue
      if trg_dirtree.does_sha1_exist_in_thisdirtree(src_dirnode):
        print(
          self.n_looped_rows, '/', self.total_srcfiles_in_db,
          'sha1_exist_in_thisdirtree for', srcfilepath
        )
        continue
      self.copy_over(src_dirnode, src_dirtree, trg_dirtree)

  def copy_onedirtree_to_another(self, src_dirtree, trg_dirtree):
    generated_rows = src_dirtree.do_select_all_w_limit_n_offset()
    for src_rows in generated_rows:
      self.copy_missing_files_to_trg(src_rows, src_dirtree, trg_dirtree)

  def process(self):
    self.copy_onedirtree_to_another(self.ori_dt, self.bak_dt)
    self.report()

  def report(self):
    print('=_+_+_='*3, 'CopyAcross Report', '=_+_+_='*3)
    print('Before the copying-across process:')
    print('total_srcfiles_in_db =', self.total_srcfiles_in_db, '| total_trgfiles_in_db =', self.total_trgfiles_in_db)
    print('total_unique_srcfiles =', self.total_unique_srcfiles,
          '| total_unique_trgfiles =', self.total_unique_trgfiles)
    print('total_of_repeat_srcfiles =', self.total_of_repeat_srcfiles,
          '| total_of_repeat_trgfiles =', self.total_of_repeat_trgfiles)
    print('n_files_processed =', self.n_files_processed, '| n_rows_deleted =', self.n_rows_deleted)
    print('n_looped_rows =', self.n_looped_rows, '| zzzzz =', 1)
    print('n_copied_files =', self.n_copied_files, '| n_moved_files =', self.n_moved_files)
    print('n_failed_copies =', self.n_failed_copies, '| n_deleted_files =', self.n_deleted_files)
    print('n_file_not_backable (.part, z-del etc) =', self.n_file_not_backable)
    end_time = datetime.datetime.now()
    elapsed_time = end_time - self.start_time
    print('After the copying-across process:')
    self.fetch_total_files_in_src_n_trg()
    self.fetch_total_unique_files_in_src_n_trg()
    print('total_srcfiles_in_db =', self.total_srcfiles_in_db, '| total_trgfiles_in_db =', self.total_trgfiles_in_db)
    print('total_unique_srcfiles =', self.total_unique_srcfiles,
          '| total_unique_trgfiles =', self.total_unique_trgfiles)
    print('total_of_repeat_srcfiles =', self.total_of_repeat_srcfiles,
          '| total_of_repeat_trgfiles =', self.total_of_repeat_trgfiles)
    print("Script's Runtime:", elapsed_time, '| today/now = ', datetime.datetime.now())
    print('=_+_+_='*3, 'End of the CopyAcross Report', '=_+_+_='*3)


def get_cli_arg_r1_r2_restart_at_if_any():
  r1, r2 = (None, None)
  for arg in sys.argv:
    if arg.startswith('-r1='):
      r1 = int(arg[len('-r1='):])
    elif arg.startswith('-r2='):
      r2 = int(arg[len('-r2='):])
    if (r1, r2) != (None, None):
      break
  return r1, r2


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  r1_restart_at, r2_restart_at = get_cli_arg_r1_r2_restart_at_if_any()
  if r2_restart_at is None:
    copier = DoubleDirectionCopier(src_mountpath, trg_mountpath, r1_restart_at)
    copier.process()
  copier = DoubleDirectionCopier(trg_mountpath, src_mountpath, r2_restart_at)
  copier.process()


if __name__ == '__main__':
  process()
