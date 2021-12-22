#!/usr/bin/env python3
"""
force_delete_repeatfiles_lookingup_targetdirtree_mod.py

This script takes two folder paths (source and target) and deletes file copies (repeats (*))
  in the target dirtree.

(*) repeats are based on the sha1-hash of its content

Notice the main class here needs 4 parameters to do its job. This is an important different from
  script:
    force_delete_every_equal_sha1_in_targetdirtree_mod
  which only needs 2 parameters, ie src_dirtree (or src_mountpath) and trg_dirtree (or trg_mountpath)

IMPORTANT:
  1) all file deletions are always somehow dangerous but this script only deletes copies
     (ie the original copy is not to be deleted in this script);
  2) once a file is considered "source" its db-id prevents it from being deleted itself;

/media/friend/D1 4T Bak B CS EEE M Sc So
/media/friend/D1 4T Bak B CS EEE M Sc So/
Soc Sci vi/Philosophy vi/Individual Philosophers vi/Aa Lang Individual Philosophers vi/
Philosophie yu (von Radio) ytpls/Ant 7-v 27' 2014 Philosophie der Antike yu Philosophie ytpl
"""
import os.path

import fs.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults


def prume_dirtree_deleting_empty_folders(base_dirpath):
  entries = os.listdir(base_dirpath)
  for e in entries:
    abspath = os.path.join(base_dirpath, e)
    if os.path.isfile(abspath):
      continue
    if os.path.isdir(abspath):
      prume_dirtree_deleting_empty_folders(abspath)
  entries = os.listdir(base_dirpath)
  if len(entries) == 0:
    print('Folder-deleting', base_dirpath)
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


def put_ellipsis_in_str_middle(line, linecharsize=80):
  if line is None:
    return ''
  if len(line) <= linecharsize:
    return line
  sizediff = len(line) - linecharsize
  half_sizediff = sizediff // 2
  midpos = len(line) // 2
  p1_midpos = midpos - half_sizediff
  p2_midpos = midpos + half_sizediff
  p1 = line[: p1_midpos]
  p2 = line[p2_midpos:]
  newline = p1 + '...' + p2
  return newline


class TargetSameSha1ForceDeleter:
  """
  This class implements the deletion of all repeat-sha1's in target that exist in source.
  The files to be deleted are gathered first and will only be deleted with the user's confirmation.
  IMPORTANT: if confirmed, the deletion operation here cannot be undone.
  """

  def __init__(self, ori_mountpath, bak_mountpath):
    self.deletion_confirmed = False
    self.n_processed_trg_deletes = 0
    self.n_processed_entries = 0
    self.n_processed_deletes = 0
    self.trg_delete_ids = []
    self.ori_dbtree = dbdt.DBDirTree(ori_mountpath)
    self.bak_dbtree = dbdt.DBDirTree(bak_mountpath)
    self.src_total_files_in_db = 0
    self.trg_total_files_in_db = 0
    self.src_total_files = 0
    self.src_total_dirs = 0
    self.trg_total_files = 0
    self.trg_total_dirs = 0
    self.calc_totals()

  def count_files_in_db(self):
    self.src_total_files_in_db = self.ori_dbtree.count_rows_as_int()
    self.trg_total_files_in_db = self.bak_dbtree.count_rows_as_int()

  def count_files_in_dirtrees(self):
    self.src_total_files = 0
    self.src_total_dirs = 0
    print('Counting src_total_files and folders. Please wait.')
    self.src_total_files, self.src_total_dirs = count_total_files_n_folders(self.ori_dbtree.mountpath)
    self.trg_total_files, self.trg_total_dirs = count_total_files_n_folders(self.bak_dbtree.mountpath)

  def calc_totals(self):
    self.count_files_in_db()
    self.count_files_in_dirtrees()

  def register_all_trgfiles_with_specific_sha1_for_later_deletion(self, src_sha1):
    sql = 'SELECT id FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (src_sha1, )
    fetched_rows = self.bak_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    n_added_dels = 0
    for row in fetched_rows:
      n_added_dels += 1
      _id = row[0]
      self.trg_delete_ids.append(_id)
    sha1hex = src_sha1.hex()
    print(
      self.n_processed_entries, 'of', self.trg_total_files_in_db,
      sha1hex[:10], '| delete total:',
      len(self.trg_delete_ids), '| qty with sha1:', n_added_dels
    )

  def does_sha1_exist_in_src(self, trg_sha1):
    sql = 'SELECT id FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (trg_sha1, )
    fetched_rows = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(fetched_rows) > 0:
      return True
    return False

  def lookup_sha1s_in_trg_that_exist_in_src(self, trg_rows):
    for trg_row in trg_rows:
      self.n_processed_entries += 1
      _id = trg_row[0]
      if _id in self.trg_delete_ids:
        continue
      idx = self.bak_dbtree.fieldnames.index('sha1')
      trg_sha1 = trg_row[idx]
      if self.n_processed_entries % 20 == 0:
        dirnode = dn.DirNode.create_with_tuplerow(trg_row, self.bak_dbtree.fieldnames)
        sha1hex = trg_sha1.hex()
        print(
          self.n_processed_entries, 'of', self.trg_total_files_in_db,
          sha1hex[:10], dirnode.name
        )
        ppath = put_ellipsis_in_str_middle(dirnode.get_abspath_with_mountpath(self.bak_dbtree.mountpath))
        print(ppath)
      if self.does_sha1_exist_in_src(trg_sha1):
        src_sha1 = trg_sha1
        self.register_all_trgfiles_with_specific_sha1_for_later_deletion(src_sha1)

  def loop_thru_targetdirtree_db_entries(self):
    generated_rows = self.bak_dbtree.do_select_all_w_limit_n_offset()
    for trg_rows in generated_rows:
      self.lookup_sha1s_in_trg_that_exist_in_src(trg_rows)

  def delete_entry_in_os_n_in_db(self, _id):
    """
    IMPORTANT: only method do_batch_deletion_if_confirmed() can call this one,
      for it checks the user's confirmation before calling here.
    """
    if not self.deletion_confirmed:
      error_msg = 'Program Error: erroneous logical call for delete_entry_in_os_n_in_db()' \
                  ' when delete depends on confirmation'
      raise ValueError(error_msg)
    fetched_list = self.bak_dbtree.fetch_node_by_id(_id)
    if fetched_list is None or len(fetched_list) == 0:
      return
    row = fetched_list[0]
    dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dbtree.fieldnames)
    fpath = dirnode.get_abspath_with_mountpath(self.bak_dbtree.mountpath)
    if os.path.isfile(fpath):
      trg_total_to_del = len(self.trg_delete_ids)
      self.n_processed_trg_deletes += 1
      print(self.n_processed_trg_deletes, '/', trg_total_to_del, ' >>> DELETING', _id, dirnode.name)
      print(fpath)
      os.remove(fpath)
      self.bak_dbtree.delete_row_by_id(_id)

  def do_batch_deletion_if_confirmed(self):
    """
      if os.path.isfile(fpath):
        os.remove(fpath)
        self.bak_dbtree.delete_row_by_id(_id)
        self.n_deletes += 1
        print(self.n_deletes, 'deleted', dirnode.name)
        print(fpath)
    """
    if not self.deletion_confirmed:
      return 0
    for _id in self.trg_delete_ids:
      self.delete_entry_in_os_n_in_db(_id)

  def print_out_all_files_to_delete(self):
    for _id in self.trg_delete_ids:
      fetched_rows = self.bak_dbtree.fetch_node_by_id(_id)
      if fetched_rows is None or len(fetched_rows) == 0:
        print('id', _id, 'is empty. Continuing')
        continue
      row = fetched_rows[0]
      dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dbtree.fieldnames)
      print(_id, '/', self.trg_total_files, dirnode.name)
      fpath = dirnode.get_abspath_with_mountpath(self.bak_dbtree.mountpath)
      ppath = put_ellipsis_in_str_middle(fpath)
      print(ppath)

  def confirm_deletion(self):
    self.deletion_confirmed = False
    if len(self.trg_delete_ids) == 0:
      print('Empty list. No trg deletes to confirm.')
      print('=' * 40)
      return
    print('Confirm deletion: ids:')
    print('='*40)
    for i, _id in enumerate(self.trg_delete_ids):
      fetched_rows = self.bak_dbtree.fetch_node_by_id(_id)
      if fetched_rows is None or len(fetched_rows) == 0:
        continue
      row = fetched_rows[0]
      dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dbtree.fieldnames)
      print(i+1, dirnode.name)
      ppath = put_ellipsis_in_str_middle(dirnode.parentpath)
      print(ppath)
    screen_msg = 'Confirm the deletion of the %d ids above? (*Y/n) ' % len(self.trg_delete_ids)
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      self.deletion_confirmed = True

  def remove_empty_folders_in_trg(self):
    prume_dirtree_deleting_empty_folders(self.bak_dbtree.mountpath)

  def report(self):
    print('Report:')
    print('=======')
    print('dirtrees:', self.ori_dbtree.mountpath, self.bak_dbtree.mountpath)
    print('len(delete_ids)', len(self.trg_delete_ids))
    print('n_processed_delete', self.n_processed_trg_deletes)
    print('src_total_files', self.src_total_files)
    print('src_total_dirs', self.src_total_dirs)
    print('trg_total_files', self.trg_total_files)
    print('trg_total_dirs', self.trg_total_dirs)
    self.calc_totals()
    print('After process:')
    print('='*40)
    print('src_total_files_in_db', self.src_total_files_in_db)
    print('trg_total_files_in_db', self.trg_total_files_in_db)
    print('src_total_files', self.src_total_files)
    print('src_total_dirs', self.src_total_dirs)
    print('trg_total_files', self.trg_total_files)
    print('trg_total_dirs', self.trg_total_dirs)

  def process(self):
    self.loop_thru_targetdirtree_db_entries()
    self.print_out_all_files_to_delete()
    self.confirm_deletion()
    self.do_batch_deletion_if_confirmed()
    self.remove_empty_folders_in_trg()
    self.report()


def process():
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  forcedeleter = TargetSameSha1ForceDeleter(src_mountpath, trg_mountpath)
  forcedeleter.process()


if __name__ == '__main__':
  process()
