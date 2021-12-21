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
import fs.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults


class TargetSameSha1ForceDeleter:
  """
  This class implements the deletion of all repeat-sha1's in target that exist in source.
  The files to be deleted are gathered first and will only be deleted with the user's confirmation.
  IMPORTANT: if confirmed, the deletion operation here cannot be undone.
  """

  def __init__(self, ori_mountpath, bak_mountpath):
    self.deletion_confirmed = False
    self.n_processed_trg_delete = 0
    self.trg_delete_ids = []
    self.ori_dbtree = dbdt.DBDirTree(ori_mountpath)
    self.bak_dbtree = dbdt.DBDirTree(bak_mountpath)
    self.total_files_in_src = self.ori_dbtree.count_rows_as_int()
    self.total_files_in_trg = self.bak_dbtree.count_rows_as_int()

  def register_all_trgfiles_with_specific_sha1_for_later_deletion(self, src_sha1):
    sql = 'SELECT id FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (src_sha1, )
    fetched_rows = self.bak_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    for row in fetched_rows:
      _id = row[0]
      self.trg_delete_ids.append(_id)

  def does_sha1_exist_in_src(self, trg_sha1):
    sql = 'SELECT id FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (trg_sha1, )
    fetched_rows = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(fetched_rows) > 0:
      return True
    return False

  def lookup_sha1s_in_trg_that_exist_in_src(self, trg_rows):
    for trg_row in trg_rows:
      _id = trg_row[0]
      if _id in self.trg_delete_ids:
        continue
      idx = self.bak_dbtree.fieldnames.index('sha1')
      trg_sha1 = trg_row[idx]
      if self.does_sha1_exist_in_src(trg_sha1):
        src_sha1 = trg_sha1
        self.register_all_trgfiles_with_specific_sha1_for_later_deletion(src_sha1)

  def loop_thru_targetdirtree_db_entries(self):
    generated_rows = self.bak_dbtree.do_select_all_w_limit_n_offset()
    for trg_rows in generated_rows:
      self.lookup_sha1s_in_trg_that_exist_in_src(trg_rows)

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
    self.n_processed_trg_delete = 0
    for _id in self.trg_delete_ids:
      row = self.bak_dbtree.fetch_node_by_id(_id)
      dirnode = dn.DirNode.create_with_tuplerow(row)
      print(_id, dirnode.name)
      fpath = dirnode.get_abspath_with_mountpath(self.bak_dbtree.mountpath)
      print(fpath)

  def print_out_all_files_to_delete(self):
    for _id in self.trg_delete_ids:
      row = self.bak_dbtree.fetch_node_by_id(_id)
      dirnode = dn.DirNode.create_with_tuplerow(row)
      print(_id, dirnode.name)
      fpath = dirnode.get_abspath_with_mountpath(self.bak_dbtree.mountpath)
      print(fpath)

  def confirm_deletion(self):
    print('Confirm deletion: ids:')
    print('='*40)
    n_to_delete = 0
    for i, _id in enumerate(self.trg_delete_ids):
      n_to_delete += 1
      print(n_to_delete, _id)
    screen_msg = 'Confirm the deletion of the %d ids above? (*Y/n) ' % len(self.trg_delete_ids)
    ans = input(screen_msg)
    self.deletion_confirmed = False
    if ans in ['Y', 'y', '']:
      self.deletion_confirmed = True

  def report(self):
    print('Report:')
    print('=======')
    print('dirtrees:', self.ori_dbtree.mountpath, self.bak_dbtree.mountpath)
    print('len(delete_ids)', len(self.trg_delete_ids))
    print('n_processed_delete', self.n_processed_trg_delete)
    print('total_files_in_src', self.total_files_in_src)
    print('total_files_in_trg', self.total_files_in_trg)

  def process(self):
    self.loop_thru_targetdirtree_db_entries()
    self.print_out_all_files_to_delete()
    self.confirm_deletion()
    self.do_batch_deletion_if_confirmed()
    self.report()


def process():
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  forcedeleter = TargetSameSha1ForceDeleter(src_mountpath, trg_mountpath)
  forcedeleter.process()


if __name__ == '__main__':
  process()
