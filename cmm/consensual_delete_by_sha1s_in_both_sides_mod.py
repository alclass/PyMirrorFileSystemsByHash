#!/usr/bin/env python3
"""
consensual_delete_by_sha1s_in_both_sides_mod.py

This script does the following:
  if a sha1 (ie a file under a certain sha1) exists in the same relative position (name and parentpath)
  and in at least one side it's unique, delete excess files (ie repeats in the other side)
"""
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dirn
import default_settings as defaults
import lib.dirfilefs.dir_n_file_fs_mod as dirf
import lib.strnlistfs.strfunctions_mod as strf


class ConsensualBothSidesSha1Deleter:

  def __init__(self, src_mountpath, trg_mountpath):
    self.ori_dt = dt.DirTree('ori', src_mountpath)
    self.bak_dt = dt.DirTree('bak', trg_mountpath)
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    self.total_srcfiles_in_os = 0
    self.total_trgfiles_in_os = 0
    self.total_srcdirs_in_os = 0
    self.total_trgdirs_in_os = 0
    self.n_src_processed_files = 0
    self.n_processed_sha1 = 0
    self.n_deletes = 0
    self.n_copied_files = 0
    self.n_moved_files = 0
    self.n_move_not_needed = 0
    self.n_failed_moves = 0
    self.n_failed_copies = 0
    self.n_empty_dirs_removed = 0
    self.n_empty_dirs_fail_rm = 0
    self.calc_totals()

  def calc_totals(self):
    print('Counting files and dirs in db and os. Please wait.')
    self.total_unique_srcfiles = self.ori_dt.dbtree.count_unique_sha1s_as_int()
    self.total_unique_trgfiles = self.bak_dt.dbtree.count_unique_sha1s_as_int()
    self.total_srcfiles_in_db = self.ori_dt.dbtree.count_rows_as_int()
    self.total_trgfiles_in_db = self.bak_dt.dbtree.count_rows_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.ori_dt.mountpath)
    self.total_srcfiles_in_os = total_files
    self.total_srcdirs_in_os = total_dirs
    total_files, total_dirs = dirf.count_total_files_n_folders_excl_root(self.bak_dt.mountpath)
    self.total_trgfiles_in_os = total_files
    self.total_trgdirs_in_os = total_dirs

  def delete_dirnodes_in_dirtree(self, dirnodes_to_del, dbtree):
    if len(dirnodes_to_del) == 0:
      print('Nothing to delete in delete_dirnodes_in_dirtree()')
    for dirnode_to_del in dirnodes_to_del:
      self.n_deletes += 1
      print(
        self.n_deletes, '/', self.total_trgfiles_in_os,
        'deleting', dirnode_to_del.name, '@', strf.put_ellipsis_in_str_middle(dirnode_to_del.parentpath, 50)
      )
      print(' ********** DELETE', dirnode_to_del.get_db_id(), dirnode_to_del.name, '@', dirnode_to_del.parentpath)
      # _ = dbtree.delete_row_by_id(dirnode_to_del.get_db_id())
      _ = dbtree
    return True

  def delete_excess_in_dirtree_by_sha1(self, dirnode, is_src=True):
    if is_src:
      dbtree = self.ori_dt.dbtree
    else:
      dbtree = self.bak_dt.dbtree
    sql = 'select * from %(tablename)s where sha1=?;'
    tuplevalues = (dirnode.sha1,)
    fetched_list = dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list is None or len(fetched_list) == 0:
      print('Nothing to delete in opposite dirtree from', dirnode.name, dirnode.get_db_id())
      return False
    trg_dirnodes_to_remain_one_tupl = []
    _id_to_remain = None
    for row in fetched_list:
      trg_dirnode = dirn.DirNode.create_with_tuplerow(row, dbtree.fieldnames)
      _id = row[0]
      trg_dirnodes_to_remain_one_tupl.append((_id, trg_dirnode))
      if trg_dirnode.name == dirnode.name and trg_dirnode.parentpath == dirnode.parentpath:
        _id_to_remain = _id
    if _id_to_remain is None:
      # pick the first one
      row = fetched_list[0]
      _id_to_remain = row[0]
    idx_to_remove_from_dels = None
    for i, tupl in enumerate(trg_dirnodes_to_remain_one_tupl):
      if tupl[0] == _id_to_remain:
        idx_to_remove_from_dels = i
    if idx_to_remove_from_dels is None:
      return False
    del trg_dirnodes_to_remain_one_tupl[idx_to_remove_from_dels]
    # list comprehension to transpose dirnodes from tuple list to list
    trg_dirnodes_to_del = [tupl[1] for tupl in trg_dirnodes_to_remain_one_tupl]
    print_ids = [dn.get_db_id() for dn in trg_dirnodes_to_del]
    print(
      self.n_processed_sha1, '/', self.total_unique_srcfiles,
      'going to delete ids', print_ids
    )
    return self.delete_dirnodes_in_dirtree(trg_dirnodes_to_del, dbtree)

  def treat_src_unique_sha1(self, sha1):
    sql = 'select * from %(tablename)s where sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = self.ori_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) == 1:
      row = fetched_list[0]
      src_dirnode = dirn.DirNode.create_with_tuplerow(row, self.ori_dt.dbtree.fieldnames)
      self.n_processed_sha1 += 1
      print(
        self.n_processed_sha1, '/', self.total_unique_srcfiles,
        'treating unique sha1', sha1.hex()[:10],
        src_dirnode.name, '@', src_dirnode.parentpath
      )
      return self.delete_excess_in_dirtree_by_sha1(src_dirnode, is_src=False)
    # this method considers that by sha1 only one row is fetched (unique sha1 in db)
    return False

  def treat_sha1s_with_repeats_in_scr(self, sha1):
    """
    if program flow got up to here, srcfile exist.
    """
    sql = 'select * from %(tablename)s where sha1=?;'
    tuplevalues = (sha1,)
    fetched_list = self.bak_dt.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) == 1:
      row = fetched_list[0]
      trg_dirnode = dirn.DirNode.create_with_tuplerow(row, self.ori_dt.dbtree.fieldnames)
      return self.delete_excess_in_dirtree_by_sha1(trg_dirnode, is_src=True)
    # inconclusive, ie there are repeats in both sides (ori and bak)
    print(' >>>>>>>>>>> inconclusive, ie there are repeats in both sides (ori and bak)')
    return False

  def fetch_n_process_sha1s_in_scr(self):
    sql = 'select DISTINCT sha1, count(sha1) as c from %(tablename)s group by sha1;'  # group by sha1 having c = 1
    fetched_list = self.ori_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    for shorter_row in fetched_list:
      sha1 = shorter_row[0]
      qtd = shorter_row[1]
      print('qtd', qtd, 'sha1', sha1.hex())
      if qtd == 1:
        self.treat_src_unique_sha1(sha1)
      else:
        self.treat_sha1s_with_repeats_in_scr(sha1)

  def process(self):
    self.fetch_n_process_sha1s_in_scr()
    self.prune_empty_folders()
    self.report()

  def prune_empty_folders(self):
    """
    The method calls a function to erase empty directories (it is automatic, no confirmation is asked).
    """
    n_visited, n_removed, n_failed = dirf.prune_dirtree_deleting_empty_folders(self.bak_dt.mountpath)
    self.n_empty_dirs_removed = n_removed
    self.n_empty_dirs_fail_rm = n_failed

  def print_counters(self):
    print('total_srcfiles_in_db:', self.total_srcfiles_in_db)
    print('total_trgfiles_in_db:', self.total_trgfiles_in_db)
    print('total_srcfiles_in_os:', self.total_srcfiles_in_os)
    print('total_srcdirs_in_os:', self.total_srcdirs_in_os)
    print('total_trgfiles_in_os:', self.total_trgfiles_in_os)
    print('total_trgdirs_in_os:', self.total_trgdirs_in_os)
    print('total_unique_srcfiles:', self.total_unique_srcfiles)
    print('total_unique_trgfiles:', self.total_unique_trgfiles)

  def report(self):
    print('=_+_+_='*3, 'ConsensualBothSidesSha1Deleter Report', '=_+_+_='*3)
    self.print_counters()
    print('ori', self.ori_dt.mountpath)
    print('bak', self.bak_dt.mountpath)
    print('n_src_processed_files:', self.n_src_processed_files)
    print('n_processed_sha1:', self.n_processed_sha1)
    print('n_deletes:', self.n_deletes)
    print('n_failed_deletes:', 0, 'not yet implemented')
    print('n_empty_dirs_removed:', self.n_empty_dirs_removed)
    print('n_empty_dirs_fail_rm:', self.n_empty_dirs_fail_rm)


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  deleter = ConsensualBothSidesSha1Deleter(src_mountpath, trg_mountpath)
  deleter.process()


if __name__ == '__main__':
  process()
