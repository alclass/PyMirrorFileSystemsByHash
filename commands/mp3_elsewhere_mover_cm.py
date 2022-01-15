#!/usr/bin/env python3
"""
mp3_elsewhere_mover_cm.py
This script moves mp3 files from a source dirtree to a target dirtree and then removes them from source.

This script's main application happens when mp3's are derived (extracted) from videos and kept in the same disk.
Because these mp3's are generated from videos, they do not need back-up and may reside in a separate disk.
  If this separate disk in any case get damaged or lost,
  the mp3's may be regenerated from the videos that themselves have back-up.
"""
import datetime
import os.path
import shutil
import models.entries.dirnode_mod as dn
import fs.db.dbdirtree_mod as dbdt
import default_settings as defaults


class Mp3ElsewhereMover:

  def __init__(self, ori_mountpath, bak_mountpath):
    self.start_time = datetime.datetime.now()
    self.ori_dt = dbdt.DBDirTree(ori_mountpath)
    self.bak_dt = dbdt.DBDirTree(bak_mountpath)
    self.n_files_processed = 0
    self.n_mp3s = 0
    self.n_copied_files = 0
    self.n_failed_copy = 0
    self.n_moved_files = 0
    self.n_deleted_files = 0
    self.delete_idlist = []
    self.n_file_not_backable = 0
    self.n_rows_deleted = 0
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    # self.fetch_total_files_in_src_n_trg()  # this call will be repeated for method report() at the end of processing
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    # self.fetch_total_unique_files_in_src_n_trg()  # idem

  @property
  def total_of_repeat_srcfiles(self):
    return self.total_srcfiles_in_db - self.total_unique_srcfiles

  def move_to_target_or_delete_if_its_already_there(self, src_dirnode):
    src_filepath = src_dirnode.get_abspath_with_mountpath(self.ori_dt.mountpath)
    trg_filepath = src_dirnode.get_abspath_with_mountpath(self.bak_dt.mountpath)
    if not os.path.isfile(src_filepath):
      # file not present, give up and return (continue)
      print(self.n_files_processed, 'file not present for move:', src_filepath)
      return
    if os.path.isfile(trg_filepath):
      # target file is already there, add source to delete_list (user may confirm deletion later on)
      print(self.n_files_processed, 'Adding mp3 to delete idlist', src_filepath)
      # TO-DO: consider having delete idlist in db if its size grows too large
      self.delete_idlist.append(src_dirnode.db_id)
      return
    trg_folder_abspath, _ = os.path.split(trg_filepath)
    if not os.path.isdir(trg_folder_abspath):
      os.makedirs(trg_folder_abspath)
    shutil.move(src_filepath, trg_filepath)
    self.n_moved_files += 1
    print(self.n_moved_files, 'Moved mp3 to target dirtree:', trg_filepath)

  def confirm_deletion_if_any(self):
    if len(self.delete_idlist) == 0:
      return
    print('-'*40)
    print('Confirm Deletion')
    print('-'*40)
    n_to_confirm = 0
    for i, file_to_delete_id in enumerate(self.delete_idlist):
      dirnode = dn.DirNode.fetch_dirnode_by_id_n_db(row_id=file_to_delete_id, dbtree=self.ori_dt)
      if dirnode is None:
        print(i+1, 'id', file_to_delete_id, 'was not found in db. Continuing for next.')
        continue
      n_to_confirm += 1
      print(i+1, n_to_confirm, dirnode)
    print('-'*40)
    screen_msg = 'Confirm deletion of the %d source files above (they exist in target)? (*Y/n) ' \
                 % len(self.delete_idlist)
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      for file_to_delete_id in self.delete_idlist:
        dirnode = dn.DirNode.fetch_dirnode_by_id_n_db(row_id=file_to_delete_id, dbtree=self.ori_dt)
        if dirnode is None:
          continue
        filetodeletepath = dirnode.get_abspath_with_mountpath(self.ori_dt)
        if os.path.isfile(filetodeletepath):
          os.remove(filetodeletepath)
          self.n_deleted_files += 1
          print(self.n_deleted_files, 'Deleted', filetodeletepath)

  def count_n_set_n_mp3s_from_db(self):
    self.n_mp3s = 0
    sql = 'SELECT count(*) FROM %(tablename)s WHERE SUBSTR(name, -4)=?;'
    tuplevalues = ('.mp3', )
    fetched_list = self.ori_dt.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) == 1:
      try:
        self.n_mp3s = int(fetched_list[0][0])
        return
      except ValueError:
        pass
    return

  def find_mp3s_thru_db(self):
    sql = 'SELECT * FROM %(tablename)s WHERE SUBSTR(name, -4)=? ORDER BY parentpath;'
    tuplevalues = ('.mp3', )
    generator_rows = self.ori_dt.do_select_sql_n_tuplevalues_w_limit_n_offset(sql, tuplevalues)
    for rows in generator_rows:
      for src_row in rows:
        self.n_files_processed += 1
        print(src_row)
        src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.fieldnames)
        self.move_to_target_or_delete_if_its_already_there(src_dirnode)

  def process(self):
    self.count_n_set_n_mp3s_from_db()
    self.find_mp3s_thru_db()
    self.confirm_deletion_if_any()
    self.report()

  def report(self):
    """
    self.n_files_processed = 0
    self.n_copied_files = 0
    self.n_failed_copies = 0
    self.n_moved_files = 0
    self.n_deleted_files = 0
    self.total_srcfiles_in_db = 0
    self.total_trgfiles_in_db = 0
    self.total_unique_srcfiles = 0
    self.total_unique_trgfiles = 0
    """
    print('   >>>>>>>>> Mp3ElsewhereMover Report: >>>>>>>>>')
    print('n_files_processed', self.n_files_processed)
    print('n_mp3s', self.n_mp3s)
    print('n_moved_files', self.n_moved_files)
    print('n_deleted_files', self.n_deleted_files)
    print('n_failed_copies', self.n_failed_copy)
    print('   >>>>>> End of Mp3ElsewhereMover Report: >>>>>>')


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  mover = Mp3ElsewhereMover(src_mountpath, trg_mountpath)
  mover.process()


if __name__ == '__main__':
  process()
