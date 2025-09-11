#!/usr/bin/env python3
"""
This is a one-use script.
It's intended to move txt or url files from one dirtree to another.
  In the case here, when epub files were moved previously,
  these txt or url files were left behind in their original (previous) dirtree.
  This script then is an opportunity for them to join the epubs back together again.

Obs: the scripts shows a list and asks the user for confirmation whether or not to move files.
"""
import os
import shutil


class AdhocFileMover:

  def __init__(self, src_abspath, trg_abspath):
    """
    trg_abspath is also the relative-paths guide, ie its structure will guide look-up files in source
    """
    self.src_abspath = src_abspath
    self.trg_abspath = trg_abspath
    self.current_abspath = None
    self.n_dirs_processed = 0
    self.entries_abspaths_to_move = []
    self.n_filemoves = 0
    self.n_failed_filemoves = 0
    self.n_not_moved_by_pres_or_abs = 0

  @property
  def relativepath(self):
    relpath = self.current_abspath[len(self.trg_abspath):]
    relpath = relpath.lstrip('/')  # in case it has a leading forward slash (/)
    return relpath

  def get_trg_counterpart(self, movefilepath):
    post_base_abspath = movefilepath[len(self.src_abspath):]
    post_base_abspath = post_base_abspath.lstrip('/')
    return os.path.join(self.trg_abspath, post_base_abspath)

  def lookup_folder_n_accumulate_movefiles_if_any(self):
    supposed_scrpath = os.path.join(self.src_abspath, self.relativepath)
    if not os.path.isdir(supposed_scrpath):
      return False
    scrpath = supposed_scrpath
    entries = os.listdir(scrpath)
    entries = filter(lambda fn: fn.endswith('.url') or fn.endswith('.txt'), entries)
    entries_abspaths = map(lambda fn: os.path.join(scrpath, fn), entries)
    self.entries_abspaths_to_move += entries_abspaths

  def traverse_guided_by_trg_dirtree(self):
    self.n_dirs_processed = 0
    for self.current_abspath, _, _ in os.walk(self.trg_abspath):
      if self.current_abspath == self.trg_abspath:
        # jump over root folder
        continue
      self.n_dirs_processed += 1
      print(self.n_dirs_processed, 'look up', self.current_abspath)
      self.lookup_folder_n_accumulate_movefiles_if_any()

  def show_filemoves(self):
    total_to_move = len(self.entries_abspaths_to_move)
    seq = 0
    print('Report:')
    print('=======')
    for movefilepath in self.entries_abspaths_to_move:
      seq += 1
      print(seq, '/', total_to_move)
      print('FROM: ', movefilepath)
      print('TO:   ', self.get_trg_counterpart(movefilepath))

  def confirm_filemoves(self):
    total_to_move = len(self.entries_abspaths_to_move)
    if total_to_move == 0:
      print('No files to move.')
      return False
    screen_msg = 'Confirm the above %d file moves? (*Y/n) [ENTER] means Yes ' % total_to_move
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def move_files(self):
    seq = 0
    print('Moving:')
    print('=======')
    total_to_move = len(self.entries_abspaths_to_move)
    for src_filepath in self.entries_abspaths_to_move:
      seq += 1
      print(seq, '/', total_to_move)
      print('FROM: ', src_filepath)
      trg_filepath = self.get_trg_counterpart(src_filepath)
      print('TO:   ', trg_filepath)
      if not os.path.isfile(src_filepath):
        self.n_not_moved_by_pres_or_abs += 1
        print(self.n_not_moved_by_pres_or_abs,  '/', total_to_move, 'Source does not exist. Continuing.')
        continue
      if os.path.isfile(trg_filepath):
        self.n_not_moved_by_pres_or_abs += 1
        print(self.n_not_moved_by_pres_or_abs,  '/', total_to_move, 'Target already exists. Continuing.')
        continue
      try:
        shutil.move(src_filepath, trg_filepath)
        self.n_filemoves += 1
        print('Moved', seq, '/', self.n_filemoves, '/', total_to_move)
      except (IOError, OSError):
        self.n_failed_filemoves += 1

  def process(self):
    self.traverse_guided_by_trg_dirtree()
    self.show_filemoves()
    if self.confirm_filemoves():
      self.move_files()
    self.report()

  def report(self):
    print('n_dirs_processed', self.n_dirs_processed)
    print('n of gathered file moves', len(self.entries_abspaths_to_move))
    print('n_filemoves', self.n_filemoves)
    print('n_failed_filemoves', self.n_failed_filemoves)
    print('n_not_moved_by_pres_or_abs', self.n_not_moved_by_pres_or_abs)


SRC_PATH = '/home/dados/Books/Books (mostly epubs)'  # from where the txt|url files will be looked up
TRG_PATH = '/home/dados/Books/Books epub'  # this path is also the relative-paths guide


def process():
  """
  """
  filemover = AdhocFileMover(SRC_PATH, TRG_PATH)
  filemover.process()


if __name__ == '__main__':
  process()
