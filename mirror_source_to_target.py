#!/usr/bin/env python3
"""
  This script mirrors either copying or moving (or doing nothing) source fs to target fs.
  fs = file system
  There are TWO pre-requisites to run this script, ie:
    1) a fs-to-db checker should assure that files are in sync with their sha1hexes in db;
      in fact, db keeps triple filename, middlepath and sha1hex, all three should be in sync;
    2) a delete script may be necessary to run prior to this
      if disk space may be missing in target due to its having other non-source files;
      if thus, these non-source files, when deleted, will release space for possible copyings;
"""
import os
import time
import models.samodels as sam
import fs.db.sqlalchemy_conn as con
import fs.os.prep_fs_counts_mod as prep
import models.pathpositioning.metafilemod as metaf
import config


class MirrorDirTree:

  def __init__(self, src_mount_abspath, trg_mount_abspath, l1_dirs_to_avoid=None):
    self.src_mount_abspath = src_mount_abspath
    self.trg_mount_abspath = trg_mount_abspath
    self.l1_dirs_to_avoid = []
    self.treat_l1_dirs_to_avoid(l1_dirs_to_avoid)
    self.src_totalf = 0
    self.trg_totalf = 0
    self.total_moved = 0
    self.total_copied = 0
    self.total_already_in_pos = 0
    self.totalf_out_of_mirroring = 0
    self.files_with_error = 0
    self.src_session = None
    self.trg_session = None

  def treat_l1_dirs_to_avoid(self, l1_dirs_to_avoid=None):
    """
    l1_dirs_to_avoid is a list of level1 foldernames
      that should be ignored in mirroring.

    The default at the time of writing in config.py is:
      ['z Extra', 'z Tmp', 'z Triage', 'z-Extra', 'z-Tmp', 'z-Triage']

    Here follow the attempts in sequential order to set attribute l1_dirs_to_avoid:
      1) the first try is to cast incoming parameter to a list;
      2) if that fails, try to pick it up from config.py;
      3) if that still fails, set it as an empty list (ie [], no l1 folders to avoid);
    """
    if l1_dirs_to_avoid is not None:
      try:
        self.l1_dirs_to_avoid = list(l1_dirs_to_avoid)
        return
      except ValueError:
        pass
    try:
      self.l1_dirs_to_avoid = config.L1DIRS_TO_AVOID_IN_MIRRORING
      return
    except AttributeError:
      pass
    self.l1_dirs_to_avoid = []

  def mirror_source_to_target(self):
    """

    """
    self.src_totalf = prep.prepare_sweep_files_count(self.src_mount_abspath)
    self.src_session = con.get_session_from_sqlitefilepath(source=True)
    self.trg_session = con.get_session_from_sqlitefilepath(source=False)
    for abspath, dirs, files in os.walk(os.path.abspath(self.src_mount_abspath)):
      self.oswalk_mirror_source_folderfiles_iteration(abspath, files)

    self.src_session.close()
    self.trg_session.close()
    print('total_moved', self.total_moved)
    print('total_copied', self.total_copied)
    print('total_already_in_pos', self.total_already_in_pos)
    print('totalf_out_of_mirroring', self.totalf_out_of_mirroring)
    print('files_with_error', self.files_with_error)
    print('totalf', self.src_totalf)

  def oswalk_mirror_source_folderfiles_iteration(self, abspath, files):
    middlepath = prep.extract_middlepath_as_excess_abspath_on_mountpath(self.src_mount_abspath, abspath)
    print('=>->' * 10, len(files), 'files on middlepath', middlepath)
    for filename in sorted(files):
      self.mirror_source_file_entry(middlepath, filename)

  def mirror_source_file_entry(self, middlepath, filename):
    if middlepath in self.l1_dirs_to_avoid:
      self.totalf_out_of_mirroring += 1
    src_dbentry = self.src_session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.entryname == filename). \
        filter(sam.FSEntryInDB.middlepath == middlepath). \
        first()
    sha1hex = src_dbentry.sha1hex
    trg_dbentry = self.trg_session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.sha1hex == sha1hex). \
        first()
    if trg_dbentry is None:
      self.copy_file_from_src_to_trg(src_dbentry)
    if trg_dbentry.entryname == filename and trg_dbentry.middlepath == middlepath:
      # file is already mirrored, move on
      self.total_already_in_pos += 1
      return
    # target file exists but it's necessary to move it to source's middlepath relative position
    self.move_target_file_to_same_middlepath_as_srcs(src_dbentry, trg_dbentry)

  def copy_file_from_src_to_trg(self, src_dbentry):
    """
    MetaFile class can copy file over to target
    """

    src_middlepath = src_dbentry.middlepath
    src_filename = src_dbentry.entryname
    src_mfile = metaf.MetaFile(self.src_mount_abspath, src_middlepath, src_filename)
    bool_res = src_mfile.copy_to_target_fs(self.trg_mount_abspath)
    if bool_res:
      # insert it in target db
      trg_dbentry = sam.FSEntryInDB()
      trg_dbentry.middlepath = src_middlepath
      trg_dbentry.entryname = src_filename
      self.trg_session.add(trg_dbentry)
      self.trg_session.commit()
      self.total_copied += 1

  def move_target_file_to_same_middlepath_as_srcs(self, src_dbentry, trg_dbentry):
    """
    MetaFile class can move target file to its middlepath
    Obs:
      1) ideally, this process is run after no duplicates remain in target;
      2) however, it's still adviceable to check whether a duplicate
       is not already in the target folder
    """
    src_middlepath = src_dbentry.middlepath
    src_filename = src_dbentry.entryname
    src_mfile = metaf.MetaFile(self.src_mount_abspath, src_middlepath, src_filename)
    trg_middlepath = trg_dbentry.middlepath
    trg_filename = trg_dbentry.entryname
    trg_mfile = metaf.MetaFile(self.trg_mount_abspath, trg_middlepath, trg_filename)
    bool_res = trg_mfile.move_file_within_its_fs_to_relative_middepath_of_another_metafile(
      src_mfile
    )
    if bool_res:
      # move it also in db
      trg_dbentry.entryname = trg_filename
      trg_dbentry.middlepath = trg_middlepath
      self.trg_session.commit()
      self.total_moved += 1


def process_sweep_src_uptree():
  """
  mount_abspath = config.get_datatree_mountpoint_abspath(source=False)
  make_updir_sweep(mount_abspath)

  """
  start_time = time.time()
  print('[START PROCESSING TIME] sweep_src_uptree at', start_time)
  src_mount_abspath = config.get_datatree_mountpoint_abspath(source=True)
  trg_mount_abspath = config.get_datatree_mountpoint_abspath(source=False)
  mirrorer = MirrorDirTree(src_mount_abspath, trg_mount_abspath)
  mirrorer.mirror_source_to_target()
  elapsed_time = time.time() - start_time
  print('[END PROCESSING TIME] elapsed_time at', elapsed_time)


def process():
  # sweep_src_n_trg()
  start_time = time.time()
  process_sweep_src_uptree()
  elapsed_time = time.time() - start_time
  print('start_time', start_time)
  print('elapsed_time', elapsed_time)


if __name__ == '__main__':
  process()
