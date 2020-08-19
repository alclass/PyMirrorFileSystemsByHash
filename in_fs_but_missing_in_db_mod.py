#!/usr/bin/env python3
"""

"""
import os
import time
import models.samodels as sam
import fs.db.sqlalchemy_conn as con
import fs.os.prep_fs_counts_mod as prep
import models.pathpositioning.metafilemod as metaf
import config


def clean_up_folderleaftovers_in_db(session, middlepath, files):
  n_deletes = 0
  dbentries = session.query(sam.FSEntryInDB).\
      filter(sam.FSEntryInDB.middlepath == middlepath).\
      all()
  for dbentry in dbentries:
    if dbentry.entryname not in files:
      n_deletes += 1
      linemsg = '%d => ((Deleting)) filename=[%s] | middlename=[%s]' % (n_deletes, dbentry.entryname, middlepath)
      print(linemsg)
      session.delete(dbentry)
  return n_deletes


class FileSweeper:

  def __init__(self, mount_abspath):
    self.mount_abspath = mount_abspath
    self.totalf = 0
    self.totalf_swept = 0
    self.n_exists = 0
    self.n_not_exists = 0
    self.files_with_error = 0
    self.session = None

  def perfom_updir_sweep(self):
    """

    """
    self.totalf = prep.prepare_sweep_files_count(self.mount_abspath)
    self.session = con.get_session_from_sqlitefilepath(source=True)
    for abspath, dirs, files in os.walk(os.path.abspath(self.mount_abspath)):
      self.process_oswalk_abspath_iteration(abspath, files)

    print('n_exists', self.n_exists)
    print('n_not_exists', self.n_not_exists)
    print('files_with_error', self.files_with_error)
    print('totalf_swept', self.totalf_swept)
    print('totalf', self.totalf)
    self.session.close()

  def process_oswalk_abspath_iteration(self, abspath, files):
    middlepath = prep.extract_middlepath_from_abspath(self.mount_abspath, abspath)
    # print('=>->' * 10, len(files), 'files on middlepath', middlepath)
    for filename in sorted(files):
      self.process_file_entry(middlepath, filename)
      # self.n_deletes += clean_up_folderleaftovers_in_db(self.session, middlepath, files)

  def process_file_entry(self, middlepath, filename):
    if filename == config.SQLITE_UPDIRENTRIES_DEFAULT_FILENAME:
      return
    self.totalf_swept += 1
    linemsg = prep.form_fil_in_mid_with_progress_percent_line(
      self.totalf_swept, self.totalf, filename, middlepath
    )
    # print(linemsg)
    dbentry = self.session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.entryname == filename). \
        filter(sam.FSEntryInDB.middlepath == middlepath). \
        first()
    if dbentry is not None:
      self.n_exists += 1
      return
    self.n_not_exists += 1
    # from here dbsearch will be made on sha1hex
    linemsg = '%d | %d | %s/%s' %\
              (self.totalf_swept, self.n_not_exists, middlepath, filename)
    print(linemsg)


def process_sweep_src_uptree():
  """
  mount_abspath = config.get_datatree_mountpoint_abspath(source=False)
  make_updir_sweep(mount_abspath)

  """
  start_time = time.time()
  print('[START PROCESSING TIME] sweep_src_uptree at', start_time)
  mount_abspath = config.get_datatree_mountpoint_abspath(source=True)
  sweep = FileSweeper(mount_abspath)
  sweep.perfom_updir_sweep()
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
