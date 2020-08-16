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
    self.n_inserts = 0
    self.n_updates = 0
    self.n_deletes = 0
    self.n_exists = 0
    self.files_with_error = 0
    self.commit_rotate_count = 0
    self.abspath = None
    self.middlepath = None
    self.filename = None
    self.session = None

  def perfom_updir_sweep(self):
    """

    """
    self.totalf = prep.prepare_sweep_files_count(self.mount_abspath)
    self.session = con.get_session_from_sqlitefilepath(source=True)
    for self.abspath, dirs, files in os.walk(os.path.abspath(self.mount_abspath)):
      self.process_oswalk_abspath_iteration(files)

    _ = prep.commit_on_counter_rotate(self.session, self.commit_rotate_count)
    self.session.close()
    print('n_inserts', self.n_inserts)
    print('n_updates', self.n_updates)
    print('n_deletes', self.n_deletes)
    print('n_exists', self.n_exists)
    print('files_with_error', self.files_with_error)
    print('totalf_swept', self.totalf_swept)
    print('totalf', self.totalf)

  def process_oswalk_abspath_iteration(self, files):
    self.middlepath = prep.extract_middlepath_from_abspath(self.mount_abspath, self.abspath)
    print('=>->' * 10, len(files), 'files on middlepath', self.middlepath)
    for self.filename in sorted(files):
      self.process_file_entry()
      self.n_deletes += clean_up_folderleaftovers_in_db(self.session, self.middlepath, files)
      self.commit_rotate_count += self.n_deletes

  def process_file_entry(self):
    self.totalf_swept += 1
    linemsg = prep.form_fil_in_mid_with_progress_percent_line(
      self.totalf_swept, self.totalf, self.filename, self.middlepath
    )
    print(linemsg)
    if self.filename == config.SQLITE_UPDIRENTRIES_DEFAULT_FILENAME:
      return
    mfile = metaf.MetaFile(self.mount_abspath, self.middlepath, self.filename)  # mockmode=True
    dbentry = self.session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.entryname == mfile.filename). \
        filter(sam.FSEntryInDB.middlepath == mfile.middlepath). \
        first()
    if dbentry is not None:
      # IMPORTANT: it will be assumed for the time being
      # that sha1 is okay, but a flag might make a verification on it in a next upgrade
      self.n_exists += 1
      print(self.n_exists, 'mfile exists', mfile)
      return
    # from here dbsearch will be made on sha1hex
    linemsg = '%d => about to generate sha1hex for filename=[%s] | middlename=[%s]' %\
              (self.totalf_swept, self.filename, self.middlepath)
    print(linemsg)
    mfile.calc_n_set_sha1hex()
    if mfile.error_on_filepath:
      self.files_with_error += 1
      return
    dbentry = self.session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.sha1hex == mfile.sha1hex). \
        first()
    if dbentry:
      if dbentry.entryname == self.filename and dbentry.middlepath == self.middlepath:
        return
      # either one or the other is different or both are different
      if dbentry.entryname != self.filename:
        dbentry.entryname = self.filename
      if dbentry.middlepath == self.middlepath:
        dbentry.middlepath = self.middlepath
      self.n_updates += 1
      linemsg = '%d => ((Updating)) filename=[%s] | middlename=[%s]' % (self.n_updates, self.filename, self.middlepath)
      print(linemsg)
      self.commit_rotate_count = prep.commit_on_counter_rotate(self.session, self.commit_rotate_count)
      return
    self.n_inserts += 1
    linemsg = '%d => ((Inserting)) filename=[%s] | middlename=[%s]' % (self.n_inserts, self.filename, self.middlepath)
    print(linemsg)
    dbentry = sam.FSEntryInDB()
    dbentry.entryname = self.filename
    dbentry.middlepath = self.middlepath
    dbentry.sha1hex = mfile.sha1hex
    self.session.add(dbentry)
    self.commit_rotate_count = prep.commit_on_counter_rotate(self.session, self.commit_rotate_count)


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
