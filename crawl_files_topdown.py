#!/usr/bin/env python3
"""

"""
import os
import time
import models.samodels as sam
import fs.db.sqlalchemy_conn as con
import fs.os.prep_fs_counts_mod as prep
import fs.os.middlepathmakemod as midpath
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
    self.middlepathobj = midpath.MiddlePath(self.mount_abspath)

    self.totalf = 0
    self.totalf_swept = 0
    self.n_inserts = 0
    self.n_updates = 0
    self.n_deletes = 0
    self.n_exists = 0
    self.files_with_error = 0
    self.commit_rotate_count = 0
    self.session = None

  def perfom_updir_sweep(self):
    """

    """
    self.totalf = prep.prepare_sweep_files_count(self.mount_abspath)
    self.session = con.get_session_from_sqlitefilepath(source=True)
    for abspath, dirs, files in os.walk(os.path.abspath(self.mount_abspath)):
      self.process_oswalk_abspath_iteration(abspath, files)

    # final commit, send parameter finalcommit=True
    print('finalcommit=True')
    # _ = prep.commit_on_counter_rotate(self.session, self.commit_rotate_count, finalcommit=True)
    self.session.commit()
    print('n_inserts', self.n_inserts)
    print('n_updates', self.n_updates)
    print('n_deletes', self.n_deletes)
    print('n_exists', self.n_exists)
    print('files_with_error', self.files_with_error)
    print('totalf_swept', self.totalf_swept)
    print('totalf', self.totalf)
    self.session.close()

  def process_oswalk_abspath_iteration(self, abspath, files):
    middlepath = self.middlepathobj.middle_to_entry(abspath)
    if middlepath in config.L1DIRS_TO_AVOID_IN_MIRRORING:
      return
    print('=>->' * 10, len(files), 'files on middlepath [' + middlepath + ']')
    print(' [abspath]', abspath)
    for filename in sorted(files):
      if filename == '2018 Course List.txt':
        print ("filename == '2018 Course List.txt' ", filename)
        return
      self.process_file_entry(middlepath, filename)
      self.n_deletes += clean_up_folderleaftovers_in_db(self.session, middlepath, files)
      # the delete case will defer db-committing to a next limit or at the end (with finalcommit)
      self.commit_rotate_count += self.n_deletes

  def process_file_entry(self, middlepath, filename):
    linemsg = prep.form_fil_in_mid_with_progress_percent_line(
      self.totalf_swept, self.totalf, middlepath, filename
    )
    print(linemsg)
    if filename == config.SQLITE_UPDIRENTRIES_DEFAULT_FILENAME:
      return
    self.totalf_swept += 1
    mfile = metaf.MetaFile(self.mount_abspath, middlepath, filename)  # mockmode=True
    dbentry = self.session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.entryname == mfile.filename). \
        filter(sam.FSEntryInDB.middlepath == mfile.middlepath). \
        first()
    if dbentry is not None:
      return self.treat_fileentry_when_its_in_db(dbentry, mfile)
    else:
      return self.calc_sha1hex_n_insert_fileentry_into_db(mfile)

  def treat_fileentry_when_its_in_db(self, dbentry, mfile):

      self.n_exists += 1
      if dbentry.sha1hex is not None:
        print(self.n_exists, 'mfile exists', mfile)
        return
      else:
        mfile.calc_n_set_sha1hex()
        if mfile.error_on_filepath:
          self.files_with_error += 1
          error_msg = ' %d !!! OS Error when calculating sha1 %s ' % (self.files_with_error, mfile.sha1hex)
          print(error_msg)
          raise OSError(error_msg)
          # return dbentry
        dbentry.sha1hex = mfile.sha1hex
        # committing on update
        self.commit_rotate_count = prep.commit_on_counter_rotate(self.session, self.commit_rotate_count)
        return dbentry

  def calc_sha1hex_n_insert_fileentry_into_db(self, mfile):
    """
    # from here dbsearch will be made with sha1hex
    dbentry = self.session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.sha1hex == mfile.sha1hex). \
        first()
    if dbentry:
      self.update_dbentry_based_on_same_sha1hex(dbentry, mfile)
    else:
      self.insert_dbentry_with_mfile(mfile)
    """

    linemsg = '%d => about to generate sha1hex for [%s/%s]' %\
              (self.totalf_swept, mfile.middlepath, mfile.filename)
    print(linemsg)
    mfile.calc_n_set_sha1hex()
    if mfile.error_on_filepath:
      self.files_with_error += 1
      error_msg = ' %d !!! OS Error when calculating sha1 %s ' \
                  % (self.files_with_error, str(mfile))
      print(error_msg)
      raise OSError(error_msg)
      # return None
    return self.insert_dbentry_with_mfile(mfile)

  def deprec_update_dbentry_based_on_same_sha1hex(self, dbentry, mfile):
    self.n_exists += 1
    if dbentry.entryname == mfile.filename and dbentry.middlepath == mfile.middlepath:
      return
    # either one or the other is different or both are different
    self.n_updates += 1
    if dbentry.entryname != mfile.filename:
      dbentry.entryname = mfile.filename
    if dbentry.middlepath != mfile.middlepath:
      dbentry.middlepath = mfile.middlepath
    linemsg = '%d => ((Updating)) [%s/%s]' \
              % (self.n_updates, mfile.middlepath, mfile.filename)
    print(linemsg)
    # committing on update
    self.commit_rotate_count = prep.commit_on_counter_rotate(self.session, self.commit_rotate_count)
    return

  def insert_dbentry_with_mfile(self, mfile):
    self.n_inserts += 1
    linemsg = '%d => ((Inserting)) filename=[%s] | middlename=[%s]' \
              % (self.n_inserts, mfile.filename, mfile.middlepath)
    print(linemsg)
    dbentry = sam.FSEntryInDB()
    dbentry.entryname = mfile.filename
    dbentry.middlepath = mfile.middlepath
    dbentry.sha1hex = mfile.sha1hex
    self.session.add(dbentry)
    # committing on insert
    self.commit_rotate_count = prep.commit_on_counter_rotate(self.session, self.commit_rotate_count)
    return dbentry


def process_sweep_src_uptree():
  """

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
