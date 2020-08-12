#!/usr/bin/env python3
"""

"""
import os
import time
import models.samodels as sam
import fs.db.sqlalchemy_conn as con
import models.pathpositioning.metafilemod as metaf
import config


def show_mountpoint_src_n_trg_abspaths():
  pdict = config.get_mountpoint_datadirs_dict()
  source_abspath = pdict[config.MOUNTPOINT_SOURCEDATADIR_DICTKEY]
  target_abspath = pdict[config.MOUNTPOINT_SOURCEDATADIR_DICTKEY]
  print('source_abspath', source_abspath)
  print('target_abspath', target_abspath)


def make_updir_sweep(mount_abspath):
  """

  """
  session = con.get_session_from_sqlitefilepath(source=True)
  n_inserts = 0
  n_updates = 0
  n_deletes = 0
  for abspath, dirs, files in os.walk(os.path.abspath(mount_abspath)):
    # trg_session = con.get_sessionmaker_from_sqlitefilepath(source=False)
    if abspath == mount_abspath:
      middlepath = ''
    else:
      middlepath = abspath[len(mount_abspath)+1:]
    for nd, folder in enumerate(sorted(dirs)):
      print(nd+1, 'dir =>', folder, abspath)
    print('-'*30)
    for nf, filename in enumerate(sorted(files)):
      # was_changed = False
      if filename == config.SQLITE_UPDIRENTRIES_DEFAULT_FILENAME:
        continue
      mfile = metaf.MetaFile(mount_abspath, middlepath, filename)  # mockmode=True
      mfile.calc_n_set_sha1hex()
      print(nf+1, 'filename =>', filename, abspath)
      print(mfile)
      dbentry = session.query(sam.FSEntryInDB).filter(sam.FSEntryInDB.sha1hex == mfile.sha1hex).first()
      if dbentry:
        if dbentry.entryname == filename and dbentry.middlepath == middlepath:
          continue
        elif dbentry.entryname != filename:
          if dbentry.middlepath == middlepath and filename not in files:
            print('Updating dbentry.entryname = filename = %s' % filename)
            dbentry.entryname = filename
            n_updates += 1
            session.commit()
            continue
      was_changed = True
      print('Inserting into db')
      dbentry = sam.FSEntryInDB()
      dbentry.entryname = filename
      dbentry.middlepath = middlepath
      dbentry.sha1hex = mfile.sha1hex
      session.add(dbentry)
      n_inserts += 1
      # clean up folder
      n_deletes += clean_up_folderleaftovers_in_db(session, middlepath, files)
      if was_changed:
        session.commit()
  # session.commit()
  session.close()
  print('n_inserts', n_inserts)
  print('n_updates', n_updates)
  print('n_deletes', n_deletes)


def clean_up_folderleaftovers_in_db(session, middlepath, files):
  n_deletes = 0
  dbentries = session.query(sam.FSEntryInDB).\
      filter(sam.FSEntryInDB.middlepath == middlepath).\
      all()
  for dbentry in dbentries:
    if dbentry.entryname not in files:
      print('Deleting non existing or moved out %s', dbentry.entryname, 'in', middlepath)
      session.delete(dbentry)
      n_deletes += 1
  print('Committing session')
  return n_deletes


def sweep_src_uptree():
  """
  mount_abspath = config.get_datatree_mountpoint_abspath(source=False)
  make_updir_sweep(mount_abspath)

  """
  start_time = time.time()
  print('[START PROCESSING TIME] sweep_src_uptree at', start_time)
  mount_abspath = config.get_datatree_mountpoint_abspath(source=True)
  make_updir_sweep(mount_abspath)
  elapsed_time = time.time() - start_time
  print('[END PROCESSING TIME] elapsed_time at', elapsed_time)


def process():
  # sweep_src_n_trg()
  sweep_src_uptree()


if __name__ == '__main__':
  process()
