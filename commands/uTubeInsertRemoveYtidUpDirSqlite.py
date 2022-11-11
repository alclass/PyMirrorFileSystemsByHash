#!/usr/bin/env python3
"""
uTubeInsertRemoveYtidUpDirSqlite.py
"""
import os
import sqlite3
import fs.dirfilefs.ytids_functions as ytfs
import default_settings as ds


class YtidUpDirSqliteInsertor:

  def __init__(self, ppath, descend_til_db=True):
    self.descend_til_db = descend_til_db
    self.workdir_abspath = ppath
    self._sqlitefile_abspath = None
    self.updir_ytids = []
    self.updir_missing_ytids = []
    self.db_excess_ytids = []
    self.n_inserted = 0
    self.n_deleted = 0
    # self.db_ytids = []
    self.n_db_ytids = 0
    if self.workdir_abspath is None or not os.path.isdir(self.workdir_abspath):
      self.workdir_abspath = os.path.abspath('.')
    self.revise_workdir_if_descend_til_db()

  def revise_workdir_if_descend_til_db(self):
    """
    TI-DO / TO-BE implemented!
    Notice the updirtree must be done up-starting from workdir,
      but the sqlite repo may be lower down (or up) in the directory hierarchy
    """
    if self.descend_til_db:
      pass

  @property
  def sqlitefile_abspath(self):
    if self._sqlitefile_abspath is None:
      filename = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
      self._sqlitefile_abspath = os.path.join(self.workdir_abspath, filename)
    return self._sqlitefile_abspath

  def get_connection(self):
    return sqlite3.connect(self.sqlitefile_abspath)

  def walk_updirtree(self):
    updir_ytids = []
    print('Please wait. Walking updirtree from', self.workdir_abspath)
    for _, _, filenames in os.walk(self.workdir_abspath):
      ytids = ytfs.extract_ytids_from_filenames(filenames)
      updir_ytids += ytids
    updir_ytids = list(set(updir_ytids))
    return updir_ytids

  def sync_missing_n_excess_updir_n_db_ytids(self, updir_ytids):
    db_ytids = []
    sql = 'select ytid from ytids;'
    conn = self.get_connection()
    ytfs.create_table_if_not_exists_ytids(conn)
    cursor = conn.cursor()
    dbret = cursor.execute(sql)
    rows = dbret.fetchall()
    db_ytids = [row[0] for row in rows]
    self.updir_missing_ytids = [ytid for ytid in updir_ytids if ytid not in db_ytids]
    self.db_excess_ytids = [ytid for ytid in db_ytids if ytid not in updir_ytids]
    # self.db_excess_ytids = [ytid for ytid in self.db_excess_ytids if ytid not in self.updir_missing_ytids]
    self.n_db_ytids = len(db_ytids)
    conn.close()
    return self.updir_missing_ytids

  def insert_missing_ytid(self, ytid, cursor):
    sql = 'insert or ignore into ytids values (?);'
    seq = self.n_inserted + 1
    print(seq, sql, ytid)
    tuplevalues = (ytid, )
    cursor.execute(sql, tuplevalues)
    if cursor.arraysize == 1:
      self.n_inserted += 1

  def insert_missing_ytids(self):
    conn = self.get_connection()
    cursor = conn.cursor()
    for ytid in self.updir_missing_ytids:
      self.insert_missing_ytid(ytid, cursor)
    if self.n_inserted > 0:
      print('Committing inserted', self.n_inserted, 'ytids')
      conn.commit()
    conn.close()

  def delete_excess_ytid(self, ytid, cursor):
    sql = 'delete from ytids where ytid=?;'
    seq = self.n_deleted + 1
    print(seq, sql, ytid)
    tuplevalues = (ytid, )
    cursor.execute(sql, tuplevalues)
    if cursor.arraysize == 1:
      self.n_deleted += 1

  def delete_excess_ytids(self):
    conn = self.get_connection()
    cursor = conn.cursor()
    for ytid in self.db_excess_ytids:
      self.delete_excess_ytid(ytid, cursor)
    if self.n_inserted > 0:
      print('Committing deleted', self.n_deleted, 'ytids')
      conn.commit()
    conn.close()

  def process(self):
    print('Working with db', self.sqlitefile_abspath)
    updir_ytids = self.walk_updirtree()
    n_updir_ytids = len(updir_ytids)
    self.sync_missing_n_excess_updir_n_db_ytids(updir_ytids)
    print('N of updir', n_updir_ytids)
    print('N of db', self.n_db_ytids)
    print('N of missing', len(self.updir_missing_ytids), self.updir_missing_ytids)
    print('N of excess', len(self.db_excess_ytids), self.db_excess_ytids)
    self.insert_missing_ytids()
    self.delete_excess_ytids()


def get_ppath_from_args(argv):
  for arg in argv:
    if arg.startswith('-p='):
      ppath = arg[len('-p='):]
      if os.path.isdir(ppath):
        return ppath
  return None


def process(argv):
  """
  This function aims to transfer executing from a "dispatcher" script outside of this app
  """
  ppath = get_ppath_from_args(argv)
  insertor = YtidUpDirSqliteInsertor(ppath)
  insertor.process()


if __name__ == '__main__':
  """
  "/media/friend/Bio EE Sci Soc 2T Orig/Yt vi/BRA Polit yt vi/Meteoro Brasil yu"
  """
  process(sys.argv)
