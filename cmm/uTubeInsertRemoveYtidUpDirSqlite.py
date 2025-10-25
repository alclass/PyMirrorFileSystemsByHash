#!/usr/bin/env python3
"""
uTubeInsertRemoveYtidUpDirSqlite.py
"""
import os
import sqlite3
import sys
import llib.dirfilefs.ytids_functions as ytfs
import default_settings as ds


class YtidUpDirSqliteInsertor:

  def __init__(self, ppath, descend_til_db=True, stdout_dirpass=False):
    self.stdout_dirpass = stdout_dirpass
    self.descend_til_db = descend_til_db
    self.workdir_abspath = ppath
    self._sqlitefile_abspath = None
    self.updir_ytids = []
    self.updir_missing_ytids = []
    self.db_excess_ytids = []
    self.updir_ytids = []
    self.n_dir_in_passing = 0
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

  def walk_updirtree_n_fill_updirytids(self):
    print('Please wait. Walking updirtree from', self.workdir_abspath)
    for curr_dirpath, _, filenames in os.walk(self.workdir_abspath):
      ytids = ytfs.extract_ytids_from_filenames(filenames)
      self.n_dir_in_passing += 1
      self.updir_ytids += ytids
      # self.updir_ytids = list(set(self.updir_ytids))
      acc_ytids = len(self.updir_ytids)
      if self.stdout_dirpass:
        str_path = curr_dirpath if len(curr_dirpath) <= 80 else '...'+curr_dirpath[-77:]
        scrmsg = f"{self.n_dir_in_passing} | acc={acc_ytids} | [{str_path}]"
        print(scrmsg)
    self.updir_ytids = list(set(self.updir_ytids))

  def sync_missing_n_excess_updir_n_db_ytids(self):
    sql = 'select ytid from ytids;'
    conn = self.get_connection()
    ytfs.create_table_if_not_exists_ytids(conn)
    cursor = conn.cursor()
    dbret = cursor.execute(sql)
    rows = dbret.fetchall()
    db_ytids = [row[0] for row in rows]
    self.updir_missing_ytids = [ytid for ytid in self.updir_ytids if ytid not in db_ytids]
    self.db_excess_ytids = [ytid for ytid in db_ytids if ytid not in self.updir_ytids]
    self.db_excess_ytids = [ytid for ytid in self.db_excess_ytids if ytid not in self.updir_missing_ytids]
    self.n_db_ytids = len(db_ytids)
    conn.close()

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
    if self.n_deleted > 0:
      print('Committing deleted', self.n_deleted, 'ytids')
      conn.commit()
    conn.close()

  @property
  def total_updir_ytids(self):
    return len(self.updir_ytids)

  def process(self):
    print('Working with db', self.sqlitefile_abspath)
    self.walk_updirtree_n_fill_updirytids()
    self.sync_missing_n_excess_updir_n_db_ytids()
    print('total updir ytids', self.total_updir_ytids)
    print('N of db', self.n_db_ytids)
    print('N of missing', len(self.updir_missing_ytids), self.updir_missing_ytids)
    print('N of excess', len(self.db_excess_ytids), self.db_excess_ytids)
    self.insert_missing_ytids()
    self.delete_excess_ytids()

  def __str__(self):
    outstr = f"""Report [{self.__class__.__name__}]
    total updir ytids = {self.total_updir_ytids}
    updir_missing_ytids = {len(self.updir_missing_ytids)}
    db_excess_ytids = {len(self.db_excess_ytids)}
    n_insert = {self.n_inserted}
    n_deleted = {self.n_deleted}"""
    return outstr


def get_ppath_from_args_or_default(argv=None):
  ppath = None
  try:
    # 1st try: look up for a '-p=' starting in a parameter
    for arg in argv:
      if arg.startswith('-p='):
        ppath = arg[len('-p='):]
        break
    # 2nd try: if ppath is still None, pick up first arg if it exists
    if ppath is None and len(argv) > 1:
      ppath = argv[1]
  except (AttributeError, TypeError):
    pass
  # this 'if' is to avoid TypeError in case ppath is still None (which it can be)
  if ppath is None:
    ppath = os.path.abspath('.')
  # test if ppath is really an existing dir, otherwise default it to the current path
  ppath = ppath if os.path.isdir(ppath) else os.path.abspath('.')
  return ppath


def process(argv=None, stdout_dirpass=None):
  """
  Notice about the function's argv argument:
    this is so that this function may be executed from
       a "dispatcher" script outside this app sending in its own 'argv',
       if it's None, local sys.argv will be taken instead
  """
  argv = argv if argv is not None else sys.argv
  ppath = get_ppath_from_args_or_default(argv)
  scrmsg = f"dirpath given = [{ppath}]"
  print(scrmsg)
  scrmsg = f"Is this path correct? (Y/n) [ENTER] means Yes "
  ins = input(scrmsg)
  if ins not in ['Y', 'y', '']:
    return
  insertor = YtidUpDirSqliteInsertor(ppath, stdout_dirpass=stdout_dirpass)
  insertor.process()
  print(insertor)


if __name__ == '__main__':
  """
  "/media/user/Bio EE Sci Soc 2T Orig/Yt vi/BRA Polit yt vi/Meteoro Brasil yu"
  """
  process(stdout_dirpass=True)
