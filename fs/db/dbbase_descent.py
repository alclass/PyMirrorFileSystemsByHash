#!/usr/bin/env python3
"""
This module contains class DBDescent
It tries to find, by dir=descending, the PyMirror default sqlite file
  whick contains backup-data and also the ytids stored.
It can also retrieve a sqlite db-connection from that file.
"""
import os
import sqlite3
import sys
import default_settings as ds


def find_recurs_baseir(ppath=None):
  """

  """
  if ppath is None or not os.path.isdir(ppath):
    ppath = os.path.abspath('.')
  entries = os.listdir(ppath)
  for e in entries:
    if e == ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME:
      return ppath
  if ppath == '/':
    return None
  ppath, _ = os.path.split(ppath)
  return find_recurs_baseir(ppath)


def create_table_ytids(conn):
  sql = '''
  CREATE TABLE IF NOT EXISTS ytids (
    ytid CHAR(11) NOT NULL UNIQUE
  );
  '''
  cursor = conn.cursor()
  f = cursor.execute(sql)
  sql = 'CREATE INDEX IF NOT EXISTS ytid ON ytids(ytids);'
  f = cursor.execute(sql)
  print('Creating table ytds', f)
  conn.close()
  

def get_100_diffset_from_ytids(ytids, conn):
  if ytids is None or conn is None:
    error_msg = 'Error: ytids is None or conn is None in get_100_diffset_from_ytids(ytids, conn)'
    raise OSError(error_msg)
  sql = 'select ytid from ytids where ytid in ('
  whereclause = '?,' * len(ytids)
  whereclause = whereclause.rstrip(',')
  sql += whereclause
  sql += ');'
  tuplevalues = tuple(ytids)
  cursor = conn.cursor()
  try:
    fetch = cursor.execute(sql, tuplevalues)
    rows = fetch.fetchall()
    diffset = []
    for row in rows:
      ytid = row[0]
      diffset.append(ytid)
    cursor.close()
    return diffset
  except sqlite3.OperationalError as error:
    if 'no such table' in str(error):
      print('Creating table ytids')
      create_table_ytids(conn)
      return get_100_diffset_from_ytids(ytids, conn)
  return []


class DBDescent:

  def __init__(self, startdir_abspath=None):
    self._basedir = None
    if startdir_abspath is None or not os.path.isdir(startdir_abspath):
      self.startdir_abspath = os.path.abspath('.')
    else:
      self.startdir_abspath = startdir_abspath
    self.go_find_basedir()

  @property
  def basedir(self):
    return self._basedir

  @property
  def sqlite_filepath(self):
    fpath = os.path.join(self.basedir, ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME)
    if os.path.isfile(fpath):
      return fpath
    return None

  def get_connection(self):
    return sqlite3.connect(self.sqlite_filepath)

  def go_find_basedir(self):
    self._basedir = find_recurs_baseir(self.startdir_abspath)
    if self._basedir is None:
      error_msg = 'DEFAULT_DEVICEROOTDIR_SQLFILENAME (%s) not found on dirpath descent' \
                  % ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
      raise OSError(error_msg)

  def get_diffset_from_ytids(self, ytids):
    result_list = []
    conn = self.get_connection()
    lower, upper, jump = 0, 0, 100
    while lower < len(ytids):
      if lower + jump < len(ytids):
        upper = lower + jump
        dobreak = False
      else:
        jump = len(ytids) - lower
        upper = lower + jump
        dobreak = True
      chunk = ytids[lower: upper]
      result_list += get_100_diffset_from_ytids(chunk, conn)
      lower += jump
      if dobreak:
        break
    conn.close()
    return result_list


def get_args():
  for arg in sys.argv:
    if arg.startswith('-p='):
      ppath = arg[len('-p='):]
      if os.path.isdir(ppath):
        return ppath
  return None


def adhoctest():
  ppath = get_args()
  if ppath is None:
    ppath = ds.Paths.get_default_src_datafolder_abspath()
  dbd = DBDescent(ppath)
  print('basedir', dbd.basedir)
  tlist = ['5pcKS_gi-0Q']
  diffset = dbd.get_diffset_from_ytids(tlist)
  print(diffset)


def process():
  """
  """
  adhoctest()


if __name__ == '__main__':
  process()
