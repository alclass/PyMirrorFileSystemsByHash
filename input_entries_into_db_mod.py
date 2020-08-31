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


class EntriesProcessor:

  def __init__(self):
    self.mount_abspath = config.get_datatree_mountpoint_abspath(source=True)
    self.session = None
    self.n_inserts_or_updates = 0
    self.n_nothing_happed = 0
    self.n_commits = 0

  def process_missing_file_dbentry(self, middlepath, filename):
    mfile = metaf.MetaFile(self.mount_abspath, middlepath, filename)
    dbentry, bool_commit = mfile.insert_or_update_mid_n_fil_dbentry(self.session)
    if bool_commit:
      self.n_commits += 1
    if dbentry:
      self.n_inserts_or_updates += 1
      print('metaf.insert_or_update_dbentry_in_db()', dbentry)
    else:
      self.n_nothing_happed += 1
      print('metaf.insert_or_update_dbentry_in_db() => n_nothing_happed')

  def process_missing_dbentry_line(self, line=''):
    line = line.strip(' \t\r\n')
    pp = line.split('|')
    if len(pp) == 3:
      mid_n_fil = pp[-1]
      mid_n_fil = mid_n_fil.strip(' ')
      middlepath, filename = os.path.split(mid_n_fil)
      self.process_missing_file_dbentry(middlepath, filename)

  def report_totals(self):
    print('n_inserts_or_updates', self.n_inserts_or_updates)
    print('n_nothing_happed', self.n_nothing_happed)
    print('n_commits', self.n_commits)

  def process_missing_dbentries_txtfile(self):
    self.session = con.get_session_for_sqlite_source_or_target(source=True)
    fp = open('missing.log', encoding='utf8')
    line = fp.readline()
    while line:
      self.process_missing_dbentry_line(line)
      line = fp.readline()
    self.report_totals()
    self.session.close()


def process():
  eprocessor = EntriesProcessor()
  eprocessor.process_missing_dbentries_txtfile()


if __name__ == '__main__':
  process()
