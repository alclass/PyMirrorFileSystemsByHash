#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
DBSha1ModelMod.py

'''

import os, sqlite3

DEFAULT_SQLITE_FILENAME = 'top_down_sha1files.sqlite'

class DBSha1Model(object):

  def __init__(self, abs_top_basepath):
    abs_sqlite_filepath = os.path.join(abs_top_basepath, DEFAULT_SQLITE_FILENAME)
    self.conn = sqlite3.connect(abs_sqlite_filepath)

  def check_record_existence(self, sha1item):

    sql = '''
    SELECT FROM sha1entries WHERE
      sha1hex =
      filename
      relative_parent_path
      filesizE
      modified_date
    VALUES ( ?,       ?,             ?,               ?,          ?);
    '''

  def insert(self, sha1item):

    sql = '''
    INSERT INTO sha1entries
         (sha1hex, filename, relative_parent_path, filesize, modified_date)
    VALUES ( ?,       ?,             ?,               ?,          ?);
    '''
    data_tuple_record = ( \
      sha1item.sha1hex, \
      sha1item.filename, \
      sha1item.relative_parent_path, \
      sha1item.filesize, \
      sha1item.modified_date, \
    )
    cursor = self.conn.cursor()
    cursor.executemany(sql, data_tuple_record)
    self.conn.commit()

  def close(self):
    self.conn.close()


def main():

  pass



if __name__ == '__main__':
  main()
