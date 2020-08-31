#!/usr/bin/env python3
"""

"""
import models.samodels as sam
import fs.db.sqlalchemy_conn as con

SOURCE_ABSPATH_DICTKEY = 'source_abspath'
TARGET_ABSPATH_DICTKEY = 'target_abspath'


def get_abspaths():
  pdict = eval(open('data_entry_dir_source_n_target.pydict.txt').read())
  print(pdict)
  source_abspath = pdict[SOURCE_ABSPATH_DICTKEY]
  target_abspath = pdict[TARGET_ABSPATH_DICTKEY]
  print(source_abspath, target_abspath)
  return source_abspath, target_abspath


def report_equal_sha1s():
  """

  """
  src_session = con.get_session_for_sqlite_source_or_target(source=True)
  sha1dtuplelist = src_session.query(sam.FSEntryInDB.sha1hex).distinct(sam.FSEntryInDB.sha1hex).all()
  print('total of sha1hexes in db', len(sha1dtuplelist))
  print('looking for duplicates')
  n_duplicate = 0
  for sha1dtuplelist in sha1dtuplelist:
    sha1hex = sha1dtuplelist[0]
    dbentries = src_session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.sha1hex == sha1hex). \
        all()
    if len(dbentries) < 2:
      continue
    n_duplicate += 1
    print('duplicate', n_duplicate, '='*10, 'sha1', sha1hex)
    for dbentry in dbentries:
      print('[', dbentry.entryname, '] ==>>> ', dbentry.middlepath)
  print('Finished report_equal_sha1s()')
  src_session.close()


def process():
  report_equal_sha1s()


if __name__ == '__main__':
  process()
