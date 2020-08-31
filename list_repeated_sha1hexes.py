#!/usr/bin/env python3
"""

"""
import time
from sqlalchemy import func
import models.samodels as sam
import fs.db.sqlalchemy_conn as con
import fs.os.prep_fs_counts_mod as prep
import models.pathpositioning.metafilemod as metaf
import config


def list_repeated_sha1hexes():
  """
    all()
  for sha1row in sha1rows:
    print(sha1rows)

  """
  session = con.get_session_for_sqlite_source_or_target(source=True)
  sha1rows = session.query(sam.FSEntryInDB.sha1hex, func.count(sam.FSEntryInDB.sha1hex)).\
      group_by(sam.FSEntryInDB.sha1hex).\
      having(func.count(sam.FSEntryInDB.sha1hex) > 1).all()
  zeroshas_quant = 0
  for i, sha1row in enumerate(sha1rows):
    sha1hex = sha1row[0]
    quant = sha1row[1]
    if sha1hex == config.EMPTYFILE_SHA1HEX:
      zeroshas_quant = quant
      continue
    print(i+1, 'Quant', quant, sha1hex)
    dbentries = session.query(sam.FSEntryInDB).filter(sam.FSEntryInDB.sha1hex == sha1hex).all()
    for j, dbentry in enumerate(dbentries):
      print(j+1, dbentry)
  print('zeroshas_quant', zeroshas_quant)
  session.close()


def process():
  # sweep_src_n_trg()
  start_time = time.time()
  list_repeated_sha1hexes()
  elapsed_time = time.time() - start_time
  print('start_time', start_time)
  print('elapsed_time', elapsed_time)


if __name__ == '__main__':
  process()
