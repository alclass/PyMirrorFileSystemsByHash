#!/usr/bin/env python3

import config
import fs.db.sqlalchemy_conn as con
import models.samodels as sam


def create_table_if_not_exists(source=True):
  engine = con.get_engine_for_sqlitefilepath(source)
  if not engine.dialect.has_table(engine, config.get_dirtree_tablename()):
    sam.FSEntryInDB.__table__.create(bind=engine, checkfirst=True)
    return True
  return False


def process():
  pass


if __name__ == '__main__':
  process()
