#!/usr/bin/env python3
import sys
import fs.db.dbrepeats_mod as dbr
import fs.db.dbdirtree_mod as dbt


class ReportFileRepeat:

  def __init__(self, mountpath=None):
    self.dbrepeat = dbr.DBRepeat(mountpath)
    self.dbtree = dbt.DBDirTree(mountpath)

  def report(self):
    sql = 'SELECT DISTINCT(sha1), count(hkey) from %(tablename)s GROUP BY sha1;'
    rowlist = self.dbrepeat.do_select_with_sql_without_tuplevalues(sql)
    total = 0
    for i, row in enumerate(rowlist):
      sha1 = row[0]
      sha1hex = sha1.hex()
      counthkey = row[1]
      total += counthkey
      print(i+1, sha1hex, counthkey)
    print('total', total)

  def report_sha1_with_paths(self):
    sql = 'SELECT DISTINCT(sha1), count(hkey) from %(tablename)s GROUP BY sha1;'
    rowlist = self.dbrepeat.do_select_with_sql_without_tuplevalues(sql)
    total = 0
    for i, row in enumerate(rowlist):
      sha1 = row[0]
      sha1hex = sha1.hex()
      counthkey = row[1]
      total += counthkey
      print(i+1, sha1hex, counthkey)
      innersql = 'SELECT * from %(tablename)s WHERE sha1=?;'
      tuplevalues = (sha1,)
      innerrowlist = self.dbtree.do_select_with_sql_n_tuplevalues(innersql, tuplevalues)
      for j, jrow in enumerate(innerrowlist):
        name = jrow[2]
        parentpath = jrow[3]
        print(j+1, 'name', name, parentpath)
    print('total', total)


def get_arg():
  try:
    return sys.argv[1]
  except IndexError:
    pass
  return None


def process():
  mountpath = get_arg()
  report_repeats = ReportFileRepeat(mountpath)
  report_repeats.report()
  report_repeats.report_sha1_with_paths()


if __name__ == '__main__':
  process()
