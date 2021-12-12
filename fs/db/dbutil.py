#!/usr/bin/env python3
"""
dbutil.py
"""


def prep_insert_sql_from_dict_n_return_sql_n_tuplevalues(pdict, allowed_fieldnames):
  sql = 'INSERT into %(tablename)s'
  sql2 = ' (%s) VALUES (%s);'
  fields = pdict.keys()
  str_fieldlist_for_sql = ''
  listvalues = []
  for field in fields:
    if field not in allowed_fieldnames:
      return None, None
    str_fieldlist_for_sql += field + ', '
    listvalues.append(pdict[field])
  str_fieldlist_for_sql = str_fieldlist_for_sql.rstrip(', ')
  str_questionmarks = '?, ' * len(fields)
  str_questionmarks = str_questionmarks.rstrip(', ')
  sql2 = sql2 % (str_fieldlist_for_sql, str_questionmarks)
  sql = sql + sql2
  tuplevalues = tuple(listvalues)
  return sql, tuplevalues


def prep_updatesql_with_dict_n_frow_return_sql_n_tuplevalues(pdict, dbfieldnames, fetched_row):
  oldname, oldparentpath = None, None
  n_fields = len(dbfieldnames)
  for i in range(n_fields):
    fieldname = dbfieldnames[i]
    if fieldname == 'name':
      oldname = fetched_row[i]
      continue
    if fieldname == 'parentpath':
      oldparentpath = fetched_row[i]
      continue
  sql = 'UPDATE %(tablename)s \nSET\n'
  listvalues = []
  for fieldname in pdict.keys():
    sql += '\t' + fieldname + '=?,'
    listvalues.append(pdict[fieldname])
  sql = sql.rstrip(',')
  sql += '\nWHERE\n'
  sql += '\tname=? and\n'
  listvalues.append(oldname)
  sql += '\tparentpath=?;'
  listvalues.append(oldparentpath)
  tuplevalues = tuple(listvalues)
  return sql, tuplevalues


globaldict = {'name': 'test1', 'parentpath': '/dir1/folder2', 'bytesize': 2000, 'mdatetime': 87334132.656}
fieldnames = ['id', 'name', 'parentpath', 'bytesize', 'mdatetime']


def adhoc_test2():
  fetched_row = (54, 'test1', '/f2/parentpath1', 2000, 87334132.656)
  sql, tuplevalues = prep_updatesql_with_dict_n_frow_return_sql_n_tuplevalues(globaldict, fieldnames, fetched_row)
  print('sql', sql)
  print('tuplevalues', tuplevalues)


def adhoc_test1():
  sql, tuplevalues = prep_insert_sql_from_dict_n_return_sql_n_tuplevalues(globaldict, fieldnames)
  print('sql', sql)
  print('tuplevalues', tuplevalues)
  pdict = {'name': 'test1', 'greatparentpath': '/dir1/folder2', 'bytesize': 2000, 'mdatetime': 87334132.656}
  sql, tuplevalues = prep_insert_sql_from_dict_n_return_sql_n_tuplevalues(pdict, fieldnames)
  print('sql', sql)
  print('tuplevalues', tuplevalues)


def process():
  """
  adhoc_select()
  adhoc_delete_all_rows()
  adhoc_select_all()
  bool_res = adhoc_insert_some()
  print('was_inserted', bool_res)
  """
  # adhoc_test1()
  adhoc_test2()


if __name__ == '__main__':
  process()
