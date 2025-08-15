#!/usr/bin/env python3
import lib.db.dbbase_mod as dbb


class DBFailFileReadReporter(dbb.DBBase):

  default_tablename = 'failfilereads'

  def __init__(self, mountpath=None, inlocus_sqlite_filename=None, tablename=None):
    if tablename is None:
      self.tablename = self.default_tablename
    super().__init__(mountpath, inlocus_sqlite_filename)

  @property
  def fieldnames(self):
    return ['id', 'name', 'parentpath', 'bytesize', 'mdatetime', 'event_dt']

  def form_fields_line_for_createtable(self):
    """
    This method is to be implemented in child-inherited classes
    """
    middle_sql = """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      parentpath TEXT,
      bytesize INTEGER,
      mdatetime TEXT,
      event_dt TEXT
    """
    return middle_sql

  def form_update_with_all_fields_sql(self):
    """
    Notice that the interpolation %(tablename)s is not done here, it'll be done later on.
    """
    sql_before_interpol = '''
    UPDATE %(tablename)s
      SET
        name=?,
        parentpath=?,
        bytesize=?,
        mdatetime=?,
        event_dt=?
      WHERE 
        id=?
    '''
    return sql_before_interpol

  def do_insert_or_update_with_dict_to_prep_tuplevalues(self, pdict):
    listvalues = [None]*len(self.fieldnames)
    try:
      name = pdict['name']
      idx = self.fieldnames.index('name')
      listvalues[idx] = name
      parentpath = pdict['parentpath']
      idx = self.fieldnames.index('parentpath')
      listvalues[idx] = parentpath
      bytesize = pdict['bytesize']
      idx = self.fieldnames.index('bytesize')
      listvalues[idx] = bytesize
      mdatetime = pdict['mdatetime']
      idx = self.fieldnames.index('mdatetime')
      listvalues[idx] = mdatetime
      event_dt = pdict['event_dt']
      idx = self.fieldnames.index('event_dt')
      listvalues[idx] = event_dt
      # tuplevalues = (name, parentpath, bytesize, mdatetime, event_dt)
      tuplevalues = tuple(listvalues)
      return self.do_insert_or_update_with_tuplevalues(tuplevalues)
    except IndexError:
      pass
    return False


def adhoc_select():
  db = DBFailFileReadReporter()
  tuplelist = db.do_select_all()
  print(tuplelist)
  for row in tuplelist:
    _id = row[0]
    print('_id', _id)
    idx = db.fieldnames.index('name')
    name = row[idx]
    print('name', name)
    idx = db.fieldnames.index('parentpath')
    parentpath = row[idx]
    print('parentpath', parentpath)
    idx = db.fieldnames.index('bytesize')
    bytesize = row[idx]
    print('bytesize', bytesize)
    idx = db.fieldnames.index('mdatetime')
    mdatetime = row[idx]
    print('mdatetime', mdatetime)
    idx = db.fieldnames.index('event_dt')
    event_dt = row[idx]
    print('event_dt', event_dt)


def adhoc_select_all():
  db = DBFailFileReadReporter()
  result_tuple_list = db.do_select_all()
  for tuplerow in result_tuple_list:
    print(tuplerow)
  return result_tuple_list


def process():
  """
  adhoc_select()
  adhoc_delete_all_rows()
  adhoc_select_all()
  bool_res = adhoc_insert_some()
  print('was_inserted', bool_res)
  """
  adhoc_select_all()


if __name__ == '__main__':
  process()
