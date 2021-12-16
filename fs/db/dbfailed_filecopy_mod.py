#!/usr/bin/env python3
import fs.db.dbbase_mod as dbb


class DBFailFileCopyReporter(dbb.DBBase):

  default_tablename = 'failcopyfiles'

  def __init__(self, mountpath=None, inlocus_sqlite_filename=None, tablename=None):
    if tablename is None:
      self.tablename = self.default_tablename
    super().__init__(mountpath, inlocus_sqlite_filename)

  @property
  def fieldnames(self):
    return ['id', 'file_id', 'trg_mountpath', 'event_dt']

  def form_fields_line_for_createtable(self):
    """
    This method is to be implemented in child-inherited classes
    """
    middle_sql = """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      file_id INTEGER,
      trg_mountpath TEXT,
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
        file_id=?,
        trg_mountpath=?,
        event_dt=?
      WHERE 
        id=?
    '''
    return sql_before_interpol

  def do_insert_or_update_with_dict_to_prep_tuplevalues(self, pdict):
    listvalues = [None]*len(self.fieldnames)
    try:
      file_id = pdict['file_id']
      idx = self.fieldnames.index('file_id')
      listvalues[idx] = file_id
      trg_mountpath = pdict['trg_mountpath']
      idx = self.fieldnames.index('trg_mountpath')
      listvalues[idx] = trg_mountpath
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
  db = DBFailFileCopyReporter()
  tuplelist = db.do_select_all()
  print(tuplelist)
  for row in tuplelist:
    _id = row[0]
    print('_id', _id)
    idx = db.fieldnames.index('file_id')
    file_id = row[idx]
    print('file_id', file_id)
    idx = db.fieldnames.index('trg_mountpath')
    trg_mountpath = row[idx]
    print('trg_mountpath', trg_mountpath)
    idx = db.fieldnames.index('event_dt')
    event_dt = row[idx]
    print('event_dt', event_dt)


def adhoc_select_all():
  db = DBFailFileCopyReporter()
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
