#!/usr/bin/env python3
import random

from fs.util import util_mod as um
import fs.db.db_connection_factory_mod as dbfact
from fs import db as dbperf, db as dbcreat, db as dbsetts

PYMIRROR_DB_PARAMS = dbsetts.PYMIRROR_DB_PARAMS
FOLDER = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FOLDER
FILE = PYMIRROR_DB_PARAMS.ENTRY_TYPE_ID.FILE


sha1hexes = []
filesizes = []
modified_datetimes = []
for i in range(4):
  sha1hexes.append(um.take_random_sha1hex())
  filesizes.append(random.randint(1, 100001))
  modified_datetimes.append(um.take_random_datetime())


def make_tuple_list_data_for_dbinsert():
  entries = []
  e = (FOLDER, '/ab')
  entries.append(e)
  # e=(FOLDER,'/abc/ab')
  # entries.append(e)
  e = (FOLDER, '/z/abc/abc')
  entries.append(e)
  return entries


"""
  e=(FOLDER,'/ab')
  entries.append(e)
  e=(FOLDER,'/abc/abc')
  entries.append(e)
  seq=0
  e=(FILE,'/abc/abc/file1',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  seq+=1
  e=(FOLDER,'/abc/abc/file2',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  seq+=1
  e=(FOLDER,'/z')
  entries.append(e)
  seq+=1
  e=(FOLDER,'/z/z')
  entries.append(e)
  seq+=1
  e=(FILE,'/z/z/filez',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  seq+=1
  e=(FILE,'/abc/abc/file2',sha1hexes[seq],filesizes[seq],modified_datetimes[seq])
  entries.append(e)
  return entries
"""


class TestDataFiller(object):

  def __init__(self, dbms_params_dict=None):
    self.data_records_tuple_list = []
    self.conn_obj = dbfact.DBFactoryToConnection(dbms_params_dict)
    self.modquerier = dbperf.DBModificationQueryPerformer(dbms_params_dict)
    # self.make_tuple_list_data_for_dbinsert()

  def insert_file_n_get_file_id(self, file_values_dict):
    """

    :param file_values_dict:
    :return:
    """
    file_id = self.modquerier.insert_a_file_with_conventioned_filedict(file_values_dict)
    return file_id

  def insert_folder_n_get_folder_id(self, folderpath):
    """

    :param folderpath:
    :return:
    """
    folder_id = self.modquerier.insert_foldername_n_get_id_having_ossepfullpath(folderpath)

    return folder_id

  def make_tuple_list_data_for_dbinsert(self):
    self.data_records_tuple_list = make_tuple_list_data_for_dbinsert()

  def insert_data_into_db(self):
    """


    :return:
    """
    if len(self.data_records_tuple_list) == 0:
      self.make_tuple_list_data_for_dbinsert()

    for tuple_record in self.data_records_tuple_list:
      entry_type = tuple_record[0]
      if entry_type == FOLDER:
        folderpath = tuple_record[1]
        folder_id = self.insert_folder_n_get_folder_id(folderpath)
        print(folder_id, '==>>', folderpath)
      elif entry_type == FILE:
        # '/abc/abc/file1',sha1hexes[seq],filesizes[seq],modified_datetimes[seq]
        file_values_dict = {}
        tuple_values = tuple_record[1:]
        file_values_dict['filepath'] = tuple_values[0]
        file_values_dict['sha1hex'] = tuple_values[1]
        file_values_dict['filesize'] = tuple_values[2]
        file_values_dict['modified_datetime'] = tuple_values[3]
        self.insert_file_n_get_file_id(file_values_dict)

  def __str__(self):
    outstr = '''Instance of TestDataFiller:
    %s ''' % self.data_records_tuple_list
    return outstr


def test1():
  dbcreat.create_tables_and_initialize_root()
  dbcreat.delete_all_data_except_root()
  filler = TestDataFiller()
  file_data_dict = {
    'filepath': '/abc/test_dir/file1.txt',
    'sha1hex': sha1hexes[0],
    'filesize': 1000,
    'modified_datetime': modified_datetimes[0],
  }
  filler.insert_file_n_get_file_id(file_data_dict)
  pass


def test2():
  dbcreat.create_tables_and_initialize_root()
  dbcreator = dbcreat.DBTablesCreator()
  dbcreator.delete_all_data_except_root()
  filler = TestDataFiller()
  filler.insert_data_into_db()


def main():
  # test1()
  test2()


if __name__ == '__main__':
  main()
