#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''

  Written on 2015-01-23 Luiz Lewis
'''

class FolderPathAjuster(object):
  '''
  This class has functionality to help ajust the abspath to the relpath, ie, the os-absolute path must
    be stripped of the device_and_middle_path, remaining the relative path (ie, the relpath)
  '''

  def fetch_folderpath_id(self, folderpath):
    '''
    The idea here is following: having a slash-separated path (or other 'sep'),
      find the folder_id of the ending folder
    :param folderpath:
    :return:
    '''
    relative_folder = folderpath[ len(self.device_and_middle_abspath) : ]
    foldernames = relative_folder.split(os.sep)
    if len(foldernames) == 1:
      return PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID
    ids_starting_trace_path = [PYMIRROR_DB_PARAMS.CONVENTIONED_TOP_ROOT_FOLDER_ID]
    return self.fetch_folderpath_id_recursive(foldernames[1:], ids_starting_trace_path)

  def is_path_good_in_relation_to_device_prefix_abspath(self, current_abspath):
    '''
    The logic is this: the device prefix path should start the current_abs_path
    If it's not so, current_abs_path is not good and an exception should be raised.

    :return:
    '''
    # 1st check: is it an OS path?
    if not os.path.isdir(current_abspath):
      error_msg = "Path [%s] does not exist or it's a file." %current_abspath
      raise Exception(error_msg)
    # 2nd check: does the device prefix path start it?
    if self.DEVICE_PREFIX_ABSPATH != current_abspath[ : len( self.DEVICE_PREFIX_ABSPATH ) ]:
      error_msg = "Abspath [%s] does not start with the device prefix path [%s]" %(current_abspath, self.DEVICE_PREFIX_ABSPATH)
      raise Exception(error_msg)

  def extract_current_abspath_minus_device_prefix(self, current_abspath):
    '''

    :param current_abspath:
    :return:
    '''
    current_abspath_minus_device_prefix = current_abspath[ len( self.DEVICE_PREFIX_ABSPATH ) : ]
    if not current_abspath_minus_device_prefix.startswith('/'):
      current_abspath_minus_device_prefix = '/' + current_abspath_minus_device_prefix
    return current_abspath_minus_device_prefix

  def are_split_pieces_good_in_relation_to_minus_path(self, pp):
    '''

    :param pp:
    :return:
    '''
    if len(pp) < 2:
      error_msg = '''Inconsistency in internal program list manipulation
      for finding root abs dir.  The process of finding the id of a directory
      is a recursive one, starting on ROOT, the / symbolized first folder.
      Somehow, this ROOT was lost. It may be a logical error.
      To help find further:
        1) '/'.split('/') is ['',''] AND
        2) '/a'.split('/') is ['','a']
      The condition that triggered this error is that list is smaller than 2 items.'''
      raise Exception(error_msg)

  def prepare_root_minus_path(self, target_abspath):
    '''

    :param target_abspath:
    :return:
    '''
    root_minus_path = self.extract_current_abspath_minus_device_prefix(target_abspath)
    if not root_minus_path.startswith('/'):
      root_minus_path = '/' + root_minus_path
    return root_minus_path

  def find_entry_id_for_root_minus_path(self, root_minus_path):
    pp = root_minus_path.split('/')
    self.are_split_pieces_good_in_relation_to_minus_path(pp)
    if pp == ['','']:
      return PYMIRROR_DB_PARAMS.CONVENTIONED_ROOT_ENTRY_ID
    return self.loop_on_to_find_entry_id_for_dirpath(pp, root_minus_path)


class DeviceAndMiddlePathSetter(object):


  def set_default_sqlite_db_filename(self):
    self.sqlite_db_filename = PYMIRROR_DB_PARAMS.SQLITE.HASHES_ETC_DATA_FILENAME

  def set_sqlite_db_filename(self, sqlite_db_filename=None):
    if sqlite_db_filename == None or sqlite_db_filename not in [str, unicode]:
      self.set_default_sqlite_db_filename()
      return
    self.sqlite_db_filename = sqlite_db_filename

  def set_default_device_and_middle_abspath(self):
    '''

    :return:
    '''
    self.device_and_middle_abspath = os.path.abspath('.')
    if not os.path.isdir(self.device_and_middle_abspath):
      raise OSError('Cannot establish DEFAULT device_and_middle_abspath as %s' %self.device_and_middle_abspath)

  def set_device_and_middle_abspath(self, device_and_middle_abspath=None):
    if device_and_middle_abspath == None or not os.path.isdir(device_and_middle_abspath):
      self.set_default_device_and_middle_abspath()
      return
    self.device_and_middle_abspath = device_and_middle_abspath

  def get_device_and_middle_abspath(self):
    if not os.path.isdir(self.device_and_middle_abspath):
      self.set_default_device_and_middle_abspath()
    return self.device_and_middle_abspath

  def set_sqlite_db_filepath(self, sqlite_db_filename):
    self.sqlite_db_filepath = os.path.join(self.get_device_and_middle_abspath, self.sqlite_db_filename)

  def verify_dbsqlitefile_existence(self):
    dbsqlitefile_abspath = os.path.join(self.DEVICE_PREFIX_ABSPATH, PYMIRROR_DB_PARAMS.SQLITE.HASHES_ETC_DATA_FILENAME)
    if not os.path.isfile(dbsqlitefile_abspath):
      dbinit = DBInit(self.DEVICE_PREFIX_ABSPATH)
      dbinit.verify_and_create_fs_entries_sqlite_db_table()


def get_args_to_dict():
  args_dict = {}
  for arg in sys.argv:
    if arg.startswith('-p='):
      device_root_abspath = arg [ len( '-p=') : ]
      args_dict['device_root_abspath'] = device_root_abspath
  return args_dict


def test1():
  #create_sqlite_db_file_on_root_folder()
  args_dict = get_args_to_dict()
  try:
    device_root_abspath = args_dict['device_root_abspath']
    sometests = SomeTests1(device_root_abspath)
    sometests.insert_root_record_on_db_table()
    sometests.insert_a_sample_file_on_db_table()
    sometests.list_files_and_folders_contents()
  except IndexError:
    print ('Parameter -p for device root abspath is missing.')

def test2():
  pass

def main():
  test2()

if __name__ == '__main__':
  main()