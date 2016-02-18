#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
ShaModel.py


  Written on 2016-02-15 Luiz Lewis
'''
import datetime
import os
import shutil
import Sha1FileSystemComplementer as sha1complementer

def is_sha1hex_consistent(sha1hex):
  if len(sha1hex) <> 40:
    return False
  return True

class ShaItem(object):

  def __init__(self, sha1hex, filename, relative_parent_path, device_and_middle_path, modified_datetime, mockmode=False):
    self.sha1hex  = sha1hex
    self.filename = filename
    self.relative_parent_path = relative_parent_path
    self.device_and_middle_path = device_and_middle_path
    self.modified_datetime = modified_datetime
    self.mockmode = mockmode
    if not self.mockmode:
      self.verify_object_field_values()

  def verify_object_field_values(self):
    if not is_sha1hex_consistent(self.sha1hex):
      raise ValueError('ValueError: sha1hex given (%s) is not consistent.' %self.sha1hex)
    if not os.path.isdir(self.device_and_middle_path):
      raise OSError('OSError: device_and_middle_path given (%s) does not exist or is not mounted.' %self.device_and_middle_path)
    if not os.path.isdir(self.parent_folder_abspath):
      raise OSError('OSError: relative_parent_path given (%s) does not exist or is not mounted.' %self.relative_parent_path)
    if not os.path.isfile(self.file_abspath):
      raise OSError('OSError: file_abspath given (%s) does not exist.' %self.file_abspath)
    if type(self.modified_datetime) != datetime.datetime:
      raise TypeError('TypeError: modified_datetime given (%s) is not typed datetime.' %self.modified_datetime)

  def get_sha1hex_from_path(file_abspath):
    return sha1complementer.calculate_sha1hex_from_file(file_abspath)

  def equalize_dates_mock(self, otherShaItem):
    print 'ACTION: equalize_dates'
    print 'Old Target datetime:', otherShaItem.modified_datetime
    print 'Source-to-target datetime:', self.modified_datetime

  def equalize_dates(self, otherShaItem):
    if self.mockmode:
      self.equalize_dates_mock(otherShaItem)
      return True

  def move_target_file_mock(self, otherShaItem):
    print 'ACTION: move_target_file'
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_parent_path)
    print 'moving target to:', new_target_parent_path

  def file_exists_on_target(self, otherShaItem):
    target_device_and_middle_path = otherShaItem.self.device_and_middle_path
    target_parent_abspath = os.path.join(target_device_and_middle_path, self.relative_parent_path)
    target_file_abspath = os.path.join(target_parent_abspath, self.filename)
    if not os.path.isabs(target_file_abspath):
      return False
    if not os.path.isfile(target_file_abspath):
      raise ValueError('ValueError: supposed file exists either as dir or as link. Program cannot continue, please, handle this case manually.')
    return True

  def samefile_exists_on_target(self, otherShaItem):
    '''
    This method should be private. It should run after file_exists_on_target()
    :param otherShaItem:
    :return:
    '''
    # at this point, file exists on target, we should check its sha1-hash
    other_sha1hex = sha1complementer.calculate_sha1hex_from_file(otherShaItem.file_abspath)
    if other_sha1hex == self.sha1hex:
      return True
    return False

  def move_target_file(self, otherShaItem):
    if self.mockmode:
      self.move_target_file_mock(otherShaItem)
      return True
    if self.file_exists_on_target(otherShaItem):
      if self.samefile_exists_on_target(otherShaItem):
        self.queue_other_file_for_deletion(otherShaItem)
        return False
      else:
        otherShaItem.rename_file_on_the_fly_to_avoid_collision()
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_parent_path)
    if os.path.isabs(new_target_parent_path):
      if not os.path.isdir(new_target_parent_path):
        # raise exception
        raise OSError('OSError: an expected dirname (%s) is either a file or a link. Try to remove it and rerun.' %new_target_parent_path)
    if not os.path.isdir(new_target_parent_path):
      os.makedirs(new_target_parent_path)
    shutil.move(otherShaItem.abspath, new_target_parent_path)
    otherShaItem.relative_filepath = self.relative_parent_path
    return True

  def queue_other_file_for_deletion(self, otherShaItem):
    pass
    file_abspath = otherShaItem.file_abspath
    # QueuesRegister.queue_file_for_deletion_on_db(file_abspath)

  def rename_file_on_the_fly_to_avoid_collision(self):
    orig_filename = self.filename
    orig_filepath = self.file_abspath
    n_seq = 1
    while os.path.isfile(self.file_abspath):
      str_n_seq = str(n).zfill(3)
      self.filename = '%s.%s' %(self.filename, str_n_seq)
      if n > 999:
        error_msg = 'ValueError: cannot rename target file (%s) to avoid its loss by renaming source on it. number of tries = %d' %(orig_filepath, n_seq)
        raise ValueError(error_msg)
      n_seq += 1
    os.rename(orig_filepath, self.file_abspath)

  def move_rename_target_file_mock(self, otherShaItem):
    print 'ACTION: move_rename_target_file'
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_parent_path)
    #print 'moving target to:', new_target_parent_path
    new_filename = self.filename
    #print 'Source-to-target filename:', new_filename
    new_abspath = os.path.join(new_target_parent_path, new_filename)
    print 'Source-to-target abspath:', new_abspath

  def move_rename_target_file(self, otherShaItem):
    if self.mockmode:
      self.move_rename_target_file_mock(otherShaItem)
      return True
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_parent_path)
    new_filename = self.filename
    new_abspath = os.path.join(new_target_parent_path, new_filename)
    if os.path.isabs(new_abspath):
      if os.path.isfile(new_abspath):
        existing_sha1hex = self.get_sha1hex_from_path(new_abspath)
        if existing_sha1hex == self.sha1hex:
          otherShaItem.queue_me_for_later_deletion(cause='TARGET-FROM-SOURCE COPY EXISTS')
          return False
        else:
          raise OSError('OSError: TARGET-FROM-SOURCE COPY has a different sha-hash. Correct this manually and rerun.')
      else:
        raise OSError('OSError: Target-from-source exists and is not a file. Correct this manually and rerun.')
    # good to go
    shutil.move(otherShaItem.abspath, new_abspath)
    otherShaItem.abspath = new_abspath
    return True

  def nothing_to_do_mock(self):
    print 'Nothing to do! Files, bak and orig, are equal'

  def verify_action_to_do(self, otherShaItem):
    FILES_ARE_EQUAL = False
    NAMES_ARE_EQUAL = False
    RELATIVE_FOLDER_POSITIONS_ARE_EQUAL = False
    MODIFIED_DATETIMES_ARE_EQUAL = False
    if self.sha1hex == otherShaItem.sha1hex:
      FILES_ARE_EQUAL = True
    if self.filename == otherShaItem.filename:
      NAMES_ARE_EQUAL = True
    if self.relative_parent_path == otherShaItem.relative_parent_path:
      RELATIVE_FOLDER_POSITIONS_ARE_EQUAL = True
    if self.modified_datetime == otherShaItem.modified_datetime:
      MODIFIED_DATETIMES_ARE_EQUAL = True
    # 1st case: sha1 is the same
    if FILES_ARE_EQUAL:
      if NAMES_ARE_EQUAL:
        if RELATIVE_FOLDER_POSITIONS_ARE_EQUAL:
          if MODIFIED_DATETIMES_ARE_EQUAL:
            # nothing to do! Files, bak and orig, are equal
            if self.mockmode:
              self.nothing_to_do_mock()
            return
          else:
            # bak and orig, are equal, mdates are different, equalize them
            self.equalize_dates(otherShaItem)
            return
        else:
          # bak and orig, are equal, positions are different, move target
          self.move_target_file(otherShaItem)
          return
      else:
        # bak and orig, are equal by sha-hash; name and/or positions are different, move-rename target
        self.move_rename_target_file(otherShaItem)
        return
    else:
      # this 'else' represents more than one hypotheses
      # however, a 2nd process will cover these hypotheses
      # in thought, this 2nd process will queue target files
      #   not existing in source file system for deletion
      # so, nothing further needs to be done in this method
      pass
    return

  def __cmp__(self, otherShaItem):
    if self.sha1hex <> otherShaItem.sha1hex:
      return True
    return False

  def copy(self):
    copy_of_self = ShaItem( \
      self.sha1hex, \
      self.filename, \
      self.relative_parent_path, \
      self.device_and_middle_path, \
      self.modified_datetime, \
      self.mockmode, \
    )
    return copy_of_self

  @property
  def parent_folder_abspath(self):
    return os.path.join(self.device_and_middle_path, self.relative_parent_path)

  @property
  def file_abspath(self):
    return os.path.join(self.parent_folder_abspath, self.filename)

  def get_strdict(self):
    strdict = {}
    strdict['sha1hex'] = self.sha1hex
    strdict['filename'] = self.filename
    strdict['relative_parent_path'] = self.relative_parent_path
    strdict['device_and_middle_path'] = self.device_and_middle_path
    strdict['parent_folder_abspath'] = self.parent_folder_abspath
    strdict['file_abspath'] = self.file_abspath
    strdict['modified_datetime'] = self.modified_datetime
    return strdict

  def __str__(self):
    strdict = self.get_strdict()
    outstr = '''
  sha1hex                = '%(sha1hex)s'
  filename               = '%(filename)s'
  relative_parent_path   = '%(relative_parent_path)s'
  device_and_middle_path = '%(device_and_middle_path)s'
  parent_folder_abspath  = '%(parent_folder_abspath)s'
  file_abspath           = '%(file_abspath)s'
  modified_date          = '%(modified_datetime)s' ''' %strdict
    return outstr

def test1():

  mockmode = True

  sha1hex = '123'
  filename = 'blah.txt'
  relative_parent_path = 'oye/foldertest/'
  device_and_middle_path = '/media/friend/SAMSUNG/middlefolder/'
  modified_datetime = datetime.date(2016,1,1)

  shaItem = ShaItem(sha1hex, filename, relative_parent_path, device_and_middle_path, modified_datetime, mockmode)
  print shaItem

  # Case 1: Test against itself
  print '# Case 1: Test against itself'
  shaItem.verify_action_to_do(shaItem)

  # Case 2: same sha-hash, same name, different relative position
  print '# Case 2: same sha-hash, same name, different relative position'
  other_shaItem = shaItem.copy()
  other_shaItem.device_and_middle_path = '/media/friend/SAMSUNG2/middlefolder/'
  other_shaItem.relative_parent_path   = 'oye/foldertest2/'
  print other_shaItem
  shaItem.verify_action_to_do(other_shaItem)

  # Case 3: same sha-hash, different name, different relative position
  print '# Case 3: same sha-hash, different name, different relative position'
  other_shaItem = shaItem.copy()
  other_shaItem.device_and_middle_path = '/media/friend/SAMSUNG2/middlefolder/'
  other_shaItem.filename = 'blah_blah.txt'
  other_shaItem.relative_parent_path = 'oye/foldertest2/'
  print other_shaItem
  shaItem.verify_action_to_do(other_shaItem)

  # Case 4: only mdates differ
  print '# Case 4: only mdates differ'
  other_shaItem = shaItem.copy()
  other_shaItem.device_and_middle_path = '/media/friend/SAMSUNG2/middlefolder/'
  other_shaItem.modified_datetime = datetime.date(2016,1,2)
  print other_shaItem
  shaItem.verify_action_to_do(other_shaItem)


def main():
  test1()

if __name__ == '__main__':
  main()
