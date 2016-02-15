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

  def __init__(self, sha1hex, filename, relative_filepath, device_and_middle_path, modified_datetime, mockmode=False):
    if not mockmode:
      if not is_sha1hex_consistent(sha1hex):
        raise ValueError('ValueError: sha1hex given (%s) is not consistent.' %sha1hex)
    self.sha1hex  = sha1hex
    self.filename = filename
    self.relative_filepath = relative_filepath
    self.device_and_middle_path = device_and_middle_path
    self.modified_datetime = modified_datetime
    self.mockmode = mockmode


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
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_filepath)
    print 'moving target to:', new_target_parent_path

  def move_target_file(self, otherShaItem):
    if self.mockmode:
      self.move_target_file_mock(otherShaItem)
      return True
    if self.does_a_copy_exists_on_target():
      self.queue_for_deletion(otherShaItem)
      return False
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_filepath)
    if os.path.isabs(new_target_parent_path):
      if not os.path.isdir(new_target_parent_path):
        # raise exception
        raise OSError('OSError: an expected dirname (%s) is either a file or a link. Try to remove it and rerun.' %new_target_parent_path)
    if not os.path.isdir(new_target_parent_path):
      os.makedirs(new_target_parent_path)
    shutil.move(otherShaItem.abspath, new_target_parent_path)
    otherShaItem.relative_filepath = self.relative_filepath
    return True

  def move_rename_target_file_mock(self, otherShaItem):
    print 'ACTION: move_rename_target_file'
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_filepath)
    #print 'moving target to:', new_target_parent_path
    new_filename = self.filename
    #print 'Source-to-target filename:', new_filename
    new_abspath = os.path.join(new_target_parent_path, new_filename)
    print 'Source-to-target abspath:', new_abspath

  def move_rename_target_file(self, otherShaItem):
    if self.mockmode:
      self.move_rename_target_file_mock(otherShaItem)
      return True
    new_target_parent_path = os.path.join(otherShaItem.device_and_middle_path, self.relative_filepath)
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
    if self.relative_filepath == otherShaItem.relative_filepath:
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
      self.relative_filepath, \
      self.device_and_middle_path, \
      self.modified_datetime, \
      self.mockmode, \
    )
    return copy_of_self

  @property
  def parent_folder_abspath(self):
    return os.path.join(self.device_and_middle_path, self.relative_filepath)

  @property
  def abspath(self):
    return os.path.join(self.parent_folder_abspath, self.filename)

  def get_strdict(self):
    strdict = {}
    strdict['sha1hex'] = self.sha1hex
    strdict['filename'] = self.filename
    strdict['relative_filepath'] = self.relative_filepath
    strdict['device_and_middle_path'] = self.device_and_middle_path
    strdict['parent_folder_abspath'] = self.parent_folder_abspath
    strdict['abspath'] = self.abspath
    strdict['modified_datetime'] = self.modified_datetime
    return strdict

  def __str__(self):
    strdict = self.get_strdict()
    outstr = '''
  sha1hex           = '%(sha1hex)s'
  filename          = '%(filename)s'
  relative_filepath = '%(relative_filepath)s'
  device_and_middle_path = '%(device_and_middle_path)s'
  parent_folder_abspath  = '%(parent_folder_abspath)s'
  abspath           = '%(abspath)s'
  modified_date     = '%(modified_datetime)s' ''' %strdict
    return outstr

def test1():

  mockmode = True

  sha1hex = '123'
  filename = 'blah.txt'
  relative_filepath = 'oye/foldertest/'
  device_and_middle_path = '/media/friend/SAMSUNG/middlefolder/'
  modified_datetime = datetime.date(2016,1,1)

  shaItem = ShaItem(sha1hex, filename, relative_filepath, device_and_middle_path, modified_datetime, mockmode)
  print shaItem

  # Case 1: Test against itself
  print '# Case 1: Test against itself'
  shaItem.verify_action_to_do(shaItem)

  # Case 2: same sha-hash, same name, different relative position
  print '# Case 2: same sha-hash, same name, different relative position'
  other_shaItem = shaItem.copy()
  other_shaItem.device_and_middle_path = '/media/friend/SAMSUNG2/middlefolder/'
  other_shaItem.relative_filepath = 'oye/foldertest2/'
  print other_shaItem
  shaItem.verify_action_to_do(other_shaItem)

  # Case 3: same sha-hash, different name, different relative position
  print '# Case 3: same sha-hash, different name, different relative position'
  other_shaItem = shaItem.copy()
  other_shaItem.device_and_middle_path = '/media/friend/SAMSUNG2/middlefolder/'
  other_shaItem.filename = 'blah_blah.txt'
  other_shaItem.relative_filepath = 'oye/foldertest2/'
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
