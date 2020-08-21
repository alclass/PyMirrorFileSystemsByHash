#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os


def find_directory_abspaths_in_abspath(abspath, sort=True):
  entries = os.listdir(abspath)
  abspathentries = [os.path.join(abspath, e) for e in entries]
  abspathentries = filter(lambda p: os.path.isdir(p), abspathentries)
  if sort:
    abspathentries = sorted(abspathentries)
  return list(abspathentries)


def find_directory_entries_in_abspath(abspath, sort=True):
  entries = os.listdir(abspath)
  abspathentries = [os.path.join(abspath, e) for e in entries]
  abspathentries = filter(lambda p: os.path.isdir(p), abspathentries)
  entries = [os.path.split(p)[1] for p in abspathentries]
  if sort:
    entries = sorted(entries)
  return entries


def find_file_abspaths_in_abspath(abspath):
  entries = os.listdir(abspath)
  abspathentries = [os.path.join(abspath, e) for e in entries]
  abspathentries = filter(lambda p: os.path.isfile(p), abspathentries)
  return list(abspathentries)


def find_file_entries_in_abspath(abspath):
  entries = os.listdir(abspath)
  abspathentries = [os.path.join(abspath, e) for e in entries]
  abspathentries = filter(lambda p: os.path.isfile(p), abspathentries)
  entries = [os.path.split(p)[1] for p in abspathentries]
  return entries


def create_path_if_it_doesnt_exist(dir_abspath):
  """
  This method doesn't have a unit test yet.
  @see unittests below
  """
  if not os.path.isdir(dir_abspath):
    os.makedirs(dir_abspath)


def verify_path_existence_then_raise_or_return_with_trailing_slash(abspath, allow_nonexistent=False):
  if abspath is None:
    error_msg = 'Directory abspath is not initialized (ie, it is None)'
    raise OSError(error_msg)
  if not allow_nonexistent:
    if not os.path.isdir(abspath):
      error_msg = 'Directory abspath %s does not exist' % abspath
      raise OSError(error_msg)
  if not abspath.endswith('/'):
    abspath += '/'
  return abspath


def process():
  pass


if __name__ == '__main__':
  process()
