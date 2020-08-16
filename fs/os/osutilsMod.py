#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os


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
