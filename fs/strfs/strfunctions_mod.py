#!/usr/bin/env python3
"""
strfunctions_mod.py
"""


def any_dir_in_path_startswith(fpath, str_startswith):
  if fpath is None:
    return False
  try:
    pp = fpath.split('/')
  except (AttributeError, TypeError):
    # examples: a number will generate an AttributeError; a binary will generate a TypeError
    return False
  for foldername in pp:
    if foldername.startswith(str_startswith):
      return True
  return False


# noinspection PyTypeChecker
def adhoc_test():
  def print_result(phrase, p_str_startswith):
    print('for [', phrase, '] starting with [', p_str_startswith, '], return is', bool_ret)
  f = 'mp3s bla bla'
  str_startswith = 'mp3s '
  bool_ret = any_dir_in_path_startswith(f, str_startswith)
  print_result(f, str_startswith)
  f = 3
  bool_ret = any_dir_in_path_startswith(f, str_startswith)
  print_result(f, str_startswith)
  f = bytes('fasdfalsdfa'.encode('utf8'))
  bool_ret = any_dir_in_path_startswith(f, str_startswith)
  print_result(f, str_startswith)
  f = 'z-del'
  str_startswith = f
  bool_ret = any_dir_in_path_startswith(f, str_startswith)
  print_result(f, str_startswith)


if __name__ == '__main__':
  adhoc_test()
