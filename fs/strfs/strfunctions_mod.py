#!/usr/bin/env python3
"""
strfunctions_mod.py
"""
import os


def prepend_slash_if_needed(middlepath):
  try:
    if not middlepath.startswith('/'):
      middlepath = '/' + middlepath
    return middlepath
  except AttributeError:
    pass
  except ValueError:
    pass
  return ''


def put_ellipsis_in_str_middle(line, linecharsize=80):
  if line is None:
    return ''
  if len(line) <= linecharsize:
    return line
  sizediff = len(line) - linecharsize
  half_sizediff = sizediff // 2
  midpos = len(line) // 2
  p1_midpos = midpos - half_sizediff
  p2_midpos = midpos + half_sizediff
  p1 = line[: p1_midpos]
  p2 = line[p2_midpos:]
  newline = p1 + '...' + p2
  return newline


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


def clean_rename_filename_to(filename):
  if filename is None:
    return None
  name, ext = os.path.splitext(filename)
  newname = name.lstrip(' \t').rstrip(' \t\r\n').replace(':', ';')
  newext = ext.lstrip(' \t').rstrip(' \t\r\n').replace(':', ';')
  if newext is None or newext == '' or newext == '.':
    newfilename = newname
    return newfilename
  if not newext.startswith('.'):
    newext = '.' + newext
  newfilename = newname + newext
  return newfilename


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
