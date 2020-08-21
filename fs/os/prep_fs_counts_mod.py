#!/usr/bin/env python3
"""

"""
import os
import config


def commit_on_counter_rotate(session, commit_rotate_count, countlimit=None, finalcommit=False):
  commit_rotate_count += 1
  if countlimit is None:
    countlimit = config.COUNTER_ROTATE_SIZE_FOR_COMMITS
  if commit_rotate_count >= countlimit or finalcommit:
    if finalcommit:
      print('============== finalcommit ==============')
      print('finalcommit', finalcommit, commit_rotate_count)
      print('============== finalcommit ==============')
    print('session.commit() on count', commit_rotate_count)
    session.commit()
    commit_rotate_count = 0
  return commit_rotate_count


def extract_middlepath_for_files_or_subfolders_from_abspath(mount_abspath, abspath):
  if len(abspath) < len(mount_abspath):
    return None
  if abspath == mount_abspath:
    return ''
  remaining = abspath[len(mount_abspath) + 1:]
  remaining = remaining.strip('/')
  pp = remaining.split('/')
  middlepath = '/'.join(pp)
  return middlepath


def extract_middlepath_for_folders_from_abspath(mount_abspath, abspath):
  """
  examples
    1) None case
      mountpath is a/b/c and abspath is a/b/c
      dirname should be 'c'
      middlepath should be None
      (it's level 0, it's the root node)
    2) error case
      mountpath is a/b/c and abspath is a/b
      a ValueError exception should be raised
    3) empty middlepath
      mountpath is a/b/c and abspath is a/b/c/d
      dirname should be 'd'
      middlepath should be ''
      (it's level 1, empty middlepath)
    4) level 2 middlepath
      mountpath is a/b/c and abspath is a/b/c/d/e
      dirname should be 'e'
      middlepath should be 'd'
      (it's level 2, one-word middlepath)
    5) level 3 middlepath
      mountpath is a/b/c and abspath is a/b/c/d/e/f
      dirname should be 'f'
      middlepath should be 'd/e'
      (it's level 3, a two-piece separated by '/' middlepath)
  """
  if len(abspath) < len(mount_abspath):
    return None
  if abspath == mount_abspath:
    return None
  remaining = abspath[len(mount_abspath) + 1:]
  remaining = remaining.strip('/')
  pp = remaining.split('/')
  if len(pp) == 1:
    return ''
  middlepath = '/'.join(pp[:-1])
  return middlepath


def form_fil_in_mid_with_progress_percent_line(total_swept, totalf, middlepath, filename):
  filespercent = total_swept / totalf * 100
  line = '%d of %d %.2f%% [%s] in [%s]' % (total_swept, totalf, filespercent, filename, middlepath)
  return line


def show_mountpoint_src_n_trg_abspaths():
  pdict = config.get_mountpoint_datadirs_dict()
  source_abspath = pdict[config.MOUNTPOINT_SOURCEDATADIR_DICTKEY]
  target_abspath = pdict[config.MOUNTPOINT_SOURCEDATADIR_DICTKEY]
  print('source_abspath', source_abspath)
  print('target_abspath', target_abspath)


def prepare_sweep_folders_count(mount_abspath):
  """
  Because this function counts only dirs from os.walk()
    it uses sum(map(lambda)). The function (see below) that counts both dir and files uses a for-loop.
  """
  print('Counting all folders up dir tree. Please wait.')
  total_dirs = sum(map(lambda triple: len(triple[1]), os.walk(os.path.abspath(mount_abspath))))
  print('total_dirs =', total_dirs)
  return total_dirs


def prepare_sweep_files_count(mount_abspath):
  """
  Because this function counts only files from os.walk()
    it uses sum(map(lambda)). The function (see below) that counts both dir and files uses a for-loop.
  """
  print('Counting all files up dir tree. Please wait.')
  total_files = sum(map(lambda triple: len(triple[2]), os.walk(os.path.abspath(mount_abspath))))
  print('total_files =', total_files)
  return total_files


def prepare_sweep_files_n_folders_count(mount_abspath):
  """
  Because this function counts both dirs and files from os.walk()
    it doesn't use sum(map(lambda)) as the above use,
    rather it uses a for-loop for the two counters
  """
  total_dirs = 0
  total_files = 0
  for abspath, dirs, files in os.walk(os.path.abspath(mount_abspath)):
    total_dirs += len(dirs)
    total_files += len(files)
  return total_dirs, total_files


def sweep_files_showing_progress_percent(mount_abspath):
  """
  totalf = prepare_sweep_file_counts(mount_abspath)

  """
  totalf = 27821
  total_swept = 0
  for abspath, dirs, files in os.walk(os.path.abspath(mount_abspath)):
    for nf, filename in enumerate(sorted(files)):
      if mount_abspath == abspath:
        middlepath = ''
      else:
        middlepath = abspath[len(mount_abspath):]
      middlepath = middlepath.lstrip('/')
      total_swept += 1
      line = form_fil_in_mid_with_progress_percent_line(total_swept, totalf, filename, middlepath)
      print(line)


def process():
  pass


if __name__ == '__main__':
  process()
