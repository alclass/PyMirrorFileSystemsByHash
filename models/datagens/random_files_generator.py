#!/usr/bin/env python3
"""
models/datagens/random_files_generator.py

  creates random text-content files
  one use is to fill up disk space for stress testing its nominal storage capacity
  example: a 512Gb card memory might be recorded to its full capacity
    in order to avaliable its byte-size capacity

@created_at 2024-12-27
"""
import os
import random
import string
LF = '\n'
PREFIX_FOR_FILES_LINEPATH = 'F '
charcontent = string.ascii_uppercase + ' ' + string.digits + ' ' + string.ascii_lowercase


def generate_random_text_content(inisize=1024, trunksize=1024):
  charsize = inisize + random.randint(a=1, b=trunksize) * random.randint(a=1, b=trunksize)
  res = ''.join(random.choices(charcontent, k=charsize))
  print(res)
  print('size =', len(res))


class RandomFilesGenerator:
  DIR = 'DIR'
  FILE = 'FILE'
  MAX_DIRS_LEVEL = 500
  DEFAULT_N_FILES_TO_CREATE = 500

  def __init__(self, dirpath, n_files_to_create=None):
    if n_files_to_create is None:
      self.n_files_to_create = self.DEFAULT_N_FILES_TO_CREATE
    else:
      self.n_files_to_create = int(n_files_to_create)
    if dirpath is None or not os.path.isdir(dirpath):
      errmsg = 'dirpath %s is not valid. Please, enter a valid dirpath for random files creation.'
      raise OSError(dirpath)
    self.base_dirpath = dirpath

  def process(self):
    pass


def adhoc_test():
  generate_random_text_content()
  generate_random_text_content(inisize=0, trunksize=3)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
