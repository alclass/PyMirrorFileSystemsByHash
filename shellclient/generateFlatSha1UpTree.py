#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
This script dir-walks the TTC directory tree recording a md5sum file for files inside TTC courses folder
'''
import codecs, os, sys


def walk_up_tree(folder_abspath):

  sha1counter = 0
  for dirpath, dirnames, filenames in os.walk(folder_abspath):
    print('-'*40)
    print('Walking', dirpath)
    os.chdir(dirpath)
    if len(filenames) == 0:
      continue
    filenames.sort()
    text = ''
    if os.path.isfile('z-sha1sum.txt'):
      f = codecs.open('z-sha1sum.txt','r','utf-8')
      text = f.read()
      f.close()
    for filename in filenames:
      if text.find(filename) > -1:
        continue
      comm = 'sha1sum "%s" >> z-sha1sum.txt' %filename
      sha1counter += 1
      print(sha1counter, comm)
      os.system(comm)

def process():
  ok_to_process = False
  try:
    folder_abspath = os.path.abspath(sys.argv[1])
    if os.path.isdir(folder_abspath):
      ok_to_process = True
  except IndexError:
    pass
  if ok_to_process:
    walk_up_tree(folder_abspath)
    return
  print('Please, give a valid path that will be downwards walked at creating sha1sum-files store folder by folder.')


if __name__ == '__main__':
  process()
  #unittest.main()
