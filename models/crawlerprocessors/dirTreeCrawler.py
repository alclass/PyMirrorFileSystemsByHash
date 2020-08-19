#!/usr/bin/env python3
import os
from models.crawlerprocessors.config_reader import Config

config = Config()

try:
  BASE_FOLDER_ORIGIN = os.path.abspath(config.BASE_FOLDER_ORIGIN)
except AttributeError:
  BASE_FOLDER_ORIGIN = os.path.abspath('')

class DirWalker:
  def dir_walk(self):
    for entry, folders, files in os.walk(BASE_FOLDER_ORIGIN):
      print ('entry', entry)
      print ('folders', folders)
      print ('files', files)

def test_it():
  walker = DirWalker()
  walker.dir_walk()

if __name__ == '__main__':
  test_it()
