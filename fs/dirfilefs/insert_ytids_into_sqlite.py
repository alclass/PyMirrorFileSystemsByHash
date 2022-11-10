#!/usr/bin/env python3
"""
insert_ytids_into_sqlite.py

Choices made for class YtidsTxtNSqliteMaintainer:
---------
"""
import os
import fs.dirfilefs.ytids_functions as ytfs
import fs.dirfilefs.ytids_maintainer as ytmt
import default_settings as ds


EXTENSIONS = ['mp4', 'mp3', 'm4a', 'webm', 'mkv', 'empty', 'txt']


def lookup_ytids_in_filenames(filenames):
  ytids = []
  for filename in filenames:
    _, dotext = os.path.splitext(filename)
    ext = dotext.lstrip('.')
    if ext in EXTENSIONS:
      ytid = ytfs.extract_ytid_from_filename(filename)
      ytids.append(ytid)
  return ytids


class DirTreeYtidCollector:

  def __init__(self, walkstartpath):
    self.ytids = []
    self.walkstartpath = walkstartpath
    self.walk_dirtree_for_collecting_ytids()

  def walk_dirtree_for_collecting_ytids(self):
    for ppath, _, filenames in os.walk(self.walkstartpath):
      print('Walking', ppath)
      ytids = lookup_ytids_in_filenames(filenames)
      print('This folder has', ytids)
      self.ytids += ytids

  def read_ytids_file(self):
    # ytmt_obj = ytmt.YtidsSqliteMaintainer(True, self.walkstartpath)
    ytidsonly_filename = ds.DEFAULT_YTIDSONLY_FILENAME
    filepath = os.path.join(self.walkstartpath, ytidsonly_filename)
    print('Sqlite filepath', filepath)
    ytids_from_file = ytfs.read_ytids_from_ytidsonly_textfile(filepath)
    for ytid in ytids_from_file:
      print(ytid)
    return ytids_from_file

  def insertor(self):
    ytmt_obj = ytmt.YtidsSqliteMaintainer(True, self.walkstartpath)
    # n_inserted = ytmt_obj.insert_ytids(self.ytids)
    n_inserted = ytmt_obj.insert_ytids(self.ytids)
    print('missing', len(n_inserted))

  def show_ytids(self):
    for ytid in self.ytids:
      print(ytid)


def walk_dirtree_for_collecting_ytids():
  walkstartpath = '/media/friend/Bio EE Sci Soc 2T Orig/Yt vi/BRA Polit yt vi/Meteoro Brasil yu'
  dirtree = DirTreeYtidCollector(walkstartpath)
  dirtree.walk_dirtree_for_collecting_ytids()
  # dirtree.insertor()
  _ = dirtree.read_ytids_file()  # ytids


def process():
  # insert_difference_in_rootcontentfile()
  walk_dirtree_for_collecting_ytids()


if __name__ == '__main__':
  process()
