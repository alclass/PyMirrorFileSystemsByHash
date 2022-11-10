#!/usr/bin/env python3
"""
ytids_functions.py

Choices made for class YtidsTxtNSqliteMaintainer:
---------
1) either basedir is set "fix" at the beginning or it's looked up by path-descending
2) in either of the two cases above, an exception is raised if at least the sqlite file is absent
"""
import os
import sys
import fs.dirfilefs.ytids_functions as ytfs
import default_settings as ds
DEFAULT_VIDEOCODE = '18'
VIDEOCODES = ['278+249', '160+139', '160+140', '18', '392+139']


def get_path_param_arg_if_any():
  for arg in sys.argv:
    if arg.startswith('-p='):
      ppath = arg[len('-p='):]
      if os.path.isdir(ppath):
        return ppath
  return None


def move_to_working_dir():
  workdir_abspath = get_path_param_arg_if_any()
  if workdir_abspath is None:
    workdir_abspath = os.path.abspath('.')
  os.chdir(workdir_abspath)


class YtidsOnlyDownloader:

  def __init__(self, ppath=None, videocourse=None):
    self.ytids = []
    self.workdir = ppath
    self.videocourse = videocourse
    self._ytidsonly_filepath = None
    self.ytidsonly_filename = ds.DEFAULT_YTIDSONLY_FILENAME
    if self.workdir is None or not os.path.isdir(ppath):
      self.workdir = os.path.abspath('.')

  @property
  def ytidsonly_filepath(self):
    if self._ytidsonly_filepath is not None:
      return self._ytidsonly_filepath
    self._ytidsonly_filepath = os.path.join(self.workdir, self.ytidsonly_filename)
    return self._ytidsonly_filepath

  def find_ytids_on_ytidsonly_textfile(self):
    self.ytids = ytfs.read_ytids_from_ytidsonly_textfile(self.ytidsonly_filepath)
    print('ytids')
    print(self.ytids)

  def go_download(self):
    self.find_ytids_on_ytidsonly_textfile()
    print('go_download()', self.ytids)

  def __str__(self):
    outline = '<YtidsOnlyDownloader>\n'
    outline += self.ytidsonly_filepath
    return outline


def get_ppath_from_args(argv):
  for arg in argv:
    if arg.startswith('-p='):
      ppath = arg[len('-p='):]
      if os.path.isdir(ppath):
        return ppath
  return None


def get_videocodes_from_args(argv):
  videocodelist = []
  for arg in argv:
    videocodelist.append(arg)
  return videocodelist


def get_videocode_from_args(argv):
  """
  """
  videocodelist = argv
  for videocode in videocodelist:
    if videocode.startswith('-p='):
      continue
    elif videocode in VIDEOCODES:
      return videocode
  return DEFAULT_VIDEOCODE


def process(argv):
  """
  This function aims to transfer executing from a "dispatcher" script outside of this app
  """
  ppath = get_ppath_from_args(argv)
  print(ppath)
  videocode = get_videocode_from_args(argv)
  downloader = YtidsOnlyDownloader(ppath, videocode)
  print(downloader)
  downloader.go_download()


if __name__ == '__main__':
  """
  "/media/friend/Bio EE Sci Soc 2T Orig/Yt vi/BRA Polit yt vi/Meteoro Brasil yu"
  """
  process(sys.argv)
