#!/usr/bin/env python3
"""
Usage:
  $ytids_functions.py [<videocodecom>] [-p=<directory_path>]
Where:
  [<videocodecom>] optional, it means the parameter to the -f flag in youtube-dl
  [-p=<directory_path>] optional, it means the target path where download will happen if needed

Example:
  $ytids_functions.py 278+249 -p="/media/External HD/Science/Bio videos"

Functionality:
  This script compares a list of ytids in a text file (usually "youtube-ids.txt")
  against a sqlite repo file with ytids.
    Those ytids not db-stored will be queued up for downloading.
    User confirmation will be asked in the terminal prompt.
"""
import os
import sys
import fs.dirfilefs.ytids_functions as ytfs
import fs.dirfilefs.ytids_maintainer as ytmt
import default_settings as ds
VIDEOCODES = ['278+249', '160+139', '160+140', '18', '392+139']
DEFAULT_VIDEOCODE = VIDEOCODES[0]
interpol_ytdlcomm = 'youtube-dl -w -f {videocodecomb} {url}'
interpol_yturl = 'https://www.youtube.com/watch?v={ytid}'


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

  def __init__(self, ppath=None, videocodecomb=None):
    self.ytids = []
    self.ytids_missing = []
    self.workdir_abspath = ppath
    self.videocodecomb = videocodecomb
    self._ytidsonly_filepath = None
    self.ytidsonly_filename = ds.DEFAULT_YTIDSONLY_FILENAME
    if self.workdir_abspath is None or not os.path.isdir(ppath):
      self.workdir_abspath = os.path.abspath('.')

  @property
  def ytidsonly_filepath(self):
    if self._ytidsonly_filepath is not None:
      return self._ytidsonly_filepath
    self._ytidsonly_filepath = os.path.join(self.workdir_abspath, self.ytidsonly_filename)
    return self._ytidsonly_filepath

  def read_ytids_on_ytidsonly_textfile(self):
    self.ytids = ytfs.read_ytids_from_ytidsonly_textfile(self.ytidsonly_filepath)
    scr_msg = 'Read %d ytids from [%s]' % (len(self.ytids), self.ytidsonly_filepath)
    print(self.ytids)
    print(scr_msg)

  def finding_missing_ytids_from_sqlitedb(self):
    maintainer = ytmt.YtidsSqliteMaintainer(True, self.workdir_abspath)
    self.ytids_missing = maintainer.extract_missing_sqlite_ytids_from(self.ytids)
    scr_msg = 'Missing %d ytids from [%s]' % (len(self.ytids_missing), maintainer.sqlite_abspath)
    print(scr_msg)

  def verify_current_folder(self):
    entries = os.listdir(self.workdir_abspath)
    local_ytids = ytfs.extract_ytids_from_filenames(entries)
    before_n_missing = len(self.ytids_missing)
    self.ytids_missing = [ytid for ytid in self.ytids_missing if ytid not in local_ytids]
    after_n_missing = len(self.ytids_missing)
    strnumbers = 'diff %d | missing-in-db %d | local %d' %(after_n_missing, before_n_missing, len(local_ytids))
    print('Verifying local_ytids', strnumbers, self.ytids_missing)

  def prepare_download(self):
    if len(self.ytids_missing) == 0:
      print('No ytids missing in db. Returning.')
      return
    scr_msg = 'Prepare download of %d videos' % len(self.ytids_missing)
    print(scr_msg)
    for i, ytid in enumerate(self.ytids_missing):
      url = interpol_yturl.format(ytid=ytid)
      comm = interpol_ytdlcomm.format(url=url, videocodecomb=self.videocodecomb)
      seq = i + 1
      print(seq, comm)

  def confirm_download(self):
    if len(self.ytids_missing) == 0:
      return False
    scr_msg = 'Confirm download of the %d videos above? (*Y/n) ([ENTER] means Y) => ' % len(self.ytids_missing)
    ans = input(scr_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def do_download(self):
    for i, ytid in enumerate(self.ytids_missing):
      url = interpol_yturl.format(ytid=ytid)
      comm = interpol_ytdlcomm.format(url=url, videocodecomb=self.videocodecomb)
      seq = i + 1
      print(seq, comm)
      os.system(comm)

  def process(self):
    self.read_ytids_on_ytidsonly_textfile()
    self.finding_missing_ytids_from_sqlitedb()
    self.verify_current_folder()
    self.prepare_download()
    if self.confirm_download():
      self.do_download()

  def __str__(self):
    outline = '<YtidsOnlyDownloader>\n'
    outline += self.ytidsonly_filepath
    return outline


def get_ppath_from_args(argv):
  for arg in argv:
    if arg.startswith('-h') or arg.startswith('--help'):
      print(__doc__)
      sys.exit(0)
    elif arg.startswith('-p='):
      ppath = arg[len('-p='):]
      if os.path.isdir(ppath):
        return ppath
  return None


def get_videocodecomb_from_args_or_default(argv):
  videocodelist = []
  for arg in argv:
    if arg in VIDEOCODES:
      videocodelist.append(arg)
  if len(videocodelist) > 0:
    return videocodelist[0]
  return DEFAULT_VIDEOCODE


def process(argv):
  """
  This function aims to transfer executing from a "dispatcher" script outside of this app
  """
  ppath = get_ppath_from_args(argv)
  videocodecomb = get_videocodecomb_from_args_or_default(argv)
  downloader = YtidsOnlyDownloader(ppath, videocodecomb)
  downloader.prepare_download()
  downloader.process()


if __name__ == '__main__':
  """
  "/media/friend/Bio EE Sci Soc 2T Orig/Yt vi/BRA Polit yt vi/Meteoro Brasil yu"
  """
  process(sys.argv)
