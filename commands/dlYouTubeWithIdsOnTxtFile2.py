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

Detail in where the sqlite repo is found:
  In "normal" usage, a look-up path descending will search for the sqlite repo file.
  However, there are instances where this is not desiderable because there is no sqlite repo file available.
  In this latter case, parameter "--create", passed in as argument (sys.argv), will create a new repo file available
  in the working (executing or passed) directory.

Example (with parameter --create)
  $ytids_functions.py 278+249 --create -p="/media/External HD/Science/Bio videos"
  1) without --create, a path descending lookup will happen
  2) with --create, either the sqlite repo file is used in locus (working directory or the one passed with -p)
     or a new one is create there
"""
import os
import sqlite3
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

  def __init__(
      self,
      ppath=None,
      videocodecomb=None,
      create_sqlite_on_workdir=False,
      extrarepo_ytids_sqlitedirpath=None
  ):
    self.ytids = []
    self.ytids_missing = []
    self.workdir_abspath = ppath
    self.extrarepo_ytids_sqlitedirpath = extrarepo_ytids_sqlitedirpath
    self.videocodecomb = videocodecomb
    # create_sqlite_on_workdir is equivalent to "not" bool_find_root_by_pathdesc (and create it if not exists)
    self.create_sqlite_on_workdir = create_sqlite_on_workdir
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

  def verify_create_flag_n_create_sqlitefile_if_needed(self):
    sqlite_filename = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
    sqlite_filepath = os.path.join(self.workdir_abspath, sqlite_filename)
    if self.create_sqlite_on_workdir and not os.path.isfile(sqlite_filepath):
      scr_msg = 'Do you want to create sqlite repo file as [' + sqlite_filepath + \
                ']? [script cannot continuing without it under parameter --create] (*Y/n)? => '
      ans = input(scr_msg)
      if ans in ['Y', 'y', '']:
        _ = sqlite3.connect(sqlite_filepath)
        print('Created sqlitefile', sqlite_filepath)
      else:
        print('Stopping program, cannot continue for it needs the sqlite file on folder.')
        sys.exit(1)

  def finding_missing_ytids_from_sqlitedb(self):
    """
    Maintainer class has 4 input parameters
    Maintainer(
        bool_find_root_by_pathdesc=True,
        rootdirpath=None,
        txtfilename_if_known=None,
        extrarepo_ytids_sqlitedirpath=None
    )
    """
    self.verify_create_flag_n_create_sqlitefile_if_needed()
    bool_find_root_by_pathdesc = not self.create_sqlite_on_workdir
    maintainer = ytmt.YtidsSqliteMaintainer(
      bool_find_root_by_pathdesc,
      self.workdir_abspath,
      self.ytidsonly_filepath,
      self.extrarepo_ytids_sqlitedirpath
    )
    self.ytids_missing = maintainer.extract_missing_sqlite_ytids_from(self.ytids)
    if self.extrarepo_ytids_sqlitedirpath and len(self.ytids_missing) > 0:
      self.ytids_missing = maintainer.extract_ytids_existing_in_extrarepo(self.ytids_missing)
    scr_msg = 'Missing %d ytids from [%s]' % (len(self.ytids_missing), maintainer.sqlite_filepath)
    print(scr_msg)

  def verify_current_folder(self):
    entries = os.listdir(self.workdir_abspath)
    local_ytids = ytfs.extract_ytids_from_filenames(entries)
    before_n_missing = len(self.ytids_missing)
    self.ytids_missing = [ytid for ytid in self.ytids_missing if ytid not in local_ytids]
    after_n_missing = len(self.ytids_missing)
    strnumbers = 'diff %d | missing-in-db %d | local %d' % (after_n_missing, before_n_missing, len(local_ytids))
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
  ppath_n_create_dict = {'ppath': None, 'create': False}
  for arg in argv:
    if arg.startswith('-h') or arg.startswith('--help'):
      print(__doc__)
      sys.exit(0)
    elif arg.startswith('--create'):
      ppath_n_create_dict['create'] = True
    elif arg.startswith('-p='):
      ppath = arg[len('-p='):]
      if os.path.isdir(ppath):
        ppath_n_create_dict['ppath'] = ppath
  return ppath_n_create_dict


def get_videocodecomb_from_args_or_default(argv):
  videocodelist = []
  for arg in argv:
    if arg in VIDEOCODES:
      videocodelist.append(arg)
  if len(videocodelist) > 0:
    return videocodelist[0]
  return DEFAULT_VIDEOCODE


def get_extrarepo_ytids_sqlitepath_from_args(argv):
  sqlitepath = None
  for arg in argv:
    if arg.startswith('-e='):
      sqlpath = arg[len('-e='):]
      if os.path.isdir(sqlpath):
        sqlitepath = sqlpath
        break
  return sqlitepath


def process(argv):
  """
  This function aims to transfer executing from a "dispatcher" script outside of this app
  """
  ppath_n_create_dict = get_ppath_from_args(argv)
  videocodecomb = get_videocodecomb_from_args_or_default(argv)
  ppath = ppath_n_create_dict['ppath']
  bool_create = ppath_n_create_dict['create']
  extrarepo_ytids_sqlitepath = get_extrarepo_ytids_sqlitepath_from_args(argv)
  downloader = YtidsOnlyDownloader(ppath, videocodecomb, bool_create, extrarepo_ytids_sqlitepath)
  downloader.prepare_download()
  downloader.process()


if __name__ == '__main__':
  """
  "/media/friend/Bio EE Sci Soc 2T Orig/Yt vi/BRA Polit yt vi/Meteoro Brasil yu"
  """
  process(sys.argv)
