#!/usr/bin/env python3
"""
cmm/maintainSqliteRootFolderYtidsRepo.py
lib/dirfilefs/ytids_functions.py
"""
# import lib.dirfilefs.ytids_maintainer as ytmt
import llib.dirfilefs.ytids_functions as ytfs
import default_settings as ds
DEFAULT_YTDIS_TABLENAME = 'sql_ytids'


def get_read_ytids_from_textfile():
  filename = ds.DEFAULT_YTIDSONLY_FILENAME  # 'youtube-ids.txt'
  ytids = ytfs.read_ytids_from_filenamebased_textfile(filename)
  return ytids


def adhoctest():
  ytids_folderpath = '/home/dados/VideoAudio/Yt videos/yt BRA Pol vi/Meteoro tmp yu'
  ytids_filename = 'z_ls-R_contents-name1234.txt'
  print('hi')
  # ytids_folderpath, ytids_filename
  ytids_o = ytfs.YtidsTxtNSqliteMaintainer(ytids_folderpath, ytids_filename)
  ytids_o.read_ytids_from_ytidsonly_textfile()
  print(ytids_o)


def process():

  # insert_difference_in_rootcontentfile()
  adhoctest()


if __name__ == '__main__':
  process()
