#!/usr/bin/env python3
"""
cmm/yt/dlYouTubeRecupFSufixLeftovers.py

This script looks up incomplete downloaded files in a folder,
  fetches their video-format-lists and then outputs videoids that are
  autodubbed into an output textfile and
  those that are not autodubbed into another output.

These outputfiles become input for another script that will
  try to finish the incomplete downloads, observing:

a) the autodubbed videos may be downloaded with the parameter --useinputfile
  of script dlYouTubeWhenThereAreDubbed.py
  (the script here does a copying from the fsufix file (available) to the canonical filename)
  (this operation is necessary for the download continuation with dlYouTubeWhenThereAreDubbed.py)

b) the non-autodubbed videos are, in a simpler manner, downloaded
  with script "dlYouTubeWithIdsOnTxtFile2.py <format-combination>"

Notice this script does not continue the downloads, just prepare the output textfiles
  for the two options above. The user needs to run the follow-up scripts mentioned.

"""
import os
import re
import shutil
# import subprocess
# import sys
import cmm.yt.uTubeVideoFormatsExtractor as ytextr  # ytextr.YTVFTextExtractor
import cmm.yt.lib.os.osentry_class as ose  # ose.OSEntry
# DEFAULT_AUDIOVIDEO_DOT_EXT = ose.DEFAULT_AUDIOVIDEO_DOT_EXT
# OSEntry = ose.OSEntry
# default_videodld_tmpdir = ose.default_videodld_tmpdir
VIDEO_DOT_EXTENSIONS = ose.VIDEO_DOT_EXTENSIONS
restr_fsufix_n_dotext = r"^(?P<name>.*?)(?P<dotfsufix>\.f[0-9]+)(?P<dot_ext>\.[A-Za-z0-9]+)$"
recmp_fsufix_n_dotext = re.compile(restr_fsufix_n_dotext)
restr_ending_ytid = r"^.*?[ ]\[(?P<ytid>[A-Za-z0-9_\-]{11})\]$"
recmp_ending_ytid = re.compile(restr_ending_ytid)


class AutodubbedFinder:

  def __init__(self, dirpath=None):
    self.dirpath = dirpath
    self.incompl_vfilenames = []
    self.attrib_ytids = []
    self.langdict = {}
    self.autodubbed_ytids = []
    self.nonautodubbed_ytids = []
    if self.dirpath is None:
      self.dirpath = os.path.abspath('.')

  def fetch_videofiles_in_dir(self):
    entries = os.listdir(self.dirpath)
    self.incompl_vfilenames = filter(lambda fn: fn.endswith(tuple(VIDEO_DOT_EXTENSIONS)), entries)

  def listfiles(self):
    self.fetch_videofiles_in_dir()
    for i, fn in enumerate(self.incompl_vfilenames):
      print(i+1, fn)
    print(self.dirpath)

  def copy_fsufix_file_to_canonical(self, attr_o):
    if attr_o.fsufix is not None:
      fn = attr_o.filename
      fp = os.path.join(self.dirpath, fn)
      canonical_fn = attr_o.canonical_fn
      canonical_fp = os.path.join(self.dirpath, canonical_fn)
      if not os.path.exists(canonical_fp):
        scrmsg = f"Copying to [{canonical_fn}]"
        print(scrmsg)
        shutil.copy2(fp, canonical_fp)

  def onlinefetch_video_formats(self):
    for i, fn in enumerate(self.incompl_vfilenames):
      seq = i + 1
      attr_o = FileNameNFormatsVideo(fn)
      # is_complete() assures filename has name, ext and ytid (fsufix is not checked)
      if not attr_o.is_complete():
        continue
      if not os.path.isfile(attr_o.filename_w_ext_txt):
        comm = f'yt-dlp -F {attr_o.ytid} > "{attr_o.filename_w_ext_txt}"'
        scrmsg = f"\t {seq} => Fetching videoformats for {attr_o.ytid}"
        print(scrmsg)
        print(comm)
        os.system(comm)
      else:
        scrmsg = f"\t {seq} => Already fetched {attr_o.ytid}"
        print(scrmsg)
      if attr_o.is_ytid_autodubbed_by_formatfile():
        self.copy_fsufix_file_to_canonical(attr_o)
        self.autodubbed_ytids.append(attr_o.ytid)
      else:
        self.nonautodubbed_ytids.append(attr_o.ytid)
        self.attrib_ytids.append(attr_o)
      self.scrape_videoformat_output(attr_o)

  def scrape_videoformat_output(self, attr_o):
    print('\t => \t scrape_videoformat_output()')
    videoformatsoutput = open(attr_o.filename_w_ext_txt).read()
    extractor = ytextr.YTVFTextExtractor(videoformatsoutput)
    self.langdict = extractor.langdict
    print(self.langdict)

  def save_autodubbed_ytids(self):
    """
    ytvf
    :return:
    """
    pass

  def save_nonautodubbed_ytids(self):
    pass

  def save_ytids_textfiles(self):
    self.save_autodubbed_ytids()
    self.save_nonautodubbed_ytids()

  def process(self):
    self.fetch_videofiles_in_dir()
    self.onlinefetch_video_formats()
    # self.save_ytids_textfiles()

  def __str__(self):
    outstr = ""
    for attr_o in self.attrib_ytids:
      outstr += str(attr_o) + "\n"
    return outstr


def adhoctest1():
  fn = ("2025-08-08 1' Rodrigo Vianna comenta a cronologia de ações"
        " golpistas do bolsonarismo feita por Paulo Motoryn [bFYD24JM6dI].f160.mp4")
  matchobj = recmp_fsufix_n_dotext.match(fn)
  print(fn)
  print('match', matchobj)
  if matchobj:
    name = matchobj.group('name')
    fsufix = matchobj.group('dotfsufix')
    dot_ext = matchobj.group('dot_ext')
    print('name', name)
    print('fsufix', fsufix)
    print('dot_ext', dot_ext)
    match2 = recmp_ending_ytid.match(name)
    if match2:
      ytid = match2.group(1)
      print('match2', match2)
      print('ytid', ytid)


def process():
  """
  """
  dirpath = "/media/friend/BRAPol SSD2T ori/BRA Pol et-al vi/BRA Pol vi/Plantão Bra ytvi/videodld_tmpdir/f160"
  os.chdir(dirpath)
  finder = AutodubbedFinder(dirpath)
  # finder.listfiles()
  # print('finder.langdict', finder.langdict)
  finder.process()
  print(finder)


if __name__ == '__main__':
  """
  # adhoctest1()
  adhoctest2()
  """
  process()
