#!/usr/bin/env python3
"""
~/bin/dlYouTubeRecupFSufixLeftovers.py

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
import os.path
import re
import shutil
# from sympy import expand
# import shutil
# import subprocess
# import sys
# import localuserpylib.ytfunctions.yt_str_fs_vids_sufix_lang_map_etc as ytstrfs
import localuserpylib.ytfunctions.yt_videoformat_fs as ytvf  # ytvf.YTVFTextExtractor
# import localuserpylib.regexfs.filenamevalidator_cls as fnval  # .FilenameValidator
import localuserpylib.ytfunctions.osentry_class as ose  # ose.OSEntry
# import localuserpylib.ytfunctions.cliparams_for_utubewhendub as clip  # clip.CliParam
# DEFAULT_AUDIOVIDEO_DOT_EXT = ose.DEFAULT_AUDIOVIDEO_DOT_EXT
# OSEntry = ose.OSEntry
# default_videodld_tmpdir = ose.default_videodld_tmpdir
VIDEO_DOT_EXTENSIONS = ose.VIDEO_DOT_EXTENSIONS
restr_fsufix_n_dotext = r"^(?P<name>.*?)(?P<dotfsufix>\.f[0-9]+)(?P<dot_ext>\.[A-Za-z0-9]+)$"
recmp_fsufix_n_dotext = re.compile(restr_fsufix_n_dotext)
restr_ending_ytid = r"^.*?[ ]\[(?P<ytid>[A-Za-z0-9_\-]{11})\]$"
recmp_ending_ytid = re.compile(restr_ending_ytid)


class VideoNameAttr:

  def __init__(self, filename):
    self.filename = filename
    self.videoformatoutput = None
    self.cannot_read_format_file = None
    self._name = None
    self._dot_ext = None
    self._dotfsufix = None
    self._ytid = None
    self.ytvf_o = None
    self.video_is_dubbed = None
    self.audiocode = None
    self.video_is_avmerged = None
    self.composedcode = None  # example: 160+249-0
    self.set_lang_dict()
    # self.ytvf_o = ytvf.YTVFTextExtractor()

  def load_videoformatoutput(self):
    if not os.path.isfile(self.filename_w_ext_txt):
      self.cannot_read_format_file = True
      return
    try:
      self.videoformatoutput = open(self.filename_w_ext_txt).read()
      self.cannot_read_format_file = False
    except (IOError, OSError):
      self.cannot_read_format_file = True

  def set_lang_dict(self):
    self.load_videoformatoutput()
    if not self.cannot_read_format_file:
      self.ytvf_o = ytvf.YTVFTextExtractor(self.videoformatoutput)

  @property
  def langdict(self):
    if self.ytvf_o:
      return self.ytvf_o.langdict
    return {}

  @property
  def filename_w_ext_txt(self):
    try:
      fn_w_ext_txt = f"{self.name}.txt"
      return fn_w_ext_txt
    except AttributeError:
      pass
    return None

  @property
  def twolettlangs_iffound(self):
    return list(self.langdict.keys())

  def try_regexp_match(self):
    matchsufixes = recmp_fsufix_n_dotext.match(self.filename)
    if matchsufixes:
      self._name = matchsufixes.group('name')
      self._dotfsufix = matchsufixes.group('dotfsufix')
      self._dot_ext = matchsufixes.group('dot_ext')
      matchytid = recmp_ending_ytid.match(self._name)
      if matchytid:
        self._ytid = matchytid.group('ytid')
        # the ytid is already valid because it comes from a regexp
        # so this following part may be commented-out
        # if not ytstrfs.is_str_a_ytid(self._ytid):
        #   errmsg = f"Error: [{self._ytid}] is not a valid ytid."
        #   raise ValueError(errmsg)

  @property
  def ext(self):
    _ext = self.dot_ext
    if _ext:
      return _ext.lstrip('.')
    return None

  @property
  def fsufix(self):
    dotfsufix = self.dotfsufix
    if dotfsufix:
      return dotfsufix.lstrip('.')
    return None

  @property
  def dotfsufix(self):
    if self._dotfsufix is None:
      self.try_regexp_match()
    return self._dotfsufix

  @property
  def dot_ext(self):
    if self._dot_ext is None:
      self.try_regexp_match()
    return self._dot_ext

  @property
  def name(self):
    if self._name is None:
      self.try_regexp_match()
    return self._name

  @property
  def ytid(self):
    if self._ytid is None:
      self.try_regexp_match()
    return self._ytid

  def is_complete(self):
    if self.ytid and self.name and self.dot_ext:
      return True
    return False

  def is_ytid_autodubbed_by_formatfile(self):
    """

    :return:
    """
    try:
      # the idea is to instropect read filetext so that autodubbed codes are found or not
      self.ytvf_o = ytvf.YTVFTextExtractor(self.videoformatoutput)
      self.ytvf_o.find_audio_formats_or_the_smaller_video()
      # self.known_2lett_langs = self.ytvf_o.find_languages_knowing_audiocode()
      if len(self.twolettlangs_iffound):
        return True
      # self.langdict = ytstrfs.fetch_langdict_w_videoformatoutput()
      return False
    except FileNotFoundError:
      pass
    return False

  def __str__(self):
    outstr = f"""{self.__class__.__name__}
    filename = {self.filename}
    name = {self.name}
    dotfsufix = {self.dotfsufix} | fsufix = {self.fsufix}
    dot_ext = {self.dot_ext} | ext = {self.ext}
    ytid = {self.ytid} | is autodubbed = {self.is_ytid_autodubbed_by_formatfile()}
    langdict = {self.langdict}
    filename_w_ext_txt =  {self.filename_w_ext_txt}
    """
    return outstr


class AutodubbedFinder:

  def __init__(self, dirpath=None):
    self.dirpath = dirpath
    self.incompl_vfilenames = []
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
      attr_o = VideoNameAttr(fn)
      # is_complete() assures filename has name, ext and ytid (fsufix is not checked)
      if not attr_o.is_complete():
        continue
      comm = f"yt-dlp -F {attr_o.ytid} > {attr_o.filename_w_ext_txt}"
      print(comm)
      os.system(comm)
      if attr_o.is_ytid_autodubbed_by_formatfile():
        self.copy_fsufix_file_to_canonical(attr_o)
        self.autodubbed_ytids.append(attr_o.ytid)
      else:
        self.nonautodubbed_ytids.append(attr_o.ytid)
        self.attrib_ytids.append(attr_o)
      self.scrape_videoformat_output(attr_o)

  def scrape_videoformat_output(self):
    videoformatsoutput = open(self.attr_o.filename_w_ext_txt).read()
    extractor = ytvf.YTVFTextExtractor(videoformatsoutput)


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
    self.save_ytids_textfiles()


def adhoctest2():
  fn = ("2025-08-08 1' Rodrigo Vianna comenta a cronologia de ações"
        " golpistas do bolsonarismo feita por Paulo Motoryn [bFYD24JM6dI].f160.mp4")
  attr_o = VideoNameAttr(fn)
  print(attr_o)
  print('is complete', attr_o.is_complete())


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
  finder = AutodubbedFinder()
  finder.listfiles()


if __name__ == '__main__':
  """
  adhoctest2()
  """
  # process()
  adhoctest2()
  # adhoctest1()
