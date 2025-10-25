#!/usr/bin/env python3
"""
cmm/yt/ytids/file_n_formats_clss.py
  Contains, at the time of writing, classes:
   1 - FilenameNFormatsVideo which videoformats to a YouTube video
   2 - LangAttr which gives related YouTube autodubs and connected info
"""
import os
import re
import cmm.yt.ytids.ytstrfs_etc as ytfs
import cmm.yt.uTubeVideoFormatsExtractor as ytextr  # ytextr.YTVFTextExtractor
TWOLETTER_N_LANGUAGENAME_DICTMAP = ytfs.TWOLETTER_N_LANGUAGENAME_DICTMAP
restr_fsufix_n_dotext = r"^(?P<name>.*?)(?P<dotfsufix>\.f[0-9]+)(?P<dot_ext>\.[A-Za-z0-9]+)$"
recmp_fsufix_n_dotext = re.compile(restr_fsufix_n_dotext)
restr_ending_ytid = r"^.*?[ ]\[(?P<ytid>[A-Za-z0-9_\-]{11})\]$"
recmp_ending_ytid = re.compile(restr_ending_ytid)


class FilenameNFormatsVideo:

  def __init__(self, filename, folderpath=None, vf_txt_fn=None):
    self.filename = filename
    self.folderpath = folderpath or os.path.abspath('.')
    self._vformats_txtfn = vf_txt_fn
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

  @property
  def vformats_txtfn(self):
    if self._vformats_txtfn is None:
      self._vformats_txtfn = f"{self.name}.txt"
    return self._vformats_txtfn

  @property
  def vformats_txtfp(self) -> str | None:
    if self.vformats_txtfn is None:
      return None
    return os.path.join(self.folderpath, self.vformats_txtfn)

  @property
  def filepath(self):
    return os.path.join(self.folderpath, self.filename)

  def load_videoformatoutput(self):
    if not os.path.isfile(self.vformats_txtfp):
      self.cannot_read_format_file = True
      return
    try:
      self.videoformatoutput = open(self.vformats_txtfp).read()
      self.cannot_read_format_file = False
    except (IOError, OSError):
      self.cannot_read_format_file = True

  def set_lang_dict(self):
    self.load_videoformatoutput()
    if not self.cannot_read_format_file:
      self.ytvf_o = ytextr.YTVFTextExtractor(self.videoformatoutput)

  @property
  def langdict(self):
    if self.ytvf_o:
      return self.ytvf_o.langdict
    return {}

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
      self.ytvf_o = ytextr.YTVFTextExtractor(self.videoformatoutput)
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
    filename_w_ext_txt =  {self.vformats_txtfn}
    """
    return outstr


class LangAttr:
  """
  This class models attributes necessary for autodubbed audio-only-codes.
  It's useful for client scripts that download or rename audiofiles that are merged with videofiles.

  Notice:
    that this class is not intended for non-mergeable videofiles (example: code 249-5),
    but may be used with non-mergeable videos (example: code 91-3).
  Legend:
    249 is an audio-only code
      249-5 represents an audiofile of language '5'
    91 is an already merged video code
      91-3 represents a video of language '3'
  """

  def __init__(self, langless_audiocode, nsufix, twolettercode, seq_order):
    """
    Object instance example:
      LangAttr
          seq_order = 1 | twolettercode = en |  language name = English
          langless_audiocode = 91 | nsufix = 0 | audioonlycode = 91-0
    """
    self.langless_audiocode = langless_audiocode
    self.nsufix = nsufix
    self.twolettercode = twolettercode
    self.seq_order = seq_order

  @property
  def audioonlycode(self):
    aoc = f"{self.langless_audiocode}-{self.nsufix}"
    return aoc

  @property
  def langname(self) -> str:
    try:
      _langname = TWOLETTER_N_LANGUAGENAME_DICTMAP[self.twolettercode]
      return _langname
    except IndexError:
      pass
    return 'not-known'

  def __str__(self):
    la = self.langless_audiocode
    aoc = self.audioonlycode
    tlc = self.twolettercode
    ln = self.langname
    outstr = f"""{self.__class__.__name__}
    seq_order = {self.seq_order} | twolettercode = {tlc} |  language name = {ln}
    langless_audiocode = {la} | nsufix = {self.nsufix} | audioonlycode = {aoc}"""
    return outstr


def adhoctest1():
  langless_audiocode = '91'
  nsufix = '0'
  twolettercode = 'en'
  seq_order = 1
  la = LangAttr(
    langless_audiocode=langless_audiocode,
    nsufix=nsufix,
    twolettercode=twolettercode,
    seq_order=seq_order
  )
  print(la)


def adhoctest2():
  fn = ("2025-08-08 1' Rodrigo Vianna comenta a cronologia de ações"
        " golpistas do bolsonarismo feita por Paulo Motoryn [bFYD24JM6dI].f160.mp4")
  attr_o = FilenameNFormatsVideo(fn)
  print(attr_o)
  print('is complete', attr_o.is_complete())


def process():
  pass


if __name__ == '__main__':
  """
  process()
  adhoctest1()
  """
  adhoctest1()
  adhoctest2()
