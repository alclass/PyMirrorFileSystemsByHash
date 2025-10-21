#!/usr/bin/env python3
"""
cmm/yt/models/langattr_cls.py
  Contains, at the time of writing, class LangAttr related YouTube autodubs and connected info.
"""
import cmm.yt.models.ytstrfs_etc as ytfs
TWOLETTER_N_LANGUAGENAME_DICTMAP = ytfs.TWOLETTER_N_LANGUAGENAME_DICTMAP


class LangAttr:

  def __init__(self, langless_audiocode, nsufix, twolettercode, seq_order):
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


def process():
  pass


if __name__ == '__main__':
  """
  process()
  adhoctest1()
  """
  adhoctest1()
