#!/usr/bin/env python3
"""
functions
cmm/yt/models/adhoctest_ytvideoformatextractor_cls.py
  Contains functions related to YouTube's video formats.
"""
import os
import re
import cmm.yt.models.ytstrfs_etc as ytfs
import cmm.yt.uTubeVideoFormatsExtractor as ytex
YTVFTextExtractor = ytex.YTVFTextExtractor
TWOLETTER_N_LANGUAGENAME_DICTMAP = ytfs.TWOLETTER_N_LANGUAGENAME_DICTMAP
# import sys
INTEREST_LANGUAGES = ['de', 'fr', 'en', 'es', 'it', 'ru']
twoletterlangcodes = list(TWOLETTER_N_LANGUAGENAME_DICTMAP.keys())
barred_twoletterlangcodes = '|'.join(twoletterlangcodes)
# restr_2lttcds = r'\[' + barred_twoletterlangcodes + r'\]+'
restr_2lttcds = r'^.*?\[(?P<twoletlngcod>[a-z]{2})\].*$'
recmp_2lttcds = re.compile(restr_2lttcds)
TWOLETTER_N_LANGUAGENAME_DICTMAP = ytfs.TWOLETTER_N_LANGUAGENAME_DICTMAP


def adhoctest2():
  """
  dp = "/media/user/BRAPol SSD2T ori/Tmp/vi tmp/Sci tmp vi/Gen Sci tmp vi/Sabine Hossenfelder Gen Sci yu"
  fn = "video-formats-sabine-MukMOZ0J.txt"
  fn = sys.argv[1]
  """
  dp = "/media/friend/BRAPol SSD2T ori/Tmp/vi tmp/Sci tmp vi/Gen Sci tmp vi/3Blue1Brown (Sci etc) yu/test"
  fn = "video-formats 2025-09-23 8' vd-es Large Language Models explained briefly-LPZh9BOjkQs.txt"
  print(fn)
  fp = os.path.join(dp, fn)
  text = open(fp).read()
  extractor = YTVFTextExtractor(text)
  print(extractor)
  print('langdict', extractor.langdict)


def adhoctest1():
  """
  """
  print(twoletterlangcodes)
  print(barred_twoletterlangcodes)
  print(restr_2lttcds)
  test = 'dadfa kljçlf [en] ads'
  print(test)
  mo = recmp_2lttcds.match(test)
  print(mo)
  if mo:
    print('2 letter', mo.group(1))


def process():
  pass


if __name__ == '__main__':
  """
  process()
  adhoctest1()
  adhoctest2()
  """
  adhoctest2()
