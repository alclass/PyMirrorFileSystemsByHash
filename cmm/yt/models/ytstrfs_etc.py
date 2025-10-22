#!/usr/bin/env python3
"""
cmm/yt/models/ytstrfs_etc.py
  Contains functions related YouTube names, id's, languages and their codes.
"""
# from collections.abc import Iterable
# from typing import Tuple
from typing import Generator
from typing import Any
import os
import string
import re
YTID_CHARSIZE = 11
enc64_valid_chars = string.digits + string.ascii_lowercase + string.ascii_uppercase + '_-'
# Example for the regexp below: https://www.youtube.com/watch?v=abcABC123_-&pp=continuation
ytid_url_w_watch_regexp_pattern = r'watch\?v=([A-Za-z0-9_-]{11})(?=(&|$))'
cmpld_ytid_url_w_watch_re_pattern = re.compile(ytid_url_w_watch_regexp_pattern)
ytid_in_ytdlp_filename_pattern = r'\[([A-Za-z0-9_-]{11})\]'
cmpld_ytid_in_ytdlp_filename_pattern = re.compile(ytid_in_ytdlp_filename_pattern)
ytid_instr_af_equalsign = r'\=([A-Za-z0-9_-]{11})(?=(&|$))'
cmpld_ytid_instr_af_equalsign_pattern = re.compile(ytid_instr_af_equalsign)
# another pattern to extract a ytid is the following: https://www.youtube.com/shorts/<ytid>
# but the ytid will generalized as a split('/')[-1] and then tested as an 11-char ENC64 str
ytvideobaseurl = "https://www.youtube.com/watch?v="
TWOLETTER_N_LANGUAGENAME_DICTMAP = {
  'ar': 'Arabic',
  'de': 'German',
  'en': 'English',
  'es': 'Spanish',
  'fr': 'French',
  'hi': 'Hindi',
  'id': 'Indonesian',
  'it': 'Italian',
  'ja': 'Japanese',
  'ma': 'Mandarin Chinese',
  'ml': 'Malaysian',
  'po': 'Polish',
  'pt': 'Portuguese',
  'ro': 'Romanian',
  'ru': 'Russian',
  'uk': 'Ukrainian',
}


def is_str_enc64(line: str | None) -> bool:
  blist = list(map(lambda c: c in enc64_valid_chars, line))
  if False in blist:
    return False
  return True


def is_str_a_ytid(ytid: str | None) -> bool:
  if ytid is None or len(ytid) != YTID_CHARSIZE:
    return False
  return is_str_enc64(ytid)


def get_match_ytid_af_equalsign_or_itself(line):
  """
  Gets a ytid after an "=" (equal sign) or returns the input itself
  :return:
  """
  match = cmpld_ytid_instr_af_equalsign_pattern.search(line)
  return line if match is None else match.group(1)


def extract_ytid_from_yturl_or_itself_or_none(p_supposed_ytid: str | None) -> str | None:
  """
  Extracts ytid from a YouTube-type URL (when ytid is preceded by '?watch=')
  Noting:
    if ytid is None, return None
    if ytid is already a 'ytid sole', return it as is
    if ytid is preceded by '?watch=', match/extract/return ytid
    if regex above can't match, return None

  Example of an extraction from a YouTube-like URL:
    url = "https://www.youtube.com/watch?v=abcABC123_-&pp=continuation"
  The extraction result is:
    ytid = "abcABC123_-"

    Obs: "abcABC123_-" in the example is hypothetical (an ENC64 11-char string)!
  """
  if p_supposed_ytid is None:
    return None
  if is_str_a_ytid(p_supposed_ytid):
    ytid = p_supposed_ytid
    return ytid
  match = cmpld_ytid_url_w_watch_re_pattern.search(p_supposed_ytid)
  if match:
    return match.group(1)
  # lastly, test also a pattern like this one: https://www.youtube.com/shorts/<ytid>
  # but generalize it to its ending as '/' + ytid
  pp = p_supposed_ytid.split('/')
  if len(pp) < 2:  # minimally url must be at least "<site>/<ytid>" case which would make it 2: len(pp)=2
    return None
  ytid = pp[-1]
  if is_str_a_ytid(ytid):
    return ytid
  return None


def leftstrip_ytvideourl_out_of_str(s: str | None) -> str:
  """
  Returns the input string left-stripped of the YouTube's base-URL
      or empty '' (if input is None or empty)
      or itself (i.e., the input as is)
    Obs: input comes here already passed through a strip(whitespace) operation,
         so this first stripping is not needed at this point
  :param s:
  :return: s (filtered)
  """
  if s is None or s == '':
    return ''
  base = ytvideobaseurl
  if s.startswith(base):
    return s.strip(base)
  else:
    return s


def read_ytids_from_strlines(strlines: list | None) -> list:
  """
  Filters a str list into a list of ytid's

  The data text (here incoming as str lines) must be formed in two ways,
    they are:
      1 - either its ytid is at the beginning (white space ' \t\r\n is filtered out)
      2 - or its ytid is in a URL of the following kind:
        2-1 one with '=' (an equal sign) preceding the ytid
        Obs: in an obvious way, that will also include '?watch=' preceding the ytid
  """
  # strip left and right whitespace ' \t\r\n'
  lines = map(lambda line: line.strip(' \t\r\n'), strlines)
  # lines = list(lines)
  # left-strip beginning YouTube base URL in lines if any
  lines = filter(lambda line: leftstrip_ytvideourl_out_of_str(line), lines)
  # further than the filter above, remove lines if it has a ytid after the '=' sign
  # lines = list(lines)
  # pick up ytid's in string when they happen after '=' (the equal sign)
  lines = map(lambda line: get_match_ytid_af_equalsign_or_itself(line), lines)
  # lines = list(lines)
  # remove lines if it does not have exactly 11-char (YTID_CHARSIZE)
  ytdis = filter(lambda line: len(line) == YTID_CHARSIZE, lines)
  # ytdis = list(ytdis)
  # remove from the remaining 11-char lines those not ENC64-complying
  ytdis = list(filter(lambda line: is_str_enc64(line), ytdis))
  return ytdis


def read_ytids_from_file_n_get_as_list(p_filepath: str) -> list:
  """
  Reads text data file and returns its str lines

    Obs:
      At this version, OSError or IOError try/except was not implemented below,
      but if path or file is missing or a disk error occurs, an exception is expected

  """
  if p_filepath is None or not os.path.isfile(p_filepath):
    return []
  strlines = open(p_filepath, 'r').readlines()
  return read_ytids_from_strlines(strlines)


def verify_ytid_validity_or_raise(ytid):
  if not is_str_a_ytid(ytid):
    errmsg = (
      f"""
      Please check the value entered for/with ytid
        => its entered value is "{ytid}"

      Rules for a valid ytid:
      =======================
      a) it must have {YTID_CHARSIZE} characters
      b) all of them must be ENC64 *

      * All 64 ENC64 characters are: "{enc64_valid_chars}"

      Please, correct the observations(s) above and retry.
      """
    )
    raise ValueError(errmsg)


def get_validated_ytid_or_raise(ytid):
  verify_ytid_validity_or_raise(ytid)
  return ytid


def get_nsufix_fr_audioonlycode(audioonlycode: str | None) -> int | None:
  """
  audioonlycode is a str with "a number dash another number", this latter is the nsufix
  Example:
    '233-0' -> returns 0
    '233-5' -> returns 5
    and so on
  """
  try:
    pp = audioonlycode.split('-')
    nsufix = int(pp[1])
    return nsufix
  except (AttributeError, IndexError):
    pass
  return None


def trans_list_as_uniq_keeping_order_n_makingnewlist(ytids):
  copiedlist = list(ytids)
  return trans_list_as_uniq_keeping_order_n_mutable(copiedlist)


def trans_list_as_uniq_keeping_order_n_mutable(ytids):
  """
  Unicizes input list using an element to element comparison

  Obs:
    The problem with list(set(listvar)), which also unicizes a list, is that
      it does not maintain the original sequencial ordering
  """
  if ytids is None:
    return None
  if len(ytids) == 0:
    return []
  if len(ytids) == 1:
    return ytids
  i = 1
  while i < len(ytids):
    # look up backwardly
    elem_deleted = False
    for j in range(i-1, -1, -1):
      if ytids[i] == ytids[j]:
        del ytids[i]
        elem_deleted = True
        break
    if not elem_deleted:
      i += 1
  return ytids


def trans_str_sfx_n_2letlng_map_to_dict_or_raise(pdict):
  """
  Transforms a string represent a number-and-twoletterlanguagecode map into a dict, noting:
  1 - the elements (key and value) in the incoming string, do not need to be enclosed within simple or double quotes
    1 - 1 if it has quotes, no problem, they are stripped off for its dict conversion
  2 - the outgoing dict has an int key (the language index or sufix [its semantic is considered here])
    and a string value (which is the twoletterlanguagecode)

  Initially written for:
    1 - script dlYouTubeWhenThereAreDubbed that contains this dict in question as an input parameter
    2 - the output dict will serve to instantiate a SufixLangMapper mapper object
  """
  if isinstance(pdict, dict):
    return pdict
  outdict = {}
  pp = pdict.split(',')
  for elem in pp:
    try:
      if elem.find(':') < 0:
        continue
      pair = elem.split(':')
      pair0 = pair[0].strip(' \t\r\n"\'')
      number = int(pair0)
      twolettercode = pair[1]
      twolettercode = twolettercode.strip(' \t\r\n"\'')
      if len(twolettercode) != 2:
        wrnmsg = f"Warning: a twolettercode should have 2 letter, it has {len(twolettercode)} in {pdict}"
        print(wrnmsg)
        continue
      outdict.update({number: twolettercode})
    except (IndexError, ValueError) as e:
      wrnmsg = f"Error: sfx_n_2letlng_dict is malformed with elem [{elem}] in strdict [{pdict}] | {e}"
      print(wrnmsg)
      continue
  return outdict


def fetch_langdict_w_videoformatoutput(videoformatoutput):
  """
  vfo = videoformatoutput
  TWOLETTER_N_LANGUAGENAME_DICTMAP.values()
  :param videoformatoutput:
  :return:
  """

  return {}


def adhoctest4():
  print('-'*30)
  print('adhoc_test4: cmpld_ytid_instr_af_equalsign_pattern')
  t = 'https://www.youtube.com/watch?v=Gjg471uIL9k&pp=wgIGCgQQAhgD'
  print(t)
  match = cmpld_ytid_instr_af_equalsign_pattern.search(t)
  if match:
    print(match.group(1))
  else:
    print("didn't match")
  testlist = ['d', 'c', 'a', 'b', 'a', 'a', 'c']
  uniqlist = trans_list_as_uniq_keeping_order_n_makingnewlist(testlist)
  scrmsg = f"testlist {testlist} | uniqlist {uniqlist}"
  print(scrmsg)
  testlist = ['a', 'a']
  uniqlist = trans_list_as_uniq_keeping_order_n_makingnewlist(testlist)
  scrmsg = f"testlist {testlist} | uniqlist {uniqlist}"
  print(scrmsg)
  testlist = ['a']
  uniqlist = trans_list_as_uniq_keeping_order_n_makingnewlist(testlist)
  scrmsg = f"testlist {testlist} | uniqlist {uniqlist}"
  print(scrmsg)


def adhoctest3():
  print('-'*30)
  print('adhoc_test3: extract_ytid_from_yturl_or_itself_or_none')
  t = 'https://www.youtube.com/watch?v=Gjg471uIL9k&pp=wgIGCgQQAhgD'
  ytid = extract_ytid_from_yturl_or_itself_or_none(t)
  scrmsg = f"""Testing {t}
  Resulting {ytid}"""
  print(scrmsg)
  t = 'https://www.youtube.com/watch?v=abcABC123_-&pp=continuation'
  ytid = extract_ytid_from_yturl_or_itself_or_none(t)
  scrmsg = f"""Testing {t}
  Resulting {ytid}"""
  print(scrmsg)
  # return ytid


def adhoctest2():
  """
  https://www.youtube.com/watch?v=GnFNf7Q7tH4
  https://www.youtube.com/watch?v=_8iL9SdyJng

  :return:
  """
  print('-'*30)
  print('adhoctest2: extracting ytid from strings when ytid comes after "="')
  strlines = []
  url = 'https://www.youtube.com/watch?v=GnFNf7Q7tH4'
  strlines.append(url)
  print(1, url)
  url = 'https://www.youtube.com/watch?v=_8iL9SdyJng'
  strlines.append(url)
  print(2, url)
  result = read_ytids_from_strlines(strlines)
  print('result', result)


def adhoctest1():
  """
  """
  print('-'*30)
  print('adhoctest1: SufixLanguageMapFinder')
  print('='*20)
  print('example 1')
  print('='*20)
  audioonlycodes = ['233-0', '233-1', '233-5', '233-9']
  print('1 Input', audioonlycodes)
  setter = SufixLanguageMapFinder(audioonlycodes)
  setter.print_sufix_lang_map()
  nsufix = 5
  lang = setter.get_lang2lettercode_fr_numbersufix(nsufix)
  scrmsg = f"get_lang2lettercode_fr_numbersufix({nsufix}) = {lang}"
  print(scrmsg)
  audioonlycode = '233-5'
  lang = setter.get_lang2lettercode_fr_audioonlycode(audioonlycode)
  scrmsg = f"get_lang2lettercode_fr_audioonlycode({audioonlycode}) = {lang}"
  print(scrmsg)
  # example 2
  print('='*20)
  print('example 2')
  print('='*20)
  audioonlycodes = ['233-0', '233-1']
  print('2 Input', audioonlycodes)
  setter = SufixLanguageMapFinder(audioonlycodes)
  setter.print_sufix_lang_map()
  nsufix = 1
  lang = setter.get_lang2lettercode_fr_numbersufix(nsufix)
  scrmsg = f"get_lang2lettercode_fr_numbersufix({nsufix}) = {lang}"
  print(scrmsg)
  # ==============
  print('Testing a non-existing sufix-lang')
  nsufix = 1000
  lang = setter.get_lang2lettercode_fr_numbersufix(nsufix)
  scrmsg = f"get_lang2lettercode_for_numbersufix({nsufix}) = {lang}"
  print(scrmsg)


def process():
  pass


if __name__ == '__main__':
  """
  process()
  adhoc_test1()
  adhoc_test2()
  adhoc_test3()
  """
  adhoctest4()
