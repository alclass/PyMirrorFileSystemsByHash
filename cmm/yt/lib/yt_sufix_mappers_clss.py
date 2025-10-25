#!/usr/bin/env python3
"""
cmm/yt/models/ytfinders_clss.py
  Contains functions related YouTube names, id's, languages and their codes.
"""
from typing import Generator, Any
import cmm.yt.ytids.ytstrfs_etc as ytfs
import cmm.yt.ytids.file_n_formats_clss as lac  # .LangAttr
LangAttr = lac.LangAttr
TWOLETTER_N_LANGUAGENAME_DICTMAP = ytfs.TWOLETTER_N_LANGUAGENAME_DICTMAP


class SufixLanguageMapper:
  """

  The second class following up is an equivalent older class than this.
    Please refer to its docstr for general info.
  """

  def __init__(self, pdict: dict[int, str] | str, audiomainnumber=249):
    self.no_dubs = False
    self.indict: dict[int, str] = pdict
    self._dict_as_items = None
    self.at_number = 0
    self.reached_end = False
    self.twolettercodes_given = []
    self.audiomainnumber = audiomainnumber
    self.treat_indict()

  def treat_indict(self):
    if self.indict is None:
      errmsg = f"Error: the sufix and language dict {self.indict} is malformed or None"
      raise ValueError(errmsg)
    if isinstance(self.indict, str):
      self.indict = ytfs.trans_str_sfx_n_2letlng_map_to_dict_or_raise(self.indict)

  @property
  def dict_as_items_in_order(self) -> list[tuple[int, str]]:
    """
    About type-annotation:
    if the return had not the list() function, the return type-annotation should be:
      Iterable[Tuple[int, str]] when the iterable (without list()) is returned
    """
    if self._dict_as_items is not None:
      return self._dict_as_items
    ordered_dict = sorted(self.indict.items(), key=lambda pair: pair[0])
    self._dict_as_items = list(ordered_dict)
    return self._dict_as_items

  @property
  def nsufices_in_order(self) -> list[int]:
    """
    Returns the nsufix numbers in ascending order
    Example:
      suppose langdict = [1:'pt', 0:'en']  # out of sequencing order purposefully for the example
      then dict_as_items_in_order = [(0, 'en'), (1, 'pt')]  # in ascending order on nsufix
      then nsufices_in_order = [0, 1]
    :return:
    """
    nsufices = [e[0] for e in self.dict_as_items_in_order]
    return nsufices

  @property
  def audioonlycodes(self) -> list[str]:
    """
    Returns the audioonlycode list
    This list is dynamically formed at each call
    :return: aocs
    """
    aocs = []
    for item in self.dict_as_items_in_order:
      numbersufix = item[0]
      audioonlycode = f"{self.audiomainnumber}-{numbersufix}"
      aocs.append(audioonlycode)
    return aocs

  @property
  def size(self) -> int:  # Iterable[Tuple[int, str]] when the iterable (without list()) is returned
    return len(self.indict)

  def turn_off_dubs(self):
    self.no_dubs = True

  def get_first_2lettlangcode(self) -> str | None:
    if self.no_dubs:
      return 'un'  # 'unique' (no dubs)
    try:
      twolettercode = self.dict_as_items_in_order[0][1]
      return twolettercode
    except IndexError:
      pass
    return 'un'  # 'un' above means 'no dubs', but here it would mean 'unknown'

  def get_first_langobj(self) -> LangAttr:
    if self.no_dubs:
      langer = LangAttr(
        langless_audiocode=self.audiomainnumber,
        nsufix=-1,
        twolettercode='un',  # un stands for 'unique' and also that there's only the original language
        seq_order=1
      )
      return langer
    pair = self.dict_as_items_in_order[0]
    nsufix = pair[0]
    twolettercode = pair[1]
    langer = LangAttr(
      langless_audiocode=self.audiomainnumber,
      nsufix=nsufix,
      twolettercode=twolettercode,
      seq_order=1
    )
    return langer

  def get_ith_2lettlangcode_1idxbased(self, n) -> str | None:
    try:
      i = n - 1
      return self.dict_as_items_in_order[i][1]
    except IndexError:
      pass
    return 'un'

  @staticmethod
  def get_langname_fr_2lettercode(twolettercode):
    try:
      return TWOLETTER_N_LANGUAGENAME_DICTMAP[twolettercode]
    except IndexError:
      pass
    return 'unknown'

  def get_nsufix_fr_idx(self, idx) -> int:
    try:
      nsufix = self.dict_as_items_in_order[idx][0]
      return nsufix
    except KeyError:
      pass
    return -1

  def get_twolettercode_fr_idx(self, idx) -> str:
    try:
      twolettercode = self.dict_as_items_in_order[idx][1]
      return twolettercode
    except KeyError:
      pass
    return 'un'

  def get_twolettercode_fr_nsufix(self, nsfx) -> str:
    """
    The different between this method and the one is that this one the index is the key itself.
    For the other, the index is its sequencial one.
    """
    try:
      twolettercode = self.indict[nsfx]
      return twolettercode
    except KeyError:
      pass
    return 'un'

  def get_twolettercode_n_langname_fr_nsufix(self, nsfx) -> tuple[str, str]:
    twolettercode = self.get_twolettercode_fr_nsufix(nsfx)
    langname = self.get_langname_fr_2lettercode(twolettercode)
    return twolettercode, langname

  def get_langname_fr_idx(self, idx) -> str:
    twolettercode = self.get_twolettercode_fr_idx(idx)
    return self.get_langname_fr_2lettercode(twolettercode)

  def get_langname_fr_sufix_n(self, nsfx) -> str:
    twolettercode = self.get_twolettercode_fr_nsufix(nsfx)
    return self.get_langname_fr_2lettercode(twolettercode)

  def get_audioonlycodesufix_fr_idx(self, idx) -> int | None:
    try:
      pair = self.dict_as_items_in_order[idx]
      nsufix = pair[0]
      return nsufix
    except KeyError:
      pass
    return None

  def get_audioonlycode_fr_idx(self, idx) -> str | None:
    nsufix = self.get_audioonlycodesufix_fr_idx(idx)
    if nsufix is None:
      return None
    audioonlycode = f"{self.audiomainnumber}-{nsufix}"
    return audioonlycode

  def get_audioonlycode_for_1baseidx(self, onebasedidx: int) -> str | None:
    i = onebasedidx - 1
    return self.get_audioonlycode_fr_idx(i)

  def traverse_sufix_n_twolettercode(self) -> Generator[tuple[int, str], Any, None]:
    """
    Traverses (loops over with yield [a generator]) the items in self.indict
    This method is "disconnected" with next(), beucase:
      1 - next() consumed a list that is initiated from self.indict
      2 - this method, on the contrary, loops over original self.indict
    """
    items = self.indict.items()
    items = sorted(items, key=lambda i: i[0])  # this keeps the ordering from least to greatest nsufix (the langnumber)
    for item in items:
      yield item

  def loop_over_langs(self) -> Generator[LangAttr, Any, None]:
    """
    In terms of functionality, the main attribute (or property) of this object
      is the dict_mapping of nsufix with 2-letter-code items

    This method loops over the these items transforming them into LangAttr objects.
      These 'langer' objects will be "consumed" in the client-caller that issues the
      yt-dlp download runs.

    :return:
    """
    for i, pair in enumerate(self.traverse_sufix_n_twolettercode()):
      seq_order = i + 1
      nsufix = pair[0]
      twolettercode = pair[1]
      langer = LangAttr(
        langless_audiocode=self.audiomainnumber,
        nsufix=nsufix,
        twolettercode=twolettercode,
        seq_order=seq_order
      )
      yield langer

  def __str__(self):
    outstr = f"""
    {self.indict}
    """
    return outstr


class SufixLanguageMapFinder:
  """
  DEPRECATED: this class will be substituted by the one above SufixLanguageMapper

  This class finds a map (dict) of sufixes to languages
    for audio-only-codes available from the YouTube parameters
    (used via yt-dlp).

  Obs:
    1 - the parameter -F (or --format) in yt-dlp lists all available
    videocodes, including the audio-only-codes;

    2 - to be known better below, the user must enter an audio-only-code
    with sufix greater than 1 if the first scheme below is expected;

  At the time of writing, there are two main language sufix
    schemes observed from YouTube, they are:

  Scheme 1 (English as the original)
  ========
      0 -> de (Deutsch | German) [autodubbed]
      1 -> es (Español | Spanish) [autodubbed]
      2 -> fr (Français | French) [autodubbed]
      3 -> hi (Hindi) [autodubbed]
      4 -> id (Indonesian) [autodubbed]
      5 -> it (Italiano | Italian) [autodubbed]
      6 -> ja (Japanese) [autodubbed]
      7 -> pl (Poska | Polonese) [autodubbed]
      8 -> pt (Português | Portuguese) [autodubbed]
      9 -> en ((American) English) [original]

  This scheme (English as the original) will produce the following map-as-dict:
    {0: 'de', 1: 'es', 2: 'fr', 3: 'hi', ..., 9: 'en',}

  (@see also Obs-2 above)

  Scheme 2 (English autodubbed)
  ========
      0 -> en (English | English) [autodubbed]
      1 -> <ot> (<other> | "some other language") [original]

  This scheme (English autodubbed) will produce the following map-as-dict:
    {0: 'en', 1: '<ot>'}
  where:
    <ot> is the 2-letter-code for the other (original) language
  Example:
    {0: 'en', 1: 'pt'}
  """

  def __init__(self, audioonlycodes):
    self.audioonlycodes = audioonlycodes
    self.n_ongoing_lang = 0
    self._known_langs_case_ori_en = None
    self.lang_map = None
    self.eng_sufix = None
    self.process()

  @property
  def known_langs_case_ori_en(self):
    if self._known_langs_case_ori_en is not None:
      return self._known_langs_case_ori_en
    self._known_langs_case_ori_en = {
      0: 'de',
      1: 'es',
      2: 'fr',
      3: 'hi',
      4: 'id',
      5: 'it',
      6: 'ja',
      7: 'po',
      8: 'en',  # on some cases 8 may be English
      9: 'en',  # on most cases 9 may be English
      10: 'u1',  # unknown1 (the language set may grow to more ones)
      11: 'u2',  # unknown1
      12: 'en',  # unknown2
    }
    return self._known_langs_case_ori_en

  def make_lang_map_via_eng_ori(self):
    self.lang_map = {}
    for audioonlycode in self.audioonlycodes:
      pp = audioonlycode.split('-')
      if len(pp) < 1:
        self.lang_map.update({0: 'un'})  # unknown, the user must filerename later on
        continue
      sufix = int(pp[1])
      self.lang_map.update({sufix: self.known_langs_case_ori_en[sufix]})
    return self.lang_map

  def make_lang_map_via_noneng_ori(self):
    self.lang_map = {0: 'en', 1: 'pt'}  # the user must filerename 'pt' for the correct one
    return self.lang_map

  def find_sufix_number_either_dub_eng_or_ori_eng(self):
    for audioonlycode in self.audioonlycodes:
      try:
        audiocode_n_sufixnumber = audioonlycode.split('-')
        sufixnumber = audiocode_n_sufixnumber[1]
        sufixnumber = int(sufixnumber)
        if sufixnumber > 1:
          self.eng_sufix = 8 if sufixnumber < 9 else 9
      except (AttributeError, IndexError, ValueError):
        self.eng_sufix = 0

  def get_sufix_lang_dict(self):
    """
    the lang_dict maps the number sufix in audio-only-codes to
      the 2-letter language identifier

    The example below shows a 'context' known for discovering the languages:

    For example:
      ['233-0', '233-1'] should produce {0:'en': 1:'<the-other-lang>'}
      ['233-0', '233-9'] should produce {0:'de': 9:'en'}

    Obs:

      1)  when, in the audio-only-codes, no sufix greater than 1 is present:
        the mapping will be:
      0 -> en (English | English) [autodubbed]
      1 -> <ot> (<other> | "some other language") [original]

      2) when, in the audio-only-codes, a sufix greater than 1 is present,
        the mapping will be:
      0 -> de (Deutsch | German) [autodubbed]
      1 -> es (Español | Spanish) [autodubbed]
      2 -> fr (Français | French) [autodubbed]
      3 -> hi (Hindi) [autodubbed]
      4 -> id (Indonesian) [autodubbed]
      5 -> it (Italiano | Italian) [autodubbed]
      6 -> ja (Japanese) [autodubbed]
      7 -> pl (Poska | Polonese) [autodubbed]
      8 -> pt (Português | Portuguese) [autodubbed]
      9 -> en ((American) English) [original]

      Obs:
        sometimes English is sufixed 8, most times it's sufixed 9
    """
    if self.lang_map is not None:
      return self.lang_map
    self.eng_sufix = 0  # until proven differently
    # the next for is to establish English either as sufix 0 or 8 or 9
    self.find_sufix_number_either_dub_eng_or_ori_eng()
    if self.eng_sufix > 0:
      return self.make_lang_map_via_eng_ori()
    return self.make_lang_map_via_noneng_ori()

  def get_lang2lettercode_fr_numbersufix(self, nsufix):
    pdict = self.get_sufix_lang_dict()
    if pdict:
      try:
        return pdict[nsufix]
      except KeyError:
        pass
    return 'un'  # un for "unknown" instead of None

  def get_lang2lettercode_fr_audioonlycode(self, audioonlycode):
    nsufix = ytfs.get_nsufix_fr_audioonlycode(audioonlycode)
    return self.get_lang2lettercode_fr_numbersufix(nsufix)

  def process(self):
    """
    This method, using the 'process' name as a convention,
      calls another method that will initialize (lazily)'
      all the object's attributes
    :return:
    """
    self.get_sufix_lang_dict()

  def print_sufix_lang_map(self):
    print(str(self))

  def __str__(self):
    outstr = f"""SufixLanguageMapFinder:
    map = f{self.lang_map}
    """
    return outstr


def adhoctest2():
  scrmsg = """
  SufixLanguageMapper | adhoctest2()
  ============
  """
  print(scrmsg)
  strdict = '0:en,1:pt'
  langmapper = SufixLanguageMapper(strdict)
  print(langmapper)
  scrmsg = """
  langmapper.traverse_sufix_n_twolettercode():'
  ========================"""
  print(scrmsg)
  for i, item in enumerate(langmapper.traverse_sufix_n_twolettercode()):
    print(i, '\t=> ', item)
  scrmsg = """
  langmapper.get_first_2lettlangcode():'
  ========================
  """
  print(scrmsg)
  first_2lett = langmapper.get_first_2lettlangcode()
  print('\tfirst_2lett', first_2lett)
  print(langmapper.dict_as_items_in_order)
  scrmsg = """
  langmapper.get_ith_2lettlangcode_1idxbased():'
  ========================
  """
  print(scrmsg)
  any_2lett = langmapper.get_ith_2lettlangcode_1idxbased(2)
  print('\t2lett 1bidx', 2, any_2lett)
  scrmsg = """
  langmapper.get_audioonlycode_fr_idx():'
  ========================"""
  print(scrmsg)
  audioonlycode = langmapper.get_audioonlycode_fr_idx(0)
  scrmsg = f"\taudioonlycode for idx {0} = {audioonlycode}"
  print(scrmsg)
  audioonlycode = langmapper.get_audioonlycode_for_1baseidx(1)
  scrmsg = f"\taudioonlycode for 1-base idx {1} = {audioonlycode}"
  print(scrmsg)
  onebasedidx = 2
  audioonlycode = langmapper.get_audioonlycode_for_1baseidx(onebasedidx)
  scrmsg = f"\taudioonlycode for 1-base idx {onebasedidx} = {audioonlycode}"
  print(scrmsg)
  print(langmapper.audioonlycodes)


def adhoctest1():
  # testing strdict having 'unordered keys'
  scrmsg = """
  SufixLanguageMapper | adhoctest1()
  # testing strdict having 'unordered keys'
  ============
  """
  print(scrmsg)
  strdict = '1:fr,0:de,5:it,2:es,11:en,8:ru'
  langmapper = SufixLanguageMapper(strdict)
  print('\nlangmapper =>\t', langmapper)
  print('\nlangmapper.dict_as_items_in_order =>\n\t', langmapper.dict_as_items_in_order)
  scrmsg = """
  langmapper.loop_over_langs()
  ============
  """
  for i, langer in enumerate(langmapper.loop_over_langs()):
    scrmsg = f"langer {langer}"
    print(f'\n\ti={i} langer => ', scrmsg)


def process():
  pass


if __name__ == '__main__':
  """
  process()
  adhoctest1()
  """
  adhoctest1()
  adhoctest2()
