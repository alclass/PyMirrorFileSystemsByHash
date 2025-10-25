#!/usr/bin/env python3
"""
cmm/yt/lib/os/cliparams_for_utubewhendub.py
  This module contain class CliParam which helps collect, verify
    and confirm the CLI input parameters for script
"""
import argparse
import os
# import localuserpylib.ytfunctions.osentry_class as ose  # ose.OSEntry
import cmm.yt.lib.os.osentry_class as ose  # ose.OSEntry
# import localuserpylib.ytfunctions.yt_str_fs_vids_sufix_lang_map_etc as ytstrfs
# import cmm.yt.lib.yt_sufix_mappers_clss as ytstrfs
import cmm.yt.ytids.ytstrfs_etc as ytstrfs
DEFAULT_YTIDS_FILENAME = ose.DEFAULT_YTIDS_FILENAME
DEFAULT_AUDIOVIDEO_CODE = ose.DEFAULT_AUDIOVIDEO_CODE
DEFAULT_AUDIOVIDEO_DOT_EXT = ose.DEFAULT_AUDIOVIDEO_DOT_EXT
DEFAULT_AUDIO_MAIN_NUMBER = ose.DEFAULT_AUDIO_MAIN_NUMBER  # may be -1 meaning no audio separate
DEFAULT_SFX_W_2LETLNG_MAPDCT = ose.DEFAULT_SFX_W_2LETLNG_MAPDCT
DEFAULT_VIDEO_ONLY_CODE = ose.DEFAULT_VIDEO_ONLY_CODE
VIDEO_DOT_EXTENSIONS = ose.VIDEO_DOT_EXTENSIONS
default_videodld_tmpdir = ose.default_videodld_tmpdir
# Parse command-line arguments
parser = argparse.ArgumentParser(description="Compress videos to a specified resolution.")
parser.add_argument("--docstr", action="store_true",
                    help="show docstr help and exit")
parser.add_argument("--ytid", type=str,
                    help="the video id")
parser.add_argument("--useinputfile", action='store_true',
                    help="read the default ytids input file")
parser.add_argument("--dirpath", type=str,
                    help="Directory recipient of the download")
parser.add_argument("--voc", type=int, default=f"{DEFAULT_AUDIOVIDEO_CODE}",
                    help="video only code: example: 160")
parser.add_argument("--amn", type=int, default="249",
                    help="audio only codes: example: 233-0,233-1")
parser.add_argument("--seq", type=int, default=1,
                    help="the sequencial number that accompanies the 'vd' namemarker at the last renaming")
parser.add_argument("--map", type=str, default="0:en,1:pt",
                    help="the dictionary-mapping with numbers and the 2-letter language codes (e.g. '0:en,1:pt')")
parser.add_argument("-y", action='store_true',
                    help="represents 'yes', making the user confirmation phase to be skipped off")
args = parser.parse_args()


def get_default_ytids_filepath(p_dirpath):
  ytids_filename = DEFAULT_YTIDS_FILENAME
  default_ytids_filepath = os.path.join(p_dirpath, ytids_filename)
  if not os.path.isfile(default_ytids_filepath):
    errmsg = f"YTIDs filepath [{default_ytids_filepath}] does not exist. Please, create it and retry."
    raise OSError(errmsg)
  return default_ytids_filepath


def get_langname(twolettercode) -> str:
  try:
    return ytstrfs.TWOLETTER_N_LANGUAGENAME_DICTMAP[twolettercode]
  except KeyError:
    pass
  return 'not-known'


class CliParam:
  """

      self,
      ytids=None,
      b_useinputfile=None,
      dirpath=None,
      videoonlycode=None,
      audiomainnumber=None,
      nvdseq=None,
      sfx_n_2letlng_dict=None,

    self.show_docstr_n_do_not_run = False
    self.confirmed = False  # the user may confirm params later on
    self.ytids = ytids
    self.b_useinputfile = b_useinputfile or False
    self.dirpath = dirpath or '.'
    self.videoonlycode = videoonlycode or DEFAULT_VIDEO_ONLY_CODE
    self.audiomainnumber = audiomainnumber or DEFAULT_AUDIO_MAIN_NUMBER
    self.nvdseq = nvdseq or 1
    self.sfx_n_2letlng_dict = sfx_n_2letlng_dict or DEFAULT_SFX_W_2LETLNG_MAPDCT

  """

  def __init__(self):
    self.show_docstr_n_do_not_run = False
    self.confirmed = False  # the user may confirm params later on
    self.goahead_nouserconfirm = False  # the user may confirm params later on
    self.ytid = None  # auxiliary to ytids
    self.ytids = []
    self.b_useinputfile = False
    self.dirpath = None
    self.videoonlycode = DEFAULT_VIDEO_ONLY_CODE
    self.audiomainnumber = DEFAULT_AUDIO_MAIN_NUMBER
    self.nvdseq = 1
    self.sfx_n_2letlng_dict = DEFAULT_SFX_W_2LETLNG_MAPDCT

  def verify_n_trans_sfx_n_2letlng_dict(self):
    """
    The sfx_n_2letlng_dict enters as a str, it should be transformed into a dict

      Example: input: "0: en, 1: pt" => output: {0: 'en', 1: 'pt'}
    """
    self.sfx_n_2letlng_dict = ytstrfs.trans_str_sfx_n_2letlng_map_to_dict_or_raise(self.sfx_n_2letlng_dict)

  @property
  def audioonlycodes(self):
    aocs = []
    for nsufix in self.sfx_n_2letlng_dict:
      audioonlycode = f"{self.audiomainnumber}-{nsufix}"
      aocs.append(audioonlycode)
    return aocs

  def get_cli_args(self):
    """
    Required parameters:
      src_rootdir_abspath & trg_rootdir_abspath

    Optional parameter:
      resolution_tuple

    :return: srctree_abspath, trg_rootdir_abspath, resolution_tuple
    """
    if args.docstr:
      self.show_docstr_n_do_not_run = True
      return
    self.ytid = args.ytid
    self.ytid = ytstrfs.extract_ytid_from_yturl_or_itself_or_none(self.ytid)
    self.ytids = []  # to be read from file if next parameter is set
    self.b_useinputfile = args.useinputfile
    self.goahead_nouserconfirm = args.y or False
    # default to the current working directory if none is given
    self.dirpath = args.dirpath or os.path.abspath("../../ytids")
    self.videoonlycode = args.voc or None
    # if audiomainnumber is None, it will get -1 meaning the formatcode in voc is already a+v
    # i.e., the video comes whole, no merging of a+v (audio with video)
    self.audiomainnumber = args.amn or DEFAULT_AUDIO_MAIN_NUMBER
    self.nvdseq = args.seq or 1
    self.sfx_n_2letlng_dict = args.map or DEFAULT_SFX_W_2LETLNG_MAPDCT
    self.verify_n_trans_sfx_n_2letlng_dict()

  def read_inputfile_ifneeded(self):
    if self.b_useinputfile:
      self.ytids = ytstrfs.read_ytids_from_file_n_get_as_list(get_default_ytids_filepath(self.dirpath))
      if self.ytid:
        self.ytids.append(self.ytid)
    else:
      if self.ytid:
        self.ytids = [self.ytid]
    if len(self.ytids) == 0:
      scrmsg = "No ytid given. Please, enter at least one ytid."
      print(scrmsg)
    self.ytids = ytstrfs.trans_list_as_uniq_keeping_order_n_mutable(self.ytids)

  @property
  def langnames(self):
    lns = []
    for nsufix in self.sfx_n_2letlng_dict:
      twolettercode = self.sfx_n_2letlng_dict[nsufix]
      langname = get_langname(twolettercode)
      lns.append(langname)
    return lns

  def confirm_cli_args_with_user(self):
    if self.goahead_nouserconfirm:
      # this means the running process used the '-y' CLI parameter (autoconfirm)
      self.confirmed = True
      return
    self.confirmed = False
    if not os.path.isdir(self.dirpath):
      scrmsg = f"Source directory [{self.dirpath}] does not exist. Please, retry."
      print(scrmsg)
      return
    charrule = '=' * 27
    print(charrule)
    print('Input parameters entered')
    print(charrule)
    ytids = self.ytids
    total = len(ytids)
    amn = self.audiomainnumber
    voc = self.videoonlycode
    aocs = self.audioonlycodes
    langs_in_asc_order = sorted(self.langnames)
    n_langs = len(langs_in_asc_order)
    scrmsg = f"""
    => ytids = {ytids} | total = {total} | sequential sufix for the 'vd' namemarker = {self.nvdseq} 
    -------------------
    => dirpath = [{self.dirpath}]
    (confer default subdirectory "{default_videodld_tmpdir}" or other)
    -------------------
    => videoonlycode = {voc} | audiomainnumber = {amn} | audioonlycodes = {aocs}
    => sfx_n_2letlng_dict = {self.sfx_n_2letlng_dict} | langnames = {langs_in_asc_order} | n_langs = {n_langs}
    """
    print(scrmsg)
    print(charrule)
    scrmsg = "Are the parameters above okay? (Y/n) [ENTER] means Yes "
    print(charrule)
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      self.confirmed = True
    return

  def read_n_confirm_params(self):
    self.get_cli_args()
    self.read_inputfile_ifneeded()
    # the caller has to check truthiness of self.confirmed to execute the script with this input
    self.confirm_cli_args_with_user()

  def process(self):
    return self.read_n_confirm_params()


def adhoctest1():
  ytid = 'abc+10'
  scrmsg = f'Testing verify_ytid_validity_or_raise({ytid})'
  print(scrmsg)
  ytstrfs.verify_ytid_validity_or_raise(ytid)


def process():
  """
  """
  pass


if __name__ == '__main__':
  """
  adhoctest1()
  adhoctest2()
  """
  process()
