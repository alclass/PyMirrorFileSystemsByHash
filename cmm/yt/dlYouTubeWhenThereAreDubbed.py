#!/usr/bin/env python3
"""
cmm/yt/dlYouTubeWhenThereAreDubbed.py

Obs:
  a) this script is currently located in the 'bin' repo
     and, inside $PATH, can be run everywhere in the system
  b) at some future moment (when DirTree becomes a pipx install),
     it will be moved to the DirTree repo. DirTree as a pipx install
     will also be availbable anywhere in the system.

This script uses (underlying) yt-dlp to download (from YouTube) a video
  in two or more languages.

  YouTube spoken-language-translations are, as far as we've noticed, autodubbed.

Observation about the auto-dubs and this script:

  1) if the video has only its original language available, no auto-dubs,
    this script is able to "fall back" to a one-language download;

  2) the fall-back is not "perfectly" perceived,
    at the time of writing any non-zero return from subprocess
    will make this script try a one-language download,

  3) but, if the non-zero return was caused by, say,
    a network fault, and the video is in fact multilanguage,
    the one-language attempt will also fail
    (attempting to download a multilanguage video with
     a one-language audiocode returns an error from YouTube).

Each language, dubbed or original, has its own separate audio-only-file
  that is 'fused' (or merged) to its video-only counterpart
  forming the language-dubbed video
  (@see example below)

Usage (this is about to receive some adjustments):
  (the --audioonlycodes will become --amn (for "audiomainnumber"))
  (the --map will be introduced)
  (the --videoonlycode will become --voc)
  (a sketch of this upgrade comes at the end)
======

Syntax:
  $dlYouTubeWhenThereAreDubbed.py [--ytid <ytid or yturl within "">]
    [--dirpath "<dirpath>"]
    [--useinputfile]
    [--voc <video-only-code-number>]
    [--amn <audio-main-number>]
    [--map <mapdict|dictionary-map-informing-sufixes-and-their-2-letter-language-codes>]
    [--seq <sequence-number-for-token-vdN-2letter>]

Where:
  <ytid> => the ENCODE64 11-character YouTube video id
    or a youtube-style URL with one but in the latter case
      it should come within "" (quotes) if it has an "&" (ampersand character)
      obs: the use of parameter --useinputfile looks for ytid's
           in a file named youtube-ids.txt in the working directory and ignores --ytid
  <video-format-number> => an integer representing the video-only-format
    default: this parameter defaults to 160 (a video-only-code having 256x144 resolution)
  <audiomainnumber> => the number of the audio-only-code with its dash-sufix-number
  <mapdict> => a dict mapping the dash-sufix-number with its corresponding two-letter-language-code (@see examples)
  <dirpath> is the absolute directory path from where the default download subdirectory resides
    obs: the download directory is a prenamed subdirectory inside <dirpath>
  <dictionary-map-informing-sufixes-and-their-2-letter-language-codes> => see the examples (above and below)
  <sequence-number-for-token-vdN-2letter> => a sequence number to be appended to "token" vd (example: seq=1 -> "vd1-en")

Older syntax (no longer valid):
  $dlYouTubeWhenThereAreDubbed.py [--ytid <ytid or yturl within "">]
    [--dirpath "<dirpath>"]
    [--useinputfile]
    [--videoonlycode <video-format-number>]
    --audioonlycodes <list of audio-format-codes>
    [--nvdseq <sequence-number-for-token-vdN-2letter>]

Example:
========

  1 - using audioparts (when an audio-only file is merged with a video-only file)
    $dlYouTubeWhenThereAreDubbed.py --ytid abcABC123-_
      --amn 233 -- --map "0:en,1:pt"

  2 - without audioparts (when the download file is already a+v merged)
    $dlYouTubeWhenThereAreDubbed.py --ytid abcABC123-_
      --voc 91 --amn -1 --map "0:en,1:pt"

The first example 'automatizes' the following two yt-dlp downloads:
  $yt-dlp -w -f 160+233-0 abcABC123-_
  $yt-dlp -w -f 160+233-1 abcABC123-_

The second example 'automatizes' the following two yt-dlp downloads:
  $yt-dlp -w -f 91-0 abcABC123-_
  $yt-dlp -w -f 91-1 abcABC123-_

Notice:
 1 - yt-dlp doesn't know about the two-letter-codes 'en' & 'pt', these are used later on for file renaming.
 2 - videoonlycode 160 though not given was set because it's the default
     (defaults are stll hardcoded at the time of writing, it may become a configurable in a config-file)

The result will be the download of two videofiles: one with the language audio 233-0
  and the other with the language audio 233-1 (be they original or autodubbed)
This automatization also takes care of the renaming needed
  because yt-dlp does not differentiate filenames by language audio

Care with the use of parameter --useinputfile:
=============================================
  if the user wants to download many videos at once, it can be done with --useinputfile,
    suffice a youtube-ids.txt is created with all the ytid's consolidated
  however, this script (and this is recited below) does not know beforehand if a video
   has or doesn't have a specific language for it, or more formally, it an audioonlycode fits it.

Explanation
===========

This script accepts the following input:
  1  a parameter for a youtube-video-id (ytid)
    1-1  alternatively, instead of a ytid, it may receive as input ytids in a text file (whose name is youtube-ids.txt)
  2 a parameter for the videoonlycode (--voc) (it's only one number, and it's an integer)
  4 a parameter for the audiomainnumber (--amn)
  5 a 'mapping' (--map), as text/string, telling each sufix represents which 2-letter-code language
    (example: '0: en, 1: pt' says sufix 0 is English and sufix 1 is Portuguese)
  6 the audioonlycode is formed by joining the `audiomainnumber` with its number sufix (given in the 'mapping')
    (example: audiomainnumber=249, sufix=0 (for English as in the example above), then audioonlycode="249-0"

Observation about (the lack of) standardization (the equalility) of audio format codes:
  As far as we've observed, these codes are not "fully" standardized
    For example, in general, a video that is autodubbed into English
      has sufix "-0" added to its audioonlycode (example: 233-0 or 234-0),
      but if the original audio is already English,
      it seems undeterminate what "dash-number" the YouTube (web) system will give to it
        example: it might be either a "-7", or a "-8" or a "-9"
        (ie, it's not a standard fixed dash-number for all cases)
        the user would have to find out and enter it via the audiocodes parameter

  This script as yet doesn't do an API asking for a mapping of language to audio-only format code

  On the other hand, what seems stable is when English is autodubbed and another language is the original
  In these cases, English gets a "-0" annexed to the audioonlycode and the original language gets a "-1"
    Examples:
       233-0 autodubbed English and 233-1 the original other language
       234-0 autodubbed English and 234-1 the original other language

This script downloads the video-only part exactly once and copies it to each new audio language piece.
This saves transfer bytes and redownload times, because the n-language resulting videos will be formed,
  each one, by joining its videopart (the same for all languages) with its audiopart (language by language).

For example:
  1 suppose a video with available autodubbed translation(s)
  2 suppose also that videoonlycode 160 is available
  3 suppose also that audioonlycodes are available in dubbed Italian (as 233-5) and in the original English (as 233-9)

Then, this script will:
  1st download the 160 video
  2nd copy it once (making two of them) because one will be used for Italian (it), the other for English (en)
  3rd download the 160+233-5, yt-dlp recognizes that the videopart is already present
    and proceeds to download only the audio "it" piece (yt-dlp then blends the audio with the videoonly)
  4th download the 160+233-9, idem for the "en" piece

An example of language audiocodes, having English as the original:
    (seen in a channel we visited) (notice that this pattern may not be a general rule):
    (this example shows only the dash-numbers for the audioonlycode 233)
  233-0  mp4 audio only    m3u8 [de-DE] Deutsch (Deutschland) - dubbed-auto
  233-1  mp4 audio only    m3u8 [es-US] es-US - dubbed-auto
  233-2  mp4 audio only    m3u8 [fr-FR] Français (France) - dubbed-auto
  233-3  mp4 audio only    m3u8 [hi] हिन्दी - dubbed-auto
  233-4  mp4 audio only    m3u8 [id] Indonesia - dubbed-auto
  233-5  mp4 audio only    m3u8 [it] Italiano - dubbed-auto
  233-6  mp4 audio only    m3u8 [ja] 日本語 - dubbed-auto
  233-7  mp4 audio only    m3u8 [pl] polski - dubbed-auto
  233-8  mp4 audio only    m3u8 [pt-BR] Português (Brasil) - dubbed-auto
  233-9  mp4 audio only    m3u8 [en-US] American English - original (default)

An example of language audiocodes, having Portuguese as the original:
    (seen in another channel we visited):
    (again it only shows the dash-numbers for the audioonlycode 233)
  233-0  mp4 audio only    m3u8 [en-US] American English - dubbed-auto
  233-1  mp4 audio only    m3u8 [pt-BR] Português (Brasil) - original (default)

This second case (where English is autodubbed) seems more stable in terms of standardized audiocodes.

The first case (where English is the original) seems to make English have
  either a "-9" (dash nine), most of the time,
  or "-8" (dash eight) sometimes.

The user has to use the audiocodes CLI parameter to tell the correct audiocodes (*).
  The default today uses [233-0, 233-1]
    and, as mentioned above, stable for when English is autodubbed
    and another language is the original.

 (*) to see the full list of CLI parameters for this script together with this docstr,
     run it with --docstr
     (parameter --help shows all parameters with a short description for each)
     (or scroll up the text to the beginning)

A programatic discovery of these audioonlycodes (mapping language to format code)
  via yt-dlp might be done in the future,
  but as yet this is not performed by this script

Notice as a reemphasis that in the first example above, the original English is 233-9
  and, in the second, English autodubbed, it's 233-0

===========
Limitation: what does this script not do (at least yet)?

  1 it doesn't discover the language codes,
    even less which codes for a certain video are available at all.
    (@see also docstr above)
    (The user may find out available format codes using the
      -F (or --format) parameter in yt-dlp.)
  2 it may not be able to check all problems from the command line, but if subprocess returns 0,
    it's a good sign that both the network is working and videos have "arrived" complete
    if 'subprocess' does not return with 0, at the time of writing,
      the program will halt showing its error message,
    (ie, this script does not yet treat non-0 return cases [*] in a better way)
    [*] imagining mostly that non-O returns are network problems or yt-dlp upgrade needs

On 2025-08-01:
  the main parts of this script were already in use
On 2025-09-23:
  thinking about an innovation to this script
  introducing the --map CLI parameter
  --map accepts a string like a dict without spaces, e.g.:
    => --map "0:en,1:pt"
  This informs the number sufixes for the audioonlycode and which languages those represent
  Because of that, parameter --audioonlycodes may also be changed:
    FROM: a list of "dashed" number (e.g. "[249-0, 249-1"])
    TO: a number  (e.g. "249")
  --audioonlycodes may become --audiomainnumber
On 2025-09-26:
  the parameters have been changed, the main code refactored (a new class now help form the audiocodes),
  it's running though some improvements are still needed "here and there".

Justification:
  the number of YouTube autodubbed available languages
    not only may vary from video to video (especially in English originals),
    but it may also grow in the future
      (for example: Russian appeared -- we haven't seen it before -- in some original-English videos)
"""
from pathlib import Path
import os
import shutil
import subprocess
import sys


def insert_rootpath_into_pythonpath_for_non_venv_run():
  """
  This script has a client dispatcher outside its venv (virtual environment)
  Because of that -- and also because os.system() or subprocess.run() cannot activate venv --
  this function was thought out to insert the app's root folder into the search path,
  so that local imports (as the next one that follows) may work.

  Notice also that this solution is a bit manual because the script itself does not know
    where its app's root folder (it's known when venv is active), though some convention
    (for example, a specific filename on root) might help find it. But it would perhaps be
    a bit wordy and clunky file by file.

  # print(sys.path)
  """
  this_scr_ap = Path(__file__)
  projroot_ap = this_scr_ap.parent.parent.parent
  sys.path.insert(0, str(projroot_ap))


insert_rootpath_into_pythonpath_for_non_venv_run()
import cmm.yt.ytids.yt_sufix_mappers_clss as ytstrfs # .SufixLanguageMapper
import cmm.yt.ytids.ytstrfs_etc as ytfs  # ytfs.get_validated_ytid_or_raise
import cmm.yt.lib.os.osentry_class as ose  # ose.OSEntry
import llib.os.regexfs.filenamevalidator_cls as fnval  # .FilenameValidator
import cmm.yt.lib.os.cliparams_for_utubewhendub as clip  # ose.OSEntry
# DEFAULT_AUDIOVIDEO_CODE = ose.DEFAULT_AUDIOVIDEO_CODE
# DEFAULT_AUDIOVIDEO_DOT_EXT = ose.DEFAULT_AUDIOVIDEO_DOT_EXT
OSEntry = ose.OSEntry
DEFAULT_YTIDS_FILENAME = ose.DEFAULT_YTIDS_FILENAME
DEFAULT_AUDIO_MAIN_NUMBER = ose.DEFAULT_AUDIO_MAIN_NUMBER  # may be -1 meaning no audio separate
DEFAULT_SFX_W_2LETLNG_MAPDCT = ose.DEFAULT_SFX_W_2LETLNG_MAPDCT
DEFAULT_VIDEO_ONLY_CODE = ose.DEFAULT_VIDEO_ONLY_CODE
VIDEO_DOT_EXTENSIONS = ose.VIDEO_DOT_EXTENSIONS
default_videodld_tmpdir = ose.default_videodld_tmpdir


def show_docstrhelp_n_exit():
  print(__doc__)
  sys.exit(0)


class Downloader:

  # class-wide static constants
  DEFAULT_VIDEO_ONLY_CODE = DEFAULT_VIDEO_ONLY_CODE
  DEFAULT_SFX_W_2LETLNG_MAPDCT = DEFAULT_SFX_W_2LETLNG_MAPDCT
  DEFAULT_AUDIO_MAIN_NUMBER = DEFAULT_AUDIO_MAIN_NUMBER
  videodld_tmpdirname = default_videodld_tmpdir
  # class-wide static interpolable-string constants
  comm_line_base = 'yt-dlp -w -f {ytdlp_fcode_str} "{videourl}"'
  video_baseurl = 'https://www.youtube.com/watch?v={ytid}'

  def __init__(
      self,
      ytid: str,
      dlddir_abspath: str = None,
      videoonlycode: int = None,
      audiomainnumber: int = None,
      nvdseq: int = None,
      sfx_n_2letlng_dict: dict | str = None,
    ):
    self.ytid = ytfs.get_validated_ytid_or_raise(ytid)
    self.dlddir_abspath = dlddir_abspath
    self.treat_dlddir_abspath()
    self.videoonlycode = videoonlycode or DEFAULT_VIDEO_ONLY_CODE
    # if audiomainnumber is None, it will get -1 meaning the formatcode in voc is already a+v
    # i.e., the video comes whole, no merging of a+v (audio with video)
    self.audiomainnumber = audiomainnumber or DEFAULT_AUDIO_MAIN_NUMBER  # example: 233, 234, 249 etc
    self.nvdseq = nvdseq or 1
    sfx_n_2letlng_dict = ytfs.trans_str_sfx_n_2letlng_map_to_dict_or_raise(sfx_n_2letlng_dict)
    # the langmapper object contains the attributes that help form the audioonlycodes and the filename-rename-tokens
    self.langmapper = ytstrfs.SufixLanguageMapper(sfx_n_2letlng_dict, self.audiomainnumber)
    self.cur_lng_obj = None  # each language is abstracted to a "language object" as each download happens
    self.last_nsufix_in_case_it_failed = 0
    self.b_verified_once_tmpdir_abspath = None
    # self.prename = None
    self.previously_existing_filenames_in_tmpdir = []
    self.osentry = OSEntry(
      workdir_abspath=self.child_tmpdir_abspath,
      basefilename=None,  # later to be known
      videoonly_or_audio_code=self.videoonlycode
    )

  def treat_dlddir_abspath(self):
    if self.dlddir_abspath is None or self.dlddir_abspath == '.':
      # default is the current working directory
      self.dlddir_abspath = os.path.abspath('.')
    if not os.path.isdir(self.dlddir_abspath):
      errmsg = f"Error: Download directory [{self.dlddir_abspath}] does not exist."
      raise OSError(errmsg)

  @property
  def n_ongoing_lang(self) -> int:
    """
    Returns an int that represents the sequencial order of a download language
      given its sufix number inside the map-dict-input.

    Example (the key in dict is the suffix number, the value is the 2-letter-language-code):
      1st
        if language dict is {0: 'en', 1: 'pt'}
        then 'en' has seq_order=1 and 'pt' has seq_order=2
      2nd
        if language dict is {5: 'it', 1: 'pt', '11': 'en'}
        then 'pt' has seq_order=1, 'it' has seq_order=2, and 'en' has seq_order=3
          # notice that, though example-dict may not show it, key ordering should be ascendent (1, 2, 3...)
          # i.e., as list(sorted(dict.item())): [(1: 'pt'), (5: 'it'), ('11': 'en')]
    """
    if self.cur_lng_obj is None:
      return -1
    return self.cur_lng_obj.seq_order

  @property
  def total_langs(self) -> int:
    if self.langmapper is None:
      return -1
    return self.langmapper.size

  @property
  def videourl(self):
    pdict = {'ytid': self.ytid}
    url = self.video_baseurl.format(**pdict)
    return url

  def rename_canofile_to_the_bk1sufixed(self):
    """
    The canonical filename (the one downloaded) gets renamed to the bk1_sufixed filename
      for the first language available
    :return:
    """
    srcfilepath = self.osentry.fp_for_fn_as_name_fsufix_ext
    srcfilename = self.osentry.fn_as_name_fsufix_ext
    # at this point, self.cur_lng_obj does not yet exist, because it will appear later in downloading the audios
    lng_obj = self.langmapper.get_first_langobj()
    twolettercode = lng_obj.twolettercode  # index 0 is the first language
    ln = lng_obj.langname
    nsufix = lng_obj.nsufix  # index 0 is the first language
    audioonlycode = lng_obj.audioonlycode  # index 0 is the first language
    trgfilepath = self.osentry.get_fp_for_fn_as_name_fsufix_ext_bksufix(1)
    trgfilename = self.osentry.get_fn_as_name_fsufix_ext_bksufix(1)
    if not os.path.isfile(srcfilepath):
      scrmsg = f"""Cannot rename lang={nsufix} twolettercode={twolettercode} lang={ln}
        FROM canofilename = [{srcfilename}]
        TO bk1sufixfilename = [{trgfilename}]
          => reason: because {srcfilename} does not exist in folder. Halting.
      """
      print(scrmsg)
      sys.exit(1)
    if os.path.isfile(trgfilepath):
      scrmsg = f"""Already renamed lang={nsufix} 2-letter lang code={twolettercode} lang={ln}
        -------------------- 
        FROM (canofilename)  : [{srcfilename}]
        TO (bk1sufixfilename): [{trgfilename}]
        -------------------- 
          => reason: because {trgfilename} exists in folder. Continuing.
      """
      print(scrmsg)
      return
    try:
      os.rename(srcfilepath, trgfilepath)
      scrmsg = f"Renamed accomplished: ytid={self.ytid} 2-letter lang code={twolettercode} lang={ln}"
      print(scrmsg)
    except (IOError, OSError) as e:
      errmsg = f"""Error when attempting to rename bksufix lang=1 audiocode={audioonlycode} lang={ln} 
      => {e}

      Halting.
      """
      print(errmsg)
      sys.exit(1)

  def copy_n_rename_videoonly_n_lang_times(self):
    """
    The first video is just renamed to sufix "bk<seq>" where seq is the audio sequential number
    The following videos are copied each one with its "bk<seq>" sufix
    Obs: The first video is renamed at the end, i.e., the copies are done firstly
    """
    if self.total_langs == 0:
      #  nothing to rename, return
      return
    # the path below represents the video filename as downloaded
    # the first one is a rename
    self.rename_canofile_to_the_bk1sufixed()
    if self.total_langs == 1:
      # done, return
      return
    # at this point, bk1 exists due to the previous rename (the method called above in this)
    srcfilepath = self.osentry.get_fp_for_fn_as_name_fsufix_ext_bksufix(1)
    srcfilename = self.osentry.get_fn_as_name_fsufix_ext_bksufix(1)
    if not os.path.isfile(srcfilepath):
      errmsg = f"Error: srcfilename [{srcfilename}] for copying bk's does not exist."
      print(errmsg)
      sys.exit(1)
    for self.cur_lng_obj in self.langmapper.loop_over_langs():
      seq = self.cur_lng_obj.seq_order
      audioonlycode = self.cur_lng_obj.audioonlycode
      ln = self.cur_lng_obj.langname
      trgfilepath = self.osentry.get_fp_for_fn_as_name_fsufix_ext_bksufix(seq)
      trgfilename = self.osentry.get_fn_as_name_fsufix_ext_bksufix(seq)
      scrmsg = f"""[copying bk1 to the bk{seq} for audiocode={audioonlycode} {ln}]
        -------------------- 
        FROM:  {srcfilename}
        TO:    {trgfilename}
        --------------------
      """
      print(scrmsg)
      if os.path.isfile(trgfilepath):
        scrmsg = f"Continuing: trgfilename [{trgfilename}] already exists."
        print(scrmsg)
        continue
      try:
        shutil.copy2(srcfilepath, trgfilepath)
      except (IOError, OSError) as e:
        errmsg = f"""Error: the copying above failed
          -------------------- 
          FROM:  {srcfilename}
          TO:    {trgfilename}
          -------------------- 
        {e}
        Halting.
        """
        print(errmsg)
        sys.exit(1)

  def store_files_that_already_exist_into_a_list(self, tmpdir_abspath):
    """
    routine that looked up dateprefix, discontinued later on (kept for history purpose)
      pp = filename.split(' ')
      strdate = pp[0]
      if not pydates.is_strdate_before_today(strdate):
        errmsg = fDate Sufix [{strdate}] in filename is either missing or it's not past today
        raise OSError(errmsg)
    """
    filenames = os.listdir(tmpdir_abspath)
    for filename in filenames:
      if not filename.endswith(tuple(ose.VIDEO_DOT_EXTENSIONS)):  # notice osentry has not yet been initialized at this
        continue
      self.previously_existing_filenames_in_tmpdir.append(filename)

  def verify_tmpdir_once_n_store_files_already_existing(self, tmpdir_abspath):
    """
    This method:
      1: creates tmpdir if needed
      2: ignores (jumps over) files that are not video (by extension)
      3: those are video (by extension) have their filenames stored for later finding the downloaded one
      3: in case a video exists with today's date, an OSError exception is raised
    :param tmpdir_abspath:
    :return:
    """
    self.b_verified_once_tmpdir_abspath = False
    os.makedirs(tmpdir_abspath, exist_ok=True)
    self.store_files_that_already_exist_into_a_list(tmpdir_abspath)
    self.b_verified_once_tmpdir_abspath = True

  @property
  def child_tmpdir_abspath(self):
    """
    The actions/tasks of the script happen in a newly created (intended to be temporary) directory
      (or if previously created, files should have a previous-date prefix, otherwise this script should be interrupted)

    The composition-class OSEntry also has it from here
    To avoid an "origins" bug, it may be advisable to always read it from OSEntry (though the two are the same)
    :return:
    """
    tmpdir_abspath = os.path.join(self.dlddir_abspath, self.videodld_tmpdirname)
    if not self.b_verified_once_tmpdir_abspath:
      self.verify_tmpdir_once_n_store_files_already_existing(tmpdir_abspath)
    return tmpdir_abspath

  def fallbackcase_ask_user_what_the_downloaded_filename_is(self):
    scrmsg = f"""Script was not able to find the downloaded filename,
     this can happen when the download had alread happened before,
     please check whether one can be found in the tmpdir and enter it here
     ---------------------------------------------------------------------
     the tmpdir to look up is [{self.osentry.workdir_abspath}]
     ---------------------------------------------------------------------
     In case one cannot be found, type [ENTER] to stop script and
     restart it back up after cleaning up tmpdir (ie leaving the directory empty without files)
     ---------------------------------------------------------------------
     type downloaded filename or [ENTER] => """
    ans = input(scrmsg)
    if ans == '':
      sys.exit(1)
    # check entered filename
    filename = ans
    filepath = os.path.join(self.osentry.workdir_abspath, filename)
    if not os.path.isfile(filepath):
      errmsg = f"""Error:
      entered filename "{filename}" does not exist in tmpdir:
      tmpdir = {self.osentry.workdir_abspath}'
      Please, emtpy this tmpdir directory and retry this program.
      (From an empty dir, script will be able to find the correct downloaded filename.)
      If it tmpdir was already empty, the following two actions may be looked up at:
        a) check network connection 
        b) check the status of the underlying yt-dlp command (it may need upgrading)
      """
      print(errmsg)
      sys.exit(1)
    return filename

  def discover_dldd_videofilename(self):
    """
    Discovers the filename of the downloaded yt-dlp file

    However, when a download is incomplete, the following info is important.

    The discovery process is easy when the download process in not interrupted,
      but tricky if the user is continuing a previous incomplete download.

    When a download process is completed, a prefix is prepended to the filenames.

    When download process is incomplete and the user restarts it,
      this script makes a "strong" supposition", .i.e., one
      to consider the canonical filename, if present, representing the video-only-file.

    * Chances are this is correct, but, if not, the user should restart from an empty directory.

    How to discovery works
    =======================

    Before making the "strong assumption" above, the script compares the directory contents
      looking for a new file (comparasion before versus after). This is the first try.

    The second try starts when, after the first try, a new recent file is not found.
    The second is based on a "strong supposition". This assumption may delineate as follows:
      1 - a previous download happened, but it's incomplete
      2 - a look-up for the canonical file in folder will be interpreted as the video-only-file

    The user should know this detail because if this hypothesis is wrong, the result may be wrong.
    Another hypothesis is a failure in the last renaming of the last run, i.e., the download was completed,
      and the user is restarting a new one actually without need.

    If the second try does not find a file, the third try asks the user which filename
      is the correct one.
    """
    # if self.osentry.has_basefilename_been_found():
    #   return
    videofilename_soughtfor = None
    allfilenames = os.listdir(self.osentry.workdir_abspath)
    # dot_ext = self.cur_dot_ext
    # ----------
    # first try: folder contents comparison (before versus after)
    # ----------
    videofilenames = filter(lambda f: f.endswith(tuple(VIDEO_DOT_EXTENSIONS)), allfilenames)
    videofilenames_appearing_after = list(
      filter(
        lambda f: f not in self.previously_existing_filenames_in_tmpdir,
        videofilenames
      )
    )
    n_results = len(videofilenames_appearing_after)
    if n_results == 1:
      videofilename_soughtfor = videofilenames_appearing_after[0]
      scrmsg = f"Found downloaded file as [{videofilename_soughtfor}]"
      print(scrmsg)
    else:
      # at this point, some hypotheses come to mind
      # 1 - directory may be empty (which might signal a network failure)
      # 2 - file had already been downloaded before, so the comparison before versus after does not find it
      # 3 - if 2 is so, the second try below may find it, but if this hypothesis is wrong and
      #     an error happened in the final renaming of last run,
      #     this solution will not be the correct one (at any rate, if the user finds it incorrect,
      #     a new start should be done on an empty working directory)
      # ----------
      # second try: look up filename's canonical form
      # ----------
      scrmsg = f"Looking up the downloaded file among {len(allfilenames)} files in folder"
      print(scrmsg)
      for fn in allfilenames:
        # filename is compliant to the "canonical filename", i.e., name[ytid].ext
        validator = fnval.FilenameValidator(filename=fn)
        if validator.is_filename_a_valid_ytdlp and self.ytid == validator.ytid:
          # found it (it has the canonical form and the same ytid, that's it [unique])
          videofilename_soughtfor = fn
          scrmsg = f"Found downloaded file as [{videofilename_soughtfor}]"
          print(scrmsg)
          break
    if videofilename_soughtfor is None:
      # ----------
      # third (and last) try: ask the user
      # ----------
      videofilename_soughtfor = self.fallbackcase_ask_user_what_the_downloaded_filename_is()
    if videofilename_soughtfor is None:
      # oh, oh, can't continue
      errmsg = f"Error: videofilename_soughtfor is None, script could not find it."
      raise OSError(errmsg)
    self.osentry.basefilename = videofilename_soughtfor
    # set found dot_extension to osentry
    _, self.osentry.dot_ext = os.path.splitext(self.osentry.basefilename)
    scrmsg = f"Discovered videofilename after download: [{self.osentry.fn_as_name_ext}]"
    print(scrmsg)

  def rename_from_canonical_to_fsufixedvideoonlyfile(self) -> bool:
    """
    Grafts (inserts) the ".f<number>" sufix before its extension,
      for example:
        a) if videoonlycode is 160
        b) then the in-between fsufix will be ".f160"
        c) and a filename example should be like "this-filename.f160.mp4"

    Obs: notice that the nicknamed bksufix is another different one in this object-class,
         this here is nicknamed fsufix
         this is noted because sometimes at first nod one confuses
           the former [bksufix] with the latter [fsufix]
           and this bksufix is not used in this method
    """
    if os.path.isfile(self.osentry.fp_for_fn_as_name_fsufix_ext):
      scrmsg = f"""Not renaming to the f-sufixed ({self.osentry.fsufix}) video-only-filename:
         => [{self.osentry.fn_as_name_fsufix_ext}]
       as it already exists in the working directory.
       ---------------
       Script is going to delete the canonical file [{self.osentry.fn_as_name_ext}] 
       so that after the next language download yt-dlp will be able to blend audio with video."""
      print(scrmsg)
      os.remove(self.osentry.fp_for_fn_as_name_ext)
      print('Canonical file deleted. Continuing.')
      return False
    try:
      os.rename(self.osentry.fp_for_fn_as_name_ext, self.osentry.fp_for_fn_as_name_fsufix_ext)
      scrmsg = f"""Rename succeeded (from canonical to f-sufixed).
      FROM (canonical): [{self.osentry.fn_as_name_ext}]
      FROM (f-sufixed): [{self.osentry.fn_as_name_fsufix_ext}]"""
      print(scrmsg)
      return True
    except (IOError, OSError) as e:
      errmsg = f"""Error when attempting to rename
      canonical to fsufixed_videoonlyfilename i.e.,
      FROM : [{self.osentry.fn_as_name_ext}]
      TO   : [{self.osentry.fn_as_name_fsufix_ext}]
      => {e}
      """
      raise OSError(errmsg)

  def download_video_only(self):
    """
    This method downloads the videoonlyfile via yt-dlp using Python's subprocess.run()

    At the time of this writing, when subprocess raises subprocess.CalledProcessError
      the script wraps it up with an additional error-message and then reraises ValueError

    [For later on]
    This may be improved later on if some hypotheses for the
      subprocess.CalledProcessError exception allow this program to continue on
    """
    strdict = {'ytdlp_fcode_str': self.videoonlycode, 'videourl': self.videourl}
    comm = self.comm_line_base.format(**strdict)
    scrmsg = f"@download_video_only | {comm}"
    print(scrmsg)
    try:
      """
      ans = input('Run this command above (Y/n) ? [ENTER] means Yes')
      if ans not in ['Y', 'y', '']:
        return False
      """
      subprocess.run(comm, shell=True, check=True)  # timeout=5 (how long can a download last?)
    # except subprocess.TimeoutExpired:
    #     print(f"Command timed out: {comm}")
    except subprocess.CalledProcessError as e:
      warnmsg = f"""Command: [{comm}]
       failed with return code: [{e.returncode}]
       => {e}"""
      print(warnmsg)
      # raise ValueError(errmsg)
      return False
    except KeyboardInterrupt:
      scrmsg = "Interrupted by user. Exiting loop. Continuing."
      print(scrmsg)
    return True

  def find_existfilepath_of_samecanonicalfilename_w_different_ext_or_none(self):
    """
    At this point, the canonical filepath (with the name created by yt-dlp)
      is not present in dir.
    Chances are its extension got changed with a different combination of audio and video fusion.

    Example: instead of mp4, it may now be mkv
      (the blending of audio+video may have changed a previous extension)

    This method tries to find a same canonical name with a different extension

    Obs:
      a) the approach this method tries is to look up different extensions with the same name
      b) another approach would be to try to use the ytid itself sufixed to name

    """
    canoname = self.osentry.fn_as_name_ext
    curr_dot_ext = self.osentry.dot_ext
    extensions = list(self.osentry.video_dot_extensions)
    try:
      # remove the extension it already has, go look up a matching one among the remaining ones
      extensions.remove(curr_dot_ext)
    except ValueError:
      pass
    # look up if there are still extensions in the list
    while len(extensions) > 0:
      next_dot_ext = extensions.pop()
      # recompose filename with new extension
      soughtfor_alt_filename = f"{canoname}{next_dot_ext}"
      soughtfor_alt_filepath = os.path.join(self.osentry.workdir_abspath, soughtfor_alt_filename)
      if os.path.isfile(soughtfor_alt_filepath):
        # found it
        # self.cur_dot_ext = next_dot_ext
        return soughtfor_alt_filepath
    return None

  def rename_bksufixedfilename_to_fsufixedfilename_to_avoid_the_vo_redownload(self):
    """
    The supposed videofilename is videobasefilename (*) minus its number sufix
    (*) reminding that the videopart (the audioless video) is only downloaded once,
        every language video will be formed by just getting extra its own audiopart as a complement
    For example:
      if  audiolessvideofile is "this-video-ytid.f160.mp4.bk3"
          then it needs just to strip the bk3 sufix, resulting "this-video-ytid.f160.mp4"
      ie this rename should only remove the ".bk3" sufix
      Because extensions may very, a rstrip() strategy is preferrable

    Notice also that self.n_on_going_lang -- keeping language 1, 2, 3... sequence -- is also
      the number for the bksufix

    Cases where extensions are not '.mp4'
    =====================================

    When a 'fused' video (fusion is the junction of audio with video),
      joining an audio that will generate a different extension
      (say mkv) than the first ones (say mp4), this last rename will not
      find the correct filename. At the same time, this script does not
      know which combinations generate which extensions.
    To solve this, the system falls back looking to
      a second file with the same name, i.e.,
        filename-video-such-and-such-ytid.mp4
        filename-video-such-and-such-ytid.mkv
      then, with the name part of the filename, the mkv one -- in another one in the set -- may be found.
    """
    srcfilepath = self.osentry.get_fp_for_fn_as_name_fsufix_ext_bksufix(self.n_ongoing_lang)
    srcfilename = self.osentry.get_fn_as_name_fsufix_ext_bksufix(self.n_ongoing_lang)
    trgfilepath = self.osentry.fp_for_fn_as_name_fsufix_ext
    trgfilename = self.osentry.fn_as_name_fsufix_ext
    # check existence
    if not os.path.isfile(srcfilepath):
      errmsg = f"""For the rename above
      ---------------------------------------
      FROM (bksufix):  [{srcfilename}]
      ---------------------------------------
        => is missing. Halting.
      """
      print(errmsg)
      sys.exit(1)
    if os.path.isfile(trgfilepath):
      if os.path.isfile(srcfilepath):
        # only the target should continue on folder
        errmsg = f"""For the rename above
        ---------------------------------------
        *** deleting (bksufix):  [{srcfilename}]
            existing (fsufix) :  [{trgfilename}]
        ---------------------------------------
          => reason: both files exist.
        """
        print(errmsg)
        os.remove(srcfilepath)
      return
    try:
      os.rename(srcfilepath, trgfilepath)
      scrmsg = f"""Rename succeeded: from bksufix=".bk{self.n_ongoing_lang}" to fsufix="{self.osentry.fsufix}":
      ----------------------------------
      FROM (bksufix):  [{srcfilename}]
      TO    (fsufix):  [{trgfilename}]
      ----------------------------------
        Going for downloading the audio-complement.
      """
      print(scrmsg)
    except (IOError, OSError) as e:
      errmsg = f"""Error when attempting to rename:
      ---------------------------------------
      FROM (bksufix):  [{srcfilename}]
      TO    (fsufix):  [{trgfilename}]
      ---------------------------------------
      => {e}
      Halting.
      """
      print(errmsg)
      sys.exit(1)

  def set_langmapper_to_no_dubs(self):
    """
    Deletes all audioonlycodes after ongoing_index in queue (list)

    This happens when the fallback method is called, which
      means that this script has decided (supposion as designed)
      that the video in question does not have language subcodes.

    For example:
      Suppose an attempt to download audioonlycode 233-0 fails
      returning exit 1. After that it retries with audioonlycode 233
      (audioonlycode without a dash number).
      Okay. Suppose that originally the queue is 233-0, 233-1, 233-2
      But, in this case, all subsequent dash-numbers (-1 and -2)
      in queue must be removed.

      This method does this emptying of the queue.

    Obs: unfortunalety, the way this method was planned got 'disconnected'
      from the main process loop which goes through the array of languages sequentially,
      not looking at the audioonlycodes list.  This may be replanned in the future,
      i.e., looping directly through the audioonlycodes list or testing its size there,
      this was not done yet because the call stack got bigger, so we want to do this
      correction by looking them all. (This problem only affects the download process
      when the video requested does not have the audioonlycode in passing.)
    while self.n_ongoing_lang < self.langmapper.size - 1:
      _ = self.audioonlycodes.pop()
    """
    self.langmapper.turn_off_dubs()

  def fallback_to_nondashed_audiocode_changing_it(self):
    """
    The strategy here is the following:

    1 when languages are asked in the audiocodes parameters,
      they are "dash-sufixed", for example

      ['233-0', '233-1'] or ['233-5', '233-9']

      (The hypothesis below is not guaranteed, but this program will try it.)

      When 'subprocess' returns non-0, there's a chance the video does
        not have language variations. This might be also deduced
        that, instead of audiocode, say, 233-0, 233 (without dash-0) could work
          and form the video in its original language.
        That would complete the job supposing the video does not have another translated language anyway.
    """
    self.set_langmapper_to_no_dubs()
    scrmsg = "Set language mapper to 'no dubs'"
    print(scrmsg)

  @property
  def composite_av_code(self) -> str:
    """
    This property-method should only be called in the looping of the audio downloads
      because self.cur_lng_obj will be attributed later until it gets there and,
      before that, it will remain 'None'
    :return:
    """
    if self.cur_lng_obj is None:
      errmsg = f"Cannot form the composite video+audio code to yt-dlp, because current language is None"
      raise ValueError(errmsg)
    composite_ytdlp_videoaudiocode = f"{self.videoonlycode}+{self.cur_lng_obj.audioonlycode}"
    return composite_ytdlp_videoaudiocode

  def download_audiopart_to_blend_it_w_videoonly(self):
    """
    One caution here:
      the videoonly_canonical_filename (the one without sufixes) should not be present,
        otherwise yt-dlp will deduce that the composition (or video-audio merging) has already happened,
        and inform the video has already been download (when only the video-only-audioless-part has)
      So, if the videoonly_canonical_filename is present, rename it to its fsufix form.
    """
    self.rename_bksufixedfilename_to_fsufixedfilename_to_avoid_the_vo_redownload()  # it's done by removing number sufix
    aoc = self.cur_lng_obj.audioonlycode
    tlc = self.cur_lng_obj.twolettercode
    ln = self.cur_lng_obj.langname
    scrmsg = f"audioonlycode now is {aoc} | its 2-letter-lang-code is {tlc} {ln}"
    print(scrmsg)
    pdict = {'ytdlp_fcode_str': self.composite_av_code, 'videourl': self.videourl}
    comm = self.comm_line_base.format(**pdict)
    try:
      scrmsg = f"composite_av_code={self.composite_av_code} | running: {comm}"
      print(scrmsg)
      """
      scrmsg = f"Continue with the download above (Y/n)? [ENTER] means Yes"
      ans = input(scrmsg)
      if ans not in ['Y', 'y', '']:
        sys.exit(0)
      """
      subprocess.run(comm, shell=True, check=True)  # timeout=5 (how long can a download last?)
    # except subprocess.TimeoutExpired:
    #     print(f"Command timed out: {comm}")
    except subprocess.CalledProcessError as e:
      errmsg = f"""
      =+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|
      Subprocess returned with an error:
      Command failed with return code {e.returncode}: {comm}
      full error msg => {e}
      => trying a download without an audiocode with the dashed-sufix
      =+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|=+=|
      """
      print(errmsg)
      self.fallback_to_nondashed_audiocode_changing_it()
    except KeyboardInterrupt:
      print("Interrupted by user. Exiting loop.")

  def rename_videofile_after_audiovideofusion(self):
    """
    Because two or more files may be downloaded and cannot have the same name,
      (yt-dlp forms the same filename regardless of language)
      this method prefix-renames the file just downloaded so that the next
      download is "available" for yt-dlp (as its filename is available)
    """
    srccanofilepath = self.osentry.fp_for_fn_as_name_ext
    srccanofilename = self.osentry.fn_as_name_ext
    lang2lettcode = self.cur_lng_obj.twolettercode
    # "vd1" stands for "video 1", the idea is that the user may need it for organizing purposes
    langprefix = f"vd{self.nvdseq}-{lang2lettcode}"
    langprefixedfilename = f"{langprefix} {self.osentry.fn_as_name_ext}"
    langprefixedfilepath = os.path.join(self.osentry.workdir_abspath, langprefixedfilename)
    if not os.path.isfile(srccanofilepath):
      # delete the method below when the finding routing gets working in class osentry
      # srccanofilepath = self.find_existfilepath_of_samecanonicalfilename_w_different_ext_or_none()
      self.osentry.find_n_set_the_canonical_with_another_extension()
      srccanofilepath = self.osentry.fp_for_fn_as_name_ext
      srccanofilename = self.osentry.fn_as_name_ext
      if srccanofilepath is None:
        # can't rename
        wrnmsg = f"""Cannot rename canonical filename:
          ------------------------------------
          canoname : [{srccanofilename}]
          FROM (canonical)  : [{srccanofilepath}]
          TO (lang-prefixed): [{langprefixedfilename}"]
          ------------------------------------
          => reason: canonical (FROM) is not present in folder. Continuing."""
        print(wrnmsg)
        return
      # update srccanofilename from srccanofilepath
      _, srccanofilename = os.path.split(srccanofilepath)
    if os.path.isfile(langprefixedfilepath):
      # if targetfile is present, rename will raise an exception, return from here
      wrnmsg = f"""Cannot rename to lang-prefixed filename:
        ------------------------------------
        FROM (canonical)  : [{srccanofilepath}]
        TO (lang-prefixed): [{langprefixedfilename}"]
        ------------------------------------
        => reason: lang-prefixed filename (TO) is already present in folder. Continuing."""
      print(wrnmsg)
      return
    try:
      os.rename(srccanofilepath, langprefixedfilepath)
      audioonlycode = self.langmapper.get_audioonlycode_for_1baseidx(self.n_ongoing_lang)
      scrmsg = f"""Rename succeeded => lang={self.n_ongoing_lang} | audiocode={audioonlycode}
      ------------------------------------
      FROM (canonical)  : [{srccanofilename}]
      TO (lang-prefixed): [{langprefixedfilename}]
      ------------------------------------
      """
      print(scrmsg)
    except (IOError, OSError) as e:
      warnmsg = f"""Error when attempting to rename:
      ---------------------------------------
      FROM (canonical)  : [{srccanofilepath}]
      TO (lang-prefixed): [{langprefixedfilename}"]
      ---------------------------------------
      => {e}
      Continuing.
      """
      print(warnmsg)
      # sys.exit(1)

  def check_if_canonicalname_changed_its_extension(self):
    """
    The canonical file is composed as:
      {prename}{one-blank-space}{ytid_within_squarebrackets}{dot_extension}
    At a certain combination (example: when a 249 audio is downloaded with a mp4),
      the canonical name changes its extension from 'mp4' to 'mkv'
    So, this method tries to observe if this extension change happened
    """
    self.osentry.find_n_set_the_canonical_with_another_extension()

  def download_audio_complements(self):
    """
    A lang_o carries the following attributes:
        -> langless_audiocode (the same as audiomainnumber)
        -> nsufix (the same as langnumber)
        -> twolettercode (the 2-letter language abbreviation: en, es, fr, etc.)
        -> seq_order=seq_order (the sequence order of the language: e.g. {0 (seq 1), 3 (seq 2), 6 (seq 3)}
        -> audioonlycode (dynamic, it's the sum of langless_audiocode, a dash, and the langnumber)
    """
    for self.cur_lng_obj in self.langmapper.loop_over_langs():  # formerly range(1, self.total_langs + 1):
      self.download_audiopart_to_blend_it_w_videoonly()
      # TODO test if fallback to non_dashed_number_audiocode happened at this point
      # the reason is that n_ongoing_lang is not following the indices of list audioonlycodes
      # an IndexError protection is done inside the next method, but we should still think about a better solution
      # so that a last renaming may happen to the non_dashed_number_audiocode videofile
      # the way it is now, this last renaming is done manually, if he/she wants to, by the user
      self.check_if_canonicalname_changed_its_extension()
      self.rename_videofile_after_audiovideofusion()

  @property
  def vocreplacelist(self):
    """
    When audiomainnumber is -1, there is no separate audio & video download
      because the formatcode is already an a+v whole.
    In this case, the 'voc' must be appended with the nsufix dash-numbers.

    Example:
      suppose langdict = [0:'en', 1:'pt']
      and voc (videoonlycode) is 91
      then (the result) vocreplacelist = ["91-0", "91-1"]
    """
    now_vc = self.videoonlycode
    nsufices_in_order = self.langmapper.nsufices_in_order
    vrlist = [f"{now_vc}-{ns}" for ns in nsufices_in_order]
    return vrlist

  def rename_videocomplete_with_videocode(self, vc, idx):
    try:
      nsufix = int(vc.split('-')[-1])
      twolettercode, langname = self.langmapper.get_twolettercode_n_langname_fr_nsufix(nsufix)
    except (AttributeError, IndexError):
      self.last_nsufix_in_case_it_failed += 1
      nsufix = self.last_nsufix_in_case_it_failed
      twolettercode = 'un'
      langname = 'unknown'
    boolres = self.osentry.rename_canofile_with_twolettercode_n_nvdseq(twolettercode, self.nvdseq)
    ln = langname
    tlc = twolettercode
    happened = "happened" if boolres else "did not happen"
    scrmsg = f"""{idx} | {happened} rename for langname={ln} nsfx={nsufix} | twolettercode={tlc}
    workdir = [{self.osentry.workdir_abspath}]
    canofilename = [{self.osentry.fn_as_name_ext}]"""
    print(scrmsg)

  def download_as_videowhole(self):
    """
    Downloads a video with a formatcode that doesn't need a+v merging.
    When audiomainnumber is -1, this means the videoonlycode formatcode is already an audio+video formed.

    # 1st-step:
      get the format-codes for each download (for each language)
      the videoonlycode (which in this case is already a+v)
        should be 'dashed' with the language nsufix
          Example:
            suppose voc=91 and langdict=[0:'en', 1:'pt']
            then voc becomes voclist=[91-0, 91-1]

    # 2nd-step:
      loop over the video-format-codes for each download
        and prefix-rename each downloaded videofile
    """
    got_one = False
    for idx, vc in enumerate(self.vocreplacelist):
      strdict = {'ytdlp_fcode_str': vc, 'videourl': self.videourl}
      comm = self.comm_line_base.format(**strdict)
      scrmsg = f"@download_video_already_merged | {comm}"
      print(scrmsg)
      try:
        subprocess.run(comm, shell=True, check=True)  # timeout=5 (how long can a download last?)
        # the name should be the "canonical", no discovery is necessary
        # self.discover_dldd_videofilename()
        self.rename_videocomplete_with_videocode(vc, idx)
        got_one = True
      # except subprocess.TimeoutExpired:
      #     print(f"Command timed out: {comm}")
      except subprocess.CalledProcessError as e:
        warnmsg = f"""Command: [{comm}]
         failed with return code: [{e.returncode}]
         => {e}"""
        print(warnmsg)
        # raise ValueError(errmsg)
        continue
      except KeyboardInterrupt:
        scrmsg = "Interrupted by user. Exiting loop. Continuing."
        print(scrmsg)
        continue
    return got_one

  def prefixdate_n_move_videos_to_parent_dir(self):
    dext = self.osentry.dot_ext
    ext = dext.lstrip('.')
    comm = f"renameDatePrefixBasedOnOSDate.py -y -e={ext}"
    comm += f"; renameAudioDurationIncluder.py -y -e={ext}"
    comm += "; renameYtDlpBracketConventionToFormer.py tf -y"
    scrmsg = f" => Executing command: {comm}"
    print(scrmsg)
    os.system(comm)
    comm = f"mv *{dext} .."
    scrmsg = f" => Executing command: {comm}"
    print(scrmsg)
    os.system(comm)

  def process(self):
    """
      1st -> position (cd changedir) at the working tmpdir
      2nd -> download the 160 (or the entered as input) video
      3rd -> disconver the downloaded video's filename
      4th -> copy it to as many as there are audio langs entered
        (for example: if one language is Italian, another is for English, two copies are made)
      5th -> download the audio(s) for each language
        5-1 download the audiofile proper
        5-2 "fuse" (or merge) it with the videofile in store so that the composite (video with audio) results
    """
    scrmsg = f"""1st step ->
    POSITION (chdir) at the working tmpdir: [{self.osentry.workdir_abspath}]"""
    print(scrmsg)
    os.chdir(self.osentry.workdir_abspath)
    scrmsg = f"""2nd step ->
    DOWNLOAD the {self.videoonlycode} video (ytid={self.ytid})"""
    print(scrmsg)
    if self.audiomainnumber == -1:
      return self.download_as_videowhole()
    if not self.download_video_only():
      # at this point, `subprocess` exitted with non-0 (this also means the videoonlyfile did not download)
      # as the next steps depend on this, script cannot continue returning False from here
      return False
    scrmsg = f"""3rd step -> 
    DISCOVER the downloaded video's filename (with ytid={self.ytid})
      and rename it to the videoonlyfile
      that one will serve the audio files to later compose audio+video"""
    print(scrmsg)
    self.discover_dldd_videofilename()
    self.rename_from_canonical_to_fsufixedvideoonlyfile()
    scrmsg = f"""4th step ->
    COPY it  (with ytid={self.ytid}) to as many as there are audio lang entered"""
    print(scrmsg)
    self.copy_n_rename_videoonly_n_lang_times()
    audioonlycodes = self.langmapper.audioonlycodes
    scrmsg = f"""5th step ->
    DOWNLOAD (with ytid={self.ytid}) the audio(s) complements | audioonlycodes={audioonlycodes}"""
    print(scrmsg)
    self.download_audio_complements()
    # move all videos from child_tmpdir_abspath to its parent dir
    self.prefixdate_n_move_videos_to_parent_dir()
    return True


def loop_over_ytids(cliprm_o):
  """

    scrmsg = f"ytid = {ytid}"
    print(scrmsg)

  :param cliprm_o:
  :return:
  """
  for ytid in cliprm_o.ytids:
    downloader = Downloader(
      ytid=ytid,
      dlddir_abspath=cliprm_o.dirpath,
      videoonlycode=cliprm_o.videoonlycode,
      audiomainnumber=cliprm_o.audiomainnumber,
      nvdseq=cliprm_o.nvdseq,
      sfx_n_2letlng_dict=cliprm_o.sfx_n_2letlng_dict
    )
    _ = downloader.process()  # process() returns a boolean (True | False)
  return True


def process():
  """
  """
  cliprm_o = clip.CliParam()
  cliprm_o.read_n_confirm_params()
  if cliprm_o.show_docstr_n_do_not_run:
    show_docstrhelp_n_exit()
  if len(cliprm_o.ytids) == 0:
    scrmsg = f"No ytids ({cliprm_o.ytids}) to download from CLI."
    print(scrmsg)
    return False
  if not cliprm_o.confirmed:
    scrmsg = f"Not running scripting, confirmation {cliprm_o.confirmed})."
    print(scrmsg)
    return False
  return loop_over_ytids(cliprm_o)


if __name__ == '__main__':
  """
  adhoctest1()
  adhoctest2()
  """
  process()
