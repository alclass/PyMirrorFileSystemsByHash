#!/usr/bin/env python3
"""
~/bin/localuserpylib/ytfunctions/osentry_class.py

The OSEntry class models the files and filenames that are downloaded
  to form the various available autodubbed language videos from YouTube together with its original language.
This class is used by dlYouTubeWhenThereAreDubbed.py that, at the time of writing,
  is placed in the (Linux) user's bin directory.
"""
import os
import sys
import lib.regexfs.filenamevalidator_cls as fnval  # .FilenameValidator
import cmm.yt.models.ytstrfs_etc as ytstrfs
DEFAULT_YTIDS_FILENAME = 'youtube-ids.txt'
DEFAULT_AUDIOVIDEO_CODE = 160  # previously it was 602, both are 256x144 but 602 has become "more available..."
DEFAULT_AUDIOVIDEO_DOT_EXT = '.mp4'
DEFAULT_AUDIO_MAIN_NUMBER = -1   # formerly 249 (webm), -1 means the formatcode in voc is already a video+audio complete
DEFAULT_VIDEO_ONLY_CODE = 160
DEFAULT_SFX_W_2LETLNG_MAPDCT = {0: 'en', 1: 'pt'}  # en is English, pt is Portuguese
VIDEO_DOT_EXTENSIONS = ['.mp4', '.mkv', '.webm', '.m4v', '.avi', '.wmv']
default_videodld_tmpdir = 'videodld_tmpdir'


class OSEntry:
  """
  This class organizes the OSEntries (files and folders) needed for the Downloader class.
    (At the time of writing, this class is located in ~/bin/dlYouTubeWhenThereAreDubbed.py)
    OSEntry is used by composition in Downloader.
  """

  video_dot_extensions = VIDEO_DOT_EXTENSIONS

  def __init__(self, workdir_abspath, basefilename, videoonly_or_audio_code):
    self.prename, self._dot_ext, self.ytid = None, None, None
    # the vf dot extension is the one that is set only once contrasted to the dot_ext that may change
    self.vf_dot_ext = None
    self._basefilename = None
    self.basefilename = basefilename
    if videoonly_or_audio_code is None:
      self.videoonly_or_audio_code = DEFAULT_AUDIOVIDEO_CODE
    else:
      self.videoonly_or_audio_code = videoonly_or_audio_code
    self.workdir_abspath = workdir_abspath
    self.treat_workdir_abspath()

  def treat_workdir_abspath(self):
    if self.workdir_abspath is None or self.workdir_abspath == '.':
      self.workdir_abspath = os.path.abspath('.')
      return
    if not os.path.isdir(self.workdir_abspath):
      # this directory is set at client Class Downloader and passed on here
      # but at this instantiation time chances are it may still need to be made (mkdir)
      os.makedirs(self.workdir_abspath, exist_ok=True)
      return
    if not os.path.isdir(self.workdir_abspath):
      # this is a logical condition that it will probably never happen
      # for an exception should have probably been already raised above
      errmsg = f"Error: workdir_abspath {self.workdir_abspath} does not exist. Please, retry reentering it."
      raise OSError(errmsg)

  @property
  def dot_ext(self):
    return self._dot_ext

  @dot_ext.setter
  def dot_ext(self, _dot_ext):
    """
    The need for this property-setter is because the following:
      1 - the dot extension may change along program execution:
          example: from mp4 to mkv (@see more info below)
      2 - in any case, the first vf (videoformat) dot extension is set once
          and does not change along program execution
          noting:
            - if it got mp4 for the videoonly, it'll continue mp4 until the end of execution
            - if it's webm for the videoonly, it'll continue to be webm idem

    If an extension without its preceding dot is given as input, a dot will be prepended to it
    """
    if isinstance(_dot_ext, str) and not _dot_ext.startswith('.'):
      _dot_ext = '.' + _dot_ext
    if _dot_ext in self.video_dot_extensions:
      if self._dot_ext is None:
        # ok, None indicates that this is the first setting
        # the vf dot extension is the one that keeps (is not changed to) in the videoformat file
        # so it's set only upon first attribution, not when dot_ext may change later on
        self.vf_dot_ext = _dot_ext
      self._dot_ext = _dot_ext
      return
    errmsg = f"Error: given dot extension [{_dot_ext}] is not in the registered ones."
    raise ValueError(errmsg)

  @property
  def ytid_with_brackets(self):
    _ytid_with_brackets = f"[{self.ytid}]"
    return _ytid_with_brackets

  @property
  def name(self):
    _name = f"{self.prename} {self.ytid_with_brackets}"  # notice a blank-space in-between
    return _name

  @name.setter
  def name(self, pname):
    """
    This property sets two attributes, they are:
      1 - prename
      2 - ytid
    Recomposing, name = f'{prename} [{ytid}]'

    More Explanation
    ================
    name is the filename's part without its dot_extension
    name is also composed as:
      "{self.prename} {ytid_with_squarebrackets}"
      Noticing:
        1 - prename can be any NTFS valid name (if ext4, "name space" gets a bit ampler)
        2 - followed by a blank-space separating `prename` from `ytid_with_squarebrackets`
        3 - followed by a ytid, which is an 11-char ENC64 string, within square bracks
    """
    raise_value_error = False
    reasons = []
    try:
      self.prename = pname[:-14]  # the 14 is 11 for ytid, 2 for the squarebrackets, 1 for the blank-space
      supposed_blank = pname[-14]
      if supposed_blank != ' ':
        raise_value_error = True
        reason_str = "blank-space is misssing between prename and [ytid]"
        reasons.append(reason_str)
      supposed_rightbracket = pname[-1]
      if supposed_rightbracket != ']':
        raise_value_error = True
        reason_str = "rightbracket is either missing or mispositioned"
        reasons.append(reason_str)
      supposed_leftbracket = pname[-13]
      if supposed_leftbracket != '[':
        raise_value_error = True
        reason_str = "leftbracket is either missing or mispositioned"
        reasons.append(reason_str)
      supposed_yitd = pname[-12:-1]
      booval = ytstrfs.is_str_a_ytid(supposed_yitd)
      if booval:
        self.ytid = supposed_yitd
      else:
        raise_value_error = True
        reason_str = "ytid is either missing or is invalid"
        reasons.append(reason_str)
    except (IndexError, ValueError):
      raise_value_error = True
    if raise_value_error:
      errmsg = f"Error: filename part [{pname}] with prename and ytid is not valid: reasons = {reasons}."
      raise ValueError(errmsg)

  @property
  def basefilename(self) -> str:
    """
    basefilename is a filename that is composed as:
      "{name}{dot_extension}"
      @see also above how 'name' is composed
    """
    _basefilename = f"{self.name}{self.dot_ext}"
    return _basefilename

  def has_basefilename_been_found(self) -> bool:
    """
    Returns true if self.name is not None

    Important:
      The client-caller must not issue this method if basefilename is subject to getting renamed
        during program execution
    """
    if self.name is not None:
      return True
    return False

  @basefilename.setter
  def basefilename(self, filename: str | None):
    """
    basefilename is a filename that is composed as:
      "{name}{dot_extension}"
      @see also above how 'name' is composed
      Notice also that self.name is a dynamic attribute (@property)
    """
    if filename is None:
      # this should happen at the beginning when filename is not yet known
      return
    try:
      self.name, self.dot_ext = os.path.splitext(filename)
    except ValueError:
      pass
    if self.dot_ext not in VIDEO_DOT_EXTENSIONS:
      errmsg = (f"extension {self.dot_ext} not in the REGISTERED_VIDEO_DOT_EXTENSIONS"
                f" list = {VIDEO_DOT_EXTENSIONS}")
      raise OSError(errmsg)

  @property
  def old_basefilename(self):
    """
    basefilename is video filename as it comes from yt-dlp
      In this class:
        1 - it's the same as fn_as_name_ext
        2 - and also decomposable as "{name}{dot_ext}"
    it could be further decomposed because of the video-id sufixing the name,
      but that goes for a different method below (TODO)
    :return:
    """
    return self.fn_as_name_ext

  @property
  def old_ytid(self) -> str | None:
    """
    The attribute is the ytid (youtube-video-id)

    Important:
      a) the way ytid is extracted here from the filename is the convention (*) used in yt-dlp
      b) there is another convention used in the older ytdl project, this is still
         used in some of our scripts
      c) because this script looks up the downloaded file from yt-dlp,
         the correct convention is the one in 'a' above
      (*) the convention is to have the 11-char ytid enclosed within "[]" at the end of name

    ------------------
    # [ALTERNATIVELY] code to try find ytid (the ENC64 11-character id) via RegExp
    match = cmpld_ytid_in_ytdlp_filename_pattern.search(self.name)
    self.ytid = match.group(1) if match else None
    """
    try:
      sufix = self.name[-13:]
      ytid = sufix.lstrip('[').rstrip(']')
      if not ytstrfs.is_str_a_ytid(ytid):
        return None
      return ytid
    except (IndexError, ValueError):
      return None

  @property
  def fsufix(self) -> str:
    """
    Examples of videoonlycodes are: 160 (a 256x144), 602 (another 256x144), etc.
    A fsufix is the string "f" + str(videoonly_or_audio_code)
    With the example above fsufix = "f160"
    """
    _fsufix = f"f{self.videoonly_or_audio_code}"
    return _fsufix

  @property
  def dot_fsufix(self) -> str:
    """
    It prepends a "." (dot) before fsufix (@see it above)
    """
    _dot_fsufix = f'.{self.fsufix}'
    return _dot_fsufix

  @property
  def fn_as_name_ext(self) -> str:
    """
    Notice this property is the same as `basefilename`
      This was chosen to give it an alternative attribute-name
    """
    return self.basefilename

  @property
  def fp_for_fn_as_name_ext(self) -> str:
    return os.path.join(self.workdir_abspath, self.fn_as_name_ext)

  @property
  def fn_as_name_fsufix_ext(self) -> str:
    """
    name_fsufix_ext means the canonical filename with the f<videoonlycode> sufix.

    An example:
      title.f160.ext.bk3

    For example, parts after title are:
      f160 => an "f" followed by the videoonlycode
      ext => the current extension
      bk3 => means it's the 3rd copy of the videoonlyfile that will compose with
        a 3rd language audio file

      Let's look at the fsufix with an example:
            a-videofilename-ytid.f160.mp4
      In this example, ".f160" is a videoonly sufix coming before the extension
      (it's videoonly because videoformat code 160 is videoonly, i.e., video without audio)

      So this method does the following:
      a) it takes the canonical filename:
        In the example:
            a-videofilename-ytid.mp4
      b) and grafts (inserts) the ".f160" before the extension
        In the example:
            a-videofilename-ytid.f160.mp4

      Notice that:
       1 - this method does not use the other sufix in this script, the bksuffix (@see above)
       2 - this videoonlycode is not ambiguous because it can be only one
    """
    name_fsufix_ext = f"{self.name}{self.dot_fsufix}{self.vf_dot_ext}"
    return name_fsufix_ext

  @property
  def name_from_fn_as_name_fsufix_wo_ext(self) -> str:
    """
    For example:
      name:
        from "a-videofilename-ytid.f160.mp4"
          is "a-videofilename-ytid.f160"
    :return:
    """
    name, _ = os.path.splitext(self.fn_as_name_fsufix_ext)
    return name

  @property
  def fp_for_fn_as_name_fsufix_ext(self) -> str:
    return os.path.join(self.workdir_abspath, self.fn_as_name_fsufix_ext)

  @staticmethod
  def get_dot_bksufix(n_bksufix) -> str:
    """
    This method composes a bksufix-fsufix audio|video filename
    This filename is composed with the following chunks:
      "{name}{dot_fsufix}{dot_ext}{dot_bksufix}"
    Where:
      {name} is the name part of the filename altogether
      {dot_fsufix} is a dot, followed by "f" followed by the videoonlycode
        * it could also be an audioonlycode, but in this clas, its main use aims the vocode
      {dot_ext} is a dot followed by the file-extension
      {dot_bksufix} is a dot, followed by "bk" followed by a sequence number

    Example:
      name = "this-video"
      dot_fsufix = ".f160"
      dot_ext = ".mp4"
      dot_bksufix = ".bk3"
    This will compose as:
      "this-video.f160.mp4.bk3"
    """
    _dot_bksufix = f".bk{n_bksufix}"
    return _dot_bksufix

  def get_fn_as_name_fsufix_ext_bksufix(self, n_bksufix) -> str:
    """
    An example of a filename name_fsufix_ext_bksufix is as follows:
      it takes a fsufix video, say, videoonlycode=160 (a 256x144 video), forming:

      a) filename.f160.mp4 (in the example, this is self.fn_as_name_fsufix_ext)
    and appends to it a bksufix, say:
      b) filename.f160.mp4.bk2

    Notice that the forming also considers the dirpath, in a nutshell, the forming is:
      a) INPUT: <dirabspath>/filename.f160.mp4
      b) OUTPUT: <dirabspath>/filename.f160.mp4.bk2
    """
    name_fsufix_ext_bksufix = f"{self.fn_as_name_fsufix_ext}{self.get_dot_bksufix(n_bksufix)}"
    return name_fsufix_ext_bksufix

  def get_fp_for_fn_as_name_fsufix_ext_bksufix(self, n_bksufix) -> str:
    return os.path.join(self.workdir_abspath, self.get_fn_as_name_fsufix_ext_bksufix(n_bksufix))

  def find_n_set_the_canonical_with_another_extension(self):
    """
    This method tries to find the new canonical filename upon an extension change
      (example: when mp4 becomes mkv)
    Steps:
      1 - if the original canonical exists, do nothing, return
      2 - look up a canonical name joint with a different extension
      3 - if it finds that canonical with a different extension, set that extension
          (remind the canonical filename is formed joining the two parts ({name}{dot_ext})
    """
    # step 1 - if the original canonical exists, do nothing, return
    if os.path.isfile(self.fp_for_fn_as_name_ext):
      return False
    # step 2 - look up a canonical name joint with a different extension
    for dot_ext in VIDEO_DOT_EXTENSIONS:
      newfilepath = fnval.change_fileextension_in_fp_fr_w(self.fp_for_fn_as_name_ext, dot_ext)
      if os.path.isfile(newfilepath):
        # found, set found fileextension
        self.dot_ext = dot_ext
        scrmsg = f"\tFound CHANGED dot_ext as {dot_ext} | canonical = {self.fn_as_name_ext}"
        print(scrmsg)
        return True
    scrmsg = f"\tNot found a CHANGED dot_ext for canonical = {self.fn_as_name_ext}"
    print(scrmsg)
    return False

  def rename_canofile_with_twolettercode_n_nvdseq(self, twolettercode, seq=1, ntries=1):
    canofilepath = self.fp_for_fn_as_name_ext
    if not os.path.isfile(canofilepath):
      wrnmsg = f"canofilepath {canofilepath} does not exist. Returning."
      print(wrnmsg)
      # nothing to do, return
      return False
    canofilename = self.fn_as_name_ext
    if ntries > 10:
      errmsg = f"Error: more than 10 attempts (={ntries}) to rename canofile {canofilename}"
      raise OSError(errmsg)
    if twolettercode is None:
      twolettercode = 'un'
    prefix = f"vd{seq}-{twolettercode} "
    newfilename = prefix + canofilename
    newfilepath = os.path.join(self.workdir_abspath, newfilename)
    if os.path.isfile(newfilename):
      return self.rename_canofile_with_twolettercode_n_nvdseq(twolettercode, seq, ntries+1)
    # rename
    os.rename(canofilepath, newfilepath)
    scrmsg = f"""Renamed:
    FROM: [{canofilename}]
    TO:   [{newfilename}]  
    in: [{self.workdir_abspath}]"""
    print(scrmsg)
    return True

  def rename_canofile_to_next_available_lang_n_prefixed_or_raise(self):
    """
    This method renames existing canofile to the next available langN prefix
    if prefix gets greater than 1000, raise an exception
    """
    canofilepath = self.fp_for_fn_as_name_ext
    if not os.path.isfile(canofilepath):
      # nothing to do, return
      return
    n_iter, max_iter = 0, 1000
    changing_filepath = canofilepath
    while os.path.isfile(changing_filepath):
      n_iter += 1
      if n_iter > max_iter:
        canofilename = self.fn_as_name_ext
        _, changing_filename = os.path.split(changing_filepath)
        errmsg = (f"Maximum iteration cycles reaches when trying to rename {canofilename}"
                  f" | {changing_filename} | {changing_filepath}")
        raise OSError(errmsg)
      _, changing_filename = os.path.split(canofilepath)
      prefix = f"lang{n_iter} "
      changing_filename = prefix + changing_filename
      changing_filepath = os.path.join(self.workdir_abspath, changing_filename)
    try:
      os.rename(canofilepath, changing_filepath)
      scrmsg = f"""Rename succeeded for incrementing langN prefix to canofile
      ---------------------------------------
      FROM (canofilepath):    [{canofilepath}]
      TO (changing_filepath): [{changing_filepath}]
      ---------------------------------------
      """
      print(scrmsg)
    except (OSEntry, IOError) as e:
      errmsg = f"""Error: could not rename canofile to next available prefix
      ---------------------------------------
      FROM (canofilepath):    [{canofilepath}]
      TO (changing_filepath): [{changing_filepath}]
      ---------------------------------------
      {e}
      Halting.    
      """
      print(errmsg)
      sys.exit(1)
    return

  def __str__(self):
    bksufix_example = 3
    outstr = f"""OSEntry object:
    name_ext = [{self.fn_as_name_ext}]
    prename = [{self.prename}] | dot_ext = {self.dot_ext} | ytid = {self.ytid}
    fp_name_ext = [{self.fp_for_fn_as_name_ext}]
    workdir = [{self.workdir_abspath}]
    --------------------------------
    name_fsufix_ext = [{self.fn_as_name_fsufix_ext}]
    fp_name_fsufix_ext = [{self.fp_for_fn_as_name_fsufix_ext}]
    --------------------------------
    Example with bksufix = [{bksufix_example}]
    name_fsufix_ext_bksufix[{bksufix_example}] = {self.get_fn_as_name_fsufix_ext_bksufix(bksufix_example)}
    fp_name_fsufix_ext = [{self.get_fp_for_fn_as_name_fsufix_ext_bksufix(bksufix_example)}]
    """
    return outstr


def adhoctest2():
  osentry = OSEntry(
    workdir_abspath='.',
    basefilename='bla [123abcABC-_].mp4',
    videoonly_or_audio_code=160
  )
  print(osentry)
  fn = "REDE GLOBO SE INCOMODA COM VÍDEOS DE IA CONTRA O CONGRESSO NACIONAL [D6anIztaYCE].mp4"  # .f160
  fnvalidator = fnval.FilenameValidator(filename=fn)
  print(fnvalidator)


def adhoctest1():
  """
  """
  pass


def process():
  """
  """
  pass


if __name__ == '__main__':
  """
  adhoctest1()
  adhoctest2()
  process()
  """
  adhoctest2()
