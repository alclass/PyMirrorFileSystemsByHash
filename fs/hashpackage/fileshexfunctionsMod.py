#!/usr/bin/env python3
"""
  docstring
"""
import hashlib
import os
import config

HEXS_ABOVE_9 = 'abcdef'  # lowercase
HEX_DIGITS = '0123456789' + HEXS_ABOVE_9
SHA1_CHUNK_SIZE = 40
hex_40digit_max = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1', 'windows-1250', 'windows-1252']


def get_tag_text_and_encoding_if_necessary(sha1file_tag, filename):
  """
  This is an important function and has also been documented at the beginning of this module.
  It is called when UnicodeDecodeError is raised, meaning that we need to try some encodings to
   "make it go" to the node.text field.
  Once it's successful, an extra XML-attribute, called 'encoding', is added to the file tag.
  This extra attribute is essential at the moment when this file-tag is read from the XML.
  """
  sha1file_tag.text = None
  encoding = None
  for encoding in ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES:
    try:
      sha1file_tag.text = str(filename, encoding, 'strict')
      # print 'filename in tag is', filename, 'encoding is', encoding
      break
    except UnicodeDecodeError:
      pass
  if sha1file_tag.text is None:
    error_msg = 'File System Has Filenames That Have An Unknown Encoding, ' \
                'please try to rename them removing accents.'
    raise OSError(error_msg)
    # raise FileSystemHasFilenamesThatHaveAnUnknownEncoding(error_msg)
  sha1file_tag.set('encoding', encoding)


def generate_sha1hexdigest_from_filepath(file_abspath):
  """
  This functions mimics, so to say, the sha1sum "bash" executable from the command line.
  It reads a file and passes its contents to the sha.new() method,
    then, returns the object's hex-digest 40-char hexadecimal string.
  """
  if os.path.isfile(file_abspath):
    content = open(file_abspath, 'rb').read()
    sha1hash = hashlib.sha1()
    sha1hash.update(content)
    return sha1hash.hexdigest()
  return None


def adhoc_test():
  datafolder_abspath = config.get_data_abspath()
  file_abspath = os.path.join(datafolder_abspath, 'src/d1/d1f1.txt')
  print(file_abspath)
  sha1hex = generate_sha1hexdigest_from_filepath(file_abspath)
  print(sha1hex)
  file_abspath = os.path.join(datafolder_abspath, 'src/d2/d2f1.txt')
  print(file_abspath)
  sha1hex = generate_sha1hexdigest_from_filepath(file_abspath)
  print(sha1hex)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
