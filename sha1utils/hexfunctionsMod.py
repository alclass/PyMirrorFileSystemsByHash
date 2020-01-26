#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib  # hashlib is the replacement of the deprecated sha module in Python2
import os, sys
from random import randint
import __init__
from sha1classes.XmlSha1ExceptionClassesMod import FileSystemHasFilenamesThatHaveAnUnknownEncoding

HEXS_ABOVE_9 = 'abcdef' # lowercase
HEX_DIGITS = '0123456789' + HEXS_ABOVE_9
SHA1_CHUNK_SIZE = 40
hex_40digit_max = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
# lambda_str_hex_digit = lambda digit : HEX_DIGITS[digit] or None
lambda_randint_0_15  = lambda _ : randint(0,15)
nonhex_filter        = lambda s : s.lower() not in HEX_DIGITS

SHA1_CHUNK_SIZE = 40
ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1','windows-1250','windows-1252']

def get_tag_text_and_encoding_if_necessary(sha1file_tag, filename):
  '''
  This is an important function and has been also documented at the beginning of this modolue.
  It is called when UnicodeDecodeError is raised, meaning that we need to try some encodings to "make it go" to the node.text field.
  Once it's successful, an extra XML-attribute, called 'encoding', is added to the file tag.
  This extra attribute is essential at the moment when this file-tag is read from the XML.
  '''
  sha1file_tag.text = None
  for encoding in ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES:
    try:
      sha1file_tag.text = str(filename, encoding, 'strict')
      #print 'filename in tag is', filename, 'encoding is', encoding 
      break
    except UnicodeDecodeError:
      pass
  if sha1file_tag.text == None:
    error_msg = 'File System Has Filenames That Have An Unknown Encoding, please try to rename them removing accents.'
    raise FileSystemHasFilenamesThatHaveAnUnknownEncoding(error_msg)
  sha1file_tag.set('encoding', encoding)

def convertHexIntToChar(hexInt):
  '''
  This function is equivalent to lambda lambda_str_hex_digit
  The lambda is preferrable in terms of performance than this function
  '''
  if not (0 <= hexInt <= 15):
    raise ValueError('hexInt, in the char-convert context, should be from 0 to 15. It is %d' %hexInt)
  if hexInt < 10:
    return str(hexInt)
  return HEXS_ABOVE_9[hexInt-10]

def generate_a_40char_random_hex_plan_b():
  '''
  Please, for performance reasons, instead of using this function,
  use function generate_a_40char_random_hex() below
   
  This function will be called from the latter in case 
  Python's change the way it represents a long hexadecimal number
  '''
  hex_40digit_list = list(map(lambda_randint_0_15, range(40)))
  hex_40char_list  = list(map(convertHexIntToChar, hex_40digit_list))
  # hex_40char_list  = map(convertHexIntToChar, hex_40digit_list)
  return ''.join(hex_40char_list).lower()

def stuff_hex_number_to_a_40char_str(long_n):
  '''
  Test Case:
  
  TC1
  long_n = 0xA
  return should be '000000000...000A', 39 zeroes followed by 'A'

  TC2
  long_n = hex_40digit_max
  return should be a 40-char string all filled-in with "F's"

  @see other Test Cases in the unit tests
  '''
  #str_n = hex(long_n)
  str_n = '%x' %long_n
  #if not str_n.startswith('0x') or not str_n.endswith('L') or 
  if len(str_n) > 43:
    return generate_a_40char_random_hex_plan_b()
    #raise ValueError, 'Program Error: '
  #str_n = str_n[2:-1] # strip off the 0x at the beginning and the L (long number) at the end
  if len(str_n) < 40:
    str_n = str_n.zfill(40) 
  return str_n.lower()

def generate_sha1hexdigest_from_filepath(file_abspath):
  '''
  This functions mimics, so to say, the sha1sum "bash" executable from the command line.
  It reads a file and passes its contents to the sha.new() method,
    then, returns the object's hex-digest 40-char hexadecimal string. 
  '''
  if os.path.isfile(file_abspath):
    content = open(file_abspath, 'rb').read()
    sha1hash = hashlib.sha1()
    sha1hash.update(content)
    return sha1hash.hexdigest()
  return None

def generate_a_40char_random_hex(not_colliding_with=[], n_of_tries=0):
  '''
  Test Case:
  Generate a number and check it's in-between 0 and hex_40digit_max 
  
  '''
  if n_of_tries > 3 + len(not_colliding_with):
    raise IndexError('Giving up generate_a_40char_random_hex() :: n_of_tries (=%d) surpassed limit' %n_of_tries)
  #hex_list_40 = map(xrange(40), randint(16))
  long_n = randint(0, hex_40digit_max)
  random_40char_sha1hex = stuff_hex_number_to_a_40char_str(long_n)
  if random_40char_sha1hex in not_colliding_with:
    return generate_a_40char_random_hex(not_colliding_with, n_of_tries+1)
  return random_40char_sha1hex
  
def transpose_sha1sum_and_filename_dict_into_tuple_list(sha1sum_and_filename_dict):
  '''
  
  DEPRECATED
  
  This function is not longer needed or useful because filenames sorting is more difficult 
    with the changes that connected one or more filenames to one sha1sum.
  
  
  This set of functions can use the double "sha1hex" and "filename" either as a dict or as tuple/list
  As a dict, the sha1hex is the key and the filename is the value, in the key:value paradigm
  As a tuple/list, the pairs sha1hex/filename are tuples in a list
  
  The strategy in using a tuple/list is to ease out the sorting/ordering, so that
    processing could loop thru files in alphabetical/alphanumerical order,
    as with dicts, this is more difficult to accomplish (at least using Python versions below 3!)
  '''
  sha1sum_and_filename_tuple_list = []
  for sha1hex in list(sha1sum_and_filename_dict.keys()):
    filenames = sha1sum_and_filename_dict[sha1hex]
    sha1sum_and_filename_tuple_list.append((sha1hex, filenames))
  # order it by filename!
  # sha1sum_and_filename_tuple_list.sort(key = lambda list_obj: list_obj[1])
  return sha1sum_and_filename_tuple_list 

def fetch_sha1sum_and_filename_tuple_list_if_a_conventioned_flatsha1file_exists(abspath):
  '''
  This function is a wrapper around the above one (fetch_sha1sum_and_filename_dict_if_a_conventioned_flatsha1file_exists())
  If a dict is returned, it will return its "tuple list transpose".
  '''
  sha1sum_and_filename_dict = fetch_sha1sum_and_filename_dict_if_a_conventioned_flatsha1file_exists(abspath)
  if sha1sum_and_filename_dict == None:
    return None
  return transpose_sha1sum_and_filename_dict_into_tuple_list(sha1sum_and_filename_dict)

  
  # the validate() method guarantees there won't be a filename having 2 sha1sums
  # if it does, an exception will be raised
  sha1sum_and_filename_dict.validate()
  return sha1sum_and_filename_dict


    
def is_it_a_sha1hex(sha1hex):
  '''
  '''
  if len(str(sha1hex)) != 40:
    return False
  if type(sha1hex) == int:
    return True
  try:
    boolean_list = list(map(nonhex_filter, sha1hex))
    if True in boolean_list:
      return False
    else:
      return True
  except AttributeError:
    # if this exception has been raised, sha1hex is neither an int nor a string-like
    # however, if it's int-convertible, it will pass this checking below
    pass
  try:
    int(sha1hex, 16)
  except ValueError:
    return False
  return True
  


import unittest
class Test1(unittest.TestCase):
  
  def test_convert_one_digit_hex_to_str(self):
    
    hex_int = 0xA
    hex_str = convertHexIntToChar(hex_int)
    self.assertEqual(hex_str, 'a')

    hex_int = 10
    hex_str = convertHexIntToChar(hex_int)
    self.assertEqual(hex_str, 'a')

  def test_generate_40char_both_methods(self):
    
    # 1st method
    hex_str = generate_a_40char_random_hex()
    self.assertEqual(len(hex_str), 40)
    self.assertEqual(hex_str, hex_str.lower())

    # 2nd method
    hex_str = generate_a_40char_random_hex_plan_b()
    self.assertEqual(len(hex_str), 40)
    self.assertEqual(hex_str, hex_str.lower())

  def test_equality_between_generated_and_given(self):
    hex_int = 0xA
    hex_str = stuff_hex_number_to_a_40char_str(hex_int)
    s_39_zeroes_plus_ending_A = '0'*39 + 'A'
    self.assertEqual(hex_str, s_39_zeroes_plus_ending_A.lower())

def unittests():
  unittest.main()


def process():
  '''
  '''
  pass

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()
