#!/usr/bin/env python3
"""
  docstring
"""
import hashlib  # hashlib is the replacement of the deprecated sha module in Python2
import os
from random import randint

HEXS_ABOVE_9 = 'abcdef'  # lowercase
HEX_DIGITS = '0123456789' + HEXS_ABOVE_9
SHA1_CHUNK_SIZE = 40
hex_40digit_max = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
ENCODINGS_TO_TRY_IN_ORDER_FOR_FILENAMES = ['iso-8859-1', 'windows-1250', 'windows-1252']


def draw_randomint_0_15():
  return randint(0, 15)


def convert_hexint_to_char(hexint):
  """
  This function is equivalent to lambda lambda_str_hex_digit
  The lambda is preferrable in terms of performance than this function
  """
  if not (0 <= hexint <= 15):
    raise ValueError('hexInt, in the char-convert context, should be from 0 to 15. It is %d' % hexint)
  if hexint < 10:
    return str(hexint)
  return HEXS_ABOVE_9[hexint-10]


def generate_a_40char_random_hex_plan_b():
  """
  Please, for performance reasons, instead of using this function,
  use function generate_a_40char_random_hex() below

  This function will be called from the latter in case
  Python's change the way it represents a long hexadecimal number
  """
  hex_40digit_list = list(map(lambda n: draw_randomint_0_15, range(40)))
  hex_40char_list = list(map(convert_hexint_to_char, hex_40digit_list))
  return ''.join(hex_40char_list).lower()


def stuff_hex_number_to_a_40char_str(long_n):
  """
  Test Case:

  TC1
  long_n = 0xA
  return should be '000000000...000A', 39 zeroes followed by 'A'

  TC2
  long_n = hex_40digit_max
  return should be a 40-char string all filled-in with "F's"

  @see other Test Cases in the unit tests
  """
  # str_n = hex(long_n)
  str_n = '%x' % long_n
  # if not str_n.startswith('0x') or not str_n.endswith('L') or
  if len(str_n) > 43:
    return generate_a_40char_random_hex_plan_b()
    # raise ValueError, 'Program Error: '
  # str_n = str_n[2:-1] # strip off the 0x at the beginning and the L (long number) at the end
  if len(str_n) < 40:
    str_n = str_n.zfill(40)
  return str_n.lower()


def generate_a_40char_random_hex(not_colliding_with=None, n_of_tries=0):
  """
  Test Case:
  Generate a number and check it's in-between 0 and hex_40digit_max 
  """
  if not_colliding_with is None:
    not_colliding_with = []
  if n_of_tries > 3 + len(not_colliding_with):
    raise IndexError('Giving up generate_a_40char_random_hex() :: n_of_tries (=%d) surpassed limit' % n_of_tries)
  # hex_list_40 = map(xrange(40), randint(16))
  long_n = randint(0, hex_40digit_max)
  random_40char_sha1hex = stuff_hex_number_to_a_40char_str(long_n)
  if random_40char_sha1hex in not_colliding_with:
    return generate_a_40char_random_hex(not_colliding_with, n_of_tries+1)
  return random_40char_sha1hex


def is_it_a_sha1hex(sha1hex):
  """
  """
  if len(str(sha1hex)) != 40:
    return False
  if type(sha1hex) == int:
    return True
  try:
    boolean_list = list(map(lambda s: s.lower() not in HEX_DIGITS, sha1hex))
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
  

def adhoc_test():
  rnd40hex = generate_a_40char_random_hex(not_colliding_with=None, n_of_tries=0)
  print('generate_a_40char_random_hex()', rnd40hex)
  retbool = is_it_a_sha1hex(rnd40hex)
  print('is_it_a_sha1hex()', retbool)
  rint = draw_randomint_0_15()
  print('draw_randomint_0_15()', rint)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
