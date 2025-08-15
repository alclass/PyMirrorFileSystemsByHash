#!/usr/bin/env python3
"""
  hash_mod.py
This script contains class HashSimple which does the following:
  1) it receives a string a parameter;
  2) it calculates its sha1;
  3) it transforms it to its hexadecimal representation (which is a 40-digit hexadecimal string);
  4) it consideres the first 10 digits (out of the 40) and uses it as the hash itself.

The current client caller is class DirNode which hashes string [parentpath + name].

Collision Treatment
-------------------
In the client app, the db-field hkey is declared as UNIQUE. In case a collision ever happens
  (due to the 10-digit compression explained above) the db-engine will raise an exception
  avoiding a possible consequent data error.  On the other side,
  one solution might be to increase the charsize of the hash.
"""
import hashlib
import binascii
EMPTY_SHA1HEX_STR = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
EMPTY_SHA1_AS_BIN = binascii.unhexlify(EMPTY_SHA1HEX_STR)
BUF_SIZE = 65536


def calc_sha1_from_file(filepath):
  sha1 = hashlib.sha1()
  with open(filepath, 'rb') as f:
    while True:
      try:
        data = f.read(BUF_SIZE)
        if not data:
          break
        sha1.update(data)
      except OSError:
        return None
    return sha1.digest()


def convert_to_size_w_unit(bytesize):
  if bytesize is None:
    return "0KMG"
  kilo = 1024
  if bytesize < kilo:
    return str(bytesize) + 'b'
  mega = 1024*1024
  if bytesize < mega:
    bytesize = round(bytesize / kilo, 1)
    return str(bytesize) + 'K'
  giga = 1024*1024*1024
  if bytesize < giga:
    bytesize = round(bytesize / mega, 1)
    return str(bytesize) + 'M'
  bytesize = round(bytesize / giga, 1)
  tera = 1024*1024*1024*1024
  if bytesize < tera:
    return str(bytesize) + 'G'
  bytesize = round(bytesize / tera, 3)
  return str(bytesize) + 'T'


def get_the_empty_sha1_bin_n_hex_tuple():
  """
  The hex constant above (da39a3ee5e6b4b0d3255bfef95601890afd80709) is the empty one
    copied from a previously calculated one
  """
  sha1o = hashlib.sha1()
  sha1o.update(''.encode('utf8'))
  sha1 = sha1o.digest()
  sha1hex = sha1o.hexdigest()
  return sha1, sha1hex


class HashSimple:

  def __init__(self, s):
    self.s = s
    self.hex40 = None
    self._hex = None
    self._num = None
    self.hash()

  def hash(self):
    """
    This function receives a string and takes its sha1 hash.
    With its hexadecimal representation, having 40 digits, it takes the first 10 ie it cuts out 3/4 of it
    Then it transforms the cut-hex into a number and returns it

    Because this hash is kept in a sql-table-field that is UNIQUE,
      if a collision ever happens, a runtime error will be raised.
      If this happens, one solution is to increase the size of the cut
      (for example, instead of first 10 as now take the first 20)
    """
    sha1 = hashlib.sha1()
    sha1.update(self.s.encode('utf8'))
    self.hex40 = sha1.hexdigest()
    # hexdigest() produces the 40-char representation (as a binary it's a 20-byte number, ie half the size of hexdigest)
    self._hex = self.hex40[:10]  # as explained above, the hash here compresses the 40-char hex to the small 10-char
    self._num = eval('0x'+self._hex)

  @property
  def hex(self):
    return self._hex

  @property
  def num(self):
    return self._num

  def as_dict(self):
    outdict = {
      's': self.s,
      'hex': self.hex,
      'hex40': self.hex40,
      'num': self.num,
    }
    return outdict

  def __str__(self):
    outstr = '''HashSimple s="%(s)s"
    hex=%(hex)s
    hex40=%(hex40)s
    num=%(num)s
    ''' % self.as_dict()
    return outstr


def adhoc_test1():
  """
  test function hashsimple()
  """
  s = 'blah blah'
  hs = HashSimple(s)
  print(hs.hex, hs.num, 'hashsimple of ', hs.s)
  s = 'bld dsfçç sdfº[[]]]~ah blah bla'
  hs = HashSimple(s)
  print(hs.hex, hs.num, 'hashsimple of ', hs.s)
  print(hs)


def process():
  adhoc_test1()


if __name__ == '__main__':
  process()
