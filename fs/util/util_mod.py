#!/usr/bin/env python3
import datetime
import random

hexadecimal_str_seq = '0123456789ABCDEF'
def take_random_sha1hex():
  '''

  :return:
  '''
  zero_to_15 = random.randint(0, 15)
  random_sha1hex = ''
  for i in range(40):
    zero_to_15 = random.randint(0, 15)
    random_sha1hex += hexadecimal_str_seq[zero_to_15]
  return random_sha1hex

def is_leap(year):
  '''
  To be implemented
  :return:
  '''
  return False

def take_random_datetime():
  year = random.randint(1970, 2030)
  month = random.randint(1, 12)
  if month in [1,3,5,7,8,10,12]:
    day = random.randint(1, 31)
  elif month in [4,6,9,11]:
    day = random.randint(1, 30)
  else:
    if is_leap(year):
      day = random.randint(1, 29)
    else:
      day = random.randint(1, 28)
  hour   = random.randint(0, 23)
  minute = random.randint(0, 59)
  second = random.randint(0, 59)
  return datetime.datetime(year,month,day,hour,minute,second)

def test1():
  for i in xrange(10):
    print take_random_sha1hex()
    print take_random_datetime()

def main():
  test1()

if __name__ == '__main__':
  main()
