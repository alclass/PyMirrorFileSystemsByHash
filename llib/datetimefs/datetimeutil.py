#!/usr/bin/env python3
import datetime
from datetime import timedelta
import time


def datetime_within_radius(mdatetime_src, mdatetime_trg, radius_in_sec):
  timediff = abs(mdatetime_src - mdatetime_trg)
  print('radius_in_sec', radius_in_sec, ':: timediff', timediff)
  if timediff < timedelta(seconds=radius_in_sec):
    return True
  return False


def adhoc_verify_timediff_within_deltatime(dt, radius_in_sec):
  print('-'*40)
  print('adhoc_verify_timediff_within_deltatime()')
  print('-'*40)
  t1 = datetime.datetime.now()
  print('t1', t1)
  print('wait', dt, 'seconds')
  time.sleep(dt)
  t2 = datetime.datetime.now()  # t1 + timedelta(seconds=dt)
  print('t2', t2)
  bool_res = datetime_within_radius(t1, t2, radius_in_sec)
  print('bool_res', bool_res)
  if bool_res:
    print('\tTrue: ie timediff is within (less than) radius.')
  else:
    print('\tFalse: ie timediff is greater than (outside) radius.')


def adhoc_test():
  dt = 2
  radius_in_sec = 1
  adhoc_verify_timediff_within_deltatime(dt, radius_in_sec)
  dt = 1
  radius_in_sec = 2
  adhoc_verify_timediff_within_deltatime(dt, radius_in_sec)


if __name__ == '__main__':
  adhoc_test()
