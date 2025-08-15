#!/usr/bin/env python3
"""
listfunctions_mod.py
"""
# import os


def remove_larger_number_n_return_the_ids(ids_n_numbers_tuplelist):
  if type(ids_n_numbers_tuplelist) not in [list, tuple] or len(ids_n_numbers_tuplelist) == 0:
    return []
  try:
    if len(ids_n_numbers_tuplelist) == 1:
      _id, _ = ids_n_numbers_tuplelist[0]
      ids = [_id]
      return ids
    ids_n_numbers_tuplelist = sorted(ids_n_numbers_tuplelist, key=lambda x: x[1])
    del ids_n_numbers_tuplelist[-1]
    ids = [tupl[0] for tupl in ids_n_numbers_tuplelist]
    ids = sorted(ids)
    return ids
  except IndexError:
    error_msg = 'IndexError: bad input data to remove_larger_number_n_return_the_ids() [%s]' \
                ' :: it should be a tuple list' % str(ids_n_numbers_tuplelist)
    raise IndexError(error_msg)


def adhoc_test():
  id_n_number_tuplelist = []
  id_n_number = (15, 24)
  id_n_number_tuplelist.append(id_n_number)
  id_n_number = (7, 21)
  id_n_number_tuplelist.append(id_n_number)
  id_n_number = (3, 19)
  id_n_number_tuplelist.append(id_n_number)
  print('Input')
  print(id_n_number_tuplelist)
  print('Output')
  ids = remove_larger_number_n_return_the_ids(id_n_number_tuplelist)
  print(ids)
  id_n_number_tuplelist = []
  id_n_number = (30, 11)
  id_n_number_tuplelist.append(id_n_number)
  id_n_number = (10, 21)
  id_n_number_tuplelist.append(id_n_number)
  id_n_number = (3, 19)
  id_n_number_tuplelist.append(id_n_number)
  print('Input')
  print(id_n_number_tuplelist)
  print('Output')
  ids = remove_larger_number_n_return_the_ids(id_n_number_tuplelist)
  print(ids)


if __name__ == '__main__':
  adhoc_test()
