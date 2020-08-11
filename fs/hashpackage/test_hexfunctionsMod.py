#!/usr/bin/env python3
"""
  docstring
"""
import sys
import unittest
import fs.hashpackage.hexfunctionsMod as hexM


class Test1(unittest.TestCase):
  
  def test_convert_one_digit_hex_to_str(self):
    
    hex_int = 0xA
    hex_str = hexM.convert_hexint_to_char(hex_int)
    self.assertEqual(hex_str, 'a')

    hex_int = 10
    hex_str = hexM.convert_hexint_to_char(hex_int)
    self.assertEqual(hex_str, 'a')

  def test_generate_40char_both_methods(self):
    
    # 1st method
    hex_str = hexM.generate_a_40char_random_hex()
    self.assertEqual(len(hex_str), 40)
    self.assertEqual(hex_str, hex_str.lower())

    # 2nd method
    # hex_str = hexM.generate_a_40char_random_hex_plan_b()
    # self.assertEqual(len(hex_str), 40)
    #  self.assertEqual(hex_str, hex_str.lower())

  def test_equality_between_generated_and_given(self):
    hex_int = 0xA
    hex_str = hexM.stuff_hex_number_to_a_40char_str(hex_int)
    s_39_zeroes_plus_ending_a = '0'*39 + 'A'
    self.assertEqual(hex_str, s_39_zeroes_plus_ending_a.lower())


def unittests():
  unittest.main()


def process():
  pass


if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()
