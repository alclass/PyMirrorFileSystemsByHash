#!/usr/bin/env python3
"""
~/bin/localuserpylib/regexfs/filenamevalidator_cls.py
"""
import localuserpylib.regexfs.filenamevalidator_cls as fnval


def adhoctest1():
  # test
  n_test = 1
  filename = "cool video abcABC0123-_.mp4"
  scrmsg = f"test {n_test} Testing filename string: [{filename}]"
  print(scrmsg)
  fnvalidator = fnval.FilenameValidator(filename=filename)
  print(fnvalidator)
  # test
  n_test += 1
  filename = "REDE GLOBO SE INCOMODA COM VÍDEOS DE IA CONTRA O CONGRESSO NACIONAL [D6anIztaYCE].mp4"
  scrmsg = f"test {n_test} Testing filename string: [{filename}]"
  print(scrmsg)
  fnvalidator = fnval.FilenameValidator(filename=filename)
  print(fnvalidator)
  # test
  n_test += 1
  filename = "REDE GLOBO SE INCOMODA COM VÍDEOS DE IA CONTRA O CONGRESSO NACIONAL [D6anIztaYCE].160.mp4"
  scrmsg = f"test {n_test} Testing filename string: [{filename}]"
  print(scrmsg)
  fnvalidator = fnval.FilenameValidator(filename=filename)
  print(fnvalidator)


def process():
  pass


if __name__ == '__main__':
  """
  adhoctest1()
  process()
  """
  adhoctest1()
  process()
