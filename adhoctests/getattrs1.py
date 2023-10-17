#!/usr/bin/env python3
"""
getattrs1.py
"""


class Abc:

  def __init__(self, name, descr, qtd):
    self.name = name
    self.descr = descr
    self.qtd = qtd
    self.process()

  @property
  def namedescr(self):
    return self.name + ' | ' + self.descr

  def outdict(self):
    outdict = {
      name: attr for name, attr in self.__dict__.items()
      if not name.startswith('__')
         and not name.startswith('_')
         and not callable(attr)
         and not type(attr) is staticmethod
    }
    return outdict

  def process(self):
    _ = self.namedescr
    return


def process():
  name = 'name'
  descr = 'descr'
  qtd = -1
  o = Abc(name, descr, qtd)
  attrs = o.outdict()
  print(attrs)
  print(o.__dict__)


if __name__ == '__main__':
  process()
