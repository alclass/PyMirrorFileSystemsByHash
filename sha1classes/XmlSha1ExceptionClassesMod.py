#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
  
'''


class Sha1FileOrFolderInexistence(OSError):
  pass

class XmlSha1FolderDoesNotExist(Sha1FileOrFolderInexistence):
  pass

class PassedAXmlSha1FileThatIsNotOnTopOfADir(Sha1FileOrFolderInexistence):
  pass

class PassedAXmlSha1FileThatIsADirNotAFile(Sha1FileOrFolderInexistence):
  pass

class XmlSha1FilePassedDoesNotExist(Sha1FileOrFolderInexistence):
  pass

class FolderPassedToXmlSha1GenerationDoesNotExist(Sha1FileOrFolderInexistence):
  pass

class FilePassedInToBeHashedAndAddedToSha1ListingsDoesNotExist(Sha1FileOrFolderInexistence):
  pass

class XmlManipulationError(ValueError):
  pass

class RenameFileWithNoCorrespondingSha1hex(XmlManipulationError):
  pass    

class IncorrectFilenameXmlInsertionWithErroneousSha1hex(XmlManipulationError):
  pass

class IssuedAFilenameXmlInsertionForAFileAlreadyInserted(XmlManipulationError):
  pass

class CorrespondingFileInSha1XmlNotFound(XmlManipulationError):
  pass    

class Sha1hexPassedAsNone(ValueError):
  pass    

class LogicalProgramFlowError(ValueError):
  pass

class LogicalErrorFilenameAndSha1hexPairWereNotXmlInserted(LogicalProgramFlowError):
  pass

class LogicalProgramErrorCorrespondingFileAndSha1hexMismatch(LogicalProgramFlowError):
  pass

class FileSystemHasFilenamesThatHaveAnUnknownEncoding(XmlManipulationError):
  pass

def process():
  pass
  
if __name__ == '__main__':
  pass
  #process()

