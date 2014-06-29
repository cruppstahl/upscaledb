#
# Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
#

import unittest

# set the library path, otherwise hamsterdb.so/.dll is not found
import os
import sys
import distutils.util
p  = distutils.util.get_platform()
ps   = ".%s-%s" % (p, sys.version[0:3])
sys.path.insert(0, os.path.join('..', 'build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', '..', 'src', '.libs'))
import hamsterdb

error_count = 0

def my_error_handler(code, message):
  global error_count
  print "catching error, counter is", error_count + 1
  error_count += 1

class LibraryTestCase(unittest.TestCase):
  def testGetVersion(self):
    print "version: ", hamsterdb.get_version()

  def testIsPro(self):
    print "is_pro: ", hamsterdb.is_pro()

  def testIsDebug(self):
    print "is_debug: ", hamsterdb.is_debug()

  def testIsProEvaluation(self):
    print "is_pro_evaluation: ", hamsterdb.is_pro_evaluation()

  def testSetErrhandler(self):
    global error_count
    hamsterdb.set_errhandler(my_error_handler)
    error_count = 0
    try:
      hamsterdb.env().open("asxxxldjf")
    except hamsterdb.error, (errno, strerror):
      assert hamsterdb.HAM_FILE_NOT_FOUND == errno
    assert error_count == 1

    hamsterdb.set_errhandler(None)
    error_count = 0
    try:
      hamsterdb.env().open("asxxxldjf")
    except hamsterdb.error, (errno, strerror):
      assert hamsterdb.HAM_FILE_NOT_FOUND == errno
    assert error_count == 0

unittest.main()

