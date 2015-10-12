#
# Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
# All Rights Reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# See the file COPYING for License information.
#

import unittest

# set the library path, otherwise upscaledb.so/.dll is not found
import os
import sys
import distutils.util
p  = distutils.util.get_platform()
ps   = ".%s-%s" % (p, sys.version[0:3])
sys.path.insert(0, os.path.join('..', 'build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', '..', 'src', '.libs'))
import upscaledb

error_count = 0

def my_error_handler(code, message):
  global error_count
  print "catching error, counter is", error_count + 1
  error_count += 1

class LibraryTestCase(unittest.TestCase):
  def testGetVersion(self):
    print "version: ", upscaledb.get_version()

  def testIsDebug(self):
    print "is_debug: ", upscaledb.is_debug()

  def testSetErrhandler(self):
    global error_count
    upscaledb.set_error_handler(my_error_handler)
    error_count = 0
    try:
      upscaledb.env().open("asxxxldjf")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_FILE_NOT_FOUND == errno
    assert error_count == 1

    upscaledb.set_error_handler(None)
    error_count = 0
    try:
      upscaledb.env().open("asxxxldjf")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_FILE_NOT_FOUND == errno
    assert error_count == 0

unittest.main()

