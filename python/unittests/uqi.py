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
sys.path.insert(0, os.path.join('build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', 'build', 'lib' + ps))
import upscaledb

class UqiTestCase(unittest.TestCase):
  def remove_file(self, fname):
    if os.path.isfile(fname):
      os.remove(fname)

  def testSum(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    db.insert(None, "1", "value")
    db.insert(None, "2", "value")
    db.insert(None, "3", "value")
    result = env.select("COUNT($key) FROM DATABASE 1")
    assert result.get_row_count() == 1
    # this is a 64bit integer with the value "3" in little endian
    assert result.get_record(0) == "\3\0\0\0\0\0\0\0"
    db.close()
    env.close()


unittest.main()

