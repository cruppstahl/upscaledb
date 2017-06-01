#
# Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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

