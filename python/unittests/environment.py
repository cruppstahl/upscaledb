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

class EnvironmentTestCase(unittest.TestCase):
  def remove_file(self, fname):
    if os.path.isfile(fname):
      os.remove(fname)

  def testCreate(self):
    env = upscaledb.env()
    self.remove_file("test.db")
    env.create("test.db")
    env.close()
    assert(os.path.isfile("test.db"))
    env.create("test.db", 0)
    env.close()
    env.create("test.db", 0, 0644)
    env.close()
    assert(os.path.isfile("test.db"))

  def testCreateExtended(self):
    env = upscaledb.env()
    env.create("test.db", 0, 0644, \
        ((upscaledb.UPS_PARAM_CACHESIZE, 20), (0, 0)))
    env.close()

  def testCreateExtendedNegative(self):
    self.remove_file("test.db")
    env = upscaledb.env()
    try:
      env.create("test.db", 0, 0644, ((1, 2, 3)))
    except TypeError:
      pass
    try:
      env.create("test.db", 0, 0644, (1, 2, 3))
    except TypeError:
      pass
    try:
      env.create("test.db", 0, 0644, (("1", 2)))
    except TypeError:
      pass
    try:
      env.create("test.db", 0, 0644, ((1, None)))
    except TypeError:
      pass
    try:
      env.create("test.db", 0, 0644, ((1, "None")))
    except TypeError:
      pass

  def testCreateInMemory(self):
    self.remove_file("test.db")
    env = upscaledb.env()
    env.create("", upscaledb.UPS_IN_MEMORY)
    env.close()
    env.create(None, upscaledb.UPS_IN_MEMORY)
    env.close()
    assert(os.path.isfile("test.db") == False)

  def testCreateNegative(self):
    env = upscaledb.env()
    try:
      env.create("test.db", 0, 0644, "asdf")
    except TypeError:
      pass
    try:
      env.create("test.db", 9999)
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_INV_PARAMETER == errno

  def testOpenNegative(self):
    self.remove_file("test.db")
    env = upscaledb.env()
    try:
      env.open("test.db", 0, "asdf")
    except TypeError:
      pass
    try:
      env.open("test.db", upscaledb.UPS_IN_MEMORY)
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_INV_PARAMETER == errno

  def testOpenExtended(self):
    env = upscaledb.env()
    # TODO if i remove (0,0), a TypeError exception is thrown
    try:
      env.open("test.db", 0, \
          ((upscaledb.UPS_PARAM_CACHESIZE, 20), (0, 0)))
      env.close()
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_FILE_NOT_FOUND == errno

  def testOpenExtendedNegative(self):
    env = upscaledb.env()
    try:
      env.open("test.db", 0, ((1, 2, 3)))
    except TypeError:
      pass
    try:
      env.open("test.db", 0, (1, 2, 3))
    except TypeError:
      pass
    try:
      env.open("test.db", 0, (("1", 2)))
    except TypeError:
      pass
    try:
      env.open("test.db", 0, ((1, None)))
    except TypeError:
      pass
    try:
      env.open("test.db", 0, ((1, "None")))
    except TypeError:
      pass

  def testCreateDb(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(3)
    db.close()
    db = env.open_db(3)
    db.close()
    db = env.create_db(4)
    db.close()
    db = env.open_db(4)
    db.close()
    db = env.create_db(5)
    db.close()
    db = env.open_db(5)
    db.close()
    env.close()

  def testCreateDbParam(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(3, upscaledb.UPS_RECORD_NUMBER64)
    db.close()
    db = env.open_db(3)
    db.close()
    db = env.create_db(4, 0, ((upscaledb.UPS_PARAM_KEYSIZE, 20), (0,0)))
    db.close()
    db = env.open_db(4)
    db.close()
    env.close()

  def testCreateDbNestedClose(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(3)
    env.close()
    db.close()

  def testCreateDbNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    try:
      db = env.create_db(0)
      db.close()
    except upscaledb.error, (errno, message):
      assert upscaledb.UPS_INV_PARAMETER == errno
    try:
      db = env.create_db()
      db.close()
    except TypeError:
      pass
    env.close()

  def testOpenDbNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    db.close()
    try:
      db = env.open_db(5)
    except upscaledb.error, (errno, message):
      assert upscaledb.UPS_DATABASE_NOT_FOUND == errno
    try:
      db = env.open_db()
      db.close()
    except TypeError:
      pass
    env.close()

  def testRenameDb(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    db.close()
    env.rename_db(1, 2)
    db = env.open_db(2)
    db.close()
    env.close()

  def testRenameDbNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    try:
      env.rename_db(1, 2)
    except upscaledb.error, (errno, message):
      assert upscaledb.UPS_DATABASE_NOT_FOUND == errno
    try:
      env.rename_db(1, 2, 3)
    except TypeError:
      pass
    try:
      env.rename_db()
    except TypeError:
      pass
    env.close()

  def testEraseDb(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    db.close()
    env.erase_db(1)
    try:
      db = env.open_db(1)
    except upscaledb.error, (errno, message):
      assert upscaledb.UPS_DATABASE_NOT_FOUND == errno
    env.close()

  def testEraseDbNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    try:
      env.erase_db(1)
    except upscaledb.error, (errno, message):
      assert upscaledb.UPS_DATABASE_NOT_FOUND == errno
    try:
      env.erase_db()
    except TypeError:
      pass
    try:
      env.erase_db(3,4,5)
    except TypeError:
      pass
    env.close()

  def testGetDatabaseNames(self):
    env = upscaledb.env()
    env.create("test.db")
    n = env.get_database_names()
    assert n == ()
    db = env.create_db(1)
    db.close()
    n = env.get_database_names()
    assert n == (1,)
    db = env.create_db(2)
    db.close()
    n = env.get_database_names()
    assert n == (1, 2,)
    db = env.create_db(3)
    db.close()
    n = env.get_database_names()
    assert n == (1, 2, 3,)
    env.close()

  def testGetDatabaseNamesNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    try:
      n = env.get_database_names(4)
    except TypeError:
      pass
    env.close()

  def testFlush(self):
    env = upscaledb.env()
    env.create("test.db")
    env.flush()

unittest.main()

