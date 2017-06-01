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
p    = distutils.util.get_platform()
ps   = ".%s-%s" % (p, sys.version[0:3])
sys.path.insert(0, os.path.join('build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', 'build', 'lib' + ps))
import upscaledb

c = 0

class DatabaseTestCase(unittest.TestCase):
  def remove_file(self, fname):
    if os.path.isfile(fname):
      os.remove(fname)

  def testInsert(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1, upscaledb.UPS_ENABLE_DUPLICATE_KEYS)
    db.insert(None, "key1", "value")
    db.insert(None, "key2", "value", 0)
    db.insert(None, "key1", "value", upscaledb.UPS_OVERWRITE)
    db.insert(None, "key1", "value", upscaledb.UPS_DUPLICATE)
    db.close()
    env.close()

  def testInsertNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    db.insert(None, "key1", "value")
    try:
      db.insert(None, "key1", "value")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_DUPLICATE_KEY == errno
    try:
      db.insert(None, "key1", "value", upscaledb.UPS_DUPLICATE)
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_INV_PARAMETER == errno
    try:
      db.insert(None, None, "value")
    except TypeError:
      pass
    try:
      db.insert(None, None, "value")
    except TypeError:
      pass
    db.close()

  def testInsertRecno(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1, upscaledb.UPS_RECORD_NUMBER32)
    db.insert(None, "key1", "value")
    db.insert(None, 5, "value")
    db.insert(None, None, "value")
    db.close()

  def testFind(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    db.insert(None, "key1", "value1")
    db.insert(None, "key2", "value2", 0)
    assert "value1" == db.find(None, "key1")
    assert "value2" == db.find(None, "key2")
    db.close()

  def testFindNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    db.insert(None, "key1", "value1")
    try:
      db.find(None, "key1", 0)
    except TypeError:
      pass
    try:
      db.find(5)
    except TypeError:
      pass
    try:
      db.find(None)
    except TypeError:
      pass
    try:
      db.find(None, "key2")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    db.close()

  def testFindRecno(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1, upscaledb.UPS_RECORD_NUMBER64)
    db.insert(None, "", "value1")
    db.insert(None, "", "value2")
    db.insert(None, "", "value3")
    db.insert(None, "", "value4")
    assert "value1" == db.find(None, 1)
    assert "value2" == db.find(None, 2)
    assert "value3" == db.find(None, 3)
    assert "value4" == db.find(None, 4)
    try:
      db.find(None, 5)
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno

  def testErase(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)

    db.insert(None, "key1", "value1")
    db.insert(None, "key2", "value2")
    assert "value1" == db.find(None, "key1")
    assert "value2" == db.find(None, "key2")
    db.erase(None, "key1")
    db.erase(None, "key2")
    try:
      db.find(None, "key1")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    try:
      db.find(None, "key2")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    db.close()

  def testEraseNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    try:
      db.find(None, "key1")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    try:
      db.erase()
    except TypeError:
      pass
    try:
      db.erase(5)
    except TypeError:
      pass
    try:
      db.erase(None)
    except TypeError:
      pass
    try:
      db.erase("asdf", 0)
    except TypeError:
      pass
    db.close()

  def testEraseRecno(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1, upscaledb.UPS_RECORD_NUMBER32)
    db.insert(None, "", "value1")
    db.insert(None, "", "value2")
    db.insert(None, "", "value3")
    db.insert(None, "", "value4")
    db.erase(None, 1)
    db.erase(None, 2)
    db.erase(None, 3)
    db.erase(None, 4)
    try:
      db.erase(None, 5)
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    db.close()

  def callbackCompare1(db, lhs, rhs):
    global c
    c += 1
    return c

  def testSetCompareFunc(self):
    global c
    c = 0
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1, 0, \
          ((upscaledb.UPS_PARAM_KEY_TYPE, upscaledb.UPS_TYPE_CUSTOM), (0, 0)))
    db.set_compare_func(self.callbackCompare1)
    db.insert(None, "1", "value")
    db.insert(None, "2", "value")
    db.insert(None, "3", "value")
    assert c == 2
    db.set_compare_func(None)
    db.insert(None, "4", "value")
    db.insert(None, "5", "value")
    assert c == 6
    db.close()

  def testSetCompareFuncNegative(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1)
    try:
      db.set_compare_func(self.callbackCompare1)
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_INV_PARAMETER == errno
    try:
      db.set_compare_func(self.callbackCompare1, 3)
    except TypeError:
      pass
    db.close()

  def callbackCompare2(db, lhs, rhs):
    i = 3 / 0 # raises ZeroDivisionError

  def callbackCompare3(db, lhs, rhs):
    if lhs < rhs:
      return -1
    if lhs > rhs:
      return +1
    return 0

  def testSetCompareFuncExcept(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1, 0, \
          ((upscaledb.UPS_PARAM_KEY_TYPE, upscaledb.UPS_TYPE_CUSTOM), (0, 0)))

    db.set_compare_func(self.callbackCompare2)
    try:
      db.insert(None, "1", "value")
      db.insert(None, "2", "value")
    except ZeroDivisionError:
      pass
    db.close()

  def testSetCompareFunc2(self):
    upscaledb.register_compare("cmp", self.callbackCompare3);

    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(1, 0, \
          ((upscaledb.UPS_PARAM_KEY_TYPE, upscaledb.UPS_TYPE_CUSTOM),
           (upscaledb.UPS_PARAM_CUSTOM_COMPARE_NAME, "cmp"),
           (0, 0)))
    db.insert(None, "1", "value")
    db.insert(None, "2", "value")
    db.insert(None, "3", "value")
    db.close()
    env.close()

    env.open("test.db")
    db = env.open_db(1, 0)
    assert "value" == db.find(None, "1")
    assert "value" == db.find(None, "2")
    assert "value" == db.find(None, "3")
    db.close()
    env.close()

  def testRecnoReopen(self):
    env = upscaledb.env()
    env.create("test.db")
    db = env.create_db(3, upscaledb.UPS_RECORD_NUMBER64)
    db.insert(None, "key1", "value")
    db.insert(None, 5, "value")
    db.insert(None, None, "value")
    db.close()
    db = env.open_db(3)
    db.insert(None, None, "value")
    #c = upscaledb.cursor(db)
    #c.find(4)
    #assert 4 == c.get_key()
    db.close()
    env.close()

unittest.main()

