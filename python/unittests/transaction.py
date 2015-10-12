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
p    = distutils.util.get_platform()
ps   = ".%s-%s" % (p, sys.version[0:3])
sys.path.insert(0, os.path.join('build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', 'build', 'lib' + ps))
import upscaledb

class TransactionTestCase(unittest.TestCase):
  def testBeginAbort(self):
    env = upscaledb.env()
    env.create("test.db", upscaledb.UPS_ENABLE_TRANSACTIONS)
    db = env.create_db(1)
    txn = upscaledb.txn(env)
    txn.abort()
    db.close()

  def testBeginCommit(self):
    env = upscaledb.env()
    env.create("test.db", upscaledb.UPS_ENABLE_TRANSACTIONS)
    db = env.create_db(1)
    txn = upscaledb.txn(env)
    db.insert(txn, "key1", "value1")
    db.insert(txn, "key2", "value2")
    db.insert(txn, "key3", "value3")
    db.erase(txn, "key1")
    db.erase(txn, "key2")
    try:
      db.find(txn, "key1")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    try:
      db.find(txn, "key2")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    txn.commit()
    db.close()

  def testCursor(self):
    env = upscaledb.env()
    env.create("test.db", upscaledb.UPS_ENABLE_TRANSACTIONS)
    db = env.create_db(1)
    txn = upscaledb.txn(env)
    c = upscaledb.cursor(db, txn)
    c.insert("key1", "value1")
    c.insert("key2", "value2")
    c.insert("key3", "value3")
    c.find("key1")
    c.erase()
    try:
      c.find("key2")
    except upscaledb.error, (errno, strerror):
      assert upscaledb.UPS_KEY_NOT_FOUND == errno
    c.close()
    txn.commit()
    db.close()

unittest.main()

