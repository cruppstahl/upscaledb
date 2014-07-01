#
# Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or 
# (at your option) any later version.
#
# See file COPYING.GPL2 and COPYING.GPL3 for License information.
#

import unittest

# set the library path, otherwise hamsterdb.so/.dll is not found
import os
import sys
import distutils.util
p    = distutils.util.get_platform()
ps   = ".%s-%s" % (p, sys.version[0:3])
sys.path.insert(0, os.path.join('build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', 'build', 'lib' + ps))
import hamsterdb

class TransactionTestCase(unittest.TestCase):
  def testBeginAbort(self):
    env = hamsterdb.env()
    env.create("test.db", hamsterdb.HAM_ENABLE_TRANSACTIONS)
    db = env.create_db(1)
    txn = hamsterdb.txn(env)
    txn.abort()
    db.close()

  def testBeginCommit(self):
    env = hamsterdb.env()
    env.create("test.db", hamsterdb.HAM_ENABLE_TRANSACTIONS)
    db = env.create_db(1)
    txn = hamsterdb.txn(env)
    db.insert(txn, "key1", "value1")
    db.insert(txn, "key2", "value2")
    db.insert(txn, "key3", "value3")
    db.erase(txn, "key1")
    db.erase(txn, "key2")
    try:
      db.find(txn, "key1")
    except hamsterdb.error, (errno, strerror):
      assert hamsterdb.HAM_KEY_NOT_FOUND == errno
    try:
      db.find(txn, "key2")
    except hamsterdb.error, (errno, strerror):
      assert hamsterdb.HAM_KEY_NOT_FOUND == errno
    txn.commit()
    db.close()

  def testCursor(self):
    env = hamsterdb.env()
    env.create("test.db", hamsterdb.HAM_ENABLE_TRANSACTIONS)
    db = env.create_db(1)
    txn = hamsterdb.txn(env)
    c = hamsterdb.cursor(db, txn)
    c.insert("key1", "value1")
    c.insert("key2", "value2")
    c.insert("key3", "value3")
    c.find("key1")
    c.erase()
    try:
      c.find("key2")
    except hamsterdb.error, (errno, strerror):
      assert hamsterdb.HAM_KEY_NOT_FOUND == errno
    c.close()
    txn.commit()
    db.close()

unittest.main()

