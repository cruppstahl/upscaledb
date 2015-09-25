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

#
# A simple example which creates a database, inserts some values,
# looks them up and erases them.
#
# This sample is similar to samples/db1.c.
#

# set the library path, otherwise hamsterdb.so/.dll is not found
import os
import sys
import struct
import distutils.util
p     =  distutils.util.get_platform()
ps    =  ".%s-%s" % (p, sys.version[0:3])
sys.path.insert(0, os.path.join('build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', 'build', 'lib' + ps))
import hamsterdb

LOOP = 100
DATABASE_NAME = 1

# First create a new hamsterdb Environment
env = hamsterdb.env()
env.create("test.db")

# And then create a Database in this Environment. The Database is
# configured for uint32 keys and uint32 records
db = env.create_db(DATABASE_NAME, 0,  \
       ((hamsterdb.HAM_PARAM_KEY_TYPE, hamsterdb.HAM_TYPE_UINT32), \
        (hamsterdb.HAM_PARAM_RECORD_SIZE, 4)))

#
# Now we can insert, delete or lookup values in the database
#
# For our test program, we just insert a few values, then look them
# up, then delete them and try to look them up again (which will fail).
# For simplicity we set keys and records to the same value.
#
for i in range(0, LOOP):
  # The first parameter specifies the Transaction. We don't use any,
  # therefore set to None
  s = struct.pack('I', i)
  db.insert(None, s, s)

#
# Now lookup all values
#
for i in range(0, LOOP):
  s = struct.pack('I', i)
  record = db.find(None, s)
  # verify the value
  assert i == struct.unpack('I', record)[0]
  assert s == record

#
# Close the database handle, then re-open it (to demonstrate how to open
# an Environment and a Database)
#
db.close()
env.close()

env.open("test.db")
db = env.open_db(DATABASE_NAME)

# now erase all values
for i in range(0, LOOP):
  s = struct.pack('I', i)
  db.erase(None, s)

#
# Once more try to find all values. every db.find() call must
# now fail with HAM_KEY_NOT_FOUND
#
for i in range(0, LOOP):
  try:
    s = struct.pack('I', i)
    record = db.find(None, s)
  except hamsterdb.error, (errno, strerror):
    assert hamsterdb.HAM_KEY_NOT_FOUND == errno

#
# done! Close all handles
#
db.close()
env.close()
print "success!"

