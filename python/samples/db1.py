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

#
# A simple example which creates a database, inserts some values,
# looks them up and erases them.
#
# This sample is similar to samples/db1.c.
#

# set the library path, otherwise upscaledb.so/.dll is not found
import os
import sys
import struct
import distutils.util
p     =  distutils.util.get_platform()
ps    =  ".%s-%s" % (p, sys.version[0:3])
sys.path.insert(0, os.path.join('build', 'lib' + ps))
sys.path.insert(1, os.path.join('..', 'build', 'lib' + ps))
import upscaledb

LOOP = 100
DATABASE_NAME = 1

# First create a new upscaledb Environment
env = upscaledb.env()
env.create("test.db")

# And then create a Database in this Environment. The Database is
# configured for uint32 keys and uint32 records
db = env.create_db(DATABASE_NAME, 0,  \
       ((upscaledb.UPS_PARAM_KEY_TYPE, upscaledb.UPS_TYPE_UINT32), \
        (upscaledb.UPS_PARAM_RECORD_SIZE, 4)))

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
# now fail with UPS_KEY_NOT_FOUND
#
for i in range(0, LOOP):
  try:
    s = struct.pack('I', i)
    record = db.find(None, s)
  except upscaledb.error, (errno, strerror):
    assert upscaledb.UPS_KEY_NOT_FOUND == errno

#
# done! Close all handles
#
db.close()
env.close()
print "success!"

