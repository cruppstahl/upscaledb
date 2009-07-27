
#
# make sure that our modified CFLAGS are also respected by g++
#
AM_CXXFLAGS = $(CFLAGS)
CXXFLAGS = $(CFLAGS)

noinst_PROGRAMS = test

AM_CPPFLAGS       = -I$(top_builddir)/include

test_SOURCES    = log.cpp \
                  btree_erase.cpp \
                  check.cpp \
                  error.cpp \
                  filter.cpp \
                  cppapi.cpp \
                  recno.cpp \
                  duplicates.cpp \
                  env.cpp \
                  hamsterdb.cpp \
                  blob.cpp \
                  extkeys.cpp \
                  keys.cpp \
                  bt_cursor.cpp \
                  util.cpp \
                  device.cpp \
                  os.cpp \
                  cache.cpp \
                  freelist.cpp \
                  db.cpp \
                  main.cpp \
                  mem.cpp \
                  endian.cpp \
                  page.cpp \
                  txn.cpp \
                  memtracker.cpp

test_LDADD      = $(top_builddir)/src/libhamsterdb.la -ldl

noinst_BIN      = test
