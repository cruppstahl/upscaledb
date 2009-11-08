
#
# make sure that our modified CFLAGS are also respected by g++
#
AM_CXXFLAGS     = $(CFLAGS)
CXXFLAGS        = $(CFLAGS)

EXTRA_DIST      = valgrind.supp

noinst_PROGRAMS = test bfc_sample

AM_CPPFLAGS     = -I$(top_builddir)/include

test_SOURCES    = log.cpp \
                  api110.cpp \
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
                  memtracker.cpp \
                  memtracker.h \
                  empty_sample.cpp \
                  bfc-testsuite.cpp \
                  bfc-testsuite.hpp \
		          os.hpp \
		          hamster_fixture.hpp \
                  bfc-signal.h \
                  bfc-signal.c

test_SOURCES    = api110.cpp \
                  main.cpp \
                  memtracker.cpp \
                  memtracker.h \
		          os.hpp \
                  bfc-signal.h \
                  bfc-testsuite.cpp \
                  bfc-testsuite.hpp \
                  bfc-signal.c

test_LDADD      = $(top_builddir)/src/libhamsterdb.la -ldl

bfc_sample_SOURCES = bfc-sample.cpp \
                  empty_sample.cpp \
                  bfc-testsuite.cpp \
                  bfc-testsuite.hpp \
		  		  os.hpp \
                  bfc-signal.h \
                  bfc-signal.c

bfc_sample_LDADD = -ldl

noinst_BIN      = test bfc_sample

