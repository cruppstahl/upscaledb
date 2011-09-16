
EXTRA_DIST      = valgrind.supp

noinst_PROGRAMS = test bfc_sample

AM_CPPFLAGS     = -I$(top_builddir)/include

test_SOURCES    = log.cpp \
                  journal.cpp \
                  api110.cpp \
                  btree_erase.cpp \
                  btree_insert.cpp \
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
                  btree_key.cpp \
                  partial.cpp \
                  btree_cursor.cpp \
                  misc.cpp \
                  device.cpp \
                  os.cpp \
                  cache.cpp \
                  freelist.cpp \
                  db.cpp \
                  main.cpp \
                  mem.cpp \
                  endian.cpp \
                  page.cpp \
                  changeset.cpp \
                  txn.cpp \
                  txn_cursor.cpp \
                  cursor.cpp \
                  memtracker.cpp \
                  memtracker.h \
                  empty_sample.cpp \
                  bfc-testsuite.cpp \
                  bfc-testsuite.hpp \
		          os.hpp \
		          hamster_fixture.hpp \
                  bfc-signal.h \
                  bfc-signal.c

test_LDADD      = $(top_builddir)/src/libhamsterdb.la -ldl

if ENABLE_REMOTE
test_SOURCES   += remote.cpp
CFLAGS         += -DHAM_ENABLE_REMOTE
test_LDADD     += $(top_builddir)/src/server/libhamserver.la
endif

bfc_sample_SOURCES = bfc-sample.cpp \
                  empty_sample.cpp \
                  bfc-testsuite.cpp \
                  bfc-testsuite.hpp \
		  		  os.hpp \
                  bfc-signal.h \
                  bfc-signal.c

bfc_sample_LDADD = -ldl

noinst_BIN      = test bfc_sample

#
# make sure that our modified CFLAGS are also respected by g++
#
AM_CXXFLAGS     = $(CFLAGS)
CXXFLAGS        = $(CFLAGS)
