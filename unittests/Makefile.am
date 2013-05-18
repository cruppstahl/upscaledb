
EXTRA_DIST      = recovery.pl suppressions.valgrind data/*

noinst_PROGRAMS = test bfc_sample recovery

AM_CPPFLAGS     = -I../include -I$(top_srcdir)/include

test_SOURCES    = bfc-testsuite.cpp \
                  bfc-testsuite.hpp \
                  os.hpp \
                  hamster_fixture.hpp \
                  bfc-signal.h \
                  bfc-signal.c \
                  check.cpp \
                  error.cpp \
                  os.cpp \
                  endian.cpp \
                  journal.cpp \
                  duplicates.cpp \
                  device.cpp \
                  approx.cpp \
                  recno.cpp \
                  cache.cpp \
                  changeset.cpp \
                  misc.cpp \
                  freelist.cpp \
                  btree_erase.cpp \
                  btree_insert.cpp \
                  page.cpp \
                  extkeys.cpp \
                  api110.cpp \
                  blob.cpp \
                  btree_cursor.cpp \
                  btree_key.cpp \
                  env.cpp \
                  hamsterdb.cpp \
                  partial.cpp \
                  db.cpp \
                  txn.cpp \
                  txn_cursor.cpp \
                  cursor.cpp \
                  log.cpp \
                  cppapi.cpp \
                  main.cpp

test_LDADD      = $(top_builddir)/src/libhamsterdb.la -lpthread -ldl

if ENABLE_REMOTE
test_SOURCES   += remote.cpp
AM_CPPFLAGS    += -DHAM_ENABLE_REMOTE
test_LDADD     += $(top_builddir)/src/server/libhamserver.la -lprotobuf -lgnutls
endif

bfc_sample_SOURCES = bfc-sample.cpp \
                  empty_sample.cpp \
                  bfc-testsuite.cpp \
                  bfc-testsuite.hpp \
                  os.hpp \
                  bfc-signal.h \
                  bfc-signal.c

valgrind:
	valgrind --suppressions=suppressions.valgrind --leak-check=full \
			--num-callers=20 --show-reachable=yes .libs/lt-test 

bfc_sample_LDADD = -ldl

recovery_SOURCES = recovery.cpp

recovery_LDADD   = $(top_builddir)/src/libhamsterdb.la -ldl

noinst_BIN       = test bfc_sample recovery

test_recovery:
	perl recovery.pl
