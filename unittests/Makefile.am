
EXTRA_DIST      = recovery.pl suppressions.valgrind data/*

noinst_PROGRAMS = test recovery

AM_CPPFLAGS     = -I../include -I$(top_srcdir)/include

test_SOURCES    = api110.cpp \
			      approx.cpp \
                  blob_manager.cpp \
                  btree_cursor.cpp \
                  btree_erase.cpp \
                  btree_insert.cpp \
				  btree_key.cpp \
			 	  cache.cpp \
				  changeset.cpp \
				  check.cpp \
                  cppapi.cpp \
                  cursor.cpp \
                  db.cpp \
                  device.cpp \
                  duplicates.cpp \
				  endian.cpp \
                  env.cpp \
                  error.cpp \
                  extkeys.cpp \
                  freelist.cpp \
                  hamsterdb.cpp \
                  journal.cpp \
                  log.cpp \
				  main.cpp \
                  misc.cpp \
                  os.cpp \
				  os.hpp \
                  page.cpp \
                  page_manager.cpp \
                  partial.cpp \
                  recno.cpp \
                  txn.cpp \
                  txn_cursor.cpp

test_LDADD      = $(top_builddir)/src/.libs/libhamsterdb.a -lpthread -ldl

if ENABLE_REMOTE
test_SOURCES   += remote.cpp
AM_CPPFLAGS    += -DHAM_ENABLE_REMOTE
test_LDADD     += $(top_builddir)/src/server/.libs/libhamserver.a \
				  -lprotobuf -lcurl
endif

valgrind:
	valgrind --suppressions=suppressions.valgrind --leak-check=full \
			--num-callers=20 --show-reachable=yes .libs/lt-test 

recovery_SOURCES = recovery.cpp

recovery_LDADD   = $(top_builddir)/src/libhamsterdb.la -ldl

noinst_BIN       = test recovery

test_recovery:
	perl recovery.pl
