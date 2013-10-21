
EXTRA_DIST      = recovery.pl valgrind.supp data/*

noinst_PROGRAMS = test recovery
noinst_BIN      = test recovery


AM_CPPFLAGS     = -I../include -I$(top_srcdir)/include

test_SOURCES    = aes.cpp \
				  api110.cpp \
			      approx.cpp \
                  blob_manager.cpp \
                  btree.cpp \
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
                  globals.h \
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
                  partial-overwrite-inmem-ps16.h \
                  partial-overwrite-inmem-ps1.h \
                  partial-overwrite-inmem-ps2.h \
                  partial-overwrite-inmem-ps4.h \
                  partial-overwrite-inmem-ps64.h \
                  partial-overwrite-ps16.h \
                  partial-overwrite-ps1.h \
                  partial-overwrite-ps2.h \
                  partial-overwrite-ps4.h \
                  partial-overwrite-ps64.h \
                  partial-read-direct-ps16.h \
                  partial-read-direct-ps1.h \
                  partial-read-direct-ps2.h \
                  partial-read-direct-ps4.h \
                  partial-read-direct-ps64.h \
                  partial-read-inmem-ps16.h \
                  partial-read-inmem-ps1.h \
                  partial-read-inmem-ps2.h \
                  partial-read-inmem-ps4.h \
                  partial-read-inmem-ps64.h \
                  partial-read-ps16.h \
                  partial-read-ps1.h \
                  partial-read-ps2.h \
                  partial-read-ps4.h \
                  partial-read-ps64.h \
                  partial-write-inmem-ps16.h \
                  partial-write-inmem-ps1.h \
                  partial-write-inmem-ps2.h \
                  partial-write-inmem-ps4.h \
                  partial-write-inmem-ps64.h \
                  partial-write-ps16.h \
                  partial-write-ps1.h \
                  partial-write-ps2.h \
                  partial-write-ps4.h \
                  partial-write-ps64.h \
                  recno.cpp \
                  txn.cpp \
                  txn_cursor.cpp

test_LDADD      = $(top_builddir)/src/.libs/libhamsterdb.a -lpthread -ldl
test_LDFLAGS    =

if DARWIN
test_LDADD     += $(BOOST_SYSTEM_LIBS)
test_LDFLAGS   += $(BOOST_SYSTEM_LDFLAGS)
endif

if ENABLE_REMOTE
test_SOURCES   += remote.cpp
AM_CPPFLAGS    += -DHAM_ENABLE_REMOTE
test_LDADD     += $(top_builddir)/src/server/.libs/libhamserver.a \
				  -lprotobuf -luv -ldl
endif

valgrind:
	valgrind --suppressions=valgrind.supp --leak-check=full \
			--num-callers=20 --show-reachable=yes \
			--log-file=valgrind-out.txt ./test

recovery_SOURCES = recovery.cpp

recovery_LDADD   = $(top_builddir)/src/libhamsterdb.la \
				   $(BOOST_SYSTEM_LIBS) -ldl 
recovery_LDFLAGS = $(BOOST_SYSTEM_LDFLAGS)

test_recovery:
	perl recovery.pl
