
noinst_PROGRAMS = test

INCLUDES        = -I$(top_builddir)/include

test_SOURCES    = bt_cursor.cpp \
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
test_LDADD      = $(top_builddir)/src/libhamsterdb.la -lcppunit -ldl

noinst_BIN      = test
