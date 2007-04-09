
if HAVE_CPPUNIT
noinst_PROGRAMS = test
endif

INCLUDES        = -I$(top_builddir)/include

test_SOURCES    = device.cpp \
                  cache.cpp \
                  freelist.cpp \
                  db.cpp \
                  main.cpp \
                  mem.cpp \
                  endian.cpp \
                  page.cpp \
                  os.cpp \
                  memtracker.cpp
test_LDADD      = $(top_builddir)/src/libhamsterdb.la -lcppunit

noinst_BIN      = test
