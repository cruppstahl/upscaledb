

if HAVE_DB_H
noinst_PROGRAMS	= test
endif

INCLUDE   = -I$(top_builddir)/include

AM_CFLAGS =

# little or big endian?
if BIG_ENDIAN
    AM_CFLAGS += -DHAM_BIG_ENDIAN
else
    AM_CFLAGS += -DHAM_LITTLE_ENDIAN
endif

# debug or release mode? 
if DEBUG
    AM_CFLAGS += -DDEBUG -DHAM_DEBUG
else
    AM_CFLAGS += -DHAM_RELEASE 
endif

# use profiling?
if PROFILE
    AM_CFLAGS += -pg
endif

test_SOURCES    = db.c getopts.c
test_LDADD      = $(top_builddir)/src/libhamsterdb.la -ldb
