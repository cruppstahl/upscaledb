
INCLUDES		= -I$(top_builddir)/include

noinst_PROGRAMS	= test

# TODO little or big endian?
AM_CFLAGS = -DHAM_LITTLE_ENDIAN

# TODO kein -O bei gcc < 4.1.1
if DEBUG
    AM_CFLAGS += -DDEBUG -DHAM_DEBUG
else
    AM_CFLAGS += -DHAM_RELEASE -O3 -ffast-math
endif

if PROFILE
    AM_CFLAGS += -pg
endif

test_SOURCES    = db.c getopts.c
test_LDADD      = $(top_builddir)/src/libhamsterdb.la -ldb

EXTRA_DIST      = getopts.h script.tst
