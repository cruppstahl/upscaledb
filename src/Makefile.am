
INCLUDES = -I../include 

AM_CFLAGS = 

# little or big endian?
if BIG_ENDIAN
    AM_CFLAGS += -DHAM_BIG_ENDIAN
else
    AM_CFLAGS += -DHAM_LITTLE_ENDIAN
endif

# debug or release mode? release mode: don't use optimization if gcc 
# version is < 4.1.1; all other GCC builds: disable all predefined flags
# to get rid of optimization
if USING_GCC
    CFLAGS     =
endif
if DEBUG
    AM_CFLAGS += -DDEBUG -DHAM_DEBUG
else
    AM_CFLAGS += -DHAM_RELEASE -ffast-math
if ENABLE_GCC_OPTIMIZATION
    AM_CFLAGS += -O3 -ffast-math
endif
endif

# use profiling?
if PROFILE
    AM_CFLAGS += -pg
endif

lib_LTLIBRARIES = libhamsterdb.la

libhamsterdb_la_SOURCES = blob.c btree.c btree_check.c btree_enum.c btree_erase.c btree_find.c btree_insert.c cache.c db.c error.c extkeys.c freelist.c hamsterdb.c keys.c mem.c os_posix.c page.c txn.c util.c

libhamsterdb_la_LDFLAGS = -version-info 0:1:0
