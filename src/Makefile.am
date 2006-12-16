
lib_LTLIBRARIES = libhamsterdb.la

INCLUDES = -I../include 

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

libhamsterdb_la_SOURCES = blob.c btree.c btree_check.c btree_enum.c btree_erase.c btree_find.c btree_insert.c cache.c db.c error.c extkeys.c freelist.c hamsterdb.c keys.c mem.c os_posix.c page.c txn.c util.c

libhamsterdb_la_LDFLAGS = 0.0.4
