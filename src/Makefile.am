
INCLUDES = -I../include 

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

lib_LTLIBRARIES = libhamsterdb.la

libhamsterdb_la_SOURCES = blob.c btree.c btree_check.c btree_enum.c btree_erase.c btree_find.c btree_insert.c cache.c db.c error.c extkeys.c freelist.c hamsterdb.c keys.c mem.c os_posix.c page.c txn.c util.c

libhamsterdb_la_LDFLAGS = -version-info 0:3:0

#lib_LIBRARIES = libhamsterdb.a

#libhamsterdb_a_SOURCES = blob.c btree.c btree_check.c btree_enum.c btree_erase.c btree_find.c btree_insert.c cache.c db.c error.c extkeys.c freelist.c hamsterdb.c keys.c mem.c os_posix.c page.c txn.c util.c

#libhamsterdb_a_LDFLAGS = -version-info 0:1:0
