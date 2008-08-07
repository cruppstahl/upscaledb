
INCLUDES = -I../include -I$(top_builddir)/3rdparty/aes

lib_LTLIBRARIES = libhamsterdb.la

libhamsterdb_la_SOURCES = log.c blob.c btree.c btree_check.c btree_enum.c btree_erase.c btree_find.c btree_insert.c cache.c db.c error.c extkeys.c freelist.c hamsterdb.c keys.c mem.c os_posix.c page.c txn.c util.c btree_cursor.c device.c

libhamsterdb_la_LDFLAGS = -version-info 1:0:1

if ENABLE_ENCRYPTION
libhamsterdb_la_LDFLAGS += $(top_builddir)/3rdparty/aes/libaes.la
endif

if ENABLE_COMPRESSION
libhamsterdb_la_LDFLAGS += $(top_builddir)/3rdparty/zlib/libzlib.la
endif
