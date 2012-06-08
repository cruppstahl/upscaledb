
if ENABLE_REMOTE
SUBDIRS = protocol
if ENABLE_SERVER 
SUBDIRS += server
endif # ENABLE_SERVER
endif # ENABLE_REMOTE

AM_CPPFLAGS = -I../include -I$(top_builddir)/3rdparty/aes $(BOOST_CPPFLAGS)
AM_LDFLAGS = $(BOOST_THREAD_LDFLAGS)

lib_LTLIBRARIES = libhamsterdb.la

EXTRA_DIST = os_win32.cc

libhamsterdb_la_SOURCES = log.cc \
			cursor.cc \
			blob.cc \
			btree.cc \
			btree_check.cc \
			btree_enum.cc \
			btree_erase.cc \
			btree_find.cc \
			btree_insert.cc \
			btree_key.cc \
			cache.cc \
			db.cc \
			env.cc \
			error.cc \
			freelist.cc \
			freelist_statistics.cc \
			hamsterdb.cc \
			remote.cc \
			mem.cc \
			os_posix.cc \
			page.cc \
			btree_stats.cc \
			txn.cc \
			txn_cursor.cc \
			util.cc \
			btree_cursor.cc \
			journal.cc \
			changeset.cc \
			device.cc

libhamsterdb_la_LDFLAGS = -version-info 3:0:0 $(BOOST_THREAD_LIBS) -lpthread 

if ENABLE_ENCRYPTION
libhamsterdb_la_LDFLAGS += $(top_builddir)/3rdparty/aes/libaes.la
endif

if ENABLE_COMPRESSION
if SYSTEM_ZLIB
libhamsterdb_la_LDFLAGS += -lz
else
libhamsterdb_la_LDFLAGS += $(top_builddir)/3rdparty/zlib/libzlib.la
endif
endif

if ENABLE_REMOTE
libhamsterdb_la_LDFLAGS += -lcurl
libhamsterdb_la_LDFLAGS += $(top_builddir)/src/protocol/libprotocol.la
CFLAGS += -DHAM_ENABLE_REMOTE
CXXFLAGS += -DHAM_ENABLE_REMOTE
endif
LIBS = $(BOOST_THREAD_LIBS)

