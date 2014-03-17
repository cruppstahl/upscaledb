
if ENABLE_REMOTE
SUBDIRS = protocol server
endif # ENABLE_REMOTE

lib_LTLIBRARIES = libhamsterdb.la

libhamsterdb_la_SOURCES = \
	abi.h \
	blob_manager.h \
	blob_manager.cc \
	blob_manager_inmem.h \
	blob_manager_inmem.cc \
	blob_manager_disk.h \
	blob_manager_disk.cc \
	blob_manager_factory.h \
	btree_check.cc \
	btree_cursor.cc \
	btree_cursor.h \
	btree_enum.cc \
	btree_erase.cc \
	btree_find.cc \
	btree_flags.h \
	btree_impl_default.h \
	btree_impl_pax.h \
	btree_index.cc \
	btree_index.h \
	btree_index_factory.h \
	btree_insert.cc \
	btree_node.h \
	btree_node_proxy.h \
	btree_stats.cc \
	btree_stats.h \
	cache.h \
	changeset.cc \
	changeset.h \
	compressor.h \
	compressor_factory.h \
	compressor_factory.cc \
	compressor_lzf.h \
	compressor_lzop.h \
	compressor_snappy.h \
	compressor_zlib.h \
	config.h \
	cursor.cc \
	cursor.h \
	db.cc \
	db.h \
	db_local.cc \
	db_local.h \
	db_remote.cc \
	db_remote.h \
	device.h \
	device_disk.h \
	device_inmem.h \
	device_factory.h \
	endianswap.h \
	env.h \
	env_header.h \
	env_local.h \
	env_local.cc \
	env_remote.h \
	env_remote.cc \
	error.cc \
	error.h \
	errorinducer.h \
	hamsterdb.cc \
	journal.cc \
	journal_entries.h \
	journal.h \
	mem.cc \
	mem.h \
	mutex.h \
	os.h \
	os.cc \
	os_posix.cc \
	packstart.h \
	packstop.h \
	page.cc \
	page.h \
	page_manager.cc \
	page_manager.h \
	pickle.h \
	rb.h \
	txn_cursor.cc \
	txn_cursor.h \
	txn_factory.h \
	txn_local.cc \
	txn_local.h \
	txn_remote.cc \
	txn_remote.h \
	txn.h \
	util.cc \
	util.h \
	version.h

EXTRA_DIST = os_win32.cc

AM_CPPFLAGS = -I../include -I$(top_srcdir)/include $(BOOST_CPPFLAGS)
libhamsterdb_la_LDFLAGS = -version-info 5:1:0 $(BOOST_SYSTEM_LDFLAGS)
libhamsterdb_la_LIBADD  = $(BOOST_SYSTEM_LIBS)

if ENABLE_COMPRESSION
AM_CPPFLAGS += -DHAM_ENABLE_COMPRESSION
libhamsterdb_la_LIBADD  += $(top_builddir)/3rdparty/liblzf/liblzf.la
if WITH_ZLIB
libhamsterdb_la_LIBADD  += -lz
endif
if WITH_SNAPPY
libhamsterdb_la_LIBADD  += -lsnappy
endif
if WITH_LZOP
libhamsterdb_la_LIBADD  += -llzo2
endif
endif

if ENABLE_ENCRYPTION
AM_CPPFLAGS += -DHAM_ENABLE_ENCRYPTION
endif

if ENABLE_REMOTE
AM_CPPFLAGS += -DHAM_ENABLE_REMOTE
libhamsterdb_la_LIBADD += $(top_builddir)/src/protocol/libprotocol.la
endif

