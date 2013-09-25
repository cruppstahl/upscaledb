
if ENABLE_REMOTE
SUBDIRS = protocol server
endif # ENABLE_REMOTE

lib_LTLIBRARIES = libhamsterdb.la

libhamsterdb_la_SOURCES = \
	aes.h \
	blob_manager.h \
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
	btree_index.cc \
	btree_index.h \
	btree_index_factory.h \
	btree_insert.cc \
	btree_key.h \
	btree_node.h \
	btree_node_legacy.h \
	btree_node_legacy.cc \
	btree_node_proxy.h \
	btree_stats.cc \
	btree_stats.h \
	cache.cc \
	cache.h \
	changeset.cc \
	changeset.h \
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
	duplicates.cc \
	duplicates.h \
	endianswap.h \
	env.h \
	env.cc \
	env_header.h \
	env_local.h \
	env_local.cc \
	env_remote.h \
	env_remote.cc \
	error.cc \
	error.h \
	errorinducer.h \
	extkeys.h \
	extkeys.cc \
	freelist.cc \
	freelist.h \
	freelist_stats.cc \
	freelist_stats.h \
	hamsterdb.cc \
	hash-table.h \
	journal.cc \
	journal_entries.h \
	journal.h \
	log.cc \
	log.h \
	mem.cc \
	mem.h \
	mutex.h \
	os.h \
	os_posix.cc \
	packstart.h \
	packstop.h \
	page.cc \
	page.h \
	page_manager.cc \
	page_manager.h \
	rb.h \
	serial.h \
	txn.cc \
	txn_cursor.cc \
	txn_cursor.h \
	txn.h \
	util.cc \
	util.h \
	version.h

EXTRA_DIST = os_win32.cc

AM_CPPFLAGS = -I../include -I$(top_srcdir)/include $(BOOST_CPPFLAGS)
libhamsterdb_la_LDFLAGS = -version-info 4:0:0 $(BOOST_SYSTEM_LDFLAGS)
libhamsterdb_la_LIBADD = 

if DARWIN
libhamsterdb_la_LIBADD += $(BOOST_SYSTEM_LIBS)
endif

if ENABLE_ENCRYPTION
AM_CPPFLAGS += -DHAM_ENABLE_ENCRYPTION
endif

if ENABLE_REMOTE
AM_CPPFLAGS += -DHAM_ENABLE_REMOTE
libhamsterdb_la_LIBADD += $(top_builddir)/src/protocol/libprotocol.la
endif

